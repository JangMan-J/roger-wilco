//! Multi-session lifecycle, durable Store commit-before-Ack loop, ordered SSE event log,
//! and concurrent async Inbox reader.

use crate::cli::DaemonSocket;
use crate::daemon::{self, DaemonReader, DaemonWriter};
use crate::durlog::{
    blocking_io, cursor_path, entries_after, gen_uuid, load_cursor, read_store_entries,
    save_cursor, store_path, DurLog,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
// no-poll: Instant bounds client-requested wait_seconds on roger_inbox and roger_registered.
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Reader side of one session's Store, tracking `.cursor` across `roger_inbox` calls.
pub struct AsyncInbox {
    store: PathBuf,
    pos_file: PathBuf,
    initial: Option<String>,
    consumed: tokio::sync::Mutex<Option<String>>,
    arrived: Notify,
    closed: AtomicBool,
}

impl AsyncInbox {
    pub fn open(store: &Path) -> io::Result<Self> {
        let pos_file = cursor_path(store);
        let consumed = blocking_io(|| load_cursor(&pos_file))?;
        Ok(Self {
            store: store.to_path_buf(),
            pos_file,
            initial: consumed.clone(),
            consumed: tokio::sync::Mutex::new(consumed),
            arrived: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    pub fn initial_cursor(&self) -> Option<&str> {
        self.initial.as_deref()
    }

    pub fn notify(&self) {
        self.arrived.notify_waiters();
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.arrived.notify_waiters();
    }

    pub async fn read(
        &self,
        limit: usize,
        wait: Duration,
        peek: bool,
    ) -> io::Result<(Vec<Value>, Option<String>)> {
        // no-poll: bounds client-requested wait_seconds; Store commit notification ends the wait.
        let deadline = Instant::now() + wait;
        let mut consumed = self.consumed.lock().await;
        loop {
            let notified = self.arrived.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let store_file = self.store.clone();
            let all = blocking_io(move || read_store_entries(&store_file))?;
            let page = entries_after(&all, consumed.as_deref(), limit);
            if !page.is_empty() {
                let last = page
                    .last()
                    .and_then(|e| e.get("record"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if !peek {
                    if let Some(last_rec) = &last {
                        let pos_file = self.pos_file.clone();
                        let rec = last_rec.clone();
                        blocking_io(move || save_cursor(&pos_file, &rec))?;
                        *consumed = Some(last_rec.clone());
                    }
                }
                return Ok((page, last));
            }
            if self.closed.load(Ordering::SeqCst) {
                return Ok((Vec::new(), None));
            }
            // no-poll: computes remaining wait_seconds budget; never scans on an interval.
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok((Vec::new(), None));
            }
            let _ = tokio::time::timeout(left, notified).await;
        }
    }
}

#[derive(Default)]
struct RegistrationState {
    confirmed: bool,
    superseded: Option<Value>,
}

pub struct EventLog {
    pub events: Vec<(u64, String)>,
    pub next_seq: u64,
    pub delivered_up_to: u64,
    pub closed: bool,
}

impl EventLog {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            next_seq: 1,
            delivered_up_to: 0,
            closed: false,
        }
    }

    fn push(&mut self, json_line: String) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.push((seq, json_line));
        seq
    }
}

struct StoreGate {
    closing: bool,
    store: Option<DurLog>,
}

pub struct GatewaySession {
    pub session_id: String,
    pub address: String,
    pub socket: DaemonSocket,
    pub store_path: PathBuf,
    pub inbox: Arc<AsyncInbox>,
    reg_state: Mutex<RegistrationState>,
    reg_notify: Notify,
    pub event_log: Mutex<EventLog>,
    pub event_notify: Arc<Notify>,
    store_gate: Mutex<StoreGate>,
    daemon_writer: DaemonWriter,
    reader_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

pub fn format_notification(method: &str, params: Value) -> String {
    let mut obj = Map::new();
    obj.insert("jsonrpc".into(), Value::String("2.0".into()));
    obj.insert("method".into(), Value::String(method.into()));
    obj.insert("params".into(), params);
    serde_json::to_string(&Value::Object(obj)).expect("JSON-RPC notification serializes")
}

impl GatewaySession {
    /// Wait up to `wait` for coordinator `registered` confirmation or a `superseded` notice.
    pub async fn registered_status(&self, wait: Duration) -> (bool, Option<Value>) {
        // no-poll: bounds client-requested wait_seconds on roger_registered; notice arrival ends wait.
        let deadline = Instant::now() + wait;
        loop {
            let notified = self.reg_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let st = self.reg_state.lock().unwrap();
                if st.confirmed || st.superseded.is_some() {
                    return (
                        st.confirmed && st.superseded.is_none(),
                        st.superseded.clone(),
                    );
                }
            }
            if self.event_log.lock().unwrap().closed {
                let st = self.reg_state.lock().unwrap();
                return (
                    st.confirmed && st.superseded.is_none(),
                    st.superseded.clone(),
                );
            }
            // no-poll: computes remaining wait_seconds budget; never scans on an interval.
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                let st = self.reg_state.lock().unwrap();
                return (
                    st.confirmed && st.superseded.is_none(),
                    st.superseded.clone(),
                );
            }
            let _ = tokio::time::timeout(left, notified).await;
        }
    }

    pub fn initial_sse_cursor(&self, last_event_id: Option<u64>) -> u64 {
        match last_event_id {
            Some(n) => n,
            None => self.event_log.lock().unwrap().delivered_up_to,
        }
    }

    /// Tear down an older session for the same Address during container restart re-initialization:
    /// closes SSE streams, releases the Store file lock immediately, and disconnects from the
    /// daemon WITHOUT sending `unregister` (since the Address is immediately re-registering).
    pub async fn shutdown_for_reinit(&self) {
        {
            let mut gate = self.store_gate.lock().unwrap();
            gate.closing = true;
            let _ = gate.store.take();
        }
        self.signal_closed();
        let _ = self.daemon_writer.shutdown().await;
        if let Some(handle) = self.reader_task.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
    }

    /// Tear down on `DELETE`: send `unregister`, close SSE streams, release Store lock, and
    /// remove empty Store/Cursor files if 0 entries were ever written.
    pub async fn close_and_unregister(&self) {
        {
            let mut gate = self.store_gate.lock().unwrap();
            gate.closing = true;
            let _ = gate.store.take();
        }
        let mut unreg = Map::new();
        unreg.insert("type".into(), Value::String("unregister".into()));
        let _ = self.daemon_writer.send(&Value::Object(unreg)).await;
        let _ = self.daemon_writer.shutdown().await;
        self.signal_closed();
        if let Some(handle) = self.reader_task.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
        let store_path = self.store_path.clone();
        blocking_io(move || {
            if read_store_entries(&store_path).is_ok_and(|e| e.is_empty()) {
                let _ = std::fs::remove_file(&store_path);
                let _ = std::fs::remove_file(cursor_path(&store_path));
            }
        });
    }

    fn signal_closed(&self) {
        self.event_log.lock().unwrap().closed = true;
        self.event_notify.notify_waiters();
        self.inbox.close();
        self.reg_notify.notify_waiters();
    }
}

struct RegistryMaps {
    by_id: HashMap<String, Arc<GatewaySession>>,
    id_by_address: HashMap<String, String>,
}

pub struct SessionManager {
    socket: DaemonSocket,
    store_dir: PathBuf,
    #[cfg(feature = "test-hooks")]
    hold_before_ack: Option<PathBuf>,
    init_lock: tokio::sync::Mutex<()>,
    maps: Mutex<RegistryMaps>,
}

pub struct InitParams {
    pub address: Option<String>,
    pub label: Option<String>,
    pub declared: Option<String>,
}

impl SessionManager {
    pub fn new(
        socket: DaemonSocket,
        store_dir: PathBuf,
        #[cfg(feature = "test-hooks")] hold_before_ack: Option<PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            socket,
            store_dir,
            #[cfg(feature = "test-hooks")]
            hold_before_ack,
            init_lock: tokio::sync::Mutex::new(()),
            maps: Mutex::new(RegistryMaps {
                by_id: HashMap::new(),
                id_by_address: HashMap::new(),
            }),
        })
    }

    pub fn get(&self, session_id: &str) -> Option<Arc<GatewaySession>> {
        self.maps.lock().unwrap().by_id.get(session_id).cloned()
    }

    pub async fn delete(&self, session_id: &str) -> bool {
        let removed = {
            let mut maps = self.maps.lock().unwrap();
            let Some(session) = maps.by_id.remove(session_id) else {
                return false;
            };
            if maps
                .id_by_address
                .get(&session.address)
                .is_some_and(|id| id == session_id)
            {
                maps.id_by_address.remove(&session.address);
            }
            session
        };
        removed.close_and_unregister().await;
        true
    }

    /// Initialize a new MCP session, cleanly dropping any older session in this gateway
    /// process that holds the same `Address` before opening the durable Store.
    pub async fn initialize(
        self: &Arc<Self>,
        params: InitParams,
    ) -> io::Result<Arc<GatewaySession>> {
        let _guard = self.init_lock.lock().await;

        let address = match params.address.filter(|a| !a.is_empty()) {
            Some(a) => a,
            None => blocking_io(gen_uuid)?,
        };

        let prior = {
            let mut maps = self.maps.lock().unwrap();
            if let Some(old_id) = maps.id_by_address.remove(&address) {
                maps.by_id.remove(&old_id)
            } else {
                None
            }
        };
        if let Some(old_session) = prior {
            old_session.shutdown_for_reinit().await;
        }

        let session_id = blocking_io(gen_uuid)?;
        let s_path = store_path(&self.store_dir, &address);

        let s_path_open = s_path.clone();
        let (store, records, replayed_entries) = blocking_io(move || -> io::Result<_> {
            let (store, replay) = DurLog::open(&s_path_open)?;
            let mut records = HashMap::new();
            let mut entries = Vec::new();
            for line in replay {
                let line = line?;
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if let (Some(mail), Some(record)) = (
                        v.get("mail").and_then(Value::as_str),
                        v.get("record").and_then(Value::as_str),
                    ) {
                        records.insert(mail.to_string(), record.to_string());
                        entries.push(v);
                    }
                }
            }
            Ok((store, records, entries))
        })?;

        let inbox = Arc::new(AsyncInbox::open(&s_path)?);
        let mut event_log = EventLog::new();
        for unconsumed in entries_after(&replayed_entries, inbox.initial_cursor(), usize::MAX) {
            event_log.push(format_notification("notifications/roger/mail", unconsumed));
        }

        let (daemon_reader, daemon_writer) = daemon::connect(&self.socket).await?;
        let mut reg_msg = Map::new();
        reg_msg.insert("type".into(), Value::String("register".into()));
        reg_msg.insert("session".into(), Value::String(address.clone()));
        if let Some(label) = &params.label {
            reg_msg.insert("label".into(), Value::String(label.clone()));
        }
        if let Some(declared) = &params.declared {
            reg_msg.insert("declared".into(), Value::String(declared.clone()));
        }
        reg_msg.insert("confirm_register".into(), Value::Bool(true));
        reg_msg.insert("notify_superseded".into(), Value::Bool(true));
        daemon_writer.send(&Value::Object(reg_msg)).await?;

        let session = Arc::new(GatewaySession {
            session_id: session_id.clone(),
            address: address.clone(),
            socket: self.socket.clone(),
            store_path: s_path,
            inbox,
            reg_state: Mutex::new(RegistrationState::default()),
            reg_notify: Notify::new(),
            event_log: Mutex::new(event_log),
            event_notify: Arc::new(Notify::new()),
            store_gate: Mutex::new(StoreGate {
                closing: false,
                store: Some(store),
            }),
            daemon_writer,
            reader_task: tokio::sync::Mutex::new(None),
        });

        let weak_mgr = Arc::downgrade(self);
        let session_for_task = Arc::clone(&session);
        #[cfg(feature = "test-hooks")]
        let hold_marker = self.hold_before_ack.clone();
        let handle = tokio::spawn(async move {
            reader_loop(
                session_for_task,
                daemon_reader,
                records,
                weak_mgr,
                #[cfg(feature = "test-hooks")]
                hold_marker,
            )
            .await;
        });
        *session.reader_task.lock().await = Some(handle);

        {
            let mut maps = self.maps.lock().unwrap();
            maps.id_by_address
                .insert(address.clone(), session_id.clone());
            maps.by_id.insert(session_id, Arc::clone(&session));
        }

        Ok(session)
    }

    fn remove_if_active(&self, session_id: &str, address: &str) {
        let mut maps = self.maps.lock().unwrap();
        if maps.by_id.remove(session_id).is_some()
            && maps
                .id_by_address
                .get(address)
                .is_some_and(|id| id == session_id)
        {
            maps.id_by_address.remove(address);
        }
    }
}

async fn reader_loop(
    session: Arc<GatewaySession>,
    mut reader: DaemonReader,
    mut records: HashMap<String, String>,
    manager: Weak<SessionManager>,
    #[cfg(feature = "test-hooks")] hold_before_ack: Option<PathBuf>,
) {
    let mut next_record = records.len() as u64 + 1;
    let mut req = 0i64;

    while let Ok(Some(v)) = reader.next_value().await {
        if session.store_gate.lock().unwrap().closing {
            continue;
        }
        let Ok(msg_type) = daemon::field(&v, "type") else {
            break;
        };
        match msg_type {
            "deliver" => {
                let (Ok(mail), Ok(from), Ok(body)) = (
                    daemon::field(&v, "mail"),
                    daemon::field(&v, "from"),
                    daemon::field(&v, "body"),
                ) else {
                    break;
                };
                let mail = mail.to_string();
                let record = match records.get(&mail) {
                    Some(existing) => existing.clone(),
                    None => {
                        let kind = daemon::kind_of(&v);
                        let headers = daemon::headers_of(&v);
                        let candidate = format!("r-{next_record}");
                        let mut entry = Map::new();
                        entry.insert("record".into(), Value::String(candidate.clone()));
                        entry.insert("mail".into(), Value::String(mail.clone()));
                        entry.insert("from".into(), Value::String(from.to_string()));
                        entry.insert("body".into(), Value::String(body.to_string()));
                        entry.insert("kind".into(), Value::String(kind));
                        entry.insert("headers".into(), headers);
                        let entry_val = Value::Object(entry);
                        let Ok(line) = serde_json::to_string(&entry_val) else {
                            break;
                        };
                        let notif = format_notification("notifications/roger/mail", entry_val);

                        let committed = blocking_io(|| {
                            let mut gate = session.store_gate.lock().unwrap();
                            if gate.closing {
                                return Ok(false);
                            }
                            let Some(store) = gate.store.as_mut() else {
                                return Ok(false);
                            };
                            store.append(&line)?;
                            session.event_log.lock().unwrap().push(notif);
                            Ok::<bool, io::Error>(true)
                        });
                        match committed {
                            Ok(true) => {
                                next_record += 1;
                                records.insert(mail.clone(), candidate.clone());
                                session.inbox.notify();
                                session.event_notify.notify_waiters();
                                candidate
                            }
                            Ok(false) => continue,
                            Err(e) => {
                                let _ = writeln!(
                                    io::stderr().lock(),
                                    "roger-mcp-gateway: Store append failed for {}: {e}",
                                    session.address
                                );
                                break;
                            }
                        }
                    }
                };

                #[cfg(feature = "test-hooks")]
                if let Some(marker) = &hold_before_ack {
                    if !marker.exists() {
                        let marker = marker.clone();
                        let wrote = blocking_io(move || -> io::Result<()> {
                            let mut f = std::fs::File::create(&marker)?;
                            f.write_all(b"held\n")?;
                            f.sync_all()
                        });
                        if wrote.is_ok() {
                            std::future::pending::<()>().await;
                        }
                    }
                }

                req += 1;
                let mut ack = Map::new();
                ack.insert("type".into(), Value::String("ack".into()));
                ack.insert("req".into(), Value::Number(req.into()));
                ack.insert("mail".into(), Value::String(mail));
                ack.insert("record".into(), Value::String(record));
                if session
                    .daemon_writer
                    .send(&Value::Object(ack))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            "registered" => {
                session.reg_state.lock().unwrap().confirmed = true;
                session.reg_notify.notify_waiters();

                let mut params = Map::new();
                params.insert("session".into(), Value::String(session.address.clone()));
                let notif =
                    format_notification("notifications/roger/registered", Value::Object(params));
                session.event_log.lock().unwrap().push(notif);
                session.event_notify.notify_waiters();
            }
            "superseded" => {
                let (Ok(sess_name), Ok(host)) =
                    (daemon::field(&v, "session"), daemon::field(&v, "host"))
                else {
                    continue;
                };
                let at_ms = v.get("at_ms").cloned().unwrap_or(Value::Null);

                let mut tool_notice = Map::new();
                tool_notice.insert("type".into(), Value::String("superseded".into()));
                tool_notice.insert("session".into(), Value::String(sess_name.to_string()));
                tool_notice.insert("host".into(), Value::String(host.to_string()));
                tool_notice.insert("at_ms".into(), at_ms.clone());
                let tool_notice_val = Value::Object(tool_notice);

                let _ = writeln!(
                    io::stderr().lock(),
                    "SUPERSEDED {}: client should stop sending as this Address, notify its supervisor/user, and not automatically re-register",
                    serde_json::to_string(&tool_notice_val).unwrap_or_default()
                );

                session.reg_state.lock().unwrap().superseded = Some(tool_notice_val);
                session.reg_notify.notify_waiters();

                let mut sse_params = Map::new();
                sse_params.insert("session".into(), Value::String(sess_name.to_string()));
                sse_params.insert("host".into(), Value::String(host.to_string()));
                sse_params.insert("at_ms".into(), at_ms);
                let notif = format_notification(
                    "notifications/roger/superseded",
                    Value::Object(sse_params),
                );
                session.event_log.lock().unwrap().push(notif);
                session.event_notify.notify_waiters();
            }
            _ => {}
        }
    }

    // Daemon connection closed or session shut down: drop Store lock and finish SSE streams.
    {
        let mut gate = session.store_gate.lock().unwrap();
        gate.closing = true;
        let _ = gate.store.take();
    }
    session.signal_closed();
    if let Some(mgr) = manager.upgrade() {
        mgr.remove_if_active(&session.session_id, &session.address);
    }
}

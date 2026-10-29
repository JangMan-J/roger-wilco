//! The coordinator: single central durable store for sessions, mail, and
//! receipts. Host daemons connect to it; committed mail is pushed to the
//! recipient's host daemon, and pending mail is pushed when a session or
//! host (re)registers. Claims are decided against their committed deadlines
//! at the moment of each request or board read, and a session's registration
//! ends by its own unregister or, once it has been disconnected for
//! `unregister_after_ms`, at the next request that reads the registered set.
//! Nothing here scans on a timer.

use crate::claims::{Change, Claims, Held, Op};
use crate::durlog::DurLog;
use crate::headers;
use crate::json::{self, obj, s, Value};
use crate::net::{self, Writer};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Mail {
    from: String,
    to: String,
    body: String,
    /// How the Mail reached its recipient: `net::DIRECTED` or `net::BROADCAST`.
    kind: &'static str,
    /// The sender's headers, validated once at the send and carried verbatim.
    headers: Value,
    /// When this coordinator committed the send and answered Accepted
    /// (`accepted_at_ms`, coordinator epoch ms). None for a Mail replayed from
    /// a log written before the stamp existed: absent, never zero.
    accepted_at: Option<i64>,
    /// The Receipt, once the recipient has acked.
    receipt: Option<Receipt>,
}

/// The Receipt: the recipient's Record id, and when this coordinator recorded
/// it (`receipted_at_ms`, the same clock as `accepted_at`, so the difference is
/// end-to-end latency with no skew between hosts). None when replayed from a
/// log written before the stamp existed.
#[derive(Clone)]
struct Receipt {
    record: String,
    at: Option<i64>,
}

/// An optional stamp from a committed event: absent stays absent.
fn stamp_of(v: &Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

/// The Address a send names to reach every registered session at once.
pub const BROADCAST: &str = "*";

/// Joins a Broadcast id to a recipient Address in that recipient's copy id.
/// No accepted Mail id may contain it, so a client-chosen id can never equal
/// a copy id, and two copy ids are equal only for the same broadcast and
/// recipient.
const COPY_SEP: char = '/';

/// How long a registered session may stay disconnected before it is
/// unregistered: longer than a host daemon or coordinator restart takes.
pub const UNREGISTER_AFTER_MS: i64 = 300_000;

/// One registered session.
struct Session {
    host: String,
    /// Display text, stored verbatim; empty until a Register or relabel names one.
    label: String,
    /// Opaque text the session declared at its latest Register, verbatim.
    declared: Option<String>,
}

struct State {
    log: DurLog,
    sessions: HashMap<String, Session>,
    /// When this coordinator started: a replayed session that never
    /// re-registers counts as disconnected from then.
    started: i64,
    unregister_after_ms: i64,
    /// Presence: when each Address last acted (register, send, a live ack, a
    /// Board read naming it) or disconnected, on the coordinator clock. Set at
    /// the moment of the event and only read by the Board; memory only, since
    /// every live session re-registers when its host daemon reconnects (and
    /// that re-registration counts as a Register). Only registered Addresses
    /// are stamped.
    seen: HashMap<String, i64>,
    /// Addresses whose session connection to their host daemon is open and
    /// whose host daemon's link here is up. Set by Register, cleared by that
    /// daemon's disconnect report or by its link dropping.
    connected: HashSet<String>,
    mails: HashMap<String, Mail>,
    order: Vec<String>,
    /// Broadcast ids already committed, so a resent broadcast fans out once.
    broadcasts: HashSet<String>,
    hosts: HashMap<String, (u64, Writer)>, // host -> (connection id, live connection)
    next_conn: u64,
    claims: Claims,
}

/// Coordinator wall clock in milliseconds since the Epoch: the one clock
/// every claim deadline is set and read against, whichever host asks.
///
/// Deadlines are absolute wall-clock instants because they must survive a
/// restart, which a monotonic clock cannot give. The cost is that the
/// coordinator trusts its own clock: a forward step (NTP, suspend and
/// resume) frees held keys early, and a backward step extends them.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub fn run(
    bind: &str,
    port_file: Option<PathBuf>,
    data: PathBuf,
    token: String,
    unregister_after_ms: i64,
) -> io::Result<()> {
    let (log, prior) = DurLog::open(&data)?;
    let mut state = State {
        log,
        sessions: HashMap::new(),
        started: now_ms(),
        unregister_after_ms,
        seen: HashMap::new(),
        connected: HashSet::new(),
        mails: HashMap::new(),
        order: Vec::new(),
        broadcasts: HashSet::new(),
        hosts: HashMap::new(),
        next_conn: 1,
        claims: Claims::default(),
    };
    // DurLog::open already drops a crash-truncated tail, so every line here
    // was appended whole. A line that is not JSON is skipped as before; a
    // claim event that parses but lacks a field fails start-up (see `apply`).
    for line in &prior {
        if let Ok(v) = json::parse(line) {
            apply(&mut state, &v)?;
        }
    }
    let state = Arc::new(Mutex::new(state));

    let listener = TcpListener::bind(bind)?;
    if let Some(pf) = &port_file {
        publish_port(pf, listener.local_addr()?.port())?;
    }
    eprintln!("coordinator listening on {}", listener.local_addr()?);

    for conn in listener.incoming() {
        let conn = conn?;
        let state = Arc::clone(&state);
        let token = token.clone();
        std::thread::spawn(move || {
            if let Err(e) = serve(conn, Arc::clone(&state), &token) {
                eprintln!("coordinator connection ended: {e}");
                // A failed append may have left the disk ahead of memory.
                // The log refuses every later append; stop here so a
                // restart replays the disk instead of serving a split view.
                if state.lock().unwrap().log.is_poisoned() {
                    eprintln!("coordinator stopping: durable log failed, restart to replay it");
                    std::process::exit(1);
                }
            }
        });
    }
    Ok(())
}

/// Publish the bound port at `path` whole: write it beside the file, flush it,
/// then rename it into place, so a reader that sees `path` reads a port and
/// never an empty file.
fn publish_port(path: &std::path::Path, port: u16) -> io::Result<()> {
    let mut aside = path.as_os_str().to_owned();
    aside.push(".tmp");
    let aside = PathBuf::from(aside);
    let wrote = (|| {
        let mut f = std::fs::File::create(&aside)?;
        f.write_all(port.to_string().as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&aside, path)
    })();
    wrote.map_err(|e| {
        let _ = std::fs::remove_file(&aside);
        io::Error::new(
            e.kind(),
            format!(
                "expected to publish port {port} at {} through {}, saw {e}",
                path.display(),
                aside.display()
            ),
        )
    })
}

/// Replay one committed event. Claim events must be whole: a `claim` or
/// `release` line missing a field is an `InvalidData` error that names the
/// line, so start-up fails instead of silently freeing a held key.
fn apply(state: &mut State, v: &Value) -> io::Result<()> {
    match v.get("ev").and_then(|x| x.as_str()) {
        Some("session") => {
            if let (Ok(session), Ok(host)) = (net::field(v, "session"), net::field(v, "host")) {
                state
                    .sessions
                    .entry(session.to_string())
                    .and_modify(|s| s.host = host.to_string())
                    .or_insert_with(|| Session {
                        host: host.to_string(),
                        label: String::new(),
                        declared: None,
                    });
            }
        }
        Some("unregister") => {
            if let Ok(session) = net::field(v, "session") {
                state.sessions.remove(session);
            }
        }
        Some("declared") => {
            if let Some(s) = net::field(v, "session")
                .ok()
                .and_then(|a| state.sessions.get_mut(a))
            {
                s.declared = net::declared_of(v);
            }
        }
        Some("label") => {
            if let (Some(s), Ok(label)) = (
                net::field(v, "session")
                    .ok()
                    .and_then(|a| state.sessions.get_mut(a)),
                net::field(v, "label"),
            ) {
                s.label = label.to_string();
            }
        }
        Some("mail") => {
            if let (Ok(mail), Ok(from), Ok(to), Ok(body)) = (
                net::field(v, "mail"),
                net::field(v, "from"),
                net::field(v, "to"),
                net::field(v, "body"),
            ) {
                state.order.push(mail.to_string());
                state.mails.insert(
                    mail.to_string(),
                    Mail {
                        from: from.into(),
                        to: to.into(),
                        body: body.into(),
                        kind: net::DIRECTED,
                        // An event written before headers existed has none.
                        headers: net::headers_of(v),
                        accepted_at: stamp_of(v, "accepted_at_ms"),
                        receipt: None,
                    },
                );
            }
        }
        Some("broadcast") => {
            if let (Ok(mail), Ok(from), Ok(body), Some(Value::Arr(to))) = (
                net::field(v, "mail"),
                net::field(v, "from"),
                net::field(v, "body"),
                v.get("to"),
            ) {
                let to: Vec<String> = to
                    .iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect();
                let headers = net::headers_of(v);
                fan_out(
                    state,
                    (mail, from, body, &headers),
                    &to,
                    stamp_of(v, "accepted_at_ms"),
                );
            }
        }
        Some("receipt") => {
            if let (Ok(mail), Ok(record)) = (net::field(v, "mail"), net::field(v, "record")) {
                if let Some(m) = state.mails.get_mut(mail) {
                    m.receipt.get_or_insert(Receipt {
                        record: record.to_string(),
                        at: stamp_of(v, "receipted_at_ms"),
                    });
                }
            }
        }
        Some("claim" | "release") => {
            let (key, change) = Change::from_event(v)?;
            state.claims.apply(&key, &change);
        }
        _ => {}
    }
    Ok(())
}

/// Stamp `session` as seen now, for its own act. Only a registered Address is
/// stamped: a send or Board read naming a stranger leaves no row behind.
fn stamp(st: &mut State, session: &str) {
    if st.sessions.contains_key(session) {
        st.seen.insert(session.to_string(), now_ms());
    }
}

/// Record a session's label verbatim. The label is display only: it never
/// touches the session's Address, host, or pending mail.
fn set_label(state: &mut State, session: &str, label: &str) -> io::Result<()> {
    commit(
        state,
        obj(vec![
            ("ev", s("label")),
            ("session", s(session)),
            ("label", s(label)),
        ]),
    )
}

/// Append one event durably, then apply it to memory: the one path every
/// session event takes, so replay and live state agree.
fn commit(state: &mut State, ev: Value) -> io::Result<()> {
    state.log.append(&json::stringify(&ev))?;
    apply(state, &ev)
}

/// End one session's registration: it leaves the Board and every later
/// Broadcast. Mail already accepted for its Address stays pending and is
/// delivered if that Address registers again.
fn unregister(st: &mut State, session: &str) -> io::Result<()> {
    commit(
        st,
        obj(vec![("ev", s("unregister")), ("session", s(session))]),
    )?;
    st.connected.remove(session);
    st.seen.remove(session);
    Ok(())
}

/// Unregister every session disconnected for `unregister_after_ms`, read
/// against now. Called by each request that reads the registered set
/// (Broadcast, Board, claim, relabel), as claim expiry is: no timer runs it.
fn expire(st: &mut State) -> io::Result<()> {
    let cutoff = now_ms() - st.unregister_after_ms;
    let mut gone: Vec<String> = st
        .sessions
        .keys()
        .filter(|a| {
            !st.connected.contains(*a)
                && st.seen.get(*a).copied().unwrap_or(0).max(st.started) <= cutoff
        })
        .cloned()
        .collect();
    gone.sort();
    for a in gone {
        unregister(st, &a)?;
    }
    Ok(())
}

fn host_of<'a>(state: &'a State, session: &str) -> Option<&'a str> {
    state.sessions.get(session).map(|s| s.host.as_str())
}

/// Pending deliveries for sessions on `host`, or for one session.
fn pending_pushes(state: &State, host: &str, only_session: Option<&str>) -> Vec<Value> {
    let mut out = Vec::new();
    for id in &state.order {
        let m = &state.mails[id];
        if m.receipt.is_some() {
            continue;
        }
        if let Some(want) = only_session {
            if m.to != want {
                continue;
            }
        } else if host_of(state, &m.to) != Some(host) {
            continue;
        }
        out.push(net::deliver_msg(
            id, &m.from, &m.to, &m.body, m.kind, &m.headers,
        ));
    }
    out
}

fn serve(conn: std::net::TcpStream, state: Arc<Mutex<State>>, token: &str) -> io::Result<()> {
    let writer = net::writer(Box::new(conn.try_clone()?));
    let mut lines = net::lines(Box::new(conn));

    let hello = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no hello"))??;
    if net::field(&hello, "type")? != "hello" || net::field(&hello, "token")? != token {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "bad hello"));
    }
    let host = net::field(&hello, "host")?.to_string();
    let conn_id = {
        let mut st = state.lock().unwrap();
        let id = st.next_conn;
        st.next_conn += 1;
        st.hosts.insert(host.clone(), (id, Arc::clone(&writer)));
        id
    };
    // However this connection ends (EOF, error, or a `?` below), the link drop
    // is handled once, when this guard goes.
    let _link = HostLink {
        state: Arc::clone(&state),
        host: host.clone(),
        conn_id,
    };
    net::send(&writer, &obj(vec![("type", s("hello_ok"))]))?;
    let pushes = {
        let st = state.lock().unwrap();
        pending_pushes(&st, &host, None)
    };
    for p in pushes {
        let _ = net::send(&writer, &p);
    }

    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "register" => {
                let session = net::field(&v, "session")?.to_string();
                let pushes = {
                    let mut st = state.lock().unwrap();
                    st.seen.insert(session.clone(), now_ms());
                    st.connected.insert(session.clone());
                    if host_of(&st, &session) != Some(host.as_str()) {
                        let ev = obj(vec![
                            ("ev", s("session")),
                            ("session", s(&session)),
                            ("host", s(&host)),
                        ]);
                        commit(&mut st, ev)?;
                    }
                    if let Some(label) = v.get("label").and_then(|x| x.as_str()) {
                        set_label(&mut st, &session, label)?;
                    }
                    let declared = net::declared_of(&v);
                    if st.sessions[&session].declared != declared {
                        let mut ev = vec![("ev", s("declared")), ("session", s(&session))];
                        if let Some(d) = &declared {
                            ev.push(("declared", s(d)));
                        }
                        commit(&mut st, obj(ev))?;
                    }
                    pending_pushes(&st, &host, Some(&session))
                };
                for p in pushes {
                    let _ = net::send(&writer, &p);
                }
            }
            "send" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let mail = net::field(&v, "mail")?.to_string();
                if mail.contains(COPY_SEP) {
                    net::send(
                        &writer,
                        &obj(vec![
                            ("type", s("error")),
                            ("req", Value::Int(req)),
                            ("mail", s(&mail)),
                            (
                                "reason",
                                s(&format!("mail id must not contain '{COPY_SEP}'")),
                            ),
                        ]),
                    )?;
                    continue;
                }
                // The one place headers are bounded, before anything commits.
                let headers = match headers::validate(v.get("headers")) {
                    Ok(h) => h,
                    Err(reason) => {
                        net::send(
                            &writer,
                            &obj(vec![
                                ("type", s("error")),
                                ("req", Value::Int(req)),
                                ("mail", s(&mail)),
                                ("reason", s(&reason)),
                            ]),
                        )?;
                        continue;
                    }
                };
                let mut push: Vec<(Writer, Value)> = Vec::new();
                {
                    let mut st = state.lock().unwrap();
                    stamp(&mut st, net::field(&v, "from")?);
                    if net::field(&v, "to")? == BROADCAST {
                        push = broadcast(
                            &mut st,
                            &mail,
                            net::field(&v, "from")?,
                            net::field(&v, "body")?,
                            &headers,
                        )?;
                    } else if !st.mails.contains_key(&mail) {
                        let ev = obj(vec![
                            ("ev", s("mail")),
                            ("mail", s(&mail)),
                            ("from", s(net::field(&v, "from")?)),
                            ("to", s(net::field(&v, "to")?)),
                            ("body", s(net::field(&v, "body")?)),
                            ("headers", headers),
                            ("accepted_at_ms", Value::Int(now_ms())),
                        ]);
                        commit(&mut st, ev)?;
                        let m = &st.mails[&mail];
                        if let Some((_, w)) = host_of(&st, &m.to).and_then(|h| st.hosts.get(h)) {
                            push.push((
                                Arc::clone(w),
                                net::deliver_msg(
                                    &mail, &m.from, &m.to, &m.body, m.kind, &m.headers,
                                ),
                            ));
                        }
                    }
                }
                net::send(
                    &writer,
                    &obj(vec![
                        ("type", s("accepted")),
                        ("req", Value::Int(req)),
                        ("mail", s(&mail)),
                    ]),
                )?;
                for (w, msg) in push {
                    let _ = net::send(&w, &msg);
                }
            }
            // The session's connection to this host daemon closed.
            "disconnect" => {
                let session = net::field(&v, "session")?.to_string();
                let mut st = state.lock().unwrap();
                // A session that has since registered through another host
                // is still connected there.
                if host_of(&st, &session) == Some(host.as_str()) && st.connected.remove(&session) {
                    st.seen.insert(session, now_ms());
                }
            }
            // The session left on purpose: its registration ends now, unless
            // it has since registered through another host.
            "unregister" => {
                let session = net::field(&v, "session")?.to_string();
                let mut st = state.lock().unwrap();
                if host_of(&st, &session) == Some(host.as_str()) {
                    unregister(&mut st, &session)?;
                }
            }
            "relabel" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let session = net::field(&v, "session")?.to_string();
                let label = net::field(&v, "label")?.to_string();
                let reply = {
                    let mut st = state.lock().unwrap();
                    expire(&mut st)?;
                    // A label belongs to a session: an Address that never
                    // registered gets none, so no label can be parked on it.
                    if st.sessions.contains_key(&session) {
                        set_label(&mut st, &session, &label)?;
                        obj(vec![
                            ("type", s("relabeled")),
                            ("req", Value::Int(req)),
                            ("session", s(&session)),
                            ("label", s(&label)),
                        ])
                    } else {
                        obj(vec![
                            ("type", s("error")),
                            ("req", Value::Int(req)),
                            (
                                "reason",
                                s(&format!(
                                    "expected a registered session to relabel, saw {session}"
                                )),
                            ),
                        ])
                    }
                };
                net::send(&writer, &reply)?;
            }
            "ack" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let mail = net::field(&v, "mail")?.to_string();
                let session = net::field(&v, "session")?.to_string();
                let record = net::field(&v, "record")?.to_string();
                let known = {
                    let mut st = state.lock().unwrap();
                    // A replayed receipt was acked earlier, while this link
                    // was down; it is not the session's activity now.
                    if v.get("replay") != Some(&Value::Bool(true)) {
                        stamp(&mut st, &session);
                    }
                    match st.mails.get(&mail) {
                        None => None,
                        Some(m) => {
                            let already = m.receipt.is_some();
                            if !already {
                                let ev = obj(vec![
                                    ("ev", s("receipt")),
                                    ("mail", s(&mail)),
                                    ("session", s(&session)),
                                    ("record", s(&record)),
                                    ("receipted_at_ms", Value::Int(now_ms())),
                                ]);
                                commit(&mut st, ev)?;
                            }
                            Some(already)
                        }
                    }
                };
                // Answering `acked` for a mail this coordinator never accepted
                // would be a false confirmation: the host daemon trims that
                // receipt from its replay list on the answer, and the receipt
                // would never be stored upstream.
                let reply = match known {
                    Some(_) => obj(vec![
                        ("type", s("acked")),
                        ("req", Value::Int(req)),
                        ("mail", s(&mail)),
                        ("record", s(&record)),
                    ]),
                    None => obj(vec![
                        ("type", s("error")),
                        ("req", Value::Int(req)),
                        ("mail", s(&mail)),
                        ("reason", s("unknown mail")),
                    ]),
                };
                net::send(&writer, &reply)?;
            }
            "receipt" => {
                let reply = receipt(&state.lock().unwrap(), &v)?;
                net::send(&writer, &reply)?;
            }
            "claim" => {
                let reply = {
                    let mut st = state.lock().unwrap();
                    expire(&mut st)?;
                    claim(&mut st, &v)?
                };
                net::send(&writer, &reply)?;
            }
            "board" => {
                let reply = {
                    let mut st = state.lock().unwrap();
                    expire(&mut st)?;
                    // A Board read that names its reader is that session's own
                    // activity.
                    if let Ok(reader) = net::field(&v, "session") {
                        stamp(&mut st, reader);
                    }
                    board(&st, &v)
                };
                net::send(&writer, &reply)?;
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown type {other}"),
                ))
            }
        }
    }
    Ok(())
}

/// One host daemon's link. When it drops, the host's slot is freed and every
/// session on that host stops being connected, at that moment.
struct HostLink {
    state: Arc<Mutex<State>>,
    host: String,
    conn_id: u64,
}

impl Drop for HostLink {
    fn drop(&mut self) {
        let Ok(mut st) = self.state.lock() else {
            return;
        };
        // Only this connection may take the slot: a host that reconnected
        // while this thread was finishing already owns it, and its
        // re-registration has already marked its live sessions connected.
        if st.hosts.get(&self.host).map(|(id, _)| *id) != Some(self.conn_id) {
            return;
        }
        st.hosts.remove(&self.host);
        let now = now_ms();
        let gone: Vec<String> = st
            .sessions
            .iter()
            .filter(|(a, s)| s.host == self.host && st.connected.contains(*a))
            .map(|(a, _)| a.clone())
            .collect();
        for a in gone {
            st.connected.remove(&a);
            st.seen.insert(a, now);
        }
    }
}

fn holder_fields(pairs: &mut Vec<(&str, Value)>, holder: Option<&Held>, now: i64) {
    if let Some(h) = holder {
        pairs.push(("holder", s(&h.owner)));
        pairs.push(("remaining_ms", Value::Int(h.expires_ms - now)));
    }
}

/// Decide one claim request, commit its change durably, then answer. The lock
/// is held throughout, so two owners racing for one key see one order.
fn claim(st: &mut State, v: &Value) -> io::Result<Value> {
    let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
    let op_name = net::field(v, "op")?;
    // A bad op is answered, not fatal: an error here would drop the whole
    // host daemon's link.
    let Some(op) = Op::parse(op_name) else {
        return Ok(obj(vec![
            ("type", s("error")),
            ("req", Value::Int(req)),
            (
                "reason",
                s(&format!(
                    "expected op acquire, renew, or release; saw {op_name}"
                )),
            ),
        ]));
    };
    let key = net::field(v, "key")?.to_string();
    let owner = net::field(v, "owner")?.to_string();
    let ttl_ms = v.get("ttl_ms").and_then(|x| x.as_i64()).unwrap_or(0);
    if op != Op::Release && ttl_ms <= 0 {
        return Ok(obj(vec![
            ("type", s("error")),
            ("req", Value::Int(req)),
            (
                "reason",
                s(&format!("ttl_ms must be positive, saw {ttl_ms}")),
            ),
        ]));
    }
    // A Claim is held by a session: an owner the coordinator has never seen
    // Register is refused before any decision, so a stray string can neither
    // hold a key nor renew one.
    if !st.sessions.contains_key(&owner) {
        return Ok(obj(vec![
            ("type", s("claim_result")),
            ("req", Value::Int(req)),
            ("ok", Value::Bool(false)),
            ("op", s(op.name())),
            ("key", s(&key)),
            (
                "reason",
                s(&format!("owner {owner} is not a registered session")),
            ),
        ]));
    }
    let now = now_ms();
    let d = st.claims.decide(&op, &key, &owner, ttl_ms, now);
    if let Some(change) = &d.change {
        st.log.append(&json::stringify(&change.to_event(&key)))?;
        st.claims.apply(&key, change);
    }
    let mut pairs = vec![
        ("type", s("claim_result")),
        ("req", Value::Int(req)),
        ("op", s(op.name())),
        ("key", s(&key)),
        ("ok", Value::Bool(d.ok)),
    ];
    holder_fields(&mut pairs, d.holder.as_ref(), now);
    Ok(obj(pairs))
}

/// The Board: registered sessions and every claim live at the moment of the
/// read. Each session row says whether it is `connected` and when it was
/// last seen (`last_seen_ms`, coordinator epoch ms): the moment of the read
/// while connected, else its disconnect or last own activity, 0 when not
/// seen since this coordinator started. Which sessions count as present is
/// the reader's choice.
fn board(st: &State, v: &Value) -> Value {
    let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
    let now = now_ms();
    let mut sessions: Vec<(&String, &Session)> = st.sessions.iter().collect();
    sessions.sort_by_key(|(a, _)| *a);
    let sessions = sessions
        .into_iter()
        .map(|(session, row)| {
            let mut pairs = vec![
                ("session", s(session)),
                ("host", s(&row.host)),
                ("label", s(&row.label)),
                ("connected", Value::Bool(st.connected.contains(session))),
                (
                    "last_seen_ms",
                    Value::Int(if st.connected.contains(session) {
                        now
                    } else {
                        st.seen.get(session).copied().unwrap_or(0)
                    }),
                ),
            ];
            if let Some(d) = &row.declared {
                pairs.push(("declared", s(d)));
            }
            obj(pairs)
        })
        .collect();
    let claims = st
        .claims
        .active(now)
        .into_iter()
        .map(|(key, h)| {
            let mut pairs = vec![("key", s(key))];
            holder_fields(&mut pairs, Some(h), now);
            obj(pairs)
        })
        .collect();
    obj(vec![
        ("type", s("board")),
        ("req", Value::Int(req)),
        ("sessions", Value::Arr(sessions)),
        ("claims", Value::Arr(claims)),
        // The clock every last_seen_ms and remaining_ms above was read
        // against, so a reader on another host measures age without skew.
        ("now_ms", Value::Int(now)),
    ])
}

/// One Mail's lifecycle as this coordinator recorded it: its recipient, when
/// it was Accepted, and, once acked, the Receipt's Record id and when it was
/// recorded. A stamp the log never held is left out, never sent as zero. A
/// Broadcast copy is read by its copy id.
fn receipt(st: &State, v: &Value) -> io::Result<Value> {
    let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
    let mail = net::field(v, "mail")?;
    let Some(m) = st.mails.get(mail) else {
        return Ok(obj(vec![
            ("type", s("error")),
            ("req", Value::Int(req)),
            ("mail", s(mail)),
            ("reason", s(&format!("unknown mail {mail}"))),
        ]));
    };
    let mut pairs = vec![
        ("type", s("receipt")),
        ("req", Value::Int(req)),
        ("mail", s(mail)),
        ("to", s(&m.to)),
        ("kind", s(m.kind)),
        ("headers", m.headers.clone()),
    ];
    if let Some(t) = m.accepted_at {
        pairs.push(("accepted_at_ms", Value::Int(t)));
    }
    if let Some(r) = &m.receipt {
        pairs.push(("record", s(&r.record)));
        if let Some(t) = r.at {
            pairs.push(("receipted_at_ms", Value::Int(t)));
        }
    }
    Ok(obj(pairs))
}

/// The Mail id of one Broadcast copy: the broadcast's id and the recipient's
/// Address, so each recipient's copy is delivered, stored, and acked once.
fn copy_id(mail: &str, to: &str) -> String {
    format!("{mail}{COPY_SEP}{to}")
}

/// Expand a committed Broadcast `(id, from, body, headers)` into one Mail of
/// kind `broadcast` per recipient, each carrying the Broadcast's headers.
/// Used on commit and on replay, so both see the same copies.
fn fan_out(
    st: &mut State,
    (mail, from, body, headers): (&str, &str, &str, &Value),
    to: &[String],
    accepted_at: Option<i64>,
) {
    if !st.broadcasts.insert(mail.to_string()) {
        return;
    }
    for addr in to {
        let id = copy_id(mail, addr);
        if st.mails.contains_key(&id) {
            continue;
        }
        st.order.push(id.clone());
        st.mails.insert(
            id,
            Mail {
                from: from.to_string(),
                to: addr.clone(),
                body: body.to_string(),
                kind: net::BROADCAST,
                headers: headers.clone(),
                accepted_at,
                receipt: None,
            },
        );
    }
}

/// Commit one Broadcast to every session registered at this moment, as a
/// single durable event, then return the deliveries to push to connected
/// host daemons. A resent broadcast id commits nothing.
fn broadcast(
    st: &mut State,
    mail: &str,
    from: &str,
    body: &str,
    headers: &Value,
) -> io::Result<Vec<(Writer, Value)>> {
    if st.broadcasts.contains(mail) {
        return Ok(Vec::new());
    }
    expire(st)?;
    let mut to: Vec<String> = st.sessions.keys().cloned().collect();
    to.sort();
    let ev = obj(vec![
        ("ev", s("broadcast")),
        ("mail", s(mail)),
        ("from", s(from)),
        ("body", s(body)),
        ("to", Value::Arr(to.iter().map(|a| s(a)).collect())),
        ("headers", headers.clone()),
        ("accepted_at_ms", Value::Int(now_ms())),
    ]);
    commit(st, ev)?;
    let mut push = Vec::new();
    for addr in &to {
        if let Some((_, w)) = host_of(st, addr).and_then(|h| st.hosts.get(h)) {
            push.push((
                Arc::clone(w),
                net::deliver_msg(
                    &copy_id(mail, addr),
                    from,
                    addr,
                    body,
                    net::BROADCAST,
                    headers,
                ),
            ));
        }
    }
    Ok(push)
}

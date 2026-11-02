//! The host daemon: one per host. Connects outbound to the coordinator,
//! holds the durable local inbox for sessions on this host, and serves
//! them over a Unix socket. Deliveries are committed to the local log
//! before they are pushed to a session; acks are committed locally, then
//! forwarded upstream. Pending mail is pushed on registration, never by
//! a timer.

use crate::durlog::DurLog;
use crate::json::{self, obj, s, Value};
use crate::net::{self, Writer};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::TcpStream;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
struct Mail {
    from: String,
    to: String,
    body: String,
    kind: String,
    /// The sender's headers, forwarded as the coordinator sent them.
    headers: Value,
}

/// A receipt committed to this host's log: which Mail, which session acked
/// it, and which Record id that session named.
#[derive(Clone, PartialEq)]
struct Receipt {
    mail: String,
    session: String,
    record: String,
}

struct Registration {
    writer: Writer,
    notices: mpsc::Sender<Value>,
    confirm: bool,
    confirmed: bool,
    notify: bool,
    superseded: bool,
}

struct State {
    log: DurLog,
    inbox: HashMap<String, Mail>,
    order: Vec<String>,
    acked: HashSet<String>,
    /// Receipts committed locally and not yet confirmed upstream, in commit
    /// order. Replayed after every reconnect, because the coordinator's ack
    /// handler is idempotent and a locally committed receipt that never
    /// arrives upstream would leave the mail pending forever.
    receipts: Vec<Receipt>,
    sessions: HashMap<String, Writer>,
    /// Each local session's declared text, repeated on every Register
    /// upstream: a Register without it clears the declaration.
    declared: HashMap<String, String>,
    current: HashMap<String, String>,
    registrations: HashMap<String, Registration>,
    pending_registers: Vec<Value>,
    upstream: Option<Writer>,
    next_req: i64,
    waiters: HashMap<i64, mpsc::Sender<Value>>,
}

/// How long a session's send or ack waits for the coordinator to answer.
const UPSTREAM_WAIT: Duration = Duration::from_secs(10);

pub struct Config {
    pub coordinator: String,
    pub token: String,
    pub host_id: String,
    pub socket: PathBuf,
    pub data: PathBuf,
}

pub fn run(cfg: Config) -> io::Result<()> {
    let (log, prior) = DurLog::open(&cfg.data)?;
    let mut state = State {
        log,
        inbox: HashMap::new(),
        order: Vec::new(),
        acked: HashSet::new(),
        receipts: Vec::new(),
        sessions: HashMap::new(),
        declared: HashMap::new(),
        current: HashMap::new(),
        registrations: HashMap::new(),
        pending_registers: Vec::new(),
        upstream: None,
        next_req: 1,
        waiters: HashMap::new(),
    };
    for line in &prior {
        if let Ok(v) = json::parse(line) {
            apply(&mut state, &v);
        }
    }
    // Bind before dialing upstream: a daemon refused its socket must not
    // have joined the coordinator first.
    let listener = bind_socket(&cfg.socket)?;
    let state = Arc::new(Mutex::new(state));
    let cfg = Arc::new(cfg);

    {
        let state = Arc::clone(&state);
        let cfg = Arc::clone(&cfg);
        std::thread::spawn(move || upstream_loop(cfg, state));
    }

    eprintln!("host daemon listening on {}", cfg.socket.display());
    for conn in listener.incoming() {
        let conn = conn?;
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            if let Err(e) = serve_local(conn, state) {
                eprintln!("local connection ended: {e}");
            }
        });
    }
    Ok(())
}

/// Bind the host socket. A socket file left by a dead daemon is replaced;
/// one a live process still answers on is refused, so a second daemon on
/// the same path (a test, a copy of the project) cannot silently take over
/// the live daemon's clients. Anything at the path that is not a socket is
/// refused too, rather than deleted.
fn bind_socket(path: &Path) -> io::Result<UnixListener> {
    use std::os::unix::fs::FileTypeExt;
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
        Ok(m) if !m.file_type().is_socket() => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "expected {} absent or a socket, saw another kind of file; refusing to remove it",
                    path.display()
                ),
            ))
        }
        Ok(_) => match UnixStream::connect(path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!(
                        "expected no live listener on {}, saw one answering; refusing to take over its socket",
                        path.display()
                    ),
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                std::fs::remove_file(path)?
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("cannot tell whether {} is live: {e}", path.display()),
                ))
            }
        },
    }
    UnixListener::bind(path)
}

fn apply(state: &mut State, v: &Value) {
    match v.get("ev").and_then(|x| x.as_str()) {
        Some("deliver") => {
            if let (Ok(mail), Ok(from), Ok(to), Ok(body)) = (
                net::field(v, "mail"),
                net::field(v, "from"),
                net::field(v, "to"),
                net::field(v, "body"),
            ) {
                if !state.inbox.contains_key(mail) {
                    state.order.push(mail.to_string());
                    state.inbox.insert(
                        mail.to_string(),
                        Mail {
                            from: from.into(),
                            to: to.into(),
                            body: body.into(),
                            kind: net::kind_of(v),
                            headers: net::headers_of(v),
                        },
                    );
                }
            }
        }
        Some("receipt") => {
            if let (Ok(mail), Ok(session), Ok(record)) = (
                net::field(v, "mail"),
                net::field(v, "session"),
                net::field(v, "record"),
            ) {
                if state.acked.insert(mail.to_string()) {
                    state.receipts.push(Receipt {
                        mail: mail.to_string(),
                        session: session.to_string(),
                        record: record.to_string(),
                    });
                }
            }
        }
        _ => {}
    }
}

fn upstream_loop(cfg: Arc<Config>, state: Arc<Mutex<State>>) {
    loop {
        match upstream_once(&cfg, &state) {
            Ok(()) => eprintln!("upstream connection closed"),
            Err(e) => eprintln!("upstream error: {e}"),
        }
        {
            let mut st = state.lock().unwrap();
            st.upstream = None;
            st.waiters.clear();
        }
        // Reconnect after failure. This never looks for work: the
        // coordinator pushes pending mail when the hello is answered.
        // no-poll: reconnect backoff after a failed connection; it never looks for work.
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn upstream_once(cfg: &Config, state: &Arc<Mutex<State>>) -> io::Result<()> {
    let conn = TcpStream::connect(&cfg.coordinator)?;
    let writer = net::writer(Box::new(conn.try_clone()?));
    let mut lines = net::lines(Box::new(conn));
    net::send(
        &writer,
        &obj(vec![
            ("type", s("hello")),
            ("token", s(&cfg.token)),
            ("host", s(&cfg.host_id)),
        ]),
    )?;
    let resp = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no hello_ok"))??;
    if net::field(&resp, "type")? != "hello_ok" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "hello refused",
        ));
    }
    let receipts = {
        let mut st = state.lock().unwrap();
        st.upstream = Some(Arc::clone(&writer));
        // Labels are not repeated here: the coordinator's log already holds
        // the latest one, and a relabel may have gone through another host.
        for msg in &st.pending_registers {
            let _ = net::send(&writer, msg);
        }
        for (id, registration) in &st.current {
            if st
                .pending_registers
                .iter()
                .any(|v| net::field(v, "registration").ok() == Some(registration.as_str()))
            {
                continue;
            }
            let _ = net::send(
                &writer,
                &register_msg(
                    id,
                    registration,
                    None,
                    st.declared.get(id).map(String::as_str),
                ),
            );
        }
        st.receipts.clone()
    };
    // A receipt committed while upstream was down still has to arrive. The
    // coordinator's acked answers trim `receipts` in the loop below.
    if !receipts.is_empty() {
        eprintln!("upstream connected, replaying {} receipts", receipts.len());
    }
    for r in &receipts {
        // Marked as a replay: the session acked earlier, so this is not its
        // activity now.
        let mut ack = net::ack_msg(&r.mail, &r.session, &r.record);
        if let Value::Obj(pairs) = &mut ack {
            pairs.push(("replay".to_string(), Value::Bool(true)));
        }
        let _ = net::send(&writer, &ack);
    }
    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "deliver" => handle_deliver(state, &v)?,
            // The coordinator holds a Register. Not a reply to any request.
            "registered" | "superseded" => {
                let mut st = state.lock().unwrap();
                let session = net::field(&v, "session")?;
                // An older coordinator has no identity to echo. Retain its
                // confirmation behavior, but never guess a notice's recipient.
                let superseded = net::field(&v, "type")? == "superseded";
                let registration = v
                    .get("registration")
                    .and_then(Value::as_str)
                    .or_else(|| {
                        if superseded {
                            None
                        } else {
                            st.current.get(session).map(String::as_str)
                        }
                    })
                    .map(str::to_string);
                if !superseded {
                    st.pending_registers
                        .retain(|v| net::field(v, "registration").ok() != registration.as_deref());
                }
                if let Some(r) = registration.and_then(|id| st.registrations.get_mut(&id)) {
                    let superseded = net::field(&v, "type")? == "superseded";
                    let tell = if superseded {
                        let tell = r.notify && !r.superseded;
                        r.superseded = true;
                        tell
                    } else {
                        let tell = r.confirm && !r.confirmed;
                        r.confirmed = true;
                        tell
                    };
                    if tell {
                        let mut notice = v.clone();
                        if let Value::Obj(pairs) = &mut notice {
                            pairs.retain(|(k, _)| k != "registration");
                        }
                        let _ = r.notices.send(notice);
                    }
                }
            }
            // Every other line answers one request, by its req.
            kind => {
                // Upstream has this receipt now, so it leaves the replay list.
                if kind == "acked" {
                    if let Ok(mail) = net::field(&v, "mail") {
                        let mail = mail.to_string();
                        state.lock().unwrap().receipts.retain(|r| r.mail != mail);
                    }
                }
                let waiter = v
                    .get("req")
                    .and_then(|x| x.as_i64())
                    .and_then(|req| state.lock().unwrap().waiters.remove(&req));
                match waiter {
                    Some(tx) => {
                        let _ = tx.send(v);
                    }
                    // A replayed receipt upstream refuses stays queued, so say
                    // so: silence here would look like a confirmed receipt.
                    None if kind == "error" => eprintln!(
                        "upstream refused the replayed receipt for {}: {}",
                        net::field(&v, "mail").unwrap_or("?"),
                        net::field(&v, "reason").unwrap_or("unexplained")
                    ),
                    None => {}
                }
            }
        }
    }
    Ok(())
}

fn notice_sender(writer: Writer) -> mpsc::Sender<Value> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for notice in rx {
            if net::send(&writer, &notice).is_err() {
                break;
            }
        }
    });
    tx
}

fn register_msg(
    session: &str,
    registration: &str,
    label: Option<&str>,
    declared: Option<&str>,
) -> Value {
    let mut pairs = vec![
        ("type", s("register")),
        ("session", s(session)),
        ("registration", s(registration)),
    ];
    if let Some(label) = label {
        pairs.push(("label", s(label)));
    }
    if let Some(d) = declared {
        pairs.push(("declared", s(d)));
    }
    obj(pairs)
}

fn handle_deliver(state: &Arc<Mutex<State>>, v: &Value) -> io::Result<()> {
    let mail_id = net::field(v, "mail")?.to_string();
    let mut push: Option<(Writer, Value)> = None;
    {
        let mut st = state.lock().unwrap();
        if !st.inbox.contains_key(&mail_id) {
            let m = Mail {
                from: net::field(v, "from")?.into(),
                to: net::field(v, "to")?.into(),
                body: net::field(v, "body")?.into(),
                kind: net::kind_of(v),
                headers: net::headers_of(v),
            };
            let ev = obj(vec![
                ("ev", s("deliver")),
                ("mail", s(&mail_id)),
                ("from", s(&m.from)),
                ("to", s(&m.to)),
                ("body", s(&m.body)),
                ("kind", s(&m.kind)),
                ("headers", m.headers.clone()),
            ]);
            st.log.append(&json::stringify(&ev))?;
            st.order.push(mail_id.clone());
            st.inbox.insert(mail_id.clone(), m);
        }
        if !st.acked.contains(&mail_id) {
            let m = st.inbox[&mail_id].clone();
            if let Some(w) = st.sessions.get(&m.to) {
                push = Some((
                    Arc::clone(w),
                    net::deliver_msg(&mail_id, &m.from, &m.to, &m.body, &m.kind, &m.headers),
                ));
            }
        }
    }
    if let Some((w, msg)) = push {
        let _ = net::send(&w, &msg);
    }
    Ok(())
}

fn upstream_request(state: &Arc<Mutex<State>>, mut v: Value) -> io::Result<Value> {
    let (writer, rx) = {
        let mut st = state.lock().unwrap();
        let req = st.next_req;
        st.next_req += 1;
        if let Value::Obj(pairs) = &mut v {
            pairs.push(("req".to_string(), Value::Int(req)));
        }
        let writer = st
            .upstream
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no upstream"))?;
        let (tx, rx) = mpsc::channel();
        st.waiters.insert(req, tx);
        (writer, rx)
    };
    net::send(&writer, &v)?;
    // Bounded: a coordinator that accepts a request and never answers must
    // not hang the session's send or ack forever.
    // no-poll: one bounded wait for a single reply, turning a silent coordinator into an error.
    rx.recv_timeout(UPSTREAM_WAIT).map_err(|e| match e {
        mpsc::RecvTimeoutError::Timeout => {
            io::Error::new(io::ErrorKind::TimedOut, "upstream did not answer")
        }
        mpsc::RecvTimeoutError::Disconnected => {
            io::Error::new(io::ErrorKind::UnexpectedEof, "upstream wait dropped")
        }
    })
}

/// An upstream reply, re-tagged with the local request number it answers.
fn with_req(mut v: Value, req: i64) -> Value {
    if let Value::Obj(pairs) = &mut v {
        pairs.retain(|(k, _)| k != "req");
        pairs.push(("req".to_string(), Value::Int(req)));
    }
    v
}

fn serve_local(conn: UnixStream, state: Arc<Mutex<State>>) -> io::Result<()> {
    let writer = net::writer(Box::new(conn.try_clone()?));
    let lines = net::lines(Box::new(conn));
    let mut session_id: Option<String> = None;
    let result = local_loop(&writer, lines, &state, &mut session_id);
    // Whatever ended the connection, a dead Writer must not stay registered:
    // the daemon would re-register that session upstream and push pending
    // mail to a closed socket. The connection closing is the committed event
    // that ends its presence.
    if let Some(id) = session_id {
        release(&state, &writer, &id, "disconnect");
    }
    result
}

/// Drop this connection's registration of `id` locally and tell the
/// coordinator `kind` (`disconnect` or `unregister`). Only this connection's
/// own registration is removed: a session that re-registered on a newer
/// connection is still connected. With no upstream there is nothing to tell:
/// the coordinator marked this host's sessions disconnected when the link
/// dropped, and the reconnect re-registers only live ones.
fn release(state: &Arc<Mutex<State>>, writer: &Writer, id: &str, kind: &str) {
    let mut st = state.lock().unwrap();
    let removed: HashSet<String> = st
        .registrations
        .iter()
        .filter(|(_, r)| Arc::ptr_eq(&r.writer, writer))
        .map(|(id, _)| id.clone())
        .collect();
    st.registrations.retain(|id, _| !removed.contains(id));
    st.pending_registers
        .retain(|v| !removed.contains(net::field(v, "registration").unwrap_or("")));
    if st.sessions.get(id).is_some_and(|w| Arc::ptr_eq(w, writer)) {
        st.sessions.remove(id);
        st.declared.remove(id);
        if let Some(registration) = st.current.remove(id) {
            if let Some(up) = &st.upstream {
                let _ = net::send(
                    up,
                    &obj(vec![
                        ("type", s(kind)),
                        ("session", s(id)),
                        ("registration", s(&registration)),
                    ]),
                );
            }
        }
    }
}

fn local_loop(
    writer: &Writer,
    lines: net::Lines,
    state: &Arc<Mutex<State>>,
    session_id: &mut Option<String>,
) -> io::Result<()> {
    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "register" => {
                let session = net::field(&v, "session")?.to_string();
                let label = v.get("label").and_then(|x| x.as_str()).map(str::to_string);
                *session_id = Some(session.clone());
                let declared = net::declared_of(&v);
                let pending = {
                    let mut st = state.lock().unwrap();
                    let registration = if st
                        .sessions
                        .get(&session)
                        .is_some_and(|w| Arc::ptr_eq(w, writer))
                    {
                        st.current[&session].clone()
                    } else {
                        crate::session::gen_uuid()?
                    };
                    let replaced = st
                        .current
                        .get(&session)
                        .filter(|old| *old != &registration)
                        .cloned()
                        .or_else(|| {
                            st.pending_registers
                                .iter()
                                .find(|v| {
                                    net::field(v, "registration").ok()
                                        == Some(registration.as_str())
                                })
                                .and_then(|v| net::field(v, "replaces").ok())
                                .map(str::to_string)
                        });
                    st.sessions.insert(session.clone(), Arc::clone(writer));
                    st.current.insert(session.clone(), registration.clone());
                    let local = st
                        .registrations
                        .entry(registration.clone())
                        .or_insert_with(|| Registration {
                            notices: notice_sender(writer.clone()),
                            writer: writer.clone(),
                            confirm: matches!(v.get("confirm_register"), Some(Value::Bool(true))),
                            confirmed: false,
                            notify: matches!(v.get("notify_superseded"), Some(Value::Bool(true))),
                            superseded: false,
                        });
                    local.confirm = matches!(v.get("confirm_register"), Some(Value::Bool(true)));
                    local.confirmed = false;
                    local.notify = matches!(v.get("notify_superseded"), Some(Value::Bool(true)));
                    match &declared {
                        Some(d) => st.declared.insert(session.clone(), d.clone()),
                        None => st.declared.remove(&session),
                    };
                    let pending: Vec<Value> = st
                        .order
                        .iter()
                        .filter(|id| !st.acked.contains(*id) && st.inbox[*id].to == session)
                        .map(|id| {
                            net::deliver_msg(
                                id,
                                &st.inbox[id].from,
                                &st.inbox[id].to,
                                &st.inbox[id].body,
                                &st.inbox[id].kind,
                                &st.inbox[id].headers,
                            )
                        })
                        .collect();
                    let mut msg = register_msg(
                        &session,
                        &registration,
                        label.as_deref(),
                        declared.as_deref(),
                    );
                    if let (Some(old), Value::Obj(pairs)) = (replaced, &mut msg) {
                        pairs.push(("replaces".into(), s(&old)));
                    }
                    st.pending_registers
                        .retain(|v| net::field(v, "registration").ok() != Some(&registration));
                    st.pending_registers.push(msg.clone());
                    if let Some(up) = &st.upstream {
                        let _ = net::send(up, &msg);
                    }
                    pending
                };
                for p in pending {
                    let _ = net::send(writer, &p);
                }
            }
            // The session is leaving on purpose: no disconnect follows.
            "unregister" => {
                if let Some(id) = session_id.take() {
                    release(state, writer, &id, "unregister");
                }
            }
            "send" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let mut up = vec![
                    ("type", s("send")),
                    ("mail", s(net::field(&v, "mail")?)),
                    ("from", s(net::field(&v, "from")?)),
                    ("to", s(net::field(&v, "to")?)),
                    ("body", s(net::field(&v, "body")?)),
                ];
                // Headers are the coordinator's to check; this hop forwards them as given.
                if let Some(h) = v.get("headers") {
                    up.push(("headers", h.clone()));
                }
                let resp = upstream_request(state, obj(up))?;
                let answer = if net::field(&resp, "type")? == "error" {
                    obj(vec![
                        ("type", s("error")),
                        ("req", Value::Int(req)),
                        ("reason", s(net::field(&resp, "reason")?)),
                    ])
                } else {
                    obj(vec![
                        ("type", s("accepted")),
                        ("req", Value::Int(req)),
                        ("mail", s(net::field(&resp, "mail")?)),
                    ])
                };
                net::send(writer, &answer)?;
            }
            "relabel" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let session = net::field(&v, "session")?.to_string();
                let label = net::field(&v, "label")?.to_string();
                let resp = upstream_request(
                    state,
                    obj(vec![
                        ("type", s("relabel")),
                        ("session", s(&session)),
                        ("label", s(&label)),
                    ]),
                )?;
                net::send(writer, &with_req(resp, req))?;
            }
            "ack" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let session = (*session_id).clone().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "ack before register")
                })?;
                let mail = net::field(&v, "mail")?.to_string();
                let record = net::field(&v, "record")?.to_string();
                {
                    let mut st = state.lock().unwrap();
                    if !st.acked.contains(&mail) {
                        let ev = obj(vec![
                            ("ev", s("receipt")),
                            ("mail", s(&mail)),
                            ("session", s(&session)),
                            ("record", s(&record)),
                        ]);
                        // Durable first: a receipt that is acked in memory but
                        // absent from the log would never be replayed, and a
                        // retry would not commit it either.
                        st.log.append(&json::stringify(&ev))?;
                        st.acked.insert(mail.clone());
                        st.receipts.push(Receipt {
                            mail: mail.clone(),
                            session: session.clone(),
                            record: record.clone(),
                        });
                    }
                }
                // Forwarding is retryable, not part of the ack: the Receipt is
                // already durable in this host daemon, which is core, so the
                // session is answered now and the receipt is replayed after
                // the next hello_ok if this forward fails. A forward whose
                // answer outlasts UPSTREAM_WAIT is still on the link: the
                // coordinator commits it when it reads it, and its late
                // `acked` trims the replay list in `upstream_once`.
                match upstream_request(state, net::ack_msg(&mail, &session, &record)) {
                    Err(e) => {
                        eprintln!("receipt {mail} committed, upstream forward deferred: {e}")
                    }
                    // A refusal means upstream never accepted that mail, so the
                    // receipt stays queued and is replayed after the next
                    // hello_ok rather than being treated as confirmed.
                    Ok(resp) if net::field(&resp, "type").unwrap_or("") == "error" => eprintln!(
                        "receipt {mail} committed, upstream refused it: {}",
                        net::field(&resp, "reason").unwrap_or("unexplained")
                    ),
                    Ok(_) => {}
                }
                net::send(
                    writer,
                    &obj(vec![
                        ("type", s("acked")),
                        ("req", Value::Int(req)),
                        ("mail", s(&mail)),
                        ("record", s(&record)),
                    ]),
                )?;
            }
            // Claims, the Board and Receipts live only at the coordinator, so
            // the host daemon relays them and answers under the session's own req.
            "claim" | "board" | "receipt" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let mut upstream = v.clone();
                if let Value::Obj(pairs) = &mut upstream {
                    pairs.retain(|(k, _)| k != "req");
                }
                net::send(writer, &with_req(upstream_request(state, upstream)?, req))?;
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

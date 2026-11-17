//! The host daemon: one per host. Connects outbound to the coordinator,
//! holds the durable local inbox for sessions on this host, and serves
//! them over a Unix socket, a TCP listener, or both. Deliveries are committed
//! to the local log before they are pushed to a session; acks are committed
//! locally, then forwarded upstream. Pending mail is pushed on registration,
//! never by a timer.

use crate::json::{obj, s, Value};
use crate::net::{self, Writer};
use crate::sql::daemon::{Delivery, Store};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    store: Arc<Store>,
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
    /// Local connections that asked for `board_event` lines (`watch`, or
    /// `"watch":true` on a Register), until they close.
    watchers: Vec<Writer>,
}

impl State {
    /// Nothing delivered or acked yet: what a replay starts from.
    fn new(store: Store) -> State {
        State {
            store: Arc::new(store),
            receipts: Vec::new(),
            sessions: HashMap::new(),
            declared: HashMap::new(),
            current: HashMap::new(),
            registrations: HashMap::new(),
            pending_registers: Vec::new(),
            upstream: None,
            next_req: 1,
            waiters: HashMap::new(),
            watchers: Vec::new(),
        }
    }
}

/// How long a session's send or ack waits for the coordinator to answer.
const UPSTREAM_WAIT: Duration = Duration::from_secs(10);

pub struct Config {
    pub coordinator: String,
    pub token: String,
    pub host_id: String,
    pub socket: Option<PathBuf>,
    pub listen: Option<String>,
    pub port_file: Option<PathBuf>,
    /// When set, every TCP client must send `{"type":"auth","token":...}`
    /// with this value as its first line. Unix-socket clients never do.
    pub client_token: Option<String>,
    pub data: PathBuf,
}

pub fn run(cfg: Config) -> io::Result<()> {
    if cfg.socket.is_none() && cfg.listen.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing --socket",
        ));
    }
    if cfg.port_file.is_some() && cfg.listen.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected --listen with --port-file, saw none",
        ));
    }
    if cfg.client_token.is_some() && cfg.listen.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected --listen with --client-token-env, saw none",
        ));
    }
    let (store, prior) = Store::open(&cfg.data)?;
    let mut state = State::new(store);
    state.receipts = prior
        .receipts
        .into_iter()
        .map(|r| Receipt {
            mail: r.mail_id,
            session: r.session,
            record: r.record_id,
        })
        .collect();
    // Bind before dialing upstream: a daemon refused its socket or TCP address
    // must not have joined the coordinator first.
    #[cfg(unix)]
    let unix_listener = match &cfg.socket {
        Some(path) => Some(bind_socket(path)?),
        None => None,
    };
    #[cfg(not(unix))]
    if let Some(path) = &cfg.socket {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "expected --listen <address:port> on this platform, saw --socket {}",
                path.display()
            ),
        ));
    }
    let tcp_listener = match &cfg.listen {
        Some(addr) => match TcpListener::bind(addr) {
            Ok(tcp) => {
                if let Some(pf) = &cfg.port_file {
                    if let Err(e) = tcp
                        .local_addr()
                        .and_then(|local| net::publish_port(pf, local.port()))
                    {
                        if let Some(path) = &cfg.socket {
                            let _ = std::fs::remove_file(path);
                        }
                        return Err(e);
                    }
                }
                Some(tcp)
            }
            Err(e) => {
                if let Some(path) = &cfg.socket {
                    let _ = std::fs::remove_file(path);
                }
                return Err(e);
            }
        },
        None => None,
    };
    let state = Arc::new(Mutex::new(state));
    let client_token: Option<Arc<str>> = cfg.client_token.as_deref().map(Arc::from);
    let cfg = Arc::new(cfg);

    {
        let state = Arc::clone(&state);
        let cfg = Arc::clone(&cfg);
        std::thread::spawn(move || upstream_loop(cfg, state));
    }

    if let Some(path) = &cfg.socket {
        eprintln!("host daemon listening on {}", path.display());
    }
    if let Some(tcp) = &tcp_listener {
        eprintln!("host daemon listening on {}", tcp.local_addr()?);
    }

    #[cfg(unix)]
    return match (unix_listener, tcp_listener) {
        (Some(unix), Some(tcp)) => {
            let tcp_state = Arc::clone(&state);
            std::thread::spawn(move || {
                if let Err(e) = accept_tcp(tcp, tcp_state, client_token) {
                    eprintln!("tcp listener ended: {e}");
                }
            });
            accept_unix(unix, state)
        }
        (Some(unix), None) => accept_unix(unix, state),
        (None, Some(tcp)) => accept_tcp(tcp, state, client_token),
        (None, None) => unreachable!("validated at start of run"),
    };
    #[cfg(not(unix))]
    match tcp_listener {
        Some(tcp) => accept_tcp(tcp, state, client_token),
        None => unreachable!("validated at start of run"),
    }
}

#[cfg(unix)]
fn accept_unix(listener: UnixListener, state: Arc<Mutex<State>>) -> io::Result<()> {
    for conn in listener.incoming() {
        let conn = conn?;
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            if let Err(e) = serve_unix(conn, state) {
                eprintln!("local connection ended: {e}");
            }
        });
    }
    Ok(())
}

fn accept_tcp(
    listener: TcpListener,
    state: Arc<Mutex<State>>,
    client_token: Option<Arc<str>>,
) -> io::Result<()> {
    for conn in listener.incoming() {
        let conn = conn?;
        let state = Arc::clone(&state);
        let client_token = client_token.clone();
        std::thread::spawn(move || {
            if let Err(e) = serve_tcp(conn, state, client_token.as_deref()) {
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
#[cfg(unix)]
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

/// One link to the coordinator, from connect to its end. Every line for the
/// coordinator goes through the link's queue, so a coordinator that stops
/// reading never holds a thread that serves a local session.
fn upstream_once(cfg: &Config, state: &Arc<Mutex<State>>) -> io::Result<()> {
    let conn = TcpStream::connect(&cfg.coordinator)?;
    // One line is one segment, sent when it is written.
    conn.set_nodelay(true)?;
    let conn = Arc::new(conn);
    let writer = net::queued(Arc::clone(&conn), |c| {
        let _ = c.shutdown(Shutdown::Both);
    });
    let result = upstream_link(cfg, state, &conn, Arc::clone(&writer));
    // The link is over either way; whatever it still held is replayed or
    // asked again on the next one.
    net::close(&writer, false);
    result
}

fn upstream_link(
    cfg: &Config,
    state: &Arc<Mutex<State>>,
    conn: &TcpStream,
    writer: Writer,
) -> io::Result<()> {
    let mut lines = net::lines(Box::new(conn.try_clone()?));
    net::send(
        &writer,
        &obj(vec![
            ("type", s("hello")),
            ("token", s(&cfg.token)),
            ("host", s(&cfg.host_id)),
            // Ask for committed Board changes; an older coordinator ignores
            // the field and never sends one.
            ("board_events", Value::Bool(true)),
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
    let notices = matches!(resp.get("registration_notices"), Some(Value::Bool(true)));
    // A coordinator that answers pings is pinged at a fixed pace, so it can
    // tell a host daemon that has gone silent from one with nothing to say.
    // This side sets no bound of its own on the coordinator's silence. A
    // coordinator that is stopped and not gone still holds this link and
    // every line on it, and a second link dialed meanwhile would have its
    // lines applied before the first one's leftovers. A coordinator that is
    // gone fails the next ping's write instead, and that ends the link.
    if matches!(resp.get("ping"), Some(Value::Bool(true))) {
        let link = Arc::clone(&writer);
        std::thread::spawn(move || {
            // Ends with the link: a closed queue refuses the next ping.
            while net::send(&link, &obj(vec![("type", s("ping"))])).is_ok() {
                // no-poll: the pace of the liveness ping; it carries no Mail and looks for no work.
                std::thread::sleep(net::ping_every());
            }
        });
    }
    let receipts = {
        let mut st = state.lock().unwrap();
        st.upstream = Some(Arc::clone(&writer));
        if !notices {
            // Old coordinators understand only the original current-session
            // restore. Do not send them the new evidence-only message.
            st.pending_registers.clear();
        }
        // Labels are not repeated here: the coordinator's log already holds
        // the latest one, and a relabel may have gone through another host.
        for pending in &st.pending_registers {
            let mut msg = pending.clone();
            if let Value::Obj(pairs) = &mut msg {
                pairs.retain(|(key, _)| key != "label");
            }
            let session = net::field(&msg, "session")?;
            let registration = net::field(&msg, "registration")?;
            if st.current.get(session).map(String::as_str) != Some(registration) {
                if msg.get("replaces").is_none() {
                    continue;
                }
                // Preserve the replacement evidence without restoring an
                // obsolete registration or changing the reconnect winner.
                if let Value::Obj(pairs) = &mut msg {
                    pairs.retain(|(key, _)| key != "type");
                    pairs.push(("type".into(), s("replacement")));
                }
            }
            let _ = net::send(&writer, &msg);
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
            // The answer to a ping: its arrival is all it says.
            "pong" => {}
            // A committed Board change, passed on as it came to the local
            // connections that asked for it, and to no other.
            "board_event" => {
                let st = state.lock().unwrap();
                for w in &st.watchers {
                    let _ = net::send(w, &v);
                }
            }
            // The coordinator holds a Register. Not a reply to any request.
            "registered" | "superseded" | "replacement_recorded" => {
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
                if net::field(&v, "type")? == "replacement_recorded" {
                    continue;
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
                        let session = v.get("session").and_then(Value::as_str);
                        let mut st = state.lock().unwrap();
                        st.store.confirm_receipt(&mail, session)?;
                        st.receipts
                            .retain(|r| r.mail != mail || session.is_some_and(|s| s != r.session));
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
        let st = state.lock().unwrap();
        let row = Delivery {
            mail_id: mail_id.clone(),
            from: net::field(v, "from")?.into(),
            to: net::field(v, "to")?.into(),
            body: net::field(v, "body")?.into(),
            kind: net::kind_of(v),
            headers: net::headers_of(v),
            group: v.get("group").and_then(Value::as_str).map(str::to_string),
            article_num: v.get("article").and_then(Value::as_i64),
        };
        st.store.insert_deliver(&row)?;
        if let Some(m) = st
            .store
            .pending_for_session(&row.to)?
            .iter()
            .find(|m| m.mail_id == mail_id)
        {
            if let Some(w) = st.sessions.get(&m.to) {
                push = Some((Arc::clone(w), delivery(m)));
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

#[cfg(unix)]
fn serve_unix(conn: UnixStream, state: Arc<Mutex<State>>) -> io::Result<()> {
    let lines = net::lines(Box::new(conn.try_clone()?));
    let conn = Arc::new(conn);
    let writer = net::queued(Arc::clone(&conn), |c| {
        let _ = c.shutdown(Shutdown::Both);
    });
    serve_local(writer, lines, state)
}

fn serve_tcp(
    conn: TcpStream,
    state: Arc<Mutex<State>>,
    client_token: Option<&str>,
) -> io::Result<()> {
    conn.set_nodelay(true)?;
    let mut lines = net::lines(Box::new(conn.try_clone()?));
    let conn = Arc::new(conn);
    let writer = net::queued(Arc::clone(&conn), |c| {
        let _ = c.shutdown(Shutdown::Both);
    });
    if let Some(expected) = client_token {
        if let Err(e) = client_auth(&mut lines, expected) {
            let _ = net::send(
                &writer,
                &obj(vec![("type", s("error")), ("error", s(&e.to_string()))]),
            );
            net::close(&writer, true);
            return Err(e);
        }
    }
    serve_local(writer, lines, state)
}

/// Until the client token is seen the peer is anyone who can reach the port:
/// its first line is bounded, and it must be the auth line.
fn client_auth(lines: &mut net::Lines, expected: &str) -> io::Result<()> {
    lines.set_cap(net::HELLO_MAX);
    let first = lines.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "expected an auth line, saw end of stream",
        )
    })??;
    let ty = net::field(&first, "type").unwrap_or("");
    if ty != "auth" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("expected a first line of type auth, saw type {ty:?}"),
        ));
    }
    let seen = net::field(&first, "token").unwrap_or("");
    if !net::same_secret(seen, expected) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "expected the client token, saw a different one",
        ));
    }
    lines.set_cap(usize::MAX);
    Ok(())
}

/// One local connection, from accept to its end. Every line for the session
/// goes through its queue, so a session that stops reading delays only its
/// own lines: the Mail it has not taken stays in the inbox, unacked.
fn serve_local(writer: Writer, lines: net::Lines, state: Arc<Mutex<State>>) -> io::Result<()> {
    let mut session_id: Option<String> = None;
    let result = local_loop(&writer, lines, &state, &mut session_id);
    // Whatever ended the connection, a dead Writer must not stay registered:
    // the daemon would re-register that session upstream and push pending
    // mail to a closed socket. The connection closing is the committed event
    // that ends its presence.
    if let Some(id) = session_id {
        release(&state, &writer, &id, "disconnect");
    }
    state
        .lock()
        .unwrap()
        .watchers
        .retain(|w| !Arc::ptr_eq(w, &writer));
    // What is already queued is written before the connection ends: a
    // client may still be reading the answers to what it asked.
    net::close(&writer, true);
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
    st.pending_registers.retain(|v| {
        v.get("replaces").is_some()
            || !removed.contains(net::field(v, "registration").unwrap_or(""))
    });
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

/// Add one local connection to those sent `board_event` lines, once.
fn watch(st: &mut State, writer: &Writer) {
    if !st.watchers.iter().any(|w| Arc::ptr_eq(w, writer)) {
        st.watchers.push(Arc::clone(writer));
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
                    if v.get("watch") == Some(&Value::Bool(true)) {
                        watch(&mut st, writer);
                    }
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
                        .store
                        .pending_for_session(&session)?
                        .iter()
                        .map(delivery)
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
            // Board changes as the coordinator commits them, on this
            // connection, which need not be a session's.
            "watch" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                watch(&mut state.lock().unwrap(), writer);
                net::send(
                    writer,
                    &obj(vec![("type", s("watching")), ("req", Value::Int(req))]),
                )?;
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
                    ("body", s(net::field(&v, "body")?)),
                ];
                match (v.get("to"), v.get("group")) {
                    (Some(to), None) => up.push(("to", to.clone())),
                    (None, Some(group)) => up.push(("group", group.clone())),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "expected to or group",
                        ))
                    }
                }
                // Headers are the coordinator's to check; this hop forwards them as given.
                if let Some(h) = v.get("headers") {
                    up.push(("headers", h.clone()));
                }
                let resp = upstream_request(state, obj(up))?;
                let mut answer = if net::field(&resp, "type")? == "error" {
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
                if let Value::Obj(pairs) = &mut answer {
                    for key in ["group", "article"] {
                        if let Some(value) = resp.get(key) {
                            pairs.push((key.into(), value.clone()));
                        }
                    }
                }
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
                    if st.store.settle_ack(&mail, &session, &record)? {
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
            "claim" | "board" | "receipt" | "subscribe" | "unsubscribe" | "groups" => {
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

fn delivery(m: &Delivery) -> Value {
    let mut v = net::deliver_msg(&m.mail_id, &m.from, &m.to, &m.body, &m.kind, &m.headers);
    if let Value::Obj(pairs) = &mut v {
        if let Some(g) = &m.group {
            pairs.push(("group".into(), s(g)));
        }
        if let Some(n) = m.article_num {
            pairs.push(("article".into(), Value::Int(n)));
        }
    }
    v
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn group_delivery_fields() {
        let m = Delivery {
            mail_id: "m".into(),
            from: "a".into(),
            to: "b".into(),
            body: "text".into(),
            kind: "group".into(),
            headers: obj(vec![]),
            group: Some("g".into()),
            article_num: Some(9),
        };
        let v = delivery(&m);
        assert_eq!(v.get("group").and_then(Value::as_str), Some("g"));
        assert_eq!(v.get("article").and_then(Value::as_i64), Some(9));
    }
}

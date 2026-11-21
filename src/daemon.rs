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
use std::io::{self, Write};
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
    /// The connection's queued writer: a notice sent under the state lock
    /// only appends to its queue.
    writer: Writer,
    confirm: bool,
    confirmed: bool,
    notify: bool,
    superseded: bool,
}

struct State {
    /// The local log, behind its own lock. That lock orders each DB write
    /// with the State change that goes with it: take it first, then State,
    /// and never wait for it while holding State (see `db`).
    store: Arc<Mutex<Store>>,
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
    /// Local connections following a Group's live `article` frames, by Group name.
    article_watchers: HashMap<String, Vec<Writer>>,
}

impl State {
    /// Nothing delivered or acked yet: what a replay starts from.
    fn new(store: Store) -> State {
        State {
            store: Arc::new(Mutex::new(store)),
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
            article_watchers: HashMap::new(),
        }
    }
}

/// How long a session's send waits for the coordinator to answer.
const UPSTREAM_WAIT: Duration = Duration::from_secs(10);

/// How long a TCP peer has to send its auth line before it is dropped.
const AUTH_WAIT: Duration = if cfg!(test) {
    Duration::from_millis(300)
} else {
    Duration::from_secs(10)
};

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
    let listener = UnixListener::bind(path)?;
    // The socket asks no token, so only this user may connect to it.
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        let _ = std::fs::remove_file(path);
        return Err(e);
    }
    Ok(listener)
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
        for g in st.article_watchers.keys() {
            let _ = net::send(
                &writer,
                &obj(vec![
                    ("type", s("articles")),
                    ("req", Value::Int(0)),
                    ("group", s(g)),
                    ("after", Value::Int(0)),
                    ("limit", Value::Int(1)),
                    ("follow", Value::Bool(true)),
                ]),
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
            "article" => {
                if let Ok(g) = net::field(&v, "group") {
                    let st = state.lock().unwrap();
                    if let Some(ws) = st.article_watchers.get(g) {
                        for w in ws {
                            let _ = net::send(w, &v);
                        }
                    }
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
                        let _ = net::send(&r.writer, &notice);
                    }
                }
            }
            // Every other line answers one request, by its req.
            kind => {
                // Upstream has this receipt now, so it leaves the replay list.
                if kind == "acked" {
                    if let Ok(mail) = net::field(&v, "mail") {
                        confirm(state, mail, v.get("session").and_then(Value::as_str))?;
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
                    // A forwarded receipt upstream refuses stays queued, so
                    // say so: silence here would look like a confirmed receipt.
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

/// The local log's lock. Taking it releases State before waiting, so DB
/// I/O never runs under State and the order is always log, then State.
fn db(state: &Arc<Mutex<State>>) -> Arc<Mutex<Store>> {
    Arc::clone(&state.lock().unwrap().store)
}

fn handle_deliver(state: &Arc<Mutex<State>>, v: &Value) -> io::Result<()> {
    let row = Delivery {
        mail_id: net::field(v, "mail")?.to_string(),
        from: net::field(v, "from")?.into(),
        to: net::field(v, "to")?.into(),
        body: net::field(v, "body")?.into(),
        kind: net::kind_of(v),
        headers: net::headers_of(v),
        group: v.get("group").and_then(Value::as_str).map(str::to_string),
        article_num: v.get("article").and_then(Value::as_i64),
    };
    let db = db(state);
    let store = db.lock().unwrap();
    // A fresh row is the pending Mail itself. A repeated one is pushed again
    // only while it is still pending for its recipient, not once acked.
    let pending = if store.insert_deliver(&row)? {
        Some(row)
    } else {
        store.pending_delivery(&row.mail_id, &row.to)?
    };
    if let Some(m) = pending {
        let w = state.lock().unwrap().sessions.get(&m.to).cloned();
        // Pushed under the log's lock, so a Register's pending replay is
        // either before this Mail's commit or includes it, never both.
        if let Some(w) = w {
            let _ = net::send(&w, &delivery(&m));
        }
    }
    Ok(())
}

/// Commit a session's ack locally, then forward it on the upstream queue
/// with no waiter. The Receipt is durable here, so the session is answered
/// at once; the coordinator's `acked` trims the replay list in `confirm`,
/// and a link that drops first replays it after the next hello_ok.
fn local_ack(state: &Arc<Mutex<State>>, mail: &str, session: &str, record: &str) -> io::Result<()> {
    let db = db(state);
    let store = db.lock().unwrap();
    let fresh = store.settle_ack(mail, session, record)?;
    let mut st = state.lock().unwrap();
    if fresh {
        st.receipts.push(Receipt {
            mail: mail.to_string(),
            session: session.to_string(),
            record: record.to_string(),
        });
    }
    if let Some(up) = &st.upstream {
        let _ = net::send(up, &net::ack_msg(mail, session, record));
    }
    Ok(())
}

/// Upstream has this receipt now, so it leaves the log and the replay list.
fn confirm(state: &Arc<Mutex<State>>, mail: &str, session: Option<&str>) -> io::Result<()> {
    let db = db(state);
    let store = db.lock().unwrap();
    store.confirm_receipt(mail, session)?;
    state
        .lock()
        .unwrap()
        .receipts
        .retain(|r| r.mail != mail || session.is_some_and(|s| s != r.session));
    Ok(())
}

fn upstream_request(state: &Arc<Mutex<State>>, v: Value) -> io::Result<Value> {
    request_with_wait(state, v, UPSTREAM_WAIT)
}

fn request_with_wait(state: &Mutex<State>, mut v: Value, wait: Duration) -> io::Result<Value> {
    let (req, writer, rx) = {
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
        (req, writer, rx)
    };
    let result = net::send(&writer, &v).and_then(|()| {
        // no-poll: one bounded wait for a single reply, turning a silent coordinator into an error.
        rx.recv_timeout(wait).map_err(|e| match e {
            mpsc::RecvTimeoutError::Timeout => {
                io::Error::new(io::ErrorKind::TimedOut, "upstream did not answer")
            }
            mpsc::RecvTimeoutError::Disconnected => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "upstream wait dropped")
            }
        })
    });
    if result.is_err() {
        state.lock().unwrap().waiters.remove(&req);
    }
    result
}

#[test]
fn failed_requests_release_their_waiter() {
    let dir = std::env::temp_dir().join(crate::session::gen_uuid().unwrap());
    let (store, _) = Store::open(&dir.join("daemon.log")).unwrap();
    let state = Mutex::new(State::new(store));
    for (writer, expected) in [
        (net::writer(Box::new(io::sink())), io::ErrorKind::TimedOut),
        (
            net::writer(Box::new(io::Cursor::new([0u8; 0]))),
            io::ErrorKind::WriteZero,
        ),
    ] {
        state.lock().unwrap().upstream = Some(writer);
        let err = request_with_wait(&state, obj(vec![]), Duration::ZERO).unwrap_err();
        assert_eq!(err.kind(), expected);
        assert!(state.lock().unwrap().waiters.is_empty());
    }
    drop(state);
    std::fs::remove_dir_all(dir).unwrap();
}

fn error_value(reason: &str) -> Value {
    obj(vec![("type", s("error")), ("reason", s(reason))])
}

/// An upstream reply or transport error, tagged with the local request it answers.
fn with_req(result: io::Result<Value>, req: i64) -> Value {
    let mut v = result.unwrap_or_else(|e| error_value(&e.to_string()));
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
    if let Some(expected) = client_token {
        // no-poll: a bounded wait for the auth line, turning a silent unauthenticated peer into an error.
        conn.set_read_timeout(Some(AUTH_WAIT))?;
        if let Err(e) = client_auth(&mut lines, expected) {
            // No queue or writer thread for an unauthenticated peer: one
            // direct line into a fresh socket buffer, then hang up.
            let mut line = crate::json::stringify(&obj(vec![
                ("type", s("error")),
                ("error", s(&e.to_string())),
            ]));
            line.push('\n');
            let _ = (&conn).write_all(line.as_bytes());
            let _ = conn.shutdown(Shutdown::Both);
            return Err(e);
        }
        // no-poll: clears the auth bound; an authenticated session may stay silent.
        conn.set_read_timeout(None)?;
    }
    let conn = Arc::new(conn);
    let writer = net::queued(Arc::clone(&conn), |c| {
        let _ = c.shutdown(Shutdown::Both);
    });
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
    lines.set_cap(net::MAX_LINE_BYTES);
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
    {
        let mut st = state.lock().unwrap();
        st.watchers.retain(|w| !Arc::ptr_eq(w, &writer));
        let groups: Vec<String> = st.article_watchers.keys().cloned().collect();
        for g in groups {
            unwatch_article(&mut st, &g, &writer);
        }
    }
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

/// Stop passing `group`'s article frames to `writer`. The Group leaves the
/// map with its last watcher, so a reconnect no longer follows it upstream,
/// and the coordinator is told to stop sending its frames now.
fn unwatch_article(st: &mut State, group: &str, writer: &Writer) {
    let Some(list) = st.article_watchers.get_mut(group) else {
        return;
    };
    list.retain(|w| !Arc::ptr_eq(w, writer));
    if list.is_empty() {
        st.article_watchers.remove(group);
        if let Some(up) = &st.upstream {
            let _ = net::send(
                up,
                &obj(vec![("type", s("unwatch_articles")), ("group", s(group))]),
            );
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
                // The log's lock spans the State change and the replay, so a
                // delivery commits wholly before (replayed) or after (pushed).
                let db = db(state);
                let store = db.lock().unwrap();
                {
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
                }
                for p in store.pending_for_session(&session)? {
                    let _ = net::send(writer, &delivery(&p));
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
                    (to, _) => {
                        let reason = if to.is_some() {
                            "expected to or group, saw both"
                        } else {
                            "expected to or group, saw neither"
                        };
                        net::send(writer, &with_req(Ok(error_value(reason)), req))?;
                        continue;
                    }
                }
                // Headers are the coordinator's to check; this hop forwards them as given.
                if let Some(h) = v.get("headers") {
                    up.push(("headers", h.clone()));
                }
                if let Some(exp) = v.get("expected_article") {
                    up.push(("expected_article", exp.clone()));
                }
                let resp = upstream_request(state, obj(up))
                    .unwrap_or_else(|e| error_value(&e.to_string()));
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
                );
                net::send(writer, &with_req(resp, req))?;
            }
            "ack" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let session = (*session_id).clone().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "ack before register")
                })?;
                let mail = net::field(&v, "mail")?.to_string();
                let record = net::field(&v, "record")?.to_string();
                local_ack(state, &mail, &session, &record)?;
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
            "articles" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let follow = v.get("follow") == Some(&Value::Bool(true));
                let group = v.get("group").and_then(Value::as_str).map(str::to_string);
                // Only a watch this request added is undone if it fails; an
                // earlier follow that succeeded stays.
                let mut added = false;
                if follow {
                    if let Some(ref g) = group {
                        if !g.is_empty() {
                            let mut st = state.lock().unwrap();
                            let list = st.article_watchers.entry(g.clone()).or_default();
                            if !list.iter().any(|w| Arc::ptr_eq(w, writer)) {
                                list.push(Arc::clone(writer));
                                added = true;
                            }
                        }
                    }
                }
                let mut upstream = v.clone();
                if let Value::Obj(pairs) = &mut upstream {
                    pairs.retain(|(k, _)| k != "req");
                }
                let resp = upstream_request(state, upstream);
                if added
                    && resp
                        .as_ref()
                        .map_or(true, |r| net::field(r, "type").unwrap_or("") == "error")
                {
                    if let Some(ref g) = group {
                        unwatch_article(&mut state.lock().unwrap(), g, writer);
                    }
                }
                net::send(writer, &with_req(resp, req))?;
            }
            "claim" | "board" | "receipt" | "subscribe" | "unsubscribe" | "groups"
            | "group_policy" => {
                let req = v.get("req").and_then(|x| x.as_i64()).unwrap_or(0);
                let mut upstream = v.clone();
                if let Value::Obj(pairs) = &mut upstream {
                    pairs.retain(|(k, _)| k != "req");
                }
                net::send(writer, &with_req(upstream_request(state, upstream), req))?;
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

    /// A temp dir removed when the test ends.
    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn scratch(tag: &str) -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "roger-daemon-{tag}-{}",
            crate::session::gen_uuid().unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    #[cfg(unix)]
    #[test]
    fn bind_socket_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("sock");
        let path = dir.0.join("s");
        let _listener = bind_socket(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    /// One `serve_tcp` per accepted connection on a loopback port, as
    /// `accept_tcp` runs it, for the first `n` connections.
    fn tcp_daemon(
        token: Option<&'static str>,
        n: usize,
    ) -> (std::net::SocketAddr, Arc<Mutex<State>>, Scratch) {
        let dir = scratch("tcp");
        let (store, _) = Store::open(&dir.0.join("daemon")).unwrap();
        let state = Arc::new(Mutex::new(State::new(store)));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let st = Arc::clone(&state);
        std::thread::spawn(move || {
            for conn in listener.incoming().take(n) {
                let st = Arc::clone(&st);
                std::thread::spawn(move || serve_tcp(conn.unwrap(), st, token));
            }
        });
        (addr, state, dir)
    }

    fn client(addr: std::net::SocketAddr) -> (TcpStream, net::Lines) {
        let c = TcpStream::connect(addr).unwrap();
        // no-poll: a test bound, so a regressed daemon fails instead of hanging the gate.
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let lines = net::lines(Box::new(c.try_clone().unwrap()));
        (c, lines)
    }

    fn say(mut c: &TcpStream, line: &str) {
        c.write_all(format!("{line}\n").as_bytes()).unwrap();
    }

    #[test]
    fn serve_tcp_refuses_bad_or_missing_auth_and_clears_the_bound_after_good_auth() {
        let (addr, _, _dir) = tcp_daemon(Some("tok"), 3);

        let (c, mut lines) = client(addr);
        say(&c, r#"{"type":"auth","token":"wrong"}"#);
        let v = lines.next().unwrap().unwrap();
        assert_eq!(net::field(&v, "type").unwrap(), "error");
        assert!(lines.next().is_none());

        // Silent past the auth bound: dropped with an error, then end of stream.
        let (_c, mut lines) = client(addr);
        let v = lines.next().unwrap().unwrap();
        assert_eq!(net::field(&v, "type").unwrap(), "error");
        assert!(lines.next().is_none());

        let (c, mut lines) = client(addr);
        say(&c, r#"{"type":"auth","token":"tok"}"#);
        // no-poll: outlasts the auth bound to show success cleared it.
        std::thread::sleep(AUTH_WAIT * 2);
        say(&c, r#"{"type":"watch","req":4}"#);
        let v = lines.next().unwrap().unwrap();
        assert_eq!(net::field(&v, "type").unwrap(), "watching");
        assert_eq!(v.get("req").and_then(Value::as_i64), Some(4));
    }

    #[test]
    fn failed_follow_leaves_no_article_watch() {
        let (addr, state, _dir) = tcp_daemon(None, 1);
        let (c, mut lines) = client(addr);
        // No upstream: only the follow request fails.
        say(
            &c,
            r#"{"type":"articles","req":1,"group":"g","follow":true}"#,
        );
        let reply = lines.next().unwrap().unwrap();
        assert_eq!(net::field(&reply, "type").unwrap(), "error");
        assert_eq!(reply.get("req").and_then(Value::as_i64), Some(1));
        assert!(state.lock().unwrap().article_watchers.is_empty());
    }

    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Sink {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn only_the_last_article_watcher_leaving_unwatches_upstream() {
        let dir = scratch("unwatch");
        let (store, _) = Store::open(&dir.0.join("daemon")).unwrap();
        let mut st = State::new(store);
        let sent = Arc::new(Mutex::new(Vec::new()));
        st.upstream = Some(net::writer(Box::new(Sink(Arc::clone(&sent)))));
        let a = net::writer(Box::new(io::sink()));
        let b = net::writer(Box::new(io::sink()));
        st.article_watchers
            .insert("g".into(), vec![Arc::clone(&a), Arc::clone(&b)]);

        unwatch_article(&mut st, "g", &a);
        assert!(sent.lock().unwrap().is_empty());
        assert!(st.article_watchers.contains_key("g"));

        unwatch_article(&mut st, "g", &b);
        assert!(!st.article_watchers.contains_key("g"));
        assert_eq!(
            String::from_utf8(sent.lock().unwrap().clone()).unwrap(),
            "{\"type\":\"unwatch_articles\",\"group\":\"g\"}\n"
        );
    }
}

#[cfg(test)]
mod store_order {
    use super::*;

    /// Lines written to one peer, read back as JSON.
    #[derive(Clone, Default)]
    struct Seen(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Seen {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Seen {
        fn lines(&self) -> Vec<Value> {
            let text = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
            text.lines()
                .map(|l| crate::json::parse(l).unwrap())
                .collect()
        }
    }

    #[test]
    fn ack_forwards_with_no_waiter_and_acked_mail_is_not_pushed_again() {
        let dir = std::env::temp_dir().join(crate::session::gen_uuid().unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        let (store, _) = Store::open(&dir.join("daemon")).unwrap();
        let (session, upstream) = (Seen::default(), Seen::default());
        let mut st = State::new(store);
        st.sessions
            .insert("b".into(), net::writer(Box::new(session.clone())));
        st.upstream = Some(net::writer(Box::new(upstream.clone())));
        let state = Arc::new(Mutex::new(st));
        let deliver = net::deliver_msg("m", "a", "b", "text", net::DIRECTED, &obj(vec![]));

        handle_deliver(&state, &deliver).unwrap();
        // A repeat while still pending is pushed again, as before.
        handle_deliver(&state, &deliver).unwrap();
        assert_eq!(session.lines().len(), 2);

        local_ack(&state, "m", "b", "r").unwrap();
        let up = upstream.lines();
        assert_eq!(up.len(), 1);
        assert_eq!(up[0].get("type").and_then(Value::as_str), Some("ack"));
        assert!(
            up[0].get("req").is_none(),
            "forward must not wait on a reply"
        );
        assert_eq!(state.lock().unwrap().receipts.len(), 1);

        // Acked locally: a repeated delivery is not pushed.
        handle_deliver(&state, &deliver).unwrap();
        assert_eq!(session.lines().len(), 2);

        confirm(&state, "m", Some("b")).unwrap();
        assert!(state.lock().unwrap().receipts.is_empty());
        // Release the DB handle before removing its directory (Windows).
        drop(state);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

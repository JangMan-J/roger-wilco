//! The coordinator: single central durable store for sessions, mail, and
//! receipts. Host daemons connect to it; committed mail is pushed to the
//! recipient's host daemon, and pending mail is pushed when a session or
//! host (re)registers. Claims are decided against their committed deadlines
//! at the moment of each request or board read, and a session's registration
//! ends by its own unregister or, once it has been disconnected for
//! `unregister_after_ms`, at the next request that reads the registered set.
//! Nothing here scans on a timer.
//!
//! Mail bodies, envelopes, receipts and Group positions live in the
//! `sql::coord::Store` alone: memory holds only the registered set, its
//! presence, live host links and claims. A send commits through the store
//! without the state lock, so concurrent sends share one group commit; the
//! lock is taken only to find the host links to push to afterwards.

use crate::claims::{Change, Claims, Held, Op};
use crate::headers;
use crate::json::{obj, s, Value};
use crate::net::{self, Writer};
use crate::sql::{
    self,
    coord::{self, Broadcast, Directed, GroupMail, Status, Store},
    AckResult, ArticlesPage, Delivery, Event, Group, Receipt,
};
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// The refusal for a second send under an accepted Mail id that differs from
/// the first in `what` (sender, recipient or body).
fn resend_refusal(mail: &str, what: &str) -> String {
    format!(
        "mail {mail}: expected a resend to repeat the Mail already accepted under this id, saw a different {what}"
    )
}

/// The store names what a conflicting resend differs in; a reason it already
/// worded whole is passed on verbatim.
fn conflict_reason(mail: &str, what: &str) -> String {
    if what.contains("expected") {
        what.to_string()
    } else {
        resend_refusal(mail, what)
    }
}

/// The Address a send names to reach every registered session at once.
pub const BROADCAST: &str = "*";

/// Joins a Broadcast id to a recipient Address in that recipient's copy id.
/// No accepted Mail id may contain it, so a client-chosen id can never equal
/// a copy id, and two copy ids are equal only for the same broadcast and
/// recipient.
const COPY_SEP: char = '/';

/// The longest hello line read from a connection that has not yet shown the
/// fleet token.
const HELLO_MAX: usize = 64 * 1024;

/// How long a registered session may stay disconnected before it is
/// unregistered: longer than a host daemon or coordinator restart takes.
pub const UNREGISTER_AFTER_MS: i64 = 300_000;

/// Set once a durable write fails: the store may then be ahead of memory, so
/// the coordinator stops and a restart reads the store again.
static STORE_FAILED: AtomicBool = AtomicBool::new(false);

/// A durable write's result, marking the coordinator failed on an error.
fn written<T>(r: io::Result<T>) -> io::Result<T> {
    if r.is_err() {
        STORE_FAILED.store(true, Ordering::SeqCst);
    }
    r
}

/// Everything `roger coordinator` is started with.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub bind: String,
    pub data: PathBuf,
    pub port_file: Option<PathBuf>,
    pub token: String,
    pub unregister_after_ms: i64,
    /// Slice databases the store spreads Mail over, at least 1.
    pub slices: usize,
    /// How long a body stays hot before the cold sweep moves it to a day file.
    pub cold_retain_ms: i64,
    /// The length of one cold day file's bucket.
    pub cold_day_ms: i64,
    /// False with `--no-cold-sweep`: nothing moves bodies to cold files.
    pub cold_sweep: bool,
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
}

/// `name`'s value as an integer of at least `min`, or `default` when absent.
fn int_flag(args: &[String], name: &str, min: i64, default: i64) -> io::Result<i64> {
    let Some(text) = flag(args, name) else {
        return Ok(default);
    };
    match text.parse::<i64>() {
        Ok(n) if n >= min => Ok(n),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} expects an integer of at least {min}, saw {text}"),
        )),
    }
}

impl Config {
    /// Read the coordinator's flags; the token comes from the environment.
    pub fn from_args(args: &[String], token: String) -> io::Result<Config> {
        let need = |name: &str| {
            flag(args, name).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}"))
            })
        };
        let days = int_flag(args, "--cold-retain-days", 1, sql::DEFAULT_RETAIN_DAYS)?;
        #[allow(unused_mut)]
        let mut cold_retain_ms = days.saturating_mul(sql::DAY_MS);
        #[allow(unused_mut)]
        let mut cold_day_ms = sql::DAY_MS;
        #[cfg(feature = "test-hooks")]
        {
            cold_retain_ms = int_flag(args, "--cold-retain-ms", 1, cold_retain_ms)?;
            cold_day_ms = int_flag(args, "--cold-day-ms", 1, cold_day_ms)?;
        }
        Ok(Config {
            bind: need("--bind")?,
            data: PathBuf::from(need("--data")?),
            port_file: flag(args, "--port-file").map(PathBuf::from),
            token,
            unregister_after_ms: int_flag(
                args,
                "--unregister-after-ms",
                i64::MIN,
                UNREGISTER_AFTER_MS,
            )?,
            slices: int_flag(args, "--slices", 1, 1)? as usize,
            cold_retain_ms,
            cold_day_ms,
            cold_sweep: !args.iter().any(|a| a == "--no-cold-sweep"),
        })
    }

    fn open_config(&self) -> coord::Config {
        coord::Config {
            data: self.data.clone(),
            num_slices: self.slices,
            retain_ms: self.cold_retain_ms,
            day_ms: self.cold_day_ms,
            sweep_enabled: self.cold_sweep,
        }
    }
}

/// One registered session.
struct Session {
    host: String,
    /// Display text, stored verbatim; empty until a Register or relabel names one.
    label: String,
    /// Opaque text the session declared at its latest Register, verbatim.
    declared: Option<String>,
}

struct State {
    store: Arc<Store>,
    sessions: HashMap<String, Session>,
    /// When this coordinator started: a stored session that never
    /// re-registers counts as disconnected from then.
    started: i64,
    unregister_after_ms: i64,
    /// Presence: when each Address last acted (register, send, a live ack, a
    /// Board read naming it) or disconnected, on the coordinator clock. Memory
    /// only, since every live session re-registers when its host daemon
    /// reconnects. Only registered Addresses are stamped.
    seen: HashMap<String, i64>,
    /// Addresses whose session connection to their host daemon is open and
    /// whose host daemon's link here is up.
    connected: HashSet<String>,
    // Daemon-generated registration identity, scoped to a host link. Not an Address token.
    registrations: HashMap<String, (u64, String)>,
    /// Takeovers already committed, by registration: the new host and stamp
    /// when recorded by this process, None when read back from the store.
    takeovers: HashMap<String, Option<(String, i64)>>,
    hosts: HashMap<String, (u64, Writer)>, // host -> (connection id, live connection)
    next_conn: u64,
    notice_writers: HashMap<u64, std::sync::mpsc::Sender<Value>>,
    /// Host links whose hello asked for `board_event` lines, by connection
    /// id: each committed session or claim change is offered to them once.
    watchers: HashMap<u64, Writer>,
    /// Connections watching a Group's live `article` frames, by Group name.
    article_watchers: HashMap<String, Vec<(u64, Writer)>>,
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

pub fn run(cfg: Config) -> io::Result<()> {
    let (store, startup) = Store::open(cfg.open_config())?;
    let store = Arc::new(store);
    let state = State {
        store: Arc::clone(&store),
        sessions: startup
            .sessions
            .into_iter()
            .map(|(a, row)| {
                (
                    a,
                    Session {
                        host: row.host,
                        label: row.label,
                        declared: row.declared,
                    },
                )
            })
            .collect(),
        started: now_ms(),
        unregister_after_ms: cfg.unregister_after_ms,
        seen: HashMap::new(),
        connected: HashSet::new(),
        registrations: HashMap::new(),
        takeovers: startup
            .takeover_registrations
            .into_iter()
            .map(|r| (r, None))
            .collect(),
        hosts: HashMap::new(),
        next_conn: 1,
        notice_writers: HashMap::new(),
        watchers: HashMap::new(),
        article_watchers: HashMap::new(),
        claims: startup.claims,
    };
    let state = Arc::new(Mutex::new(state));

    let listener = TcpListener::bind(&cfg.bind)?;
    if let Some(pf) = &cfg.port_file {
        net::publish_port(pf, listener.local_addr()?.port())?;
    }
    eprintln!("coordinator listening on {}", listener.local_addr()?);

    for conn in listener.incoming() {
        let conn = conn?;
        let state = Arc::clone(&state);
        let store = Arc::clone(&store);
        let token = cfg.token.clone();
        std::thread::spawn(move || {
            if let Err(_e) = serve(conn, state, store, &token) {
                // A failed write may have left the store ahead of memory.
                // Stop here so a restart reads the store instead of serving
                // a split view.
                if STORE_FAILED.load(Ordering::SeqCst) {
                    eprintln!("coordinator stopping: durable store failed, restart to read it");
                    std::process::exit(1);
                }
            }
        });
    }
    Ok(())
}

/// Apply one committed timeline event to memory.
fn apply(st: &mut State, ev: &Event) {
    match ev {
        Event::Session { session, host, .. } => {
            st.sessions
                .entry(session.clone())
                .and_modify(|s| s.host = host.clone())
                .or_insert_with(|| Session {
                    host: host.clone(),
                    label: String::new(),
                    declared: None,
                });
        }
        Event::Unregister { session, .. } => {
            st.sessions.remove(session);
        }
        Event::Label { session, label, .. } => {
            if let Some(row) = st.sessions.get_mut(session) {
                row.label = label.clone();
            }
        }
        Event::Declared {
            session, declared, ..
        } => {
            if let Some(row) = st.sessions.get_mut(session) {
                row.declared = declared.clone();
            }
        }
        Event::Takeover {
            host,
            at_ms,
            registration: Some(r),
            ..
        } => {
            st.takeovers
                .insert(r.clone(), at_ms.map(|at| (host.clone(), at)));
        }
        Event::Claim {
            key,
            owner,
            expires_ms,
        } => st.claims.apply(
            key,
            &Change::Grant(Held {
                owner: owner.clone(),
                expires_ms: *expires_ms,
            }),
        ),
        Event::Release { key } => st.claims.apply(key, &Change::Release),
        _ => {}
    }
}

/// Stamp `session` as seen now, for its own act. Only a registered Address is
/// stamped: a send or Board read naming a stranger leaves no row behind.
fn stamp(st: &mut State, session: &str) {
    if st.sessions.contains_key(session) {
        st.seen.insert(session.to_string(), now_ms());
    }
}

/// Commit events durably as one transaction, then apply each to memory and
/// show each Board change to watchers: the one path every session event
/// takes, so the store and live state agree.
fn commit(st: &mut State, events: Vec<Event>) -> io::Result<()> {
    written(st.store.commit_timeline(events.clone(), now_ms()))?;
    for ev in &events {
        apply(st, ev);
        if let Some(pairs) = board_event_of(ev) {
            observe(st, pairs);
        }
    }
    Ok(())
}

fn label_event(session: &str, label: &str) -> Event {
    Event::Label {
        session: session.to_string(),
        label: label.to_string(),
        at_ms: Some(now_ms()),
    }
}

/// The `board_event` a committed timeline event is shown to watchers as, if
/// any: `register`, `unregister`, `relabel`, `declared`. Takeovers, claims
/// (shown by `claim`) and receipts show nothing here.
fn board_event_of(ev: &Event) -> Option<Vec<(&'static str, Value)>> {
    let at = |at_ms: &Option<i64>| at_ms.map(|t| ("at_ms", Value::Int(t)));
    let pairs = match ev {
        Event::Session {
            session,
            host,
            at_ms,
        } => {
            let mut p = vec![
                ("event", s("register")),
                ("session", s(session)),
                ("host", s(host)),
            ];
            p.extend(at(at_ms));
            p
        }
        Event::Unregister { session, at_ms } => {
            let mut p = vec![("event", s("unregister")), ("session", s(session))];
            p.extend(at(at_ms));
            p
        }
        Event::Label {
            session,
            label,
            at_ms,
        } => {
            let mut p = vec![
                ("event", s("relabel")),
                ("session", s(session)),
                ("label", s(label)),
            ];
            p.extend(at(at_ms));
            p
        }
        Event::Declared {
            session,
            declared,
            at_ms,
        } => {
            let mut p = vec![("event", s("declared")), ("session", s(session))];
            if let Some(d) = declared {
                p.push(("declared", s(d)));
            }
            p.extend(at(at_ms));
            p
        }
        _ => return None,
    };
    Some(pairs)
}

/// The committed timeline event for one claim change on `key`.
fn claim_timeline(key: &str, change: &Change) -> Event {
    match change {
        Change::Grant(h) => Event::Claim {
            key: key.to_string(),
            owner: h.owner.clone(),
            expires_ms: h.expires_ms,
        },
        Change::Release => Event::Release {
            key: key.to_string(),
        },
    }
}

/// Offer one committed change to every host link that asked for them. Best
/// effort: an observation, not Mail, so nothing is stored or acked, and a
/// link that is gone simply misses it.
fn observe(st: &State, pairs: Vec<(&str, Value)>) {
    if st.watchers.is_empty() {
        return;
    }
    let mut all = vec![("type", s("board_event"))];
    all.extend(pairs);
    let event = obj(all);
    for w in st.watchers.values() {
        let _ = net::send(w, &event);
    }
}

/// End one session's registration: it leaves the Board and every later
/// Broadcast. Mail already accepted for its Address stays pending and is
/// delivered if that Address registers again.
fn unregister(st: &mut State, session: &str) -> io::Result<()> {
    let now = now_ms();
    let freed = st.claims.release_owner(session, now);
    let held: Vec<_> = freed
        .iter()
        .map(|(key, _)| st.claims.live(key, now).cloned())
        .collect();
    let mut events = vec![Event::Unregister {
        session: session.to_string(),
        at_ms: Some(now),
    }];
    events.extend(
        freed
            .iter()
            .map(|(key, change)| claim_timeline(key, change)),
    );
    commit(st, events)?;
    for ((key, _), before) in freed.iter().zip(held) {
        if let Some(h) = before {
            observe(
                st,
                vec![
                    ("event", s("release")),
                    ("key", s(key)),
                    ("owner", s(&h.owner)),
                    ("expires_ms", Value::Int(h.expires_ms)),
                    ("at_ms", Value::Int(now)),
                ],
            );
        }
    }
    st.connected.remove(session);
    st.seen.remove(session);
    Ok(())
}

/// The registered sessions disconnected for `unregister_after_ms` at `now`,
/// sorted.
fn stale(st: &State, now: i64) -> Vec<String> {
    let cutoff = now - st.unregister_after_ms;
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
    gone
}

/// Unregister every session disconnected for `unregister_after_ms`, read
/// against now. Called by each request that reads the registered set
/// (Broadcast, Board, claim, relabel), as claim expiry is: no timer runs it.
fn expire(st: &mut State) -> io::Result<()> {
    for a in stale(st, now_ms()) {
        unregister(st, &a)?;
    }
    Ok(())
}

fn host_of<'a>(state: &'a State, session: &str) -> Option<&'a str> {
    state.sessions.get(session).map(|s| s.host.as_str())
}

/// The live link to `session`'s host daemon, if it has one.
fn link_of(st: &State, session: &str) -> Option<Writer> {
    host_of(st, session)
        .and_then(|h| st.hosts.get(h))
        .map(|(_, w)| Arc::clone(w))
}

/// The `deliver` line for one pending delivery: a Group article also names
/// its Group and article number, right after the recipient.
fn deliver_of(p: &Delivery) -> Value {
    let mut v = net::deliver_msg(&p.mail_id, &p.from, &p.to, &p.body, &p.kind, &p.headers);
    if let Value::Obj(pairs) = &mut v {
        let mut at = pairs
            .iter()
            .position(|(k, _)| k == "to")
            .map_or(0, |i| i + 1);
        if let Some(g) = &p.group {
            pairs.insert(at, ("group".to_string(), s(g)));
            at += 1;
        }
        if let Some(n) = p.article_num {
            pairs.insert(at, ("article".to_string(), Value::Int(n)));
        }
    }
    v
}

/// Push each pending delivery to its recipient's live host link.
fn push_pending(state: &Mutex<State>, pending: &[Delivery]) -> Vec<(Writer, Value)> {
    if pending.is_empty() {
        return Vec::new();
    }
    let st = state.lock().unwrap();
    pending
        .iter()
        .filter_map(|p| link_of(&st, &p.to).map(|w| (w, deliver_of(p))))
        .collect()
}

/// One connection, from accept to its end. Every line for the peer goes
/// through its queue, so a host daemon that stops reading delays only its own
/// lines; when the connection ends the queue is closed with it.
fn serve(
    conn: TcpStream,
    state: Arc<Mutex<State>>,
    store: Arc<Store>,
    token: &str,
) -> io::Result<()> {
    // One line is one segment, sent when it is written.
    conn.set_nodelay(true)?;
    let conn = Arc::new(conn);
    let writer = net::queued(Arc::clone(&conn), |c| {
        let _ = c.shutdown(Shutdown::Both);
    });
    let result = serve_link(&conn, Arc::clone(&writer), state, &store, token);
    // What is already queued is owed to the peer: answers to requests this
    // coordinator has committed. It is written before the link ends, unless
    // the link ended because the peer went silent; then nothing can reach it,
    // and the link ends at once, under any write still waiting on it.
    if let Err(e) = &result {
        eprintln!("coordinator connection ended: {e}");
    }
    let silent = matches!(&result, Err(e) if e.kind() == io::ErrorKind::TimedOut);
    net::close(&writer, !silent);
    result
}

/// Answer the sender, then push what the send committed. A sender whose link
/// is already gone cannot be answered, and that must not keep a committed
/// Mail from its recipient: the pushes go out either way.
fn answer_then_push(writer: &Writer, answer: &Value, push: Vec<(Writer, Value)>) -> io::Result<()> {
    let answered = net::send(writer, answer);
    for (w, msg) in push {
        let _ = net::send(&w, &msg);
    }
    answered
}

fn req_of(v: &Value) -> i64 {
    v.get("req").and_then(|x| x.as_i64()).unwrap_or(0)
}

fn error_reply(req: i64, mail: Option<&str>, reason: &str) -> Value {
    let mut pairs = vec![("type", s("error")), ("req", Value::Int(req))];
    if let Some(m) = mail {
        pairs.push(("mail", s(m)));
    }
    pairs.push(("reason", s(reason)));
    obj(pairs)
}

/// Why a send's Mail id or Group cannot be accepted, if either is malformed.
fn send_shape_error(mail: &str, group: Option<&str>) -> Option<String> {
    if mail.contains(COPY_SEP) {
        return Some(format!("mail id must not contain '{COPY_SEP}'"));
    }
    match group {
        Some(_) if mail.is_empty() => Some("expected a non-empty mail id, saw none".to_string()),
        Some("") => Some("expected a non-empty group, saw none".to_string()),
        _ => None,
    }
}

fn serve_link(
    conn: &TcpStream,
    writer: Writer,
    state: Arc<Mutex<State>>,
    store: &Store,
    token: &str,
) -> io::Result<()> {
    let mut lines = net::lines(Box::new(conn.try_clone()?));
    // Until the token is seen the peer is anyone who can reach the port.
    lines.set_cap(HELLO_MAX);

    let hello = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "no hello"))??;
    if net::field(&hello, "type")? != "hello"
        || !net::same_secret(net::field(&hello, "token")?, token)
    {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "bad hello"));
    }
    let host = net::field(&hello, "host")?.to_string();
    lines.set_cap(usize::MAX);
    let (conn_id, host_sessions) = {
        let mut st = state.lock().unwrap();
        let id = st.next_conn;
        st.next_conn += 1;
        st.hosts.insert(host.clone(), (id, Arc::clone(&writer)));
        let (tx, rx) = std::sync::mpsc::channel();
        let observer = writer.clone();
        std::thread::spawn(move || {
            for notice in rx {
                if net::send(&observer, &notice).is_err() {
                    break;
                }
            }
        });
        st.notice_writers.insert(id, tx);
        // A host daemon that never asks is never sent a `board_event`.
        if hello.get("board_events") == Some(&Value::Bool(true)) {
            st.watchers.insert(id, Arc::clone(&writer));
        }
        let mut on_host: Vec<String> = st
            .sessions
            .iter()
            .filter(|(_, row)| row.host == host)
            .map(|(a, _)| a.clone())
            .collect();
        on_host.sort();
        (id, on_host)
    };
    // However this connection ends (EOF, error, or a `?` below), the link drop
    // is handled once, when this guard goes.
    let _link = HostLink {
        state: Arc::clone(&state),
        host: host.clone(),
        conn_id,
    };
    net::send(
        &writer,
        &obj(vec![
            ("type", s("hello_ok")),
            ("registration_notices", Value::Bool(true)),
            // This coordinator answers `ping`; a host daemon that reads this
            // may send them.
            ("ping", Value::Bool(true)),
        ]),
    )?;
    for p in store.pending_host(&host_sessions)? {
        let _ = net::send(&writer, &deliver_of(&p));
    }

    let mut pinged = false;
    for line in lines {
        let v = line.map_err(net::silence)?;
        match net::field(&v, "type")? {
            // The link's own liveness. A host daemon that pings keeps pinging
            // at `net::ping_every`, so from its first ping a link that stays
            // silent for `net::link_silence` has lost its peer: the read ends,
            // and the link drop marks its sessions disconnected. A host daemon
            // that never pings is never held to this.
            "ping" => {
                if !pinged {
                    pinged = true;
                    // no-poll: link liveness, not Mail; a link silent this long has no peer and ends.
                    conn.set_read_timeout(Some(net::link_silence()))?;
                }
                net::send(&writer, &obj(vec![("type", s("pong"))]))?;
            }
            "replacement" => {
                let session = net::field(&v, "session")?;
                let registration = net::field(&v, "registration")?;
                let old = net::field(&v, "replaces")?;
                {
                    let mut st = state.lock().unwrap();
                    let notice = takeover(&mut st, session, &host, &host, old, None)?;
                    if let Some(tx) = st.notice_writers.get(&conn_id) {
                        let _ = tx.send(notice);
                    }
                }
                net::send(
                    &writer,
                    &obj(vec![
                        ("type", s("replacement_recorded")),
                        ("session", s(session)),
                        ("registration", s(registration)),
                    ]),
                )?;
            }
            "register" => {
                let session = net::field(&v, "session")?.to_string();
                {
                    let mut st = state.lock().unwrap();
                    let registration = net::field(&v, "registration").unwrap_or("");
                    let at = now_ms();
                    let previous = st.registrations.get(&session).cloned();
                    if st.connected.contains(&session)
                        && previous
                            .as_ref()
                            .is_none_or(|(_, r)| r != registration || registration.is_empty())
                    {
                        let old_host = st.sessions[&session].host.clone();
                        let old_registration =
                            previous.as_ref().map(|(_, r)| r.as_str()).unwrap_or("");
                        let notice = takeover(
                            &mut st,
                            &session,
                            &old_host,
                            &host,
                            old_registration,
                            Some(at),
                        )?;
                        if let Some((old_conn, _)) = previous {
                            if let Some(tx) = st.notice_writers.get(&old_conn) {
                                let _ = tx.send(notice);
                            }
                        }
                    }
                    // A local replacement can happen while this host's link
                    // is down. Its retained edge is evidence of that live
                    // local connection, not a reconnect of the same one.
                    if let Ok(old) = net::field(&v, "replaces") {
                        let notice = takeover(&mut st, &session, &host, &host, old, None)?;
                        if let Some(tx) = st.notice_writers.get(&conn_id) {
                            let _ = tx.send(notice);
                        }
                    }
                    st.registrations
                        .insert(session.clone(), (conn_id, registration.to_string()));
                    st.seen.insert(session.clone(), at);
                    st.connected.insert(session.clone());
                    // Every Register is one stamped event, so the timeline
                    // says when an Address registered even when nothing changed.
                    let mut events = vec![Event::Session {
                        session: session.clone(),
                        host: host.clone(),
                        at_ms: Some(now_ms()),
                    }];
                    if let Some(label) = v.get("label").and_then(|x| x.as_str()) {
                        events.push(label_event(&session, label));
                    }
                    let declared = net::declared_of(&v);
                    if st
                        .sessions
                        .get(&session)
                        .and_then(|row| row.declared.as_ref())
                        != declared.as_ref()
                    {
                        events.push(Event::Declared {
                            session: session.clone(),
                            declared,
                            at_ms: Some(now_ms()),
                        });
                    }
                    commit(&mut st, events)?;
                }
                for p in store.pending_session(&session)? {
                    let _ = net::send(&writer, &deliver_of(&p));
                }
                // The coordinator holds this Register now. A host daemon
                // forwards it only to a session that asked.
                let _ = net::send(
                    &writer,
                    &obj(vec![
                        ("type", s("registered")),
                        ("session", s(&session)),
                        (
                            "registration",
                            s(net::field(&v, "registration").unwrap_or("")),
                        ),
                    ]),
                );
            }
            "send" => send(&v, &writer, &state, store)?,
            // The session's connection to this host daemon closed.
            "disconnect" => {
                let session = net::field(&v, "session")?.to_string();
                let mut st = state.lock().unwrap();
                // A session that has since registered through another host
                // is still connected there.
                if owns_registration(&st, &session, &host, conn_id, &v)
                    && st.connected.remove(&session)
                {
                    st.seen.insert(session, now_ms());
                }
            }
            // The session left on purpose: its registration ends now, unless
            // it has since registered through another host.
            "unregister" => {
                let session = net::field(&v, "session")?.to_string();
                let mut st = state.lock().unwrap();
                if owns_registration(&st, &session, &host, conn_id, &v) {
                    unregister(&mut st, &session)?;
                }
            }
            "relabel" => {
                let req = req_of(&v);
                let session = net::field(&v, "session")?.to_string();
                let label = net::field(&v, "label")?.to_string();
                let reply = {
                    let mut st = state.lock().unwrap();
                    expire(&mut st)?;
                    // A label belongs to a session: an Address that never
                    // registered gets none, so no label can be parked on it.
                    if st.sessions.contains_key(&session) {
                        commit(&mut st, vec![label_event(&session, &label)])?;
                        obj(vec![
                            ("type", s("relabeled")),
                            ("req", Value::Int(req)),
                            ("session", s(&session)),
                            ("label", s(&label)),
                        ])
                    } else {
                        error_reply(
                            req,
                            None,
                            &format!("expected a registered session to relabel, saw {session}"),
                        )
                    }
                };
                net::send(&writer, &reply)?;
            }
            "ack" => {
                let req = req_of(&v);
                let mail = net::field(&v, "mail")?.to_string();
                let session = net::field(&v, "session")?.to_string();
                let record = net::field(&v, "record")?.to_string();
                // A replayed receipt was acked earlier, while this link was
                // down; it is not the session's activity now.
                if v.get("replay") != Some(&Value::Bool(true)) {
                    stamp(&mut state.lock().unwrap(), &session);
                }
                let result = written(store.ack(&mail, &session, &record, now_ms()))?;
                net::send(&writer, &ack_reply(req, &mail, &session, &record, &result))?;
            }
            "receipt" => {
                let req = req_of(&v);
                let mail = net::field(&v, "mail")?;
                let reader = v.get("session").and_then(|x| x.as_str());
                let info = store.receipt(mail, reader)?;
                net::send(&writer, &receipt_reply(req, mail, info.as_ref()))?;
            }
            "subscribe" => {
                let req = req_of(&v);
                let session = net::field(&v, "session")?.to_string();
                let group = net::field(&v, "group")?.to_string();
                let after = v.get("after").map(|x| x.as_i64());
                let latest = v.get("latest") == Some(&Value::Bool(true));
                let refusal = if group.is_empty() {
                    Some("expected a non-empty group, saw none".to_string())
                } else {
                    match after {
                        Some(None) | Some(Some(i64::MIN..=-1)) => Some(
                            "expected after to be an integer of at least 0, saw another value"
                                .to_string(),
                        ),
                        Some(Some(_)) if latest => {
                            Some("expected after or latest, saw both".to_string())
                        }
                        _ => None,
                    }
                };
                if let Some(reason) = refusal {
                    net::send(&writer, &error_reply(req, None, &reason))?;
                    continue;
                }
                stamp(&mut state.lock().unwrap(), &session);
                let (position, high_water, catchup) =
                    written(store.subscribe(&session, &group, after.flatten(), latest, now_ms()))?;
                answer_then_push(
                    &writer,
                    &obj(vec![
                        ("type", s("subscribed")),
                        ("req", Value::Int(req)),
                        ("session", s(&session)),
                        ("group", s(&group)),
                        ("position", Value::Int(position)),
                        ("high_water", Value::Int(high_water)),
                    ]),
                    push_pending(&state, &catchup),
                )?;
            }
            "unsubscribe" => {
                let req = req_of(&v);
                let session = net::field(&v, "session")?.to_string();
                let group = net::field(&v, "group")?.to_string();
                stamp(&mut state.lock().unwrap(), &session);
                let position = written(store.unsubscribe(&session, &group, now_ms()))?;
                net::send(
                    &writer,
                    &obj(vec![
                        ("type", s("unsubscribed")),
                        ("req", Value::Int(req)),
                        ("session", s(&session)),
                        ("group", s(&group)),
                        ("position", Value::Int(position)),
                    ]),
                )?;
            }
            "groups" => {
                let req = req_of(&v);
                let session = v.get("session").and_then(|x| x.as_str());
                let prefix = v.get("prefix").and_then(|x| x.as_str());
                let include_retired = v.get("include_retired") == Some(&Value::Bool(true));
                let list = store.groups(session, prefix, include_retired)?;
                net::send(&writer, &groups_reply(req, session.is_some(), &list))?;
            }
            "articles" => {
                let req = req_of(&v);
                let group = net::field(&v, "group")?.to_string();
                let after = match v.get("after") {
                    None | Some(Value::Null) => 0,
                    Some(x) => match x.as_i64() {
                        Some(n) if n >= 0 => n,
                        _ => {
                            net::send(
                                &writer,
                                &error_reply(
                                    req,
                                    None,
                                    "expected after to be a non-negative integer",
                                ),
                            )?;
                            continue;
                        }
                    },
                };
                let limit = match v.get("limit") {
                    None | Some(Value::Null) => 100,
                    Some(x) => match x.as_i64() {
                        Some(n) if (1..=500).contains(&n) => n as usize,
                        _ => {
                            net::send(
                                &writer,
                                &error_reply(req, None, "expected limit to be in 1..=500"),
                            )?;
                            continue;
                        }
                    },
                };
                let follow = v.get("follow") == Some(&Value::Bool(true));
                if group.is_empty() {
                    net::send(
                        &writer,
                        &error_reply(req, None, "expected a non-empty group, saw none"),
                    )?;
                    continue;
                }
                if follow {
                    let mut st = state.lock().unwrap();
                    let entry = st.article_watchers.entry(group.clone()).or_default();
                    if !entry.iter().any(|(id, _)| *id == conn_id) {
                        entry.push((conn_id, Arc::clone(&writer)));
                    }
                }
                let page = store.articles(&group, after, limit)?;
                net::send(&writer, &articles_reply(req, &page, follow))?;
            }
            "group_policy" => {
                let req = req_of(&v);
                let group = net::field(&v, "group")?.to_string();
                if group.is_empty() {
                    net::send(
                        &writer,
                        &error_reply(req, None, "expected a non-empty group, saw none"),
                    )?;
                    continue;
                }
                let parse_opt_nonneg = |field: &str| -> Result<Option<Option<i64>>, String> {
                    match v.get(field) {
                        None => Ok(None),
                        Some(Value::Null) => Ok(Some(None)),
                        Some(x) => match x.as_i64() {
                            Some(n) if n >= 0 => Ok(Some(Some(n))),
                            _ => Err(format!(
                                "expected {field} to be a non-negative integer or null"
                            )),
                        },
                    }
                };
                let keep_last = match parse_opt_nonneg("keep_last") {
                    Ok(v) => v,
                    Err(r) => {
                        net::send(&writer, &error_reply(req, None, &r))?;
                        continue;
                    }
                };
                let ttl_ms = match parse_opt_nonneg("ttl_ms") {
                    Ok(v) => v,
                    Err(r) => {
                        net::send(&writer, &error_reply(req, None, &r))?;
                        continue;
                    }
                };
                let retired = match v.get("retired") {
                    None => None,
                    Some(Value::Bool(b)) => Some(*b),
                    Some(_) => {
                        net::send(
                            &writer,
                            &error_reply(req, None, "expected retired to be a boolean"),
                        )?;
                        continue;
                    }
                };
                let g = written(store.group_policy(&group, keep_last, ttl_ms, retired, now_ms()))?;
                net::send(&writer, &group_policy_reply(req, &g))?;
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

/// One `send`: directed, Broadcast (`to` is `*`) or to a Group (`group`
/// instead of `to`). The store commits without the state lock; the lock is
/// taken briefly before (presence, the registered set) and after (the host
/// links to push to).
fn send(v: &Value, writer: &Writer, state: &Mutex<State>, store: &Store) -> io::Result<()> {
    let req = req_of(v);
    let mail = net::field(v, "mail")?.to_string();
    let group = v.get("group").and_then(|x| x.as_str());
    if group.is_some() && v.get("to").is_some() {
        let reason = "expected either to or group on a send, saw both";
        return net::send(writer, &error_reply(req, Some(&mail), reason));
    }
    if let Some(reason) = send_shape_error(&mail, group) {
        return net::send(writer, &error_reply(req, Some(&mail), &reason));
    }
    // The one place headers are bounded, before anything commits.
    let headers = match headers::validate(v.get("headers")) {
        Ok(h) => h,
        Err(reason) => return net::send(writer, &error_reply(req, Some(&mail), &reason)),
    };
    let headers = Some(headers);
    let from = net::field(v, "from")?.to_string();
    let body = net::field(v, "body")?.to_string();
    let accepted = |extra: Vec<(&'static str, Value)>| {
        let mut pairs = vec![
            ("type", s("accepted")),
            ("req", Value::Int(req)),
            ("mail", s(&mail)),
        ];
        pairs.extend(extra);
        obj(pairs)
    };
    let refuse = |what: &str| {
        net::send(
            writer,
            &error_reply(req, Some(&mail), &conflict_reason(&mail, what)),
        )
    };

    let expected_article = match v.get("expected_article") {
        None | Some(Value::Null) => None,
        Some(x) => match x.as_i64() {
            Some(n) if n >= 0 => {
                if group.is_none() {
                    return net::send(
                        writer,
                        &error_reply(req, Some(&mail), "expected_article requires group"),
                    );
                }
                Some(n)
            }
            _ => {
                return net::send(
                    writer,
                    &error_reply(
                        req,
                        Some(&mail),
                        "expected expected_article to be a non-negative integer",
                    ),
                );
            }
        },
    };

    if let Some(group) = group {
        stamp(&mut state.lock().unwrap(), &from);
        let accepted_at = now_ms();
        let status = written(store.send_group(GroupMail {
            mail_id: mail.clone(),
            from: from.clone(),
            group: group.to_string(),
            body: body.clone(),
            headers: headers.clone(),
            expected_article,
            accepted_at_ms: Some(accepted_at),
        }))?;
        let (article, readers, push_watch) = match status {
            Status::Committed {
                article_num,
                subscribers,
            } => (article_num, subscribers, true),
            Status::DuplicateAccepted { article_num } => (article_num, Vec::new(), false),
            Status::Conflict(what) => return refuse(&what),
        };
        let pending: Vec<Delivery> = readers
            .into_iter()
            .map(|to| Delivery {
                seq: 0,
                mail_id: mail.clone(),
                from: from.clone(),
                to,
                group: Some(group.to_string()),
                article_num: Some(article),
                body: body.clone(),
                kind: net::GROUP.to_string(),
                headers: headers.clone().unwrap_or(Value::Obj(Vec::new())),
            })
            .collect();
        let mut pushes = push_pending(state, &pending);
        if push_watch {
            let watchers: Vec<Writer> = {
                let st = state.lock().unwrap();
                st.article_watchers
                    .get(group)
                    .map(|list| list.iter().map(|(_, w)| Arc::clone(w)).collect())
                    .unwrap_or_default()
            };
            if !watchers.is_empty() {
                let frame = obj(vec![
                    ("type", s("article")),
                    ("group", s(group)),
                    ("article", Value::Int(article)),
                    ("mail", s(&mail)),
                    ("from", s(&from)),
                    ("accepted_at_ms", Value::Int(accepted_at)),
                    ("headers", headers.clone().unwrap_or(Value::Obj(Vec::new()))),
                    ("body", s(&body)),
                ]);
                for w in watchers {
                    pushes.push((w, frame.clone()));
                }
            }
        }
        return answer_then_push(
            writer,
            &accepted(vec![("group", s(group)), ("article", Value::Int(article))]),
            pushes,
        );
    }

    let to = net::field(v, "to")?.to_string();
    let (status, recipients) = if to == BROADCAST {
        let recipients = {
            let mut st = state.lock().unwrap();
            stamp(&mut st, &from);
            expire(&mut st)?;
            let mut all: Vec<String> = st.sessions.keys().cloned().collect();
            all.sort();
            all
        };
        let status = written(store.send_broadcast(Broadcast {
            mail_id: mail.clone(),
            from: from.clone(),
            recipients: recipients.clone(),
            body: body.clone(),
            headers: headers.clone(),
            accepted_at_ms: Some(now_ms()),
        }))?;
        (status, recipients)
    } else {
        stamp(&mut state.lock().unwrap(), &from);
        let status = written(store.send_directed(Directed {
            mail_id: mail.clone(),
            from: from.clone(),
            to: to.clone(),
            body: body.clone(),
            headers: headers.clone(),
            accepted_at_ms: Some(now_ms()),
        }))?;
        (status, vec![to.clone()])
    };
    let recipients = match status {
        Status::Committed { .. } => recipients,
        // A resend commits nothing and pushes nothing: the first send did.
        Status::DuplicateAccepted { .. } => Vec::new(),
        Status::Conflict(what) => return refuse(&what),
    };
    let (kind, copies) = if to == BROADCAST {
        (net::BROADCAST, true)
    } else {
        (net::DIRECTED, false)
    };
    let pending: Vec<Delivery> = recipients
        .into_iter()
        .map(|addr| Delivery {
            seq: 0,
            mail_id: if copies {
                copy_id(&mail, &addr)
            } else {
                mail.clone()
            },
            from: from.clone(),
            to: addr,
            group: None,
            article_num: None,
            body: body.clone(),
            kind: kind.to_string(),
            headers: headers.clone().unwrap_or(Value::Obj(Vec::new())),
        })
        .collect();
    answer_then_push(writer, &accepted(Vec::new()), push_pending(state, &pending))
}

/// The answer to an ack. `acked` names the session whose receipt it confirms,
/// so its host daemon trims exactly that one from its replay list. Answering
/// `acked` for a Mail this coordinator never accepted would be a false
/// confirmation: the receipt would never be stored upstream.
fn ack_reply(req: i64, mail: &str, session: &str, record: &str, result: &AckResult) -> Value {
    match result {
        AckResult::Recorded { .. } => obj(vec![
            ("type", s("acked")),
            ("req", Value::Int(req)),
            ("mail", s(mail)),
            ("session", s(session)),
            ("record", s(record)),
        ]),
        AckResult::UnknownMail => error_reply(req, Some(mail), "unknown mail"),
    }
}

fn takeover(
    st: &mut State,
    session: &str,
    old_host: &str,
    host: &str,
    registration: &str,
    at: Option<i64>,
) -> io::Result<Value> {
    let known = st
        .takeovers
        .get(registration)
        .filter(|_| !registration.is_empty() && at.is_none())
        .cloned();
    let (new_host, at_ms) = match known {
        Some(Some(recorded)) => recorded,
        // Committed before this process started: the store holds the line,
        // so nothing is committed again.
        Some(None) => (host.to_string(), now_ms()),
        None => {
            let at_ms = at.unwrap_or_else(now_ms);
            commit(
                st,
                vec![Event::Takeover {
                    session: session.to_string(),
                    old_host: old_host.to_string(),
                    host: host.to_string(),
                    at_ms: Some(at_ms),
                    registration: Some(registration.to_string()),
                }],
            )?;
            (host.to_string(), at_ms)
        }
    };
    Ok(obj(vec![
        ("type", s("superseded")),
        ("session", s(session)),
        ("host", s(&new_host)),
        ("at_ms", Value::Int(at_ms)),
        ("registration", s(registration)),
    ]))
}

fn owns_registration(st: &State, session: &str, host: &str, conn: u64, v: &Value) -> bool {
    host_of(st, session) == Some(host)
        && st
            .registrations
            .get(session)
            .is_some_and(|(c, r)| *c == conn && r == net::field(v, "registration").unwrap_or(""))
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
        st.notice_writers.remove(&self.conn_id);
        st.watchers.remove(&self.conn_id);
        for list in st.article_watchers.values_mut() {
            list.retain(|(id, _)| *id != self.conn_id);
        }
        st.article_watchers.retain(|_, list| !list.is_empty());
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
    let req = req_of(v);
    let op_name = net::field(v, "op")?;
    // A bad op is answered, not fatal: an error here would drop the whole
    // host daemon's link.
    let Some(op) = Op::parse(op_name) else {
        return Ok(error_reply(
            req,
            None,
            &format!("expected op acquire, renew, or release; saw {op_name}"),
        ));
    };
    let key = net::field(v, "key")?.to_string();
    let owner = net::field(v, "owner")?.to_string();
    let ttl_ms = v.get("ttl_ms").and_then(|x| x.as_i64()).unwrap_or(0);
    if op != Op::Release && ttl_ms <= 0 {
        return Ok(error_reply(
            req,
            None,
            &format!("ttl_ms must be positive, saw {ttl_ms}"),
        ));
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
        // The grant this change replaces, live or not: every committed
        // deadline lies after i64::MIN, so this reads the stored entry.
        let before = st.claims.live(&key, i64::MIN).cloned();
        commit(st, vec![claim_timeline(&key, change)])?;
        let claim_event = |name: &str, h: &Held| {
            vec![
                ("event", s(name)),
                ("key", s(&key)),
                ("owner", s(&h.owner)),
                ("expires_ms", Value::Int(h.expires_ms)),
                ("at_ms", Value::Int(now)),
            ]
        };
        // An expired grant has no line of its own; it is shown once, when
        // the change committed over it is.
        if let Some(old) = before.as_ref().filter(|h| h.expires_ms <= now) {
            observe(st, claim_event("expired", old));
        }
        match change {
            Change::Grant(h) => observe(st, claim_event(op.name(), h)),
            Change::Release => {
                if let Some(old) = &before {
                    observe(st, claim_event("release", old));
                }
            }
        }
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
    let req = req_of(v);
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

/// One Mail's lifecycle as this coordinator recorded it: its recipient (a
/// Group's name for an article) and sender, when it was Accepted, and, once
/// acked, the Receipt's Record id and when it was recorded. A stamp the store
/// never held is left out, never sent as zero. A Broadcast copy is read by
/// its copy id.
fn receipt_reply(req: i64, mail: &str, info: Option<&Receipt>) -> Value {
    let Some(m) = info else {
        return error_reply(req, Some(mail), &format!("unknown mail {mail}"));
    };
    let mut pairs = vec![
        ("type", s("receipt")),
        ("req", Value::Int(req)),
        ("mail", s(mail)),
        ("to", s(&m.to)),
        ("from", s(&m.from)),
        ("kind", s(&m.kind)),
        (
            "headers",
            m.headers.clone().unwrap_or(Value::Obj(Vec::new())),
        ),
    ];
    if m.kind == net::GROUP {
        if let Some(g) = &m.group {
            pairs.push(("group", s(g)));
        }
        if let Some(n) = m.article_num {
            pairs.push(("article", Value::Int(n)));
        }
    }
    if let Some(t) = m.accepted_at_ms {
        pairs.push(("accepted_at_ms", Value::Int(t)));
    }
    if let Some(r) = &m.record_id {
        pairs.push(("record", s(r)));
        if let Some(t) = m.receipted_at_ms {
            pairs.push(("receipted_at_ms", Value::Int(t)));
        }
    }
    obj(pairs)
}

/// The answer to `groups`: every Group with its high-water article and
/// subscriber count, and, when a session asked, its own subscription.
fn group_obj(for_session: bool, g: &Group) -> Value {
    let mut pairs = vec![
        ("group", s(&g.group)),
        ("high_water", Value::Int(g.high_water)),
        ("subscribers", Value::Int(g.subscribers as i64)),
    ];
    if g.retired {
        pairs.push(("retired", Value::Bool(true)));
    }
    if let Some(kl) = g.keep_last {
        pairs.push(("keep_last", Value::Int(kl)));
    }
    if let Some(ttl) = g.ttl_ms {
        pairs.push(("ttl_ms", Value::Int(ttl)));
    }
    if for_session {
        pairs.push(("subscribed", Value::Bool(g.subscribed)));
        pairs.push(("position", g.position.map_or(Value::Null, Value::Int)));
        pairs.push(("record", g.record_id.as_deref().map_or(Value::Null, s)));
    }
    obj(pairs)
}

/// The answer to `groups`: every Group with its high-water article and
/// subscriber count, and, when a session asked, its own subscription.
fn groups_reply(req: i64, for_session: bool, list: &[Group]) -> Value {
    let groups = list.iter().map(|g| group_obj(for_session, g)).collect();
    obj(vec![
        ("type", s("groups")),
        ("req", Value::Int(req)),
        ("groups", Value::Arr(groups)),
    ])
}

fn articles_reply(req: i64, page: &ArticlesPage, following: bool) -> Value {
    let articles = page
        .articles
        .iter()
        .map(|a| {
            let mut pairs = vec![
                ("article", Value::Int(a.article_num)),
                ("mail", s(&a.mail_id)),
                ("from", s(&a.from)),
            ];
            if let Some(at) = a.accepted_at_ms {
                pairs.push(("accepted_at_ms", Value::Int(at)));
            }
            pairs.push(("headers", a.headers.clone()));
            pairs.push(("body", s(&a.body)));
            obj(pairs)
        })
        .collect();
    let mut pairs = vec![
        ("type", s("articles")),
        ("req", Value::Int(req)),
        ("group", s(&page.group)),
        ("high_water", Value::Int(page.high_water)),
        ("retired", Value::Bool(page.retired)),
    ];
    if following {
        pairs.push(("following", Value::Bool(true)));
    }
    pairs.push(("articles", Value::Arr(articles)));
    obj(pairs)
}

fn group_policy_reply(req: i64, g: &Group) -> Value {
    let mut pairs = vec![
        ("type", s("group_policy")),
        ("req", Value::Int(req)),
        ("group", s(&g.group)),
        ("high_water", Value::Int(g.high_water)),
        ("subscribers", Value::Int(g.subscribers as i64)),
        ("retired", Value::Bool(g.retired)),
    ];
    if let Some(kl) = g.keep_last {
        pairs.push(("keep_last", Value::Int(kl)));
    }
    if let Some(ttl) = g.ttl_ms {
        pairs.push(("ttl_ms", Value::Int(ttl)));
    }
    obj(pairs)
}

/// The Mail id of one Broadcast copy: the broadcast's id and the recipient's
/// Address, so each recipient's copy is delivered, stored, and acked once.
fn copy_id(mail: &str, to: &str) -> String {
    format!("{mail}{COPY_SEP}{to}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::stringify;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|a| a.to_string()).collect()
    }

    fn line(pairs: Vec<(&'static str, Value)>) -> String {
        stringify(&obj(pairs))
    }

    #[test]
    fn coordinator_flags_default_and_bound() {
        let base = ["--bind", "127.0.0.1:0", "--data", "/d/c.log"];
        let cfg = Config::from_args(&args(&base), "t".into()).unwrap();
        assert_eq!(
            cfg,
            Config {
                bind: "127.0.0.1:0".into(),
                data: PathBuf::from("/d/c.log"),
                port_file: None,
                token: "t".into(),
                unregister_after_ms: UNREGISTER_AFTER_MS,
                slices: 1,
                cold_retain_ms: 30 * sql::DAY_MS,
                cold_day_ms: sql::DAY_MS,
                cold_sweep: true,
            }
        );
        let mut more = base.to_vec();
        more.extend([
            "--slices",
            "4",
            "--cold-retain-days",
            "2",
            "--no-cold-sweep",
            "--port-file",
            "/p",
            "--unregister-after-ms",
            "7",
        ]);
        let cfg = Config::from_args(&args(&more), "t".into()).unwrap();
        assert_eq!(
            (
                cfg.slices,
                cfg.cold_retain_ms,
                cfg.cold_sweep,
                cfg.port_file,
                cfg.unregister_after_ms
            ),
            (4, 2 * sql::DAY_MS, false, Some(PathBuf::from("/p")), 7)
        );
        let open = Config::from_args(&args(&more), "t".into())
            .unwrap()
            .open_config();
        assert_eq!(
            (open.num_slices, open.retain_ms, open.sweep_enabled),
            (4, 2 * sql::DAY_MS, false)
        );
        for (name, bad) in [
            ("--slices", "0"),
            ("--slices", "x"),
            ("--cold-retain-days", "0"),
            ("--cold-retain-days", "-3"),
        ] {
            let mut a = base.to_vec();
            a.extend([name, bad]);
            let err = Config::from_args(&args(&a), "t".into()).unwrap_err();
            assert!(err.to_string().contains(name), "{name} {bad}: {err}");
        }
        assert!(Config::from_args(&args(&["--bind", "x"]), "t".into())
            .unwrap_err()
            .to_string()
            .contains("--data"));
    }

    #[cfg(feature = "test-hooks")]
    #[test]
    fn test_hooks_set_cold_retention_and_day_in_milliseconds() {
        let a = args(&[
            "--bind",
            "b",
            "--data",
            "d",
            "--cold-retain-days",
            "3",
            "--cold-retain-ms",
            "250",
            "--cold-day-ms",
            "100",
        ]);
        let cfg = Config::from_args(&a, "t".into()).unwrap();
        assert_eq!((cfg.cold_retain_ms, cfg.cold_day_ms), (250, 100));
        let open = cfg.open_config();
        assert_eq!((open.retain_ms, open.day_ms), (250, 100));
    }

    fn pending(kind: &str, group: Option<&str>, article: Option<i64>) -> Delivery {
        Delivery {
            seq: 3,
            mail_id: "m".into(),
            from: "a".into(),
            to: "b".into(),
            group: group.map(str::to_string),
            article_num: article,
            body: "hi".into(),
            kind: kind.into(),
            headers: obj(vec![("k", s("v"))]),
        }
    }

    #[test]
    fn a_group_article_delivers_with_its_group_and_article() {
        assert_eq!(
            stringify(&deliver_of(&pending(net::GROUP, Some("ops"), Some(7)))),
            r#"{"type":"deliver","mail":"m","from":"a","to":"b","group":"ops","article":7,"body":"hi","kind":"group","headers":{"k":"v"}}"#
        );
        assert_eq!(
            stringify(&deliver_of(&pending(net::DIRECTED, None, None))),
            r#"{"type":"deliver","mail":"m","from":"a","to":"b","body":"hi","kind":"directed","headers":{"k":"v"}}"#
        );
    }

    fn info(kind: &str) -> Receipt {
        Receipt {
            mail_id: "m1".into(),
            from: "a".into(),
            to: "b".into(),
            group: Some("ops".into()),
            article_num: Some(4),
            kind: kind.into(),
            headers: None,
            accepted_at_ms: Some(5),
            record_id: Some("r-1".into()),
            receipted_at_ms: Some(9),
        }
    }

    #[test]
    fn receipt_reply_reads_as_before_and_names_a_group_article() {
        let mut directed = info(net::DIRECTED);
        directed.headers = Some(obj(vec![("k", s("v"))]));
        assert_eq!(
            stringify(&receipt_reply(3, "m1", Some(&directed))),
            r#"{"type":"receipt","req":3,"mail":"m1","to":"b","from":"a","kind":"directed","headers":{"k":"v"},"accepted_at_ms":5,"record":"r-1","receipted_at_ms":9}"#
        );
        let mut group = info(net::GROUP);
        group.to = "ops".into();
        group.record_id = None;
        group.accepted_at_ms = None;
        assert_eq!(
            stringify(&receipt_reply(3, "m1", Some(&group))),
            r#"{"type":"receipt","req":3,"mail":"m1","to":"ops","from":"a","kind":"group","headers":{},"group":"ops","article":4}"#
        );
        assert_eq!(
            stringify(&receipt_reply(3, "zz", None)),
            r#"{"type":"error","req":3,"mail":"zz","reason":"unknown mail zz"}"#
        );
    }

    #[test]
    fn acked_names_the_session_and_an_unknown_mail_is_refused() {
        assert_eq!(
            stringify(&ack_reply(
                2,
                "m",
                "b",
                "r-1",
                &AckResult::Recorded {
                    newly_receipted: false
                }
            )),
            r#"{"type":"acked","req":2,"mail":"m","session":"b","record":"r-1"}"#
        );
        assert_eq!(
            stringify(&ack_reply(2, "m", "b", "r-1", &AckResult::UnknownMail)),
            r#"{"type":"error","req":2,"mail":"m","reason":"unknown mail"}"#
        );
    }

    #[test]
    fn groups_reply_adds_the_session_view_only_when_asked() {
        let list = [
            Group {
                group: "ops".into(),
                high_water: 3,
                subscribers: 2,
                position: Some(1),
                subscribed: true,
                record_id: Some("r-2".into()),
                retired: false,
                keep_last: None,
                ttl_ms: None,
            },
            Group {
                group: "dev".into(),
                high_water: 0,
                subscribers: 0,
                position: None,
                subscribed: false,
                record_id: None,
                retired: false,
                keep_last: None,
                ttl_ms: None,
            },
        ];
        assert_eq!(
            stringify(&groups_reply(1, false, &list)),
            r#"{"type":"groups","req":1,"groups":[{"group":"ops","high_water":3,"subscribers":2},{"group":"dev","high_water":0,"subscribers":0}]}"#
        );
        assert_eq!(
            stringify(&groups_reply(1, true, &list)),
            r#"{"type":"groups","req":1,"groups":[{"group":"ops","high_water":3,"subscribers":2,"subscribed":true,"position":1,"record":"r-2"},{"group":"dev","high_water":0,"subscribers":0,"subscribed":false,"position":null,"record":null}]}"#
        );
    }

    #[test]
    fn send_shape_refuses_copy_ids_empty_group_mail_and_empty_group() {
        assert!(send_shape_error("a/b", None).unwrap().contains("'/'"));
        assert!(send_shape_error("a/b", Some("g")).unwrap().contains("'/'"));
        assert!(send_shape_error("", Some("g")).unwrap().contains("mail id"));
        assert!(send_shape_error("m", Some("")).unwrap().contains("group"));
        assert_eq!(send_shape_error("m", Some("g")), None);
        assert_eq!(send_shape_error("m", None), None);
    }

    #[test]
    fn a_conflicting_resend_is_worded_once() {
        assert_eq!(
            conflict_reason("m1", "body"),
            "mail m1: expected a resend to repeat the Mail already accepted under this id, saw a different body"
        );
        let whole = resend_refusal("m1", "sender, recipient");
        assert_eq!(conflict_reason("m1", &whole), whole);
        assert_eq!(copy_id("all", "b"), "all/b");
    }

    #[test]
    fn board_events_show_session_changes_only() {
        let shown = |ev: Event| board_event_of(&ev).map(line);
        assert_eq!(
            shown(Event::Session {
                session: "a".into(),
                host: "h".into(),
                at_ms: Some(4)
            })
            .as_deref(),
            Some(r#"{"event":"register","session":"a","host":"h","at_ms":4}"#)
        );
        assert_eq!(
            shown(Event::Unregister {
                session: "a".into(),
                at_ms: Some(5)
            })
            .as_deref(),
            Some(r#"{"event":"unregister","session":"a","at_ms":5}"#)
        );
        assert_eq!(
            shown(Event::Label {
                session: "a".into(),
                label: "x".into(),
                at_ms: Some(6)
            })
            .as_deref(),
            Some(r#"{"event":"relabel","session":"a","label":"x","at_ms":6}"#)
        );
        assert_eq!(
            shown(Event::Declared {
                session: "a".into(),
                declared: None,
                at_ms: Some(7)
            })
            .as_deref(),
            Some(r#"{"event":"declared","session":"a","at_ms":7}"#)
        );
        assert_eq!(
            shown(Event::Declared {
                session: "a".into(),
                declared: Some("d".into()),
                at_ms: None
            })
            .as_deref(),
            Some(r#"{"event":"declared","session":"a","declared":"d"}"#)
        );
        assert_eq!(shown(Event::Release { key: "k".into() }), None);
        assert_eq!(
            shown(Event::Takeover {
                session: "a".into(),
                old_host: "h".into(),
                host: "g".into(),
                at_ms: Some(1),
                registration: Some("r".into())
            }),
            None
        );
    }

    #[test]
    fn a_claim_change_commits_as_its_timeline_event() {
        let grant = Change::Grant(Held {
            owner: "a".into(),
            expires_ms: 99,
        });
        assert!(matches!(
            claim_timeline("k", &grant),
            Event::Claim { key, owner, expires_ms: 99 } if key == "k" && owner == "a"
        ));
        assert!(matches!(
            claim_timeline("k", &Change::Release),
            Event::Release { key } if key == "k"
        ));
    }

    #[test]
    fn a_sender_that_cannot_be_answered_does_not_keep_the_mail_from_its_recipient() {
        fn tcp_pair() -> (TcpStream, TcpStream) {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (b, _) = listener.accept().unwrap();
            (a, b)
        }
        let hangup = |s: &TcpStream| {
            let _ = s.shutdown(Shutdown::Both);
        };
        let (gone, _) = tcp_pair();
        let sender = net::queued(Arc::new(gone), hangup);
        net::close(&sender, false);
        let (near, far) = tcp_pair();
        let recipient = net::queued(Arc::new(near), hangup);
        let deliver = net::deliver_msg("m1", "a", "b", "one", net::DIRECTED, &obj(vec![]));
        let answered = answer_then_push(
            &sender,
            &obj(vec![("type", s("accepted"))]),
            vec![(Arc::clone(&recipient), deliver.clone())],
        );
        assert!(answered.is_err(), "the closed link takes no answer");
        let pushed = net::lines(Box::new(far)).next().unwrap().unwrap();
        assert_eq!(pushed, deliver);
    }
}

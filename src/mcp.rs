//! The MCP face: `roger mcp` is a session (it registers, writes each delivery
//! to its Store, then acks) that speaks the Model Context Protocol as
//! newline-delimited JSON-RPC on stdin and stdout. The session loop is the
//! adapter's (`session::serve`); this file adds only the protocol and the
//! tools, and gives the face no semantics of its own: every tool is one
//! request the socket protocol already has.

use crate::json::{self, obj, s, Value};
use crate::net;
use crate::session::{self, Face, McpHooks};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
// no-poll: the clock reads bound inbox/heartbeat waits, never a periodic scan.
use std::time::{Duration, Instant};

pub const VERSION: &str = "2026-07-28";
const LEGACY_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_INBOX_LIMIT: i64 = 50;
const MAX_INBOX_LIMIT: i64 = 200;
const DEFAULT_WAIT_SECONDS: i64 = 10;
const MAX_WAIT_SECONDS: i64 = 60;
const DEFAULT_CLAIM_TTL_SECONDS: i64 = 120;
const DEFAULT_BOARD_WINDOW_SECONDS: i64 = 600;
const DISCOVER_TTL_MS: i64 = 300_000;
const DEFAULT_POLL_MAX_EVENTS: usize = 100;
const MAX_POLL_MAX_EVENTS: usize = 500;
const DEFAULT_POLL_NEXT_MS: i64 = 5_000;
const DEFAULT_HEARTBEAT_MS: i64 = 30_000;
const MIN_HEARTBEAT_MS: i64 = 1_000;
const MAX_HEARTBEAT_MS: i64 = 60_000;
const MAX_RING_EVENTS: usize = 256;

// ---- configuration ----

/// Where this server's session lives, resolved from flags then environment.
#[derive(Debug, PartialEq)]
pub struct Config {
    pub socket: PathBuf,
    pub address: String,
    pub store: PathBuf,
    /// True when the Store path was not given, so it is this server's to remove.
    pub store_is_default: bool,
    pub label: Option<String>,
    pub declared: Option<String>,
    pub groups: bool,
}

/// A variable that is set and not empty.
fn set(env: &dyn Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    env(name).filter(|v| !v.is_empty())
}

/// Resolve the configuration. `env` reads one environment variable and
/// `fresh` makes a new Address; both are parameters so this stays pure.
pub fn resolve(
    args: &[String],
    env: &dyn Fn(&str) -> Option<String>,
    fresh: &dyn Fn() -> io::Result<String>,
) -> io::Result<Config> {
    let f = crate::parse_flags(
        "mcp",
        args,
        &[
            "--socket",
            "--store",
            "--id",
            "--id-env",
            "--label",
            "--declare",
        ],
        &["--groups"],
    )?;
    let bad = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
    let address = match f.get("--id") {
        Some(id) => id,
        None => match f.get("--id-env").and_then(|name| set(env, &name)) {
            Some(id) => id,
            None => match set(env, "ROGER_ADDRESS") {
                Some(id) => id,
                None => fresh()?,
            },
        },
    };
    let socket = match f.get("--socket").or_else(|| set(env, "ROGER_SOCKET")) {
        Some(p) => PathBuf::from(p),
        None => match set(env, "XDG_RUNTIME_DIR") {
            Some(dir) => PathBuf::from(dir).join("roger").join("daemon.sock"),
            None => {
                return Err(bad(
                    "expected --socket, ROGER_SOCKET, or XDG_RUNTIME_DIR, saw none of them".into(),
                ))
            }
        },
    };
    let (store, store_is_default) = match f.get("--store").or_else(|| set(env, "ROGER_STORE")) {
        Some(p) => (PathBuf::from(p), false),
        None => {
            let state = match set(env, "XDG_STATE_HOME").or_else(|| set(env, "LOCALAPPDATA")) {
                Some(dir) => PathBuf::from(dir),
                None => match set(env, "HOME").or_else(|| set(env, "USERPROFILE")) {
                    Some(home) => PathBuf::from(home).join(".local").join("state"),
                    None => return Err(bad(
                        "expected --store, ROGER_STORE, XDG_STATE_HOME, LOCALAPPDATA, HOME, or USERPROFILE, saw none of them"
                            .into(),
                    )),
                },
            };
            if address.is_empty()
                || address == "."
                || address == ".."
                || address.contains(['/', '\\', '\0'])
            {
                return Err(bad(format!(
                    "expected an Address usable as a file name for the default Store, saw {address:?}; pass --store"
                )));
            }
            (
                state
                    .join("roger")
                    .join("mcp")
                    .join(format!("{}.store", store_stem(&address))),
                true,
            )
        }
    };
    Ok(Config {
        socket,
        address,
        store,
        store_is_default,
        label: f.get("--label"),
        declared: f.get("--declare"),
        groups: f.has("--groups"),
    })
}

// ---- JSON-RPC ----

/// One input line, classified.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    /// A message with a method and no id: never answered.
    Notification { method: String, params: Value },
    /// A response from the client to something this server never asked.
    Ignore,
    /// Not a usable request: answer with this error.
    Bad {
        id: Value,
        code: i64,
        message: String,
    },
}

pub fn parse_message(line: &str) -> Parsed {
    let bad = |id: Value, code: i64, message: String| Parsed::Bad { id, code, message };
    let v = match json::parse(line) {
        Ok(v) => v,
        Err(e) => {
            return bad(
                Value::Null,
                -32700,
                format!("expected one JSON value per line, saw {e}"),
            )
        }
    };
    if !matches!(v, Value::Obj(_)) {
        return bad(
            Value::Null,
            -32600,
            format!(
                "expected a JSON-RPC request object, saw {}",
                type_name(Some(&v))
            ),
        );
    }
    let id = v.get("id").cloned();
    let Some(method) = v.get("method") else {
        return if id.is_some() && (v.get("result").is_some() || v.get("error").is_some()) {
            Parsed::Ignore
        } else {
            bad(
                id.unwrap_or(Value::Null),
                -32600,
                "expected a method, saw none".into(),
            )
        };
    };
    let Some(method) = method.as_str() else {
        return bad(
            id.unwrap_or(Value::Null),
            -32600,
            format!(
                "expected method to be a string, saw {}",
                type_name(Some(method))
            ),
        );
    };
    let params = v.get("params").cloned().unwrap_or(Value::Obj(Vec::new()));
    match id {
        None => Parsed::Notification {
            method: method.to_string(),
            params,
        },
        Some(id @ (Value::Int(_) | Value::Str(_))) => Parsed::Request {
            id,
            method: method.to_string(),
            params,
        },
        Some(other) => bad(
            Value::Null,
            -32600,
            format!(
                "expected id to be a string or integer, saw {}",
                type_name(Some(&other))
            ),
        ),
    }
}

struct RpcErr {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcErr {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    fn not_found(kind: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32023,
            message: message.into(),
            data: Some(obj(vec![("kind", s(kind))])),
        }
    }

    fn unsupported(feature: &str, value: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32026,
            message: message.into(),
            data: Some(obj(vec![("feature", s(feature)), ("value", s(value))])),
        }
    }
}

fn reply_ok(id: &Value, result: Value) -> Value {
    obj(vec![
        ("jsonrpc", s("2.0")),
        ("id", id.clone()),
        ("result", result),
    ])
}

fn reply_err(id: &Value, code: i64, message: &str) -> Value {
    reply_err_data(id, code, message, None)
}

fn reply_err_data(id: &Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut err_pairs = vec![("code", Value::Int(code)), ("message", s(message))];
    if let Some(d) = data {
        err_pairs.push(("data", d));
    }
    obj(vec![
        ("jsonrpc", s("2.0")),
        ("id", id.clone()),
        ("error", obj(err_pairs)),
    ])
}

fn server_info_meta() -> Value {
    obj(vec![(
        "io.modelcontextprotocol/serverInfo",
        obj(vec![
            ("name", s("roger")),
            ("version", s(env!("CARGO_PKG_VERSION"))),
        ]),
    )])
}

fn has_modern_meta(params: &Value) -> bool {
    params
        .get("_meta")
        .and_then(|m| m.get("io.modelcontextprotocol/protocolVersion"))
        .is_some()
}

fn validate_meta(params: &Value) -> Result<(), RpcErr> {
    if !matches!(params, Value::Obj(_)) {
        return Err(RpcErr::invalid_params(format!(
            "expected params to be an object, saw {}",
            type_name(Some(params))
        )));
    }
    let Some(meta) = params.get("_meta") else {
        return Err(RpcErr::invalid_params(
            "expected params._meta object carrying io.modelcontextprotocol/protocolVersion and io.modelcontextprotocol/clientCapabilities, saw nothing",
        ));
    };
    if !matches!(meta, Value::Obj(_)) {
        return Err(RpcErr::invalid_params(format!(
            "expected params._meta to be an object, saw {}",
            type_name(Some(meta))
        )));
    }
    let pv = match meta.get("io.modelcontextprotocol/protocolVersion") {
        Some(Value::Str(v)) => v.as_str(),
        other => {
            return Err(RpcErr::invalid_params(format!(
            "expected _meta[\"io.modelcontextprotocol/protocolVersion\"] to be a string, saw {}",
            type_name(other)
        )))
        }
    };
    match meta.get("io.modelcontextprotocol/clientCapabilities") {
        Some(Value::Obj(_)) => {}
        other => {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/clientCapabilities\"] to be an object, saw {}",
                type_name(other)
            )))
        }
    }
    if let Some(info) = meta.get("io.modelcontextprotocol/clientInfo") {
        if !matches!(info, Value::Obj(_)) {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/clientInfo\"] to be an object, saw {}",
                type_name(Some(info))
            )));
        }
    }
    if let Some(level) = meta.get("io.modelcontextprotocol/logLevel") {
        if !matches!(level, Value::Str(_)) {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/logLevel\"] to be a string, saw {}",
                type_name(Some(level))
            )));
        }
    }
    if pv != VERSION {
        return Err(RpcErr {
            code: -32022,
            message: "Unsupported protocol version".into(),
            data: Some(obj(vec![
                ("supported", Value::Arr(vec![s(VERSION)])),
                ("requested", s(pv)),
            ])),
        });
    }
    Ok(())
}

/// The legacy protocol version to answer an `initialize` with.
pub fn negotiate(params: &Value) -> &'static str {
    let asked = params.get("protocolVersion").and_then(|v| v.as_str());
    LEGACY_VERSIONS
        .iter()
        .find(|v| Some(**v) == asked)
        .unwrap_or(&"2025-06-18")
}

fn initialize_result(params: &Value) -> Value {
    obj(vec![
        ("protocolVersion", s(negotiate(params))),
        ("capabilities", obj(vec![("tools", obj(vec![]))])),
        (
            "serverInfo",
            obj(vec![
                ("name", s("roger")),
                ("version", s(env!("CARGO_PKG_VERSION"))),
            ]),
        ),
    ])
}

fn discover_result() -> Value {
    obj(vec![
        ("resultType", s("complete")),
        ("supportedVersions", Value::Arr(vec![s(VERSION)])),
        (
            "capabilities",
            obj(vec![
                ("tools", obj(vec![("listChanged", Value::Bool(false))])),
                (
                    "extensions",
                    obj(vec![(
                        "io.modelcontextprotocol/events",
                        obj(vec![("listChanged", Value::Bool(false))]),
                    )]),
                ),
            ]),
        ),
        ("_meta", server_info_meta()),
        ("ttlMs", Value::Int(DISCOVER_TTL_MS)),
        ("cacheScope", s("public")),
    ])
}

// ---- tools ----

/// (name, description, input schema)
const TOOLS: [(&str, &str, &str); 9] = [
    (
        "roger_send",
        "Send one Mail to an Address, or to every session with \"*\". Headers are opaque strings the sender attaches.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"to":{"type":"string"},"body":{"type":"string"},"mail":{"type":"string"},"headers":{"type":"object","additionalProperties":{"type":"string"}}},"required":["to","body"]}"#,
    ),
    (
        "roger_inbox",
        "Read the Mail in this session's Store after its read position, oldest first, and move the read position past what is returned. With none, wait up to wait_seconds for the next delivery.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":200},"wait_seconds":{"type":"integer","minimum":0,"maximum":60},"peek":{"type":"boolean"}}}"#,
    ),
    (
        "roger_board",
        "Read the Board: registered sessions and active claims. By default only sessions connected or seen within window_seconds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"all":{"type":"boolean"},"window_seconds":{"type":"integer","minimum":0}}}"#,
    ),
    (
        "roger_label",
        "Set this session's display label. Its Address does not change.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"label":{"type":"string"}},"required":["label"]}"#,
    ),
    (
        "roger_claim",
        "Claim a key for this session, or refresh the claim it holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["key"]}"#,
    ),
    (
        "roger_renew",
        "Renew a claim this session holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["key","ttl_seconds"]}"#,
    ),
    (
        "roger_release",
        "Release a claim this session holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"}},"required":["key"]}"#,
    ),
    (
        "roger_receipt",
        "Read one Mail's Receipt: its recipient, when it was Accepted, and the recipient's Record id once acked.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"mail":{"type":"string"}},"required":["mail"]}"#,
    ),
    (
        "roger_registered",
        "Say whether the coordinator confirmed this Register and no superseded notice arrived. Returns registered and sticky superseded (null or the notice). With wait_seconds, wait for confirmation or supersession. A superseded client should stop sending as this Address, tell its supervisor/user, and not automatically re-register; tools remain usable.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"wait_seconds":{"type":"integer","minimum":0,"maximum":60}}}"#,
    ),
];

const SEND_WITH_GROUPS: (&str, &str, &str) = (
    "roger_send",
    "Send one Mail to an Address, to every session with \"*\", or to a Group with `group`. Give either `to` or `group`, never both. Headers are opaque strings the sender attaches.",
    r#"{"type":"object","properties":{"session":{"type":"string"},"to":{"type":"string"},"group":{"type":"string"},"body":{"type":"string"},"mail":{"type":"string"},"headers":{"type":"object","additionalProperties":{"type":"string"}},"expected_article":{"type":"integer","minimum":0}},"required":["body"]}"#,
);

const GROUP_TOOLS: [(&str, &str, &str); 5] = [
    (
        "roger_subscribe",
        "Subscribe this session to a Group. Optional `after` sets the article position (>= 0); `latest` starts at the Group's current high-water mark.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string"},"after":{"type":"integer","minimum":0},"latest":{"type":"boolean"}},"required":["group"]}"#,
    ),
    (
        "roger_unsubscribe",
        "Unsubscribe this session from a Group while keeping its durable position.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string"}},"required":["group"]}"#,
    ),
    (
        "roger_groups",
        "List known Groups with high-water marks, subscriber counts, and this session's subscription state and position.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"prefix":{"type":"string"},"include_retired":{"type":"boolean"}},"additionalProperties":false}"#,
    ),
    (
        "roger_articles",
        "Read a Group's articles numbered above `after` (default 0), at most `limit` (1 to 500, default 100). A read only: it moves no position and writes no receipt.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string"},"after":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1,"maximum":500}},"required":["group"],"additionalProperties":false}"#,
    ),
    (
        "roger_group_policy",
        "Set a Group's retention and retirement. `keep_last` and `ttl_ms` take an integer >= 0, or null to clear; `retired` takes a boolean. Fields left out stay as they are. Returns the Group's policy after the change.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string"},"keep_last":{"type":["integer","null"],"minimum":0},"ttl_ms":{"type":["integer","null"],"minimum":0},"retired":{"type":"boolean"}},"required":["group"],"additionalProperties":false}"#,
    ),
];

fn tool_defs(groups: bool) -> Vec<(&'static str, &'static str, &'static str)> {
    if !groups {
        return TOOLS.to_vec();
    }
    let mut out = Vec::with_capacity(TOOLS.len() + GROUP_TOOLS.len());
    out.push(SEND_WITH_GROUPS);
    out.extend_from_slice(&TOOLS[1..]);
    out.extend_from_slice(&GROUP_TOOLS);
    out
}

fn tools_list(groups: bool, modern: bool) -> Value {
    let tools = tool_defs(groups)
        .into_iter()
        .map(|(name, description, schema)| {
            obj(vec![
                ("name", s(name)),
                ("description", s(description)),
                (
                    "inputSchema",
                    json::parse(schema).expect("a tool schema is valid JSON"),
                ),
            ])
        })
        .collect();
    let mut pairs = vec![("tools", Value::Arr(tools))];
    if modern {
        pairs.push(("resultType", s("complete")));
        pairs.push(("ttlMs", Value::Int(DISCOVER_TTL_MS)));
        pairs.push(("cacheScope", s("public")));
        pairs.push(("_meta", server_info_meta()));
    }
    obj(pairs)
}

fn type_name(v: Option<&Value>) -> &'static str {
    match v {
        None => "nothing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "a boolean",
        Some(Value::Int(_)) => "an integer",
        Some(Value::Float(_)) => "a number",
        Some(Value::Str(_)) => "a string",
        Some(Value::Arr(_)) => "an array",
        Some(Value::Obj(_)) => "an object",
    }
}

fn wrong(tool: &str, name: &str, expected: &str, seen: Option<&Value>) -> String {
    format!(
        "{tool}: expected `{name}` to be {expected}, saw {}",
        type_name(seen)
    )
}

fn need_str<'a>(tool: &str, args: &'a Value, name: &str) -> Result<&'a str, String> {
    match args.get(name) {
        Some(Value::Str(t)) => Ok(t),
        other => Err(wrong(tool, name, "a string", other)),
    }
}

fn opt_str<'a>(tool: &str, args: &'a Value, name: &str) -> Result<Option<&'a str>, String> {
    match args.get(name) {
        None => Ok(None),
        Some(Value::Str(t)) => Ok(Some(t)),
        other => Err(wrong(tool, name, "a string", other)),
    }
}

fn opt_bool(tool: &str, args: &Value, name: &str) -> Result<bool, String> {
    match args.get(name) {
        None => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        other => Err(wrong(tool, name, "a boolean", other)),
    }
}

/// An optional integer in `min..=max`; above `max` is held to `max` when
/// `clamp`, else refused. Below `min` is always refused.
fn opt_int(
    tool: &str,
    args: &Value,
    name: &str,
    default: i64,
    (min, max): (i64, i64),
    clamp: bool,
) -> Result<i64, String> {
    match args.get(name) {
        None => Ok(default),
        Some(Value::Int(n)) if *n >= min && (*n <= max || clamp) => Ok((*n).min(max)),
        Some(Value::Int(n)) => Err(format!(
            "{tool}: expected `{name}` to be an integer from {min} to {max}, saw {n}"
        )),
        other => Err(wrong(tool, name, "an integer", other)),
    }
}

fn seconds_to_ms(tool: &str, name: &str, secs: i64) -> Result<i64, String> {
    secs.checked_mul(1000)
        .ok_or_else(|| format!("{tool}: expected `{name}` to be a smaller number, saw {secs}"))
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum SubKey {
    Int(i64),
    Str(String),
}

impl SubKey {
    fn from_id(id: &Value) -> Option<Self> {
        match id {
            Value::Int(n) => Some(Self::Int(*n)),
            Value::Str(t) => Some(Self::Str(t.clone())),
            _ => None,
        }
    }
}

#[derive(Default)]
struct StreamCtrl {
    cancelled: bool,
    shutting_down: bool,
    terminated: Option<String>,
}

type SubHandle = Arc<(Mutex<StreamCtrl>, Condvar)>;

struct EventFeed<T> {
    items: Vec<(u64, T)>,
    next_seq: u64,
}

impl<T> Default for EventFeed<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            next_seq: 0,
        }
    }
}

impl<T> EventFeed<T> {
    fn push(&mut self, item: T) -> u64 {
        let seq = self.next_seq + 1;
        self.next_seq = seq;
        self.items.push((seq, item));
        if self.items.len() > MAX_RING_EVENTS {
            let drain = self.items.len() - MAX_RING_EVENTS;
            self.items.drain(..drain);
        }
        seq
    }
}

struct BoardNotice {
    at_ms: i64,
    data: Value,
}

struct RegNotice {
    at_ms: i64,
    data: Value,
}

/// Everything a tool call or event subscription needs.
struct Ctx {
    me: String,
    inbox: Inbox,
    client: Mutex<session::Client>,
    groups: bool,
    legacy: AtomicBool,
    /// True once the coordinator's `registered` has arrived.
    confirmed: Mutex<(bool, Option<Value>)>,
    confirmed_cv: Condvar,
    daemon: Mutex<Option<net::Writer>>,
    watching: AtomicBool,
    mail_seq: AtomicU64,
    mail_wait: (Mutex<()>, Condvar),
    board_feed: Mutex<EventFeed<BoardNotice>>,
    board_cv: Condvar,
    reg_feed: Mutex<EventFeed<RegNotice>>,
    reg_cv: Condvar,
    subs: Mutex<HashMap<SubKey, SubHandle>>,
    sub_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Ctx {
    fn new(socket: PathBuf, me: String, inbox: Inbox, groups: bool) -> Self {
        Self {
            me,
            inbox,
            client: Mutex::new(session::Client::new(socket)),
            groups,
            legacy: AtomicBool::new(false),
            confirmed: Mutex::new((false, None)),
            confirmed_cv: Condvar::new(),
            daemon: Mutex::new(None),
            watching: AtomicBool::new(false),
            mail_seq: AtomicU64::new(0),
            mail_wait: (Mutex::new(()), Condvar::new()),
            board_feed: Mutex::new(EventFeed::default()),
            board_cv: Condvar::new(),
            reg_feed: Mutex::new(EventFeed::default()),
            reg_cv: Condvar::new(),
            subs: Mutex::new(HashMap::new()),
            sub_handles: Mutex::new(Vec::new()),
        }
    }

    fn record_mail(&self) {
        let _g = self.mail_wait.0.lock().unwrap();
        self.mail_seq.fetch_add(1, Ordering::SeqCst);
        self.mail_wait.1.notify_all();
        self.inbox.notify();
    }

    fn record_reg(&self, event_name: &str, host: Option<&str>, at_ms: Option<i64>) {
        let ts = at_ms.unwrap_or_else(now_ms);
        let mut pairs = vec![("event", s(event_name)), ("session", s(&self.me))];
        if let Some(h) = host {
            pairs.push(("host", s(h)));
        }
        if let Some(at) = at_ms {
            pairs.push(("at_ms", Value::Int(at)));
        }
        let mut feed = self.reg_feed.lock().unwrap();
        feed.push(RegNotice {
            at_ms: ts,
            data: obj(pairs),
        });
        self.reg_cv.notify_all();
    }

    fn record_board(&self, mut raw: Value) {
        let ts = raw
            .get("at_ms")
            .and_then(Value::as_i64)
            .unwrap_or_else(now_ms);
        if let Value::Obj(ref mut pairs) = raw {
            pairs.retain(|(k, _)| k != "type");
        }
        let mut feed = self.board_feed.lock().unwrap();
        feed.push(BoardNotice {
            at_ms: ts,
            data: raw,
        });
        self.board_cv.notify_all();
    }

    fn ensure_watch(&self) -> Result<(), String> {
        if self.watching.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let guard = self.daemon.lock().unwrap();
        if let Some(w) = guard.as_ref() {
            if let Err(e) = net::send(w, &obj(vec![("type", s("watch")), ("req", Value::Int(1))])) {
                self.watching.store(false, Ordering::SeqCst);
                return Err(format!("failed to enable watch on host daemon: {e}"));
            }
        }
        Ok(())
    }
}

fn check_optional_session(ctx: &Ctx, args: &Value, tool: &str) -> Result<(), String> {
    match args.get("session") {
        None => Ok(()),
        Some(Value::Str(sess)) if sess == &ctx.me => Ok(()),
        Some(Value::Str(sess)) => Err(format!(
            "{tool}: unknown session handle {sess}; expected {}",
            ctx.me
        )),
        other => Err(wrong(tool, "session", "a string", other)),
    }
}

fn call_tool(ctx: &Ctx, params: &Value) -> Result<Value, String> {
    let name = match params.get("name") {
        Some(Value::Str(n)) => n.as_str(),
        other => return Err(wrong("tools/call", "name", "a string", other)),
    };
    let empty = Value::Obj(Vec::new());
    let args = match params.get("arguments") {
        None => &empty,
        Some(a @ Value::Obj(_)) => a,
        other => return Err(wrong(name, "arguments", "an object", other)),
    };
    let known: Vec<&str> = tool_defs(ctx.groups).into_iter().map(|t| t.0).collect();
    if !known.contains(&name) {
        return Err(format!(
            "expected a tool among {}, saw {name}",
            known.join(", ")
        ));
    }
    check_optional_session(ctx, args, name)?;
    let io = |e: io::Error| e.to_string();
    match name {
        "roger_send" => {
            if !ctx.groups {
                let to = need_str(name, args, "to")?;
                let body = need_str(name, args, "body")?;
                let mail = match opt_str(name, args, "mail")? {
                    Some(m) => m.to_string(),
                    None => session::gen_uuid().map_err(io)?,
                };
                let mail = {
                    let mut client = ctx.client.lock().unwrap();
                    session::send_mail_on(
                        &mut client,
                        &ctx.me,
                        to,
                        &mail,
                        body,
                        args.get("headers"),
                    )
                    .map_err(io)?
                };
                return Ok(obj(vec![("mail", s(&mail))]));
            }
            let to = opt_str(name, args, "to")?;
            let group = opt_str(name, args, "group")?;
            if to.is_some() && group.is_some() {
                return Err("roger_send: expected either `to` or `group`, saw both".into());
            }
            if to.is_none() && group.is_none() {
                return Err("roger_send: expected `to` or `group`, saw neither".into());
            }
            let body = need_str(name, args, "body")?;
            let mail = match opt_str(name, args, "mail")? {
                Some(m) => m.to_string(),
                None => session::gen_uuid().map_err(io)?,
            };
            let expected_article = match args.get("expected_article") {
                None => None,
                Some(_) => Some(opt_int(
                    name,
                    args,
                    "expected_article",
                    0,
                    (0, i64::MAX),
                    false,
                )?),
            };
            if let Some(to) = to {
                if expected_article.is_some() {
                    return Err("roger_send: expected_article requires `group`".into());
                }
                let mail = {
                    let mut client = ctx.client.lock().unwrap();
                    session::send_mail_on(
                        &mut client,
                        &ctx.me,
                        to,
                        &mail,
                        body,
                        args.get("headers"),
                    )
                    .map_err(io)?
                };
                Ok(obj(vec![("mail", s(&mail))]))
            } else {
                let group = group.expect("checked group.is_some()");
                let (mail, grp, article) = {
                    let mut client = ctx.client.lock().unwrap();
                    session::send_group_mail_on(
                        &mut client,
                        &ctx.me,
                        group,
                        &mail,
                        body,
                        args.get("headers"),
                        expected_article,
                    )
                    .map_err(io)?
                };
                Ok(obj(vec![
                    ("mail", s(&mail)),
                    ("group", s(&grp)),
                    ("article", Value::Int(article)),
                ]))
            }
        }
        "roger_subscribe" => {
            let group = need_str(name, args, "group")?;
            let after = match args.get("after") {
                None => None,
                Some(_) => Some(opt_int(name, args, "after", 0, (0, i64::MAX), false)?),
            };
            let latest = opt_bool(name, args, "latest")?;
            if after.is_some() && latest {
                return Err("roger_subscribe: expected `after` or `latest`, saw both".into());
            }
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::subscribe_req_on(&mut client, &ctx.me, group, after, latest).map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        "roger_unsubscribe" => {
            let group = need_str(name, args, "group")?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::unsubscribe_req_on(&mut client, &ctx.me, group).map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        "roger_groups" => {
            let prefix = opt_str(name, args, "prefix")?;
            let include_retired = opt_bool(name, args, "include_retired")?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::groups_req_filtered_on(&mut client, Some(&ctx.me), prefix, include_retired)
                    .map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        "roger_articles" => {
            let group = need_str(name, args, "group")?;
            let after = opt_int(name, args, "after", 0, (0, i64::MAX), false)?;
            let limit = opt_int(name, args, "limit", 100, (1, 500), false)?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(
                    &mut client,
                    obj(vec![
                        ("type", s("articles")),
                        ("group", s(group)),
                        ("after", Value::Int(after)),
                        ("limit", Value::Int(limit)),
                    ]),
                )
                .map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        "roger_group_policy" => {
            let group = need_str(name, args, "group")?;
            let mut ask = vec![("type", s("group_policy")), ("group", s(group))];
            for key in ["keep_last", "ttl_ms"] {
                match args.get(key) {
                    None => {}
                    Some(Value::Null) => ask.push((key, Value::Null)),
                    Some(Value::Int(n)) if *n >= 0 => ask.push((key, Value::Int(*n))),
                    Some(Value::Int(n)) => {
                        return Err(format!(
                        "{name}: expected `{key}` to be an integer of at least 0 or null, saw {n}"
                    ))
                    }
                    other => return Err(wrong(name, key, "an integer or null", other)),
                }
            }
            if args.get("retired").is_some() {
                ask.push(("retired", Value::Bool(opt_bool(name, args, "retired")?)));
            }
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(&mut client, obj(ask)).map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        "roger_inbox" => {
            let limit = opt_int(
                name,
                args,
                "limit",
                DEFAULT_INBOX_LIMIT,
                (1, MAX_INBOX_LIMIT),
                true,
            )?;
            let wait = opt_int(
                name,
                args,
                "wait_seconds",
                DEFAULT_WAIT_SECONDS,
                (0, MAX_WAIT_SECONDS),
                true,
            )?;
            let peek = opt_bool(name, args, "peek")?;
            let (mail, consumed) = ctx
                .inbox
                .read(limit as usize, Duration::from_secs(wait as u64), peek)
                .map_err(io)?;
            Ok(obj(vec![
                ("mail", Value::Arr(mail)),
                ("cursor", consumed.map_or(Value::Null, |c| s(&c))),
            ]))
        }
        "roger_registered" => {
            let wait = opt_int(
                name,
                args,
                "wait_seconds",
                DEFAULT_WAIT_SECONDS,
                (0, MAX_WAIT_SECONDS),
                true,
            )?;
            // no-poll: bounds the wait for the confirmation; its arrival ends the wait.
            let deadline = Instant::now() + Duration::from_secs(wait as u64);
            let mut held = ctx.confirmed.lock().unwrap();
            while !held.0 && held.1.is_none() {
                // no-poll: the same bound; elapsed time is read only to end the wait.
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    break;
                }
                // no-poll: waits on the confirmation the face signals, with the bound above.
                held = ctx.confirmed_cv.wait_timeout(held, left).unwrap().0;
            }
            Ok(obj(vec![
                ("registered", Value::Bool(held.0 && held.1.is_none())),
                ("superseded", held.1.clone().unwrap_or(Value::Null)),
            ]))
        }
        "roger_board" => {
            let all = opt_bool(name, args, "all")?;
            let window = opt_int(
                name,
                args,
                "window_seconds",
                DEFAULT_BOARD_WINDOW_SECONDS,
                (0, i64::MAX),
                false,
            )?;
            let window_ms = seconds_to_ms(name, "window_seconds", window)?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(
                    &mut client,
                    obj(vec![("type", s("board")), ("session", s(&ctx.me))]),
                )
                .map_err(io)?
            };
            let now = reply.get("now_ms").and_then(|v| v.as_i64()).unwrap_or(0);
            let sessions = match reply.get("sessions") {
                Some(Value::Arr(rows)) => rows
                    .iter()
                    .filter(|r| all || present(r, now, window_ms))
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            let claims = match reply.get("claims") {
                Some(Value::Arr(rows)) => rows.clone(),
                _ => Vec::new(),
            };
            Ok(obj(vec![
                ("me", s(&ctx.me)),
                ("sessions", Value::Arr(sessions)),
                ("claims", Value::Arr(claims)),
                ("now_ms", Value::Int(now)),
            ]))
        }
        "roger_label" => {
            let label = need_str(name, args, "label")?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(
                    &mut client,
                    obj(vec![
                        ("type", s("relabel")),
                        ("session", s(&ctx.me)),
                        ("label", s(label)),
                    ]),
                )
                .map_err(io)?
            };
            Ok(obj(vec![
                ("session", s(net::field(&reply, "session").map_err(io)?)),
                ("label", s(net::field(&reply, "label").map_err(io)?)),
            ]))
        }
        "roger_claim" | "roger_renew" | "roger_release" => {
            let key = need_str(name, args, "key")?;
            let (op, ttl_ms) = match name {
                "roger_release" => ("release", 0),
                "roger_renew" => {
                    if args.get("ttl_seconds").is_none() {
                        return Err(wrong(name, "ttl_seconds", "an integer", None));
                    }
                    let t = opt_int(name, args, "ttl_seconds", 0, (1, i64::MAX), false)?;
                    ("renew", seconds_to_ms(name, "ttl_seconds", t)?)
                }
                _ => {
                    let t = opt_int(
                        name,
                        args,
                        "ttl_seconds",
                        DEFAULT_CLAIM_TTL_SECONDS,
                        (1, i64::MAX),
                        false,
                    )?;
                    ("acquire", seconds_to_ms(name, "ttl_seconds", t)?)
                }
            };
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(
                    &mut client,
                    obj(vec![
                        ("type", s("claim")),
                        ("op", s(op)),
                        ("key", s(key)),
                        ("owner", s(&ctx.me)),
                        ("ttl_ms", Value::Int(ttl_ms)),
                    ]),
                )
                .map_err(io)?
            };
            Ok(without_envelope(reply))
        }
        _ => {
            let mail = need_str(name, args, "mail")?;
            let reply = {
                let mut client = ctx.client.lock().unwrap();
                session::request_on(
                    &mut client,
                    obj(vec![("type", s("receipt")), ("mail", s(mail))]),
                )
                .map_err(io)?
            };
            Ok(without_envelope(reply))
        }
    }
}

/// A reply from the host daemon minus the `type` and `req` that frame it.
fn without_envelope(reply: Value) -> Value {
    match reply {
        Value::Obj(mut pairs) => {
            pairs.retain(|(k, _)| k != "type" && k != "req");
            Value::Obj(pairs)
        }
        other => other,
    }
}

/// Whether a Board row is connected or was seen within `window_ms` of the
/// Board's own clock. A `last_seen_ms` of 0 means not seen since the
/// coordinator started. Nothing else in the row is read.
fn present(row: &Value, now: i64, window_ms: i64) -> bool {
    if row.get("connected") == Some(&Value::Bool(true)) {
        return true;
    }
    match row.get("last_seen_ms").and_then(|v| v.as_i64()) {
        Some(seen) => seen > 0 && now.saturating_sub(seen) <= window_ms,
        None => false,
    }
}

fn tool_result(text: &str, is_error: bool, modern: bool) -> Value {
    let mut pairs = vec![(
        "content",
        Value::Arr(vec![obj(vec![("type", s("text")), ("text", s(text))])]),
    )];
    if is_error {
        pairs.push(("isError", Value::Bool(true)));
    }
    if modern {
        pairs.push(("resultType", s("complete")));
        pairs.push(("_meta", server_info_meta()));
    }
    obj(pairs)
}

// ---- SEP-3415 events & subscriptions ----

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub use roger_wire::wire::format_iso8601_utc;

fn events_list_result() -> Value {
    let mail_event = obj(vec![
        ("name", s("roger.mail")),
        (
            "description",
            s("Fires when Mail is durably stored in the session Store."),
        ),
        (
            "delivery",
            Value::Arr(vec![s("push"), s("poll")]),
        ),
        (
            "inputSchema",
            json::parse(
                r#"{"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string"},"groupPrefix":{"type":"string","minLength":1},"kind":{"type":"string","enum":["directed","broadcast","group"]}},"additionalProperties":false}"#,
            )
            .unwrap(),
        ),
        (
            "payloadSchema",
            json::parse(
                r#"{"type":"object","properties":{"record":{"type":"string"},"mail":{"type":"string"},"from":{"type":"string"},"group":{"type":"string"},"article":{"type":"integer"},"body":{"type":"string"},"kind":{"type":"string","enum":["directed","broadcast","group"]},"headers":{"type":"object","additionalProperties":{"type":"string"}}},"required":["record","mail","from","body","kind","headers"]}"#,
            )
            .unwrap(),
        ),
    ]);
    let board_event = obj(vec![
        ("name", s("roger.board")),
        (
            "description",
            s("Fires on committed Board session and claim changes."),
        ),
        ("delivery", Value::Arr(vec![s("push")])),
        (
            "inputSchema",
            json::parse(
                r#"{"type":"object","properties":{"session":{"type":"string"}},"additionalProperties":false}"#,
            )
            .unwrap(),
        ),
        (
            "payloadSchema",
            json::parse(
                r#"{"type":"object","properties":{"event":{"type":"string"},"session":{"type":"string"},"host":{"type":"string"},"label":{"type":"string"},"declared":{"type":"string"},"key":{"type":"string"},"owner":{"type":"string"},"expires_ms":{"type":"integer"},"at_ms":{"type":"integer"}},"required":["event"]}"#,
            )
            .unwrap(),
        ),
    ]);
    let reg_event = obj(vec![
        ("name", s("roger.registration")),
        (
            "description",
            s("Fires when the session Register is confirmed or superseded."),
        ),
        ("delivery", Value::Arr(vec![s("push")])),
        (
            "inputSchema",
            json::parse(
                r#"{"type":"object","properties":{"session":{"type":"string"}},"additionalProperties":false}"#,
            )
            .unwrap(),
        ),
        (
            "payloadSchema",
            json::parse(
                r#"{"type":"object","properties":{"event":{"type":"string","enum":["registered","superseded"]},"session":{"type":"string"},"host":{"type":"string"},"at_ms":{"type":"integer"}},"required":["event","session"]}"#,
            )
            .unwrap(),
        ),
    ]);
    obj(vec![
        ("resultType", s("complete")),
        (
            "events",
            Value::Arr(vec![mail_event, board_event, reg_event]),
        ),
        ("ttlMs", Value::Int(DISCOVER_TTL_MS)),
        ("cacheScope", s("public")),
        ("_meta", server_info_meta()),
    ])
}

#[derive(Clone, Default)]
struct MailFilter {
    group: Option<String>,
    group_prefix: Option<String>,
    kind: Option<String>,
}

impl MailFilter {
    fn matches(&self, entry: &Value) -> bool {
        if let Some(g) = &self.group {
            if entry.get("group").and_then(Value::as_str) != Some(g.as_str()) {
                return false;
            }
        }
        if let Some(p) = &self.group_prefix {
            if !entry
                .get("group")
                .and_then(Value::as_str)
                .is_some_and(|g| g.starts_with(p.as_str()))
            {
                return false;
            }
        }
        if let Some(k) = &self.kind {
            if entry.get("kind").and_then(Value::as_str) != Some(k.as_str()) {
                return false;
            }
        }
        true
    }
}

fn shape_mail_entry(e: &Value) -> Value {
    let has_group = e.get("group").and_then(Value::as_str).is_some()
        && e.get("article").and_then(Value::as_i64).is_some();
    let keys: &[&str] = if has_group {
        &[
            "record", "mail", "from", "group", "article", "body", "kind", "headers",
        ]
    } else {
        &["record", "mail", "from", "body", "kind", "headers"]
    };
    Value::Obj(
        keys.iter()
            .filter_map(|k| e.get(k).map(|v| (k.to_string(), v.clone())))
            .collect(),
    )
}

fn parse_event_args(ctx: &Ctx, event_name: &str, params: &Value) -> Result<MailFilter, RpcErr> {
    let args = match params.get("arguments") {
        None | Some(Value::Null) => return Ok(MailFilter::default()),
        Some(Value::Obj(pairs)) => pairs,
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected arguments to be an object, saw {}",
                type_name(Some(other))
            )))
        }
    };
    let mut filter = MailFilter::default();
    for (k, v) in args {
        match (event_name, k.as_str()) {
            (_, "session") => {
                let Value::Str(sess) = v else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.session to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                if sess != &ctx.me {
                    return Err(RpcErr::not_found(
                        "session",
                        format!("unknown session handle {sess}"),
                    ));
                }
            }
            ("roger.mail", "group") => {
                let Value::Str(g) = v else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.group to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                filter.group = Some(g.clone());
            }
            ("roger.mail", "groupPrefix") => {
                let Value::Str(p) = v else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.groupPrefix to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                if p.is_empty() {
                    return Err(RpcErr::invalid_params(
                        "expected arguments.groupPrefix to be non-empty, saw empty string",
                    ));
                }
                filter.group_prefix = Some(p.clone());
            }
            ("roger.mail", "kind") => {
                let Value::Str(kind) = v else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.kind to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                if !matches!(kind.as_str(), "directed" | "broadcast" | "group") {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.kind to be directed, broadcast, or group, saw {kind}"
                    )));
                }
                filter.kind = Some(kind.clone());
            }
            (_, unknown) => {
                return Err(RpcErr::invalid_params(format!(
                    "unexpected argument {unknown} for event {event_name}"
                )));
            }
        }
    }
    Ok(filter)
}

fn parse_opt_cursor(params: &Value) -> Result<Option<String>, RpcErr> {
    match params.get("cursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Str(c)) => Ok(Some(c.clone())),
        Some(other) => Err(RpcErr::invalid_params(format!(
            "expected cursor to be a string or null, saw {}",
            type_name(Some(other))
        ))),
    }
}

fn parse_opt_max_age_ms(params: &Value) -> Result<Option<i64>, RpcErr> {
    match params.get("maxAgeMs") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Int(n)) if *n >= 0 => Ok(Some(*n)),
        Some(Value::Int(n)) => Err(RpcErr::invalid_params(format!(
            "expected maxAgeMs >= 0, saw {n}"
        ))),
        Some(other) => Err(RpcErr::invalid_params(format!(
            "expected maxAgeMs to be an integer, saw {}",
            type_name(Some(other))
        ))),
    }
}

fn parse_opt_max_events(params: &Value) -> Result<usize, RpcErr> {
    match params.get("maxEvents") {
        None | Some(Value::Null) => Ok(DEFAULT_POLL_MAX_EVENTS),
        Some(Value::Int(n)) if *n >= 1 => Ok((*n as usize).min(MAX_POLL_MAX_EVENTS)),
        Some(Value::Int(n)) => Err(RpcErr::invalid_params(format!(
            "expected maxEvents >= 1, saw {n}"
        ))),
        Some(other) => Err(RpcErr::invalid_params(format!(
            "expected maxEvents to be an integer, saw {}",
            type_name(Some(other))
        ))),
    }
}

fn parse_opt_heartbeat_ms(params: &Value) -> Result<i64, RpcErr> {
    match params.get("heartbeatMs") {
        None | Some(Value::Null) => Ok(DEFAULT_HEARTBEAT_MS),
        Some(Value::Int(n)) if *n >= 1 => Ok((*n).clamp(MIN_HEARTBEAT_MS, MAX_HEARTBEAT_MS)),
        Some(Value::Int(n)) => Err(RpcErr::invalid_params(format!(
            "expected heartbeatMs >= 1, saw {n}"
        ))),
        Some(other) => Err(RpcErr::invalid_params(format!(
            "expected heartbeatMs to be an integer, saw {}",
            type_name(Some(other))
        ))),
    }
}

fn record_of(e: &Value) -> &str {
    e.get("record").and_then(Value::as_str).unwrap_or("")
}

fn tail_cursor(all: &[Value]) -> String {
    all.last()
        .map(|e| record_of(e).to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "r-0".to_string())
}

/// Resolve `(start_index, truncated, reset_cursor)` for `roger.mail` replay.
fn resolve_mail_window(
    all: &[Value],
    record_index: &HashMap<String, usize>,
    cursor: Option<&str>,
    max_age_ms: Option<i64>,
    now: i64,
) -> (usize, bool, String) {
    let tail = tail_cursor(all);
    let Some(cur) = cursor else {
        return (all.len(), false, tail);
    };
    let base_start = if cur == "r-0" {
        0
    } else if let Some(&idx) = record_index.get(cur) {
        idx + 1
    } else {
        return (all.len(), true, tail);
    };
    let mut start = base_start;
    let mut truncated = false;
    if let Some(age) = max_age_ms {
        let floor = now.saturating_sub(age);
        while start < all.len() {
            let ts = all[start]
                .get("stored_at_ms")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            if ts >= floor {
                break;
            }
            start += 1;
            truncated = true;
        }
    }
    let start_cursor = if start == 0 {
        "r-0".to_string()
    } else {
        record_of(&all[start - 1]).to_string()
    };
    (start, truncated, start_cursor)
}

fn mail_occurrence(e: &Value, include_cursor: bool) -> Value {
    let ts_ms = e.get("stored_at_ms").and_then(Value::as_i64).unwrap_or(0);
    let event_id = e
        .get("mail")
        .and_then(Value::as_str)
        .unwrap_or_else(|| record_of(e));
    let mut pairs = vec![
        ("eventId", s(event_id)),
        ("name", s("roger.mail")),
        ("timestamp", s(&format_iso8601_utc(ts_ms))),
        ("data", shape_mail_entry(e)),
    ];
    if include_cursor {
        pairs.push(("cursor", s(record_of(e))));
    }
    obj(pairs)
}

fn events_poll(ctx: &Ctx, params: &Value) -> Result<Value, RpcErr> {
    let name = match params.get("name") {
        Some(Value::Str(n)) => n.as_str(),
        other => {
            return Err(RpcErr::invalid_params(format!(
                "expected name to be a string, saw {}",
                type_name(other)
            )))
        }
    };
    match name {
        "roger.mail" => {}
        "roger.board" | "roger.registration" => {
            let _ = parse_event_args(ctx, name, params)?;
            return Err(RpcErr::unsupported(
                "deliveryMode",
                "poll",
                format!("event {name} supports only push delivery"),
            ));
        }
        other => return Err(RpcErr::not_found("event", format!("unknown event {other}"))),
    }
    let filter = parse_event_args(ctx, name, params)?;
    let cursor = parse_opt_cursor(params)?;
    let max_age_ms = parse_opt_max_age_ms(params)?;
    let max_events = parse_opt_max_events(params)?;

    let mut cache = ctx.inbox.cache.lock().unwrap();
    cache.refresh().map_err(|e| RpcErr {
        code: -32603,
        message: e.to_string(),
        data: None,
    })?;
    let now = now_ms();
    let (start, truncated, start_cursor) = cache.resolve_window(cursor.as_deref(), max_age_ms, now);

    let mut matched = Vec::new();
    let mut last_idx = None;
    let mut has_more = false;
    for (offset, entry) in cache.entries[start..].iter().enumerate() {
        if !filter.matches(entry) {
            continue;
        }
        if matched.len() == max_events {
            has_more = true;
            break;
        }
        matched.push(mail_occurrence(entry, false));
        last_idx = Some(start + offset);
    }
    let next_cursor = if has_more {
        last_idx
            .map(|i| record_of(&cache.entries[i]).to_string())
            .unwrap_or(start_cursor)
    } else if start < cache.entries.len() {
        tail_cursor(&cache.entries)
    } else {
        start_cursor
    };
    Ok(obj(vec![
        ("resultType", s("complete")),
        ("events", Value::Arr(matched)),
        ("cursor", s(&next_cursor)),
        ("truncated", Value::Bool(truncated)),
        ("hasMore", Value::Bool(has_more)),
        ("nextPollMs", Value::Int(DEFAULT_POLL_NEXT_MS)),
        ("_meta", server_info_meta()),
    ]))
}

fn sub_meta(id: &Value) -> Value {
    obj(vec![("io.modelcontextprotocol/subscriptionId", id.clone())])
}

fn send_event_notif(
    out: &net::Writer,
    method: &str,
    mut pairs: Vec<(&'static str, Value)>,
    id: &Value,
) -> io::Result<()> {
    pairs.push(("_meta", sub_meta(id)));
    net::send(
        out,
        &obj(vec![
            ("jsonrpc", s("2.0")),
            ("method", s(method)),
            ("params", obj(pairs)),
        ]),
    )
}

fn finish_sub_result(out: &net::Writer, id: &Value) {
    let _ = net::send(
        out,
        &reply_ok(
            id,
            obj(vec![
                ("resultType", s("complete")),
                (
                    "_meta",
                    obj(vec![
                        ("io.modelcontextprotocol/subscriptionId", id.clone()),
                        (
                            "io.modelcontextprotocol/serverInfo",
                            obj(vec![
                                ("name", s("roger")),
                                ("version", s(env!("CARGO_PKG_VERSION"))),
                            ]),
                        ),
                    ]),
                ),
            ]),
        ),
    );
}

fn finish_sub_terminated(out: &net::Writer, id: &Value, reason: &str) {
    let _ = send_event_notif(
        out,
        "notifications/events/terminated",
        vec![(
            "error",
            obj(vec![
                ("code", Value::Int(-32023)),
                ("message", s("NotFound")),
                (
                    "data",
                    obj(vec![("kind", s("session")), ("reason", s(reason))]),
                ),
            ]),
        )],
        id,
    );
}

fn register_sub(ctx: &Ctx, id: &Value) -> Result<(SubKey, SubHandle), RpcErr> {
    let Some(key) = SubKey::from_id(id) else {
        return Err(RpcErr::invalid_params("expected integer or string id"));
    };
    let ctrl = Arc::new((Mutex::new(StreamCtrl::default()), Condvar::new()));
    let mut map = ctx.subs.lock().unwrap();
    if map.contains_key(&key) {
        return Err(RpcErr::invalid_params("duplicate active subscription id"));
    }
    map.insert(key.clone(), Arc::clone(&ctrl));
    Ok((key, ctrl))
}

fn unregister_sub(ctx: &Ctx, key: &SubKey) {
    ctx.subs.lock().unwrap().remove(key);
}

fn start_subscriptions_listen(
    ctx: &Arc<Ctx>,
    out: &net::Writer,
    id: Value,
    params: &Value,
) -> Result<(), RpcErr> {
    match params.get("notifications") {
        None | Some(Value::Obj(_)) => {}
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected notifications to be an object, saw {}",
                type_name(Some(other))
            )))
        }
    }
    let (key, ctrl) = register_sub(ctx, &id)?;
    let _ = send_event_notif(
        out,
        "notifications/subscriptions/acknowledged",
        vec![("notifications", obj(vec![]))],
        &id,
    );
    let ctx_c = Arc::clone(ctx);
    let out_c = out.clone();
    let handle = std::thread::spawn(move || {
        let (m, cv) = &*ctrl;
        let mut g = m.lock().unwrap();
        while !g.cancelled && !g.shutting_down && g.terminated.is_none() {
            g = cv.wait(g).unwrap();
        }
        let graceful = !g.cancelled && g.shutting_down;
        drop(g);
        unregister_sub(&ctx_c, &key);
        if graceful {
            finish_sub_result(&out_c, &id);
        }
    });
    let mut handles = ctx.sub_handles.lock().unwrap();
    handles.retain(|h| !h.is_finished());
    handles.push(handle);
    Ok(())
}

fn start_events_stream(
    ctx: &Arc<Ctx>,
    out: &net::Writer,
    id: Value,
    params: &Value,
) -> Result<(), RpcErr> {
    let name = match params.get("name") {
        Some(Value::Str(n)) => n.clone(),
        other => {
            return Err(RpcErr::invalid_params(format!(
                "expected name to be a string, saw {}",
                type_name(other)
            )))
        }
    };
    if !matches!(
        name.as_str(),
        "roger.mail" | "roger.board" | "roger.registration"
    ) {
        return Err(RpcErr::not_found("event", format!("unknown event {name}")));
    }
    let filter = parse_event_args(ctx, &name, params)?;
    let cursor = parse_opt_cursor(params)?;
    let max_age_ms = parse_opt_max_age_ms(params)?;
    let heartbeat_ms = parse_opt_heartbeat_ms(params)?;
    if name == "roger.board" {
        if let Err(reason) = ctx.ensure_watch() {
            return Err(RpcErr {
                code: -32603,
                message: reason,
                data: None,
            });
        }
    }
    let (key, ctrl) = register_sub(ctx, &id)?;
    let ctx_c = Arc::clone(ctx);
    let out_c = out.clone();
    let handle = std::thread::spawn(move || {
        match name.as_str() {
            "roger.mail" => run_mail_stream(
                &ctx_c,
                &out_c,
                &id,
                &ctrl,
                &filter,
                cursor.as_deref(),
                max_age_ms,
                heartbeat_ms,
            ),
            "roger.board" => run_board_stream(&ctx_c, &out_c, &id, &ctrl, heartbeat_ms),
            _ => run_reg_stream(&ctx_c, &out_c, &id, &ctrl, heartbeat_ms),
        }
        unregister_sub(&ctx_c, &key);
    });
    let mut handles = ctx.sub_handles.lock().unwrap();
    handles.retain(|h| !h.is_finished());
    handles.push(handle);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_mail_stream(
    ctx: &Ctx,
    out: &net::Writer,
    id: &Value,
    ctrl: &Arc<(Mutex<StreamCtrl>, Condvar)>,
    filter: &MailFilter,
    cursor: Option<&str>,
    max_age_ms: Option<i64>,
    heartbeat_ms: i64,
) {
    let mut seen_seq = ctx.mail_seq.load(Ordering::SeqCst);
    let (start_cursor, truncated, mut latest_cursor, initial_entries, mut seen_gen, mut next_idx) = {
        let mut cache = ctx.inbox.cache.lock().unwrap();
        if let Err(e) = cache.refresh() {
            finish_sub_terminated(out, id, &e.to_string());
            return;
        }
        let (start, truncated, start_cursor) = cache.resolve_window(cursor, max_age_ms, now_ms());
        let mut latest_cursor = tail_cursor(&cache.entries);
        if start >= cache.entries.len() {
            latest_cursor = start_cursor.clone();
        }
        let initial = cache.entries[start..].to_vec();
        (
            start_cursor,
            truncated,
            latest_cursor,
            initial,
            cache.generation,
            cache.entries.len(),
        )
    };
    if send_event_notif(
        out,
        "notifications/events/active",
        vec![
            ("cursor", s(&start_cursor)),
            ("truncated", Value::Bool(truncated)),
            ("heartbeatMs", Value::Int(heartbeat_ms)),
        ],
        id,
    )
    .is_err()
    {
        return;
    }

    for entry in &initial_entries {
        let rec = record_of(entry).to_string();
        if rec.is_empty() {
            continue;
        }
        latest_cursor = rec;
        if filter.matches(entry) {
            let mut pairs = match mail_occurrence(entry, true) {
                Value::Obj(p) => p,
                _ => Vec::new(),
            };
            pairs.push(("_meta".to_string(), sub_meta(id)));
            if net::send(
                out,
                &obj(vec![
                    ("jsonrpc", s("2.0")),
                    ("method", s("notifications/events/event")),
                    ("params", Value::Obj(pairs)),
                ]),
            )
            .is_err()
            {
                return;
            }
        }
    }

    let hb_dur = Duration::from_millis(heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound when no Mail arrives.
    let mut next_hb = Instant::now() + hb_dur;

    loop {
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled {
                return;
            }
            if let Some(reason) = &g.terminated {
                finish_sub_terminated(out, id, reason);
                return;
            }
            if g.shutting_down {
                finish_sub_result(out, id);
                return;
            }
        }
        let cur_seq = ctx.mail_seq.load(Ordering::SeqCst);
        if cur_seq != seen_seq {
            seen_seq = cur_seq;
            let delta_entries = {
                let mut cache = ctx.inbox.cache.lock().unwrap();
                if cache.refresh().is_ok() {
                    if cache.generation != seen_gen {
                        seen_gen = cache.generation;
                        next_idx = cache.entries.len();
                        Some(cache.entries.clone())
                    } else if next_idx < cache.entries.len() {
                        let slice = cache.entries[next_idx..].to_vec();
                        next_idx = cache.entries.len();
                        Some(slice)
                    } else {
                        Some(Vec::new())
                    }
                } else {
                    None
                }
            };
            if let Some(now_all) = delta_entries {
                let mut sent_any = false;
                for entry in now_all {
                    let rec = record_of(&entry).to_string();
                    if rec.is_empty() {
                        continue;
                    }
                    latest_cursor = rec;
                    if filter.matches(&entry) {
                        let mut pairs = match mail_occurrence(&entry, true) {
                            Value::Obj(p) => p,
                            _ => Vec::new(),
                        };
                        pairs.push(("_meta".to_string(), sub_meta(id)));
                        if net::send(
                            out,
                            &obj(vec![
                                ("jsonrpc", s("2.0")),
                                ("method", s("notifications/events/event")),
                                ("params", Value::Obj(pairs)),
                            ]),
                        )
                        .is_err()
                        {
                            return;
                        }
                        sent_any = true;
                    }
                }
                if sent_any {
                    // no-poll: reset heartbeat bound after sending an event.
                    next_hb = Instant::now() + hb_dur;
                }
            }
            continue;
        }
        // no-poll: SEP-3415 stream heartbeat bound.
        let now_inst = Instant::now();
        if now_inst >= next_hb {
            if send_event_notif(
                out,
                "notifications/events/heartbeat",
                vec![("cursor", s(&latest_cursor))],
                id,
            )
            .is_err()
            {
                return;
            }
            // no-poll: schedule next SEP-3415 heartbeat.
            next_hb = Instant::now() + hb_dur;
            continue;
        }
        let wait_for = next_hb.saturating_duration_since(now_inst);
        let guard = ctx.mail_wait.0.lock().unwrap();
        if ctx.mail_seq.load(Ordering::SeqCst) != seen_seq {
            continue;
        }
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled || g.shutting_down || g.terminated.is_some() {
                continue;
            }
        }
        // no-poll: waits for Store commit or SEP-3415 heartbeat bound.
        let _ = ctx.mail_wait.1.wait_timeout(guard, wait_for);
    }
}

fn run_board_stream(
    ctx: &Ctx,
    out: &net::Writer,
    id: &Value,
    ctrl: &Arc<(Mutex<StreamCtrl>, Condvar)>,
    heartbeat_ms: i64,
) {
    let mut seq_cursor = ctx.board_feed.lock().unwrap().next_seq;
    let mut evt_counter = 0u64;
    if send_event_notif(
        out,
        "notifications/events/active",
        vec![
            ("cursor", Value::Null),
            ("truncated", Value::Bool(false)),
            ("heartbeatMs", Value::Int(heartbeat_ms)),
        ],
        id,
    )
    .is_err()
    {
        return;
    }
    let hb_dur = Duration::from_millis(heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound.
    let mut next_hb = Instant::now() + hb_dur;
    loop {
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled {
                return;
            }
            if let Some(reason) = &g.terminated {
                finish_sub_terminated(out, id, reason);
                return;
            }
            if g.shutting_down {
                finish_sub_result(out, id);
                return;
            }
        }
        let batch: Vec<(u64, i64, Value)> = {
            let feed = ctx.board_feed.lock().unwrap();
            feed.items
                .iter()
                .filter(|(s, _)| *s > seq_cursor)
                .map(|(s, n)| (*s, n.at_ms, n.data.clone()))
                .collect()
        };
        if !batch.is_empty() {
            for (seq, at_ms, data) in batch {
                seq_cursor = seq;
                evt_counter += 1;
                if send_event_notif(
                    out,
                    "notifications/events/event",
                    vec![
                        ("eventId", s(&format!("board-{seq}-{evt_counter}"))),
                        ("name", s("roger.board")),
                        ("timestamp", s(&format_iso8601_utc(at_ms))),
                        ("data", data),
                        ("cursor", Value::Null),
                    ],
                    id,
                )
                .is_err()
                {
                    return;
                }
            }
            // no-poll: reset heartbeat bound after sending an event.
            next_hb = Instant::now() + hb_dur;
            continue;
        }
        // no-poll: SEP-3415 stream heartbeat bound.
        let now_inst = Instant::now();
        if now_inst >= next_hb {
            if send_event_notif(
                out,
                "notifications/events/heartbeat",
                vec![("cursor", Value::Null)],
                id,
            )
            .is_err()
            {
                return;
            }
            // no-poll: schedule next SEP-3415 heartbeat.
            next_hb = Instant::now() + hb_dur;
            continue;
        }
        let wait_for = next_hb.saturating_duration_since(now_inst);
        let guard = ctx.board_feed.lock().unwrap();
        if guard.next_seq != seq_cursor {
            continue;
        }
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled || g.shutting_down || g.terminated.is_some() {
                continue;
            }
        }
        // no-poll: waits for board_event or SEP-3415 heartbeat bound.
        let _ = ctx.board_cv.wait_timeout(guard, wait_for);
    }
}

fn run_reg_stream(
    ctx: &Ctx,
    out: &net::Writer,
    id: &Value,
    ctrl: &Arc<(Mutex<StreamCtrl>, Condvar)>,
    heartbeat_ms: i64,
) {
    let mut seq_cursor = 0u64;
    if send_event_notif(
        out,
        "notifications/events/active",
        vec![
            ("cursor", Value::Null),
            ("truncated", Value::Bool(false)),
            ("heartbeatMs", Value::Int(heartbeat_ms)),
        ],
        id,
    )
    .is_err()
    {
        return;
    }
    let hb_dur = Duration::from_millis(heartbeat_ms as u64);
    // no-poll: SEP-3415 stream heartbeat bound.
    let mut next_hb = Instant::now() + hb_dur;
    loop {
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled {
                return;
            }
            if let Some(reason) = &g.terminated {
                finish_sub_terminated(out, id, reason);
                return;
            }
            if g.shutting_down {
                finish_sub_result(out, id);
                return;
            }
        }
        let batch: Vec<(u64, i64, Value)> = {
            let feed = ctx.reg_feed.lock().unwrap();
            feed.items
                .iter()
                .filter(|(s, _)| *s > seq_cursor)
                .map(|(s, n)| (*s, n.at_ms, n.data.clone()))
                .collect()
        };
        if !batch.is_empty() {
            for (seq, at_ms, data) in batch {
                seq_cursor = seq;
                if send_event_notif(
                    out,
                    "notifications/events/event",
                    vec![
                        ("eventId", s(&format!("reg-{seq}"))),
                        ("name", s("roger.registration")),
                        ("timestamp", s(&format_iso8601_utc(at_ms))),
                        ("data", data),
                        ("cursor", Value::Null),
                    ],
                    id,
                )
                .is_err()
                {
                    return;
                }
            }
            // no-poll: reset heartbeat bound after sending an event.
            next_hb = Instant::now() + hb_dur;
            continue;
        }
        // no-poll: SEP-3415 stream heartbeat bound.
        let now_inst = Instant::now();
        if now_inst >= next_hb {
            if send_event_notif(
                out,
                "notifications/events/heartbeat",
                vec![("cursor", Value::Null)],
                id,
            )
            .is_err()
            {
                return;
            }
            // no-poll: schedule next SEP-3415 heartbeat.
            next_hb = Instant::now() + hb_dur;
            continue;
        }
        let wait_for = next_hb.saturating_duration_since(now_inst);
        let guard = ctx.reg_feed.lock().unwrap();
        if guard.next_seq != seq_cursor {
            continue;
        }
        {
            let g = ctrl.0.lock().unwrap();
            if g.cancelled || g.shutting_down || g.terminated.is_some() {
                continue;
            }
        }
        // no-poll: waits for registration transition or SEP-3415 heartbeat bound.
        let _ = ctx.reg_cv.wait_timeout(guard, wait_for);
    }
}

fn cancel_sub(ctx: &Ctx, request_id: &Value) {
    let Some(key) = SubKey::from_id(request_id) else {
        return;
    };
    let ctrl = ctx.subs.lock().unwrap().get(&key).cloned();
    if let Some(ctrl) = ctrl {
        ctrl.0.lock().unwrap().cancelled = true;
        ctrl.1.notify_all();
        ctx.mail_wait.1.notify_all();
        ctx.board_cv.notify_all();
        ctx.reg_cv.notify_all();
    }
}

fn close_all_subs(ctx: &Ctx, terminated_reason: Option<&str>) {
    let all: Vec<_> = ctx.subs.lock().unwrap().values().cloned().collect();
    for ctrl in all {
        let mut g = ctrl.0.lock().unwrap();
        if let Some(r) = terminated_reason {
            g.terminated = Some(r.to_string());
        } else {
            g.shutting_down = true;
        }
        ctrl.1.notify_all();
    }
    ctx.mail_wait.1.notify_all();
    ctx.board_cv.notify_all();
    ctx.reg_cv.notify_all();
    let handles = std::mem::take(&mut *ctx.sub_handles.lock().unwrap());
    for h in handles {
        let _ = h.join();
    }
}

// ---- the inbox ----

/// The reader's side of the Store: the entries after a read position kept in a file
/// beside it. A delivery's Store write makes `notify` fire, which is what
/// ends a wait for the next entry.
struct StoreCache {
    tail: crate::durlog::TailReader,
    entries: Vec<Value>,
    record_index: HashMap<String, usize>,
    generation: u64,
    #[cfg(test)]
    parses: usize,
}

impl StoreCache {
    fn open(store: &Path) -> io::Result<Self> {
        let mut cache = Self {
            tail: crate::durlog::TailReader::new(store.to_path_buf()),
            entries: Vec::new(),
            record_index: HashMap::new(),
            generation: 0,
            #[cfg(test)]
            parses: 0,
        };
        cache.refresh()?;
        Ok(cache)
    }

    fn refresh(&mut self) -> io::Result<crate::durlog::TailDelta> {
        let entries = &mut self.entries;
        let record_index = &mut self.record_index;
        let generation = &mut self.generation;
        #[cfg(test)]
        let parses = &mut self.parses;
        self.tail.refresh(|ev| match ev {
            crate::durlog::TailEvent::Reset => {
                entries.clear();
                record_index.clear();
                *generation += 1;
            }
            crate::durlog::TailEvent::Line(line) => {
                #[cfg(test)]
                {
                    *parses += 1;
                }
                if let Ok(v) = json::parse(line) {
                    if let Some(rec) = v.get("record").and_then(Value::as_str) {
                        if !record_index.contains_key(rec) {
                            let idx = entries.len();
                            record_index.insert(rec.to_string(), idx);
                            entries.push(v);
                        }
                    }
                }
            }
        })
    }

    fn resolve_window(
        &self,
        cursor: Option<&str>,
        max_age_ms: Option<i64>,
        now: i64,
    ) -> (usize, bool, String) {
        resolve_mail_window(&self.entries, &self.record_index, cursor, max_age_ms, now)
    }
}

struct Inbox {
    pos_file: PathBuf,
    cache: Mutex<StoreCache>,
    /// The Record id the reader has consumed through. Held across a read and
    /// across the wait, so an entry is returned to one caller only.
    consumed: Mutex<Option<String>>,
    arrived: Condvar,
}

impl Inbox {
    fn open(store: &Path) -> io::Result<Inbox> {
        let pos_file = position_path(store);
        let consumed = match std::fs::read_to_string(&pos_file) {
            Ok(text) => Some(text.trim().to_string()).filter(|c| !c.is_empty()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("expected to read {}, saw {e}", pos_file.display()),
                ))
            }
        };
        Ok(Inbox {
            pos_file,
            cache: Mutex::new(StoreCache::open(store)?),
            consumed: Mutex::new(consumed),
            arrived: Condvar::new(),
        })
    }

    /// Called once a delivery is in the Store. Taking the lock first means a
    /// reader is either still before its read (and will see the entry) or
    /// already waiting (and is signalled).
    fn notify(&self) {
        let _held = self.consumed.lock().unwrap();
        self.arrived.notify_all();
    }

    fn is_empty(&self) -> io::Result<bool> {
        let mut cache = self.cache.lock().unwrap();
        cache.refresh()?;
        Ok(cache.entries.is_empty())
    }

    /// Up to `limit` entries after the read position. With none and `wait` above
    /// zero, waits up to `wait` for the next Store write. Unless `peek`, the
    /// read position moves past what is returned and is durable before this returns.
    fn read(
        &self,
        limit: usize,
        wait: Duration,
        peek: bool,
    ) -> io::Result<(Vec<Value>, Option<String>)> {
        // no-poll: bounds how long a reader waits for a write; the write ends the wait.
        let deadline = Instant::now() + wait;
        let mut consumed = self.consumed.lock().unwrap();
        loop {
            let page = {
                let mut cache = self.cache.lock().unwrap();
                cache.refresh()?;
                after_indexed(
                    &cache.entries,
                    &cache.record_index,
                    consumed.as_deref(),
                    limit,
                )
            };
            if !page.is_empty() {
                let last = page
                    .last()
                    .and_then(|e| e.get("record"))
                    .and_then(|r| r.as_str())
                    .map(str::to_string);
                if !peek {
                    if let Some(last) = &last {
                        save_position(&self.pos_file, last)?;
                        *consumed = Some(last.clone());
                    }
                }
                return Ok((page, last));
            }
            // no-poll: the same bound; elapsed time is read only to end the wait.
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok((Vec::new(), None));
            }
            // no-poll: waits on the condition a Store write signals, with the bound above.
            consumed = self.arrived.wait_timeout(consumed, left).unwrap().0;
        }
    }
}

/// Platform-safe file stem for the default MCP Store. On Unix the Address is
/// used directly; on Windows characters illegal in NTFS file names (such as `:`
/// in composite Addresses) are percent-encoded byte-for-byte without parsing
/// the Address.
fn store_stem(address: &str) -> String {
    #[cfg(not(windows))]
    {
        address.to_string()
    }
    #[cfg(windows)]
    {
        encode_ntfs_stem(address)
    }
}

#[cfg(any(windows, test))]
fn encode_ntfs_stem(address: &str) -> String {
    let mut out = String::with_capacity(address.len());
    for b in address.bytes() {
        if b < 0x20 || matches!(b, b'%' | b':' | b'*' | b'?' | b'"' | b'<' | b'>' | b'|') {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{b:02X}");
        } else {
            out.push(b as char);
        }
    }
    out
}

use roger_wire::wire::{cursor_path as position_path, save_cursor as save_position};

fn after_indexed(
    all: &[Value],
    record_index: &HashMap<String, usize>,
    consumed: Option<&str>,
    limit: usize,
) -> Vec<Value> {
    let start = consumed
        .and_then(|c| record_index.get(c).copied())
        .map_or(0, |i| i + 1);
    all[start..]
        .iter()
        .take(limit)
        .map(shape_mail_entry)
        .collect()
}

#[cfg(test)]
fn after(all: &[Value], consumed: Option<&str>, limit: usize) -> Vec<Value> {
    let mut index = HashMap::new();
    for (i, e) in all.iter().enumerate() {
        let rec = record_of(e);
        if !rec.is_empty() {
            index.entry(rec.to_string()).or_insert(i);
        }
    }
    after_indexed(all, &index, consumed, limit)
}

// ---- the process ----

enum Event {
    Registered(net::Writer),
    Ended(io::Result<()>),
}

pub fn run(args: &[String]) -> io::Result<()> {
    let cfg = resolve(&args[2..], &|n| std::env::var(n).ok(), &session::gen_uuid)?;
    if !cfg.socket.to_str().is_some_and(|s| s.starts_with("tcp://")) && !cfg.socket.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "expected a host daemon socket at {}, saw none",
                cfg.socket.display()
            ),
        ));
    }
    let store_existed = cfg.store.exists();
    let ctx = Arc::new(Ctx::new(
        cfg.socket.clone(),
        cfg.address.clone(),
        Inbox::open(&cfg.store)?,
        cfg.groups,
    ));
    let gate = Arc::new(Mutex::new(false));
    let (tx, rx) = mpsc::channel::<Event>();

    let hooks = McpHooks {
        superseded: Box::new({
            let ctx = Arc::clone(&ctx);
            move |notice| {
                let host = notice.get("host").and_then(Value::as_str);
                let at_ms = notice.get("at_ms").and_then(Value::as_i64);
                ctx.confirmed.lock().unwrap().1 = Some(notice.clone());
                ctx.confirmed_cv.notify_all();
                ctx.record_reg("superseded", host, at_ms);
            }
        }),
        confirmed: Box::new({
            let ctx = Arc::clone(&ctx);
            move || {
                ctx.confirmed.lock().unwrap().0 = true;
                ctx.confirmed_cv.notify_all();
                ctx.record_reg("registered", None, Some(now_ms()));
            }
        }),
        registered: Some(Box::new({
            let tx = tx.clone();
            move |daemon| {
                let _ = tx.send(Event::Registered(daemon));
            }
        })),
        stored: Box::new({
            let ctx = Arc::clone(&ctx);
            move || ctx.record_mail()
        }),
        board_event: Box::new({
            let ctx = Arc::clone(&ctx);
            move |ev| ctx.record_board(ev)
        }),
        gate: Arc::clone(&gate),
    };
    let (socket, address, store) = (cfg.socket.clone(), cfg.address.clone(), cfg.store.clone());
    let (label, declared) = (cfg.label.clone(), cfg.declared.clone());
    std::thread::spawn(move || {
        let ended = session::serve(socket, address, label, store, declared, Face::Mcp(hooks));
        let _ = tx.send(Event::Ended(ended));
    });

    let daemon = match rx.recv() {
        Ok(Event::Registered(w)) => w,
        Ok(Event::Ended(Err(e))) => {
            if cfg.store_is_default && !store_existed && ctx.inbox.is_empty().unwrap_or(false) {
                let _ = std::fs::remove_file(&cfg.store);
            }
            return Err(e);
        }
        Ok(Event::Ended(Ok(()))) | Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "expected the host daemon to accept this session, saw its connection close",
            ))
        }
    };
    *ctx.daemon.lock().unwrap() = Some(daemon.clone());
    let cfg = Arc::new(cfg);
    let lost = {
        let (cfg, gate, ctx) = (Arc::clone(&cfg), Arc::clone(&gate), Arc::clone(&ctx));
        move || {
            let reason = match rx.recv() {
                Ok(Event::Ended(Err(e))) => e.to_string(),
                _ => "its connection closed".to_string(),
            };
            close_all_subs(&ctx, Some(&reason));
            eprintln!("roger mcp: expected the host daemon connection to stay up, saw {reason}");
            finish(&cfg, &ctx.inbox, &gate, None, 1)
        }
    };
    std::thread::spawn(lost);

    let out = net::writer(Box::new(io::stdout()));
    for line in io::stdin().lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        handle_line(&ctx, &out, &line);
    }
    close_all_subs(&ctx, None);
    // Stdin closed: the client is gone, so this session is too. It leaves the
    // Board and later Broadcasts; Mail accepted for its Address stays pending.
    finish(&cfg, &ctx.inbox, &gate, Some(&daemon), 0)
}

fn handle_line(ctx: &Arc<Ctx>, out: &net::Writer, line: &str) {
    match parse_message(line) {
        Parsed::Ignore => {}
        Parsed::Notification { method, params } => {
            if method == "notifications/cancelled" {
                if let Some(req_id) = params.get("requestId") {
                    cancel_sub(ctx, req_id);
                }
            }
        }
        Parsed::Bad { id, code, message } => {
            let _ = net::send(out, &reply_err(&id, code, &message));
        }
        Parsed::Request { id, method, params } => {
            if method == "initialize" {
                ctx.legacy.store(true, Ordering::SeqCst);
                let _ = net::send(out, &reply_ok(&id, initialize_result(&params)));
                return;
            }
            if method == "ping" {
                if ctx.legacy.load(Ordering::SeqCst) {
                    let _ = net::send(out, &reply_ok(&id, obj(vec![])));
                } else {
                    let _ = net::send(out, &reply_err(&id, -32601, "Method not found: ping"));
                }
                return;
            }
            let modern = !ctx.legacy.load(Ordering::SeqCst)
                || method == "server/discover"
                || method == "subscriptions/listen"
                || method.starts_with("events/")
                || has_modern_meta(&params);
            if modern {
                if let Err(err) = validate_meta(&params) {
                    let _ = net::send(out, &reply_err_data(&id, err.code, &err.message, err.data));
                    return;
                }
            }
            match method.as_str() {
                "server/discover" => {
                    let _ = net::send(out, &reply_ok(&id, discover_result()));
                }
                "tools/list" => {
                    let _ = net::send(out, &reply_ok(&id, tools_list(ctx.groups, modern)));
                }
                // Each call runs on its own thread, so a waiting inbox read never
                // holds up another call.
                "tools/call" => {
                    let (ctx, out) = (Arc::clone(ctx), out.clone());
                    std::thread::spawn(move || {
                        let result = match call_tool(&ctx, &params) {
                            Ok(v) => tool_result(&json::stringify(&v), false, modern),
                            Err(reason) => tool_result(&reason, true, modern),
                        };
                        let _ = net::send(&out, &reply_ok(&id, result));
                    });
                }
                "subscriptions/listen" => {
                    if let Err(err) = start_subscriptions_listen(ctx, out, id.clone(), &params) {
                        let _ =
                            net::send(out, &reply_err_data(&id, err.code, &err.message, err.data));
                    }
                }
                "events/list" => {
                    let _ = net::send(out, &reply_ok(&id, events_list_result()));
                }
                "events/poll" => {
                    let res = match events_poll(ctx, &params) {
                        Ok(v) => reply_ok(&id, v),
                        Err(err) => reply_err_data(&id, err.code, &err.message, err.data),
                    };
                    let _ = net::send(out, &res);
                }
                "events/stream" => {
                    if let Err(err) = start_events_stream(ctx, out, id.clone(), &params) {
                        let _ =
                            net::send(out, &reply_err_data(&id, err.code, &err.message, err.data));
                    }
                }
                other => {
                    let _ = net::send(
                        out,
                        &reply_err(
                            &id,
                            -32601,
                            &format!(
                                "expected server/discover, tools/list, tools/call, subscriptions/listen, or events/*, saw {other}"
                            ),
                        ),
                    );
                }
            }
        }
    }
}

/// Leave. With a daemon writer, unregister first. A default Store with no
/// entries is removed with its position file: the gate is held so a delivery
/// cannot be written to a file about to go.
fn finish(
    cfg: &Config,
    inbox: &Inbox,
    gate: &Mutex<bool>,
    daemon: Option<&net::Writer>,
    code: i32,
) -> ! {
    let mut closing = gate.lock().unwrap();
    *closing = true;
    if let Some(w) = daemon {
        let _ = net::send(w, &obj(vec![("type", s("unregister"))]));
    }
    if cfg.store_is_default && inbox.is_empty().unwrap_or(false) {
        let _ = std::fs::remove_file(&cfg.store);
        let _ = std::fs::remove_file(position_path(&cfg.store));
    }
    std::process::exit(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |n| {
            pairs
                .iter()
                .find(|(k, _)| *k == n)
                .map(|(_, v)| v.to_string())
        }
    }

    fn fresh() -> io::Result<String> {
        Ok("fresh-uuid".to_string())
    }

    fn resolved(a: &[&str], e: &[(&str, &str)]) -> io::Result<Config> {
        resolve(&args(a), &env(e), &fresh)
    }

    #[test]
    fn address_precedence_is_flag_then_named_variable_then_default_variable_then_fresh() {
        let e = [("MINE", "from-named"), ("ROGER_ADDRESS", "from-default")];
        let base = [("ROGER_SOCKET", "/s"), ("HOME", "/h")];
        let all: Vec<_> = e.iter().chain(base.iter()).copied().collect();
        let a = |flags: &[&str], env: &[(&str, &str)]| resolved(flags, env).unwrap().address;
        assert_eq!(a(&["--id", "X", "--id-env", "MINE"], &all), "X");
        assert_eq!(a(&["--id-env", "MINE"], &all), "from-named");
        assert_eq!(a(&[], &all), "from-default");
        assert_eq!(a(&["--id-env", "MINE"], &base), "fresh-uuid");
        // an unset or empty named variable falls through
        let empty = [
            ("MINE", ""),
            ("ROGER_ADDRESS", "from-default"),
            base[0],
            base[1],
        ];
        assert_eq!(a(&["--id-env", "MINE"], &empty), "from-default");
        assert_eq!(a(&["--id-env", "UNSET"], &empty), "from-default");
    }

    #[test]
    fn a_composite_address_is_kept_verbatim() {
        let c = "[[1a:FLEET:zz]]22222222-2222-4222-8222-222222222222";
        let cfg = resolved(&["--id", c], &[("ROGER_SOCKET", "/s"), ("HOME", "/h")]).unwrap();
        assert_eq!(cfg.address, c);
        assert_eq!(
            cfg.store,
            PathBuf::from("/h")
                .join(".local")
                .join("state")
                .join("roger")
                .join("mcp")
                .join(format!("{}.store", store_stem(c)))
        );
        assert!(cfg.store_is_default);
    }

    #[test]
    fn socket_and_store_follow_flag_then_variable_then_default_layout() {
        let cfg = resolved(
            &[],
            &[("XDG_RUNTIME_DIR", "/run/u"), ("XDG_STATE_HOME", "/st")],
        )
        .unwrap();
        assert_eq!(
            cfg.socket,
            PathBuf::from("/run/u").join("roger").join("daemon.sock")
        );
        assert_eq!(
            cfg.store,
            PathBuf::from("/st")
                .join("roger")
                .join("mcp")
                .join("fresh-uuid.store")
        );
        let cfg = resolved(
            &["--socket", "/f.sock"],
            &[("ROGER_SOCKET", "/v.sock"), ("ROGER_STORE", "/v.store")],
        )
        .unwrap();
        assert_eq!(cfg.socket, PathBuf::from("/f.sock"));
        assert_eq!(cfg.store, PathBuf::from("/v.store"));
        assert!(!cfg.store_is_default);
        let cfg = resolved(&["--store", "/f.store"], &[("ROGER_SOCKET", "/s")]).unwrap();
        assert_eq!(cfg.store, PathBuf::from("/f.store"));
        let cfg = resolved(
            &[],
            &[("ROGER_SOCKET", "tcp://127.0.0.1:7478"), ("HOME", "/h")],
        )
        .unwrap();
        assert_eq!(cfg.socket, PathBuf::from("tcp://127.0.0.1:7478"));
        assert_eq!(
            cfg.store,
            PathBuf::from("/h")
                .join(".local")
                .join("state")
                .join("roger")
                .join("mcp")
                .join("fresh-uuid.store")
        );
        assert!(cfg.store_is_default);
        let cfg = resolved(
            &["--socket", "tcp://127.0.0.1:7478"],
            &[("XDG_STATE_HOME", "/st")],
        )
        .unwrap();
        assert_eq!(cfg.socket, PathBuf::from("tcp://127.0.0.1:7478"));
        assert_eq!(
            cfg.store,
            PathBuf::from("/st")
                .join("roger")
                .join("mcp")
                .join("fresh-uuid.store")
        );
        assert!(cfg.store_is_default);
        let cfg = resolved(
            &["--socket", "tcp://127.0.0.1:7478"],
            &[("LOCALAPPDATA", "/appdata")],
        )
        .unwrap();
        assert_eq!(
            cfg.store,
            PathBuf::from("/appdata")
                .join("roger")
                .join("mcp")
                .join("fresh-uuid.store")
        );
        let cfg = resolved(
            &["--socket", "tcp://127.0.0.1:7478"],
            &[("USERPROFILE", "/users/alice")],
        )
        .unwrap();
        assert_eq!(
            cfg.store,
            PathBuf::from("/users/alice")
                .join(".local")
                .join("state")
                .join("roger")
                .join("mcp")
                .join("fresh-uuid.store")
        );
        assert_eq!(
            encode_ntfs_stem("[[1a:FLEET:zz]]22222222-2222-4222-8222-222222222222"),
            "[[1a%3AFLEET%3Azz]]22222222-2222-4222-8222-222222222222"
        );
    }

    #[test]
    fn a_missing_socket_names_what_was_expected_and_what_was_seen() {
        let err = resolved(&["--store", "/x"], &[]).unwrap_err().to_string();
        assert!(
            err.contains("expected") && err.contains("XDG_RUNTIME_DIR"),
            "{err}"
        );
        assert!(err.contains("saw none"), "{err}");
    }

    #[test]
    fn an_address_that_is_not_a_file_name_needs_an_explicit_store() {
        let e = [("ROGER_SOCKET", "/s"), ("HOME", "/h")];
        let err = resolved(&["--id", "a/b"], &e).unwrap_err().to_string();
        assert!(err.contains("a/b") && err.contains("--store"), "{err}");
        assert!(resolved(&["--id", "a/b", "--store", "/x"], &e).is_ok());
    }

    #[test]
    fn groups_as_a_label_does_not_enable_groups() {
        let cfg = resolved(
            &["--label", "--groups"],
            &[("ROGER_SOCKET", "/s"), ("ROGER_STORE", "/x")],
        )
        .unwrap();
        assert_eq!(cfg.label.as_deref(), Some("--groups"));
        assert!(!cfg.groups);
    }

    #[test]
    fn unknown_mcp_flag_is_rejected() {
        let err = resolved(&["--unknown"], &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().ends_with("for mcp, saw --unknown"));
    }

    #[test]
    fn messages_are_classified() {
        assert_eq!(
            parse_message(r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#),
            Parsed::Request {
                id: Value::Int(3),
                method: "ping".into(),
                params: Value::Obj(vec![])
            }
        );
        assert_eq!(
            parse_message(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#),
            Parsed::Notification {
                method: "notifications/initialized".into(),
                params: Value::Obj(vec![]),
            }
        );
        assert_eq!(
            parse_message(r#"{"jsonrpc":"2.0","id":"a","result":{}}"#),
            Parsed::Ignore
        );
        match parse_message("{not json") {
            Parsed::Bad { id, code, .. } => assert_eq!((id, code), (Value::Null, -32700)),
            other => panic!("{other:?}"),
        }
        match parse_message("[1]") {
            Parsed::Bad { code, .. } => assert_eq!(code, -32600),
            other => panic!("{other:?}"),
        }
        match parse_message(r#"{"id":1,"method":7}"#) {
            Parsed::Bad { id, code, message } => {
                assert_eq!((id, code), (Value::Int(1), -32600));
                assert!(
                    message.contains("expected") && message.contains("saw"),
                    "{message}"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_protocol_version_is_the_clients_when_known_else_the_newest() {
        let v = |t: &str| negotiate(&json::parse(t).unwrap());
        assert_eq!(v(r#"{"protocolVersion":"2024-11-05"}"#), "2024-11-05");
        assert_eq!(v(r#"{"protocolVersion":"2025-03-26"}"#), "2025-03-26");
        assert_eq!(v(r#"{"protocolVersion":"1999-01-01"}"#), "2025-06-18");
        assert_eq!(v(r#"{}"#), "2025-06-18");
    }

    #[test]
    fn every_tool_has_a_valid_schema() {
        let list = tools_list(false, true);
        let Some(Value::Arr(tools)) = list.get("tools") else {
            panic!("no tools");
        };
        assert_eq!(tools.len(), 9);
        assert_eq!(list.get("resultType"), Some(&s("complete")));
        assert_eq!(list.get("ttlMs"), Some(&Value::Int(DISCOVER_TTL_MS)));
        assert_eq!(list.get("cacheScope"), Some(&s("public")));
        assert!(tools
            .iter()
            .all(|t| t.get("inputSchema").is_some() && t.get("description").is_some()));
    }

    #[test]
    fn argument_errors_say_what_was_expected_and_what_was_seen() {
        let args = json::parse(r#"{"to":5,"limit":0,"peek":"yes"}"#).unwrap();
        let e = need_str("roger_send", &args, "to").unwrap_err();
        assert_eq!(
            e,
            "roger_send: expected `to` to be a string, saw an integer"
        );
        let e = need_str("roger_send", &args, "body").unwrap_err();
        assert!(e.contains("saw nothing"), "{e}");
        let e = opt_int("roger_inbox", &args, "limit", 50, (1, 200), true).unwrap_err();
        assert!(e.contains("from 1 to 200") && e.contains("saw 0"), "{e}");
        let e = opt_bool("roger_inbox", &args, "peek").unwrap_err();
        assert!(e.contains("a boolean") && e.contains("a string"), "{e}");
    }

    #[test]
    fn an_integer_above_its_maximum_is_held_to_it_only_when_clamped() {
        let args = json::parse(r#"{"n":900}"#).unwrap();
        assert_eq!(opt_int("t", &args, "n", 1, (1, 200), true), Ok(200));
        assert!(opt_int("t", &args, "n", 1, (1, 200), false).is_err());
        assert_eq!(opt_int("t", &args, "missing", 7, (1, 200), true), Ok(7));
    }

    fn entry(record: &str, with_kind: bool) -> Value {
        let mut pairs = vec![
            ("record", s(record)),
            ("mail", s(&format!("m-{record}"))),
            ("from", s("f")),
            ("body", s("b")),
        ];
        if with_kind {
            pairs.push(("kind", s("directed")));
            pairs.push(("headers", obj(vec![("h", s("v"))])));
        }
        obj(pairs)
    }

    #[test]
    fn entries_after_the_cursor_are_limited_and_keep_only_what_the_store_holds() {
        let all = vec![entry("r-1", false), entry("r-2", true), entry("r-3", true)];
        let rec = |v: &[Value]| -> Vec<String> {
            v.iter()
                .map(|e| e.get("record").unwrap().as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(rec(&after(&all, None, 50)), ["r-1", "r-2", "r-3"]);
        assert_eq!(rec(&after(&all, Some("r-1"), 50)), ["r-2", "r-3"]);
        assert_eq!(rec(&after(&all, None, 2)), ["r-1", "r-2"]);
        assert!(after(&all, Some("r-3"), 50).is_empty());
        // a read position naming no entry starts from the first
        assert_eq!(rec(&after(&all, Some("gone"), 50)).len(), 3);
        let first = &after(&all, None, 1)[0];
        assert!(first.get("kind").is_none() && first.get("headers").is_none());
        let second = &after(&all, Some("r-1"), 1)[0];
        assert_eq!(second.get("kind"), Some(&s("directed")));
    }

    #[test]
    fn a_board_row_is_present_when_connected_or_seen_within_the_window() {
        let row = |t: &str| json::parse(t).unwrap();
        let now = 1_000_000;
        assert!(present(
            &row(r#"{"connected":true,"last_seen_ms":0}"#),
            now,
            0
        ));
        assert!(present(
            &row(r#"{"connected":false,"last_seen_ms":940000}"#),
            now,
            60_000
        ));
        assert!(!present(
            &row(r#"{"connected":false,"last_seen_ms":939999}"#),
            now,
            60_000
        ));
        assert!(!present(
            &row(r#"{"connected":false,"last_seen_ms":0}"#),
            now,
            i64::MAX
        ));
    }

    #[test]
    fn a_saved_cursor_is_read_back_and_a_wait_ends_at_its_bound() {
        let dir = std::env::temp_dir().join(format!("roger-mcp-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("x.store");
        std::fs::write(
            &store,
            format!(
                "{}\n{}\n{{\"record\":\"r-3\"",
                json::stringify(&entry("r-1", true)),
                json::stringify(&entry("r-2", true))
            ),
        )
        .unwrap();
        let inbox = Inbox::open(&store).unwrap();
        let (page, cur) = inbox.read(1, Duration::ZERO, false).unwrap();
        assert_eq!((page.len(), cur.as_deref()), (1, Some("r-1")));
        // a restart reads the saved read position; the half-written r-3 is no entry
        let again = Inbox::open(&store).unwrap();
        let (peeked, _) = again.read(10, Duration::ZERO, true).unwrap();
        assert_eq!(peeked.len(), 1);
        let (page, cur) = again.read(10, Duration::ZERO, false).unwrap();
        assert_eq!((page.len(), cur.as_deref()), (1, Some("r-2")));
        let (page, cur) = again.read(10, Duration::from_millis(50), false).unwrap();
        assert!(page.is_empty() && cur.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_and_tools_list_isolate_default_nine_tools_from_groups_flag() {
        let base_env = [("ROGER_SOCKET", "/s"), ("HOME", "/h")];
        assert!(!resolved(&[], &base_env).unwrap().groups);
        assert!(resolved(&["--groups"], &base_env).unwrap().groups);

        let names_of = |v: Value| -> Vec<String> {
            let mut ns: Vec<String> = v
                .get("tools")
                .and_then(|x| match x {
                    Value::Arr(a) => Some(a),
                    _ => None,
                })
                .unwrap()
                .iter()
                .map(|t| t.get("name").unwrap().as_str().unwrap().to_string())
                .collect();
            ns.sort();
            ns
        };
        assert_eq!(
            names_of(tools_list(false, true)),
            vec![
                "roger_board",
                "roger_claim",
                "roger_inbox",
                "roger_label",
                "roger_receipt",
                "roger_registered",
                "roger_release",
                "roger_renew",
                "roger_send",
            ]
        );
        let default_send = tools_list(false, true)
            .get("tools")
            .and_then(|v| match v {
                Value::Arr(a) => a.iter().find(|t| t.get("name") == Some(&s("roger_send"))),
                _ => None,
            })
            .cloned()
            .unwrap();
        assert!(default_send
            .get("inputSchema")
            .and_then(|s| s.get("properties"))
            .and_then(|p| p.get("group"))
            .is_none());
        assert_eq!(
            default_send
                .get("inputSchema")
                .and_then(|s| s.get("required")),
            Some(&Value::Arr(vec![s("to"), s("body")]))
        );

        let opt_in = tools_list(true, true);
        assert_eq!(
            names_of(opt_in.clone()),
            vec![
                "roger_articles",
                "roger_board",
                "roger_claim",
                "roger_group_policy",
                "roger_groups",
                "roger_inbox",
                "roger_label",
                "roger_receipt",
                "roger_registered",
                "roger_release",
                "roger_renew",
                "roger_send",
                "roger_subscribe",
                "roger_unsubscribe",
            ]
        );
        let group_send = opt_in
            .get("tools")
            .and_then(|v| match v {
                Value::Arr(a) => a.iter().find(|t| t.get("name") == Some(&s("roger_send"))),
                _ => None,
            })
            .cloned()
            .unwrap();
        assert!(group_send
            .get("inputSchema")
            .and_then(|s| s.get("properties"))
            .and_then(|p| p.get("group"))
            .is_some());
        assert_eq!(
            group_send
                .get("inputSchema")
                .and_then(|s| s.get("required")),
            Some(&Value::Arr(vec![s("body")]))
        );

        let dir = std::env::temp_dir().join(format!("roger-mcp-isolate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ctx_default = Ctx::new(
            PathBuf::from("tcp://127.0.0.1:1"),
            "s1".into(),
            Inbox::open(&dir.join("d.store")).unwrap(),
            false,
        );
        for tool in [
            "roger_subscribe",
            "roger_unsubscribe",
            "roger_groups",
            "roger_articles",
            "roger_group_policy",
        ] {
            let err = call_tool(
                &ctx_default,
                &json::parse(&format!(r#"{{"name":"{tool}","arguments":{{}}}}"#)).unwrap(),
            )
            .unwrap_err();
            assert!(
                err.contains("expected a tool among") && err.contains(tool),
                "{err}"
            );
        }
        let err = call_tool(
            &ctx_default,
            &json::parse(r#"{"name":"roger_send","arguments":{"group":"ops","body":"hi"}}"#)
                .unwrap(),
        )
        .unwrap_err();
        assert_eq!(err, "roger_send: expected `to` to be a string, saw nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mcp_groups_tools_and_inbox_projection_over_mock_daemon() {
        let dir =
            std::env::temp_dir().join(format!("roger-mcp-groups-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("g.store");
        std::fs::write(
            &store,
            concat!(
                r#"{"record":"r-1","mail":"md-1","from":"peer","body":"direct","kind":"directed","headers":{}}"#,
                "\n",
                r#"{"record":"r-2","mail":"mg-4","from":"peer","group":"ops","article":4,"body":"post","kind":"group","headers":{"x-h":"1"}}"#,
                "\n"
            ),
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let daemon = std::thread::spawn(move || {
            for _ in 0..6 {
                let (conn, _) = listener.accept().unwrap();
                let mut lines = net::lines(Box::new(conn.try_clone().unwrap()));
                let w = net::writer(Box::new(conn));
                let req = lines.next().unwrap().unwrap();
                let r = req.get("req").and_then(Value::as_i64).unwrap_or(1);
                let resp = match net::field(&req, "type").unwrap() {
                    "subscribe" => obj(vec![
                        ("type", s("subscribed")),
                        ("req", Value::Int(r)),
                        ("session", s(net::field(&req, "session").unwrap())),
                        ("group", s(net::field(&req, "group").unwrap())),
                        (
                            "position",
                            Value::Int(req.get("after").and_then(Value::as_i64).unwrap_or(4)),
                        ),
                        ("high_water", Value::Int(4)),
                    ]),
                    "send" if req.get("group").is_some() => obj(vec![
                        ("type", s("accepted")),
                        ("req", Value::Int(r)),
                        ("mail", s(net::field(&req, "mail").unwrap())),
                        ("group", s(net::field(&req, "group").unwrap())),
                        ("article", Value::Int(5)),
                    ]),
                    "send" => obj(vec![
                        ("type", s("accepted")),
                        ("req", Value::Int(r)),
                        ("mail", s(net::field(&req, "mail").unwrap())),
                    ]),
                    "unsubscribe" => obj(vec![
                        ("type", s("unsubscribed")),
                        ("req", Value::Int(r)),
                        ("session", s(net::field(&req, "session").unwrap())),
                        ("group", s(net::field(&req, "group").unwrap())),
                        ("position", Value::Int(5)),
                    ]),
                    "groups" => obj(vec![
                        ("type", s("groups")),
                        ("req", Value::Int(r)),
                        (
                            "groups",
                            Value::Arr(vec![obj(vec![
                                ("group", s("ops")),
                                ("high_water", Value::Int(5)),
                                ("subscribers", Value::Int(0)),
                                ("subscribed", Value::Bool(false)),
                                ("position", Value::Int(5)),
                                ("record", s("r-2")),
                            ])]),
                        ),
                    ]),
                    other => panic!("unexpected {other}"),
                };
                net::send(&w, &resp).unwrap();
            }
        });

        let ctx = Ctx::new(target, "me-g".into(), Inbox::open(&store).unwrap(), true);

        let inbox_res = call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_inbox","arguments":{"wait_seconds":0}}"#).unwrap(),
        )
        .unwrap();
        let Some(Value::Arr(items)) = inbox_res.get("mail") else {
            panic!("expected mail array");
        };
        assert_eq!(items.len(), 2);
        assert!(items[0].get("group").is_none() && items[0].get("article").is_none());
        assert_eq!(items[1].get("group"), Some(&s("ops")));
        assert_eq!(items[1].get("article"), Some(&Value::Int(4)));
        assert_eq!(items[1].get("kind"), Some(&s("group")));

        assert!(call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_send","arguments":{"to":"x","group":"ops","body":"b"}}"#
            )
            .unwrap()
        )
        .unwrap_err()
        .contains("either `to` or `group`"));
        assert!(call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_send","arguments":{"body":"b"}}"#).unwrap()
        )
        .unwrap_err()
        .contains("expected `to` or `group`"));

        assert!(call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_subscribe","arguments":{"group":"ops","after":1,"latest":true}}"#
            )
            .unwrap()
        )
        .unwrap_err()
        .contains("expected `after` or `latest`"));
        assert!(call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_subscribe","arguments":{"group":"ops","after":-1}}"#)
                .unwrap()
        )
        .is_err());

        let sub1 = call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_subscribe","arguments":{"group":"ops","after":1}}"#)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            sub1,
            json::parse(r#"{"session":"me-g","group":"ops","position":1,"high_water":4}"#).unwrap()
        );
        let sub2 = call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_subscribe","arguments":{"group":"ops","latest":true}}"#)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sub2.get("position"), Some(&Value::Int(4)));

        let sent_g = call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_send","arguments":{"group":"ops","mail":"mg-5","body":"hi"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            sent_g,
            json::parse(r#"{"mail":"mg-5","group":"ops","article":5}"#).unwrap()
        );
        let sent_d = call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_send","arguments":{"to":"peer","mail":"md-2","body":"hi"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(sent_d, json::parse(r#"{"mail":"md-2"}"#).unwrap());

        let unsub = call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_unsubscribe","arguments":{"group":"ops"}}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(
            unsub,
            json::parse(r#"{"session":"me-g","group":"ops","position":5}"#).unwrap()
        );

        let grps = call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_groups","arguments":{}}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(
            grps,
            json::parse(
                r#"{"groups":[{"group":"ops","high_water":5,"subscribers":0,"subscribed":false,"position":5,"record":"r-2"}]}"#
            )
            .unwrap()
        );

        daemon.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_meta_validation_and_iso8601_format() {
        let d = discover_result();
        assert_eq!(d.get("resultType"), Some(&s("complete")));
        assert_eq!(
            d.get("supportedVersions"),
            Some(&Value::Arr(vec![s(VERSION)]))
        );
        assert_eq!(d.get("ttlMs"), Some(&Value::Int(DISCOVER_TTL_MS)));
        assert_eq!(d.get("cacheScope"), Some(&s("public")));
        assert!(d
            .get("capabilities")
            .and_then(|c| c.get("extensions"))
            .and_then(|e| e.get("io.modelcontextprotocol/events"))
            .is_some());

        let missing = json::parse(r#"{}"#).unwrap();
        assert_eq!(validate_meta(&missing).unwrap_err().code, -32602);
        let bad_ver = json::parse(
            r#"{"_meta":{"io.modelcontextprotocol/protocolVersion":"1999-01-01","io.modelcontextprotocol/clientCapabilities":{}}}"#,
        )
        .unwrap();
        let err = validate_meta(&bad_ver).unwrap_err();
        assert_eq!(err.code, -32022);
        assert_eq!(
            err.data.as_ref().and_then(|d| d.get("supported")),
            Some(&Value::Arr(vec![s(VERSION)]))
        );
        let ok = json::parse(
            r#"{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}"#,
        )
        .unwrap();
        assert!(validate_meta(&ok).is_ok());

        assert_eq!(format_iso8601_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_iso8601_utc(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn events_list_and_poll_cursors_filters_and_isolation_from_inbox_cursor() {
        let listed = events_list_result();
        let Some(Value::Arr(evts)) = listed.get("events") else {
            panic!("missing events");
        };
        assert_eq!(evts.len(), 3);

        let dir =
            std::env::temp_dir().join(format!("roger-mcp-events-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("ev.store");
        let now = now_ms();
        let old_ts = now - 600_000;
        let fresh_ts = now - 1_000;
        std::fs::write(
            &store,
            format!(
                concat!(
                    "{{\"record\":\"r-1\",\"mail\":\"m-1\",\"from\":\"a\",\"body\":\"b1\",\"kind\":\"directed\",\"headers\":{{}},\"stored_at_ms\":{old}}}\n",
                    "{{\"record\":\"r-2\",\"mail\":\"m-2\",\"from\":\"a\",\"group\":\"ops\",\"article\":1,\"body\":\"b2\",\"kind\":\"group\",\"headers\":{{}},\"stored_at_ms\":{fresh}}}\n",
                    "{{\"record\":\"r-3\",\"mail\":\"m-3\",\"from\":\"a\",\"group\":\"dev\",\"article\":1,\"body\":\"b3\",\"kind\":\"group\",\"headers\":{{}},\"stored_at_ms\":{fresh}}}\n"
                ),
                old = old_ts,
                fresh = fresh_ts
            ),
        )
        .unwrap();
        let ctx = Ctx::new(
            PathBuf::from("tcp://127.0.0.1:1"),
            "me".into(),
            Inbox::open(&store).unwrap(),
            true,
        );

        // null cursor -> start from now (empty, returns tail cursor)
        let p0 = events_poll(&ctx, &json::parse(r#"{"name":"roger.mail"}"#).unwrap())
            .unwrap_or_else(|e| panic!("poll failed: {}", e.message));
        assert_eq!(p0.get("cursor"), Some(&s("r-3")));
        assert_eq!(p0.get("truncated"), Some(&Value::Bool(false)));
        assert_eq!(p0.get("hasMore"), Some(&Value::Bool(false)));
        assert_eq!(p0.get("events"), Some(&Value::Arr(vec![])));

        // r-0 with maxEvents=1 -> returns r-1, hasMore=true
        let p1 = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-0","maxEvents":1}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(p1.get("cursor"), Some(&s("r-1")));
        assert_eq!(p1.get("hasMore"), Some(&Value::Bool(true)));

        // r-0 with maxAgeMs=60_000 -> skips r-1, sets truncated=true
        let p_age = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-0","maxAgeMs":60000}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(p_age.get("truncated"), Some(&Value::Bool(true)));
        let Some(Value::Arr(ev_age)) = p_age.get("events") else {
            panic!("events");
        };
        assert_eq!(ev_age.len(), 2);
        assert_eq!(ev_age[0].get("eventId"), Some(&s("m-2")));

        // group filter
        let p_grp = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-0","arguments":{"group":"dev"}}"#)
                .unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        let Some(Value::Arr(ev_grp)) = p_grp.get("events") else {
            panic!("events");
        };
        assert_eq!(ev_grp.len(), 1);
        assert_eq!(ev_grp[0].get("eventId"), Some(&s("m-3")));
        assert_eq!(p_grp.get("cursor"), Some(&s("r-3")));

        // unknown cursor -> truncated=true and reset to tail
        let p_bad = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-999"}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(p_bad.get("truncated"), Some(&Value::Bool(true)));
        assert_eq!(p_bad.get("cursor"), Some(&s("r-3")));

        // roger.board poll -> -32026 Unsupported; unknown event -> -32023 NotFound
        assert_eq!(
            events_poll(&ctx, &json::parse(r#"{"name":"roger.board"}"#).unwrap())
                .err()
                .map(|e| e.code),
            Some(-32026)
        );
        assert_eq!(
            events_poll(&ctx, &json::parse(r#"{"name":"no.such"}"#).unwrap())
                .err()
                .map(|e| e.code),
            Some(-32023)
        );
        // Inbox cursor file was never created/modified by events/poll.
        assert!(!position_path(&store).exists());

        // groupPrefix filter
        let p_pref = events_poll(
            &ctx,
            &json::parse(
                r#"{"name":"roger.mail","cursor":"r-0","arguments":{"groupPrefix":"de"}}"#,
            )
            .unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        let Some(Value::Arr(ev_pref)) = p_pref.get("events") else {
            panic!("events");
        };
        assert_eq!(ev_pref.len(), 1);
        assert_eq!(ev_pref[0].get("eventId"), Some(&s("m-3")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_cache_tails_incrementally_handles_torn_tail_cursor_lookup_and_truncation_reset() {
        use std::io::Write as _;
        let dir = std::env::temp_dir().join(format!("roger-mcp-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("cache.store");
        let now = now_ms();
        let line1 = format!(
            "{{\"record\":\"r-1\",\"mail\":\"m-1\",\"from\":\"a\",\"body\":\"b1\",\"kind\":\"directed\",\"headers\":{{}},\"stored_at_ms\":{now}}}\n"
        );
        let line2 = format!(
            "{{\"record\":\"r-2\",\"mail\":\"m-2\",\"from\":\"a\",\"body\":\"b2\",\"kind\":\"directed\",\"headers\":{{}},\"stored_at_ms\":{now}}}\n"
        );
        std::fs::write(&store, format!("{line1}{line2}{{\"record\":\"r-torn\"")).unwrap();

        let ctx = Ctx::new(
            PathBuf::from("tcp://127.0.0.1:1"),
            "me".into(),
            Inbox::open(&store).unwrap(),
            true,
        );
        assert_eq!(ctx.inbox.cache.lock().unwrap().parses, 2);

        // Repeated poll with no file growth parses 0 additional lines.
        let p1 = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-0"}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(p1.get("cursor"), Some(&s("r-2")));
        assert_eq!(ctx.inbox.cache.lock().unwrap().parses, 2);

        // Truncate torn tail via DurLog::open and append r-3 -> parses only 1 new line.
        {
            let (mut log, replay) = crate::durlog::DurLog::open(&store).unwrap();
            let prior: Vec<String> = replay.collect::<io::Result<Vec<_>>>().unwrap();
            assert_eq!(prior.len(), 2);
            log.append(&format!(
                "{{\"record\":\"r-3\",\"mail\":\"m-3\",\"from\":\"a\",\"body\":\"b3\",\"kind\":\"directed\",\"headers\":{{}},\"stored_at_ms\":{now}}}"
            ))
            .unwrap();
        }
        let p2 = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-2"}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(ctx.inbox.cache.lock().unwrap().parses, 3);
        let Some(Value::Arr(evs)) = p2.get("events") else {
            panic!("events");
        };
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].get("eventId"), Some(&s("m-3")));
        assert_eq!(p2.get("cursor"), Some(&s("r-3")));

        // Inbox read uses the same cache without re-parsing.
        let (mail, cur) = ctx.inbox.read(10, Duration::ZERO, false).unwrap();
        assert_eq!(mail.len(), 3);
        assert_eq!(cur.as_deref(), Some("r-3"));
        assert_eq!(ctx.inbox.cache.lock().unwrap().parses, 3);

        // Truncate/replace file -> resets generation and re-reads cleanly from 0.
        {
            let mut f = std::fs::File::create(&store).unwrap();
            writeln!(
                f,
                "{{\"record\":\"r-10\",\"mail\":\"m-10\",\"from\":\"a\",\"body\":\"fresh\",\"kind\":\"directed\",\"headers\":{{}},\"stored_at_ms\":{now}}}"
            )
            .unwrap();
        }
        let p3 = events_poll(
            &ctx,
            &json::parse(r#"{"name":"roger.mail","cursor":"r-3"}"#).unwrap(),
        )
        .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(p3.get("truncated"), Some(&Value::Bool(true)));
        assert_eq!(p3.get("cursor"), Some(&s("r-10")));
        assert_eq!(ctx.inbox.cache.lock().unwrap().generation, 1);
        assert_eq!(ctx.inbox.cache.lock().unwrap().parses, 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mcp_tools_reuse_single_pooled_connection_and_reconnect_on_eof() {
        let dir = std::env::temp_dir().join(format!("roger-mcp-pool-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("pool.store");

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let server = std::thread::spawn(move || {
            // First accepted connection serves 4 consecutive tool calls, then closes.
            let (conn1, _) = listener.accept().unwrap();
            let mut lines1 = net::lines(Box::new(conn1.try_clone().unwrap()));
            let w1 = net::writer(Box::new(conn1));
            for expected_req in 1..=4 {
                let req = lines1.next().unwrap().unwrap();
                assert_eq!(req.get("req").and_then(Value::as_i64), Some(expected_req));
                let resp = match net::field(&req, "type").unwrap() {
                    "send" => obj(vec![
                        ("type", s("accepted")),
                        ("req", Value::Int(expected_req)),
                        ("mail", s(net::field(&req, "mail").unwrap())),
                    ]),
                    "board" => obj(vec![
                        ("type", s("board")),
                        ("req", Value::Int(expected_req)),
                        ("sessions", Value::Arr(vec![])),
                        ("claims", Value::Arr(vec![])),
                        ("now_ms", Value::Int(100)),
                    ]),
                    "groups" => obj(vec![
                        ("type", s("groups")),
                        ("req", Value::Int(expected_req)),
                        ("groups", Value::Arr(vec![])),
                    ]),
                    "claim" => obj(vec![
                        ("type", s("claim_result")),
                        ("req", Value::Int(expected_req)),
                        ("op", s("acquire")),
                        ("key", s("k1")),
                        ("ok", Value::Bool(true)),
                    ]),
                    other => panic!("unexpected {other}"),
                };
                net::send(&w1, &resp).unwrap();
            }
            drop((lines1, w1));

            // Second accepted connection after EOF serves req 5.
            let (conn2, _) = listener.accept().unwrap();
            let mut lines2 = net::lines(Box::new(conn2.try_clone().unwrap()));
            let w2 = net::writer(Box::new(conn2));
            let req = lines2.next().unwrap().unwrap();
            assert_eq!(req.get("req").and_then(Value::as_i64), Some(5));
            net::send(
                &w2,
                &obj(vec![
                    ("type", s("accepted")),
                    ("req", Value::Int(5)),
                    ("mail", s(net::field(&req, "mail").unwrap())),
                ]),
            )
            .unwrap();
        });

        let ctx = Ctx::new(target, "me-pool".into(), Inbox::open(&store).unwrap(), true);
        call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_send","arguments":{"to":"peer","mail":"m1","body":"hi"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_board","arguments":{}}"#).unwrap(),
        )
        .unwrap();
        call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_groups","arguments":{}}"#).unwrap(),
        )
        .unwrap();
        call_tool(
            &ctx,
            &json::parse(r#"{"name":"roger_claim","arguments":{"key":"k1"}}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(ctx.client.lock().unwrap().connects(), 1);

        // Server closed conn1; next call reconnects seamlessly.
        let res = call_tool(
            &ctx,
            &json::parse(
                r#"{"name":"roger_send","arguments":{"to":"peer","mail":"m2","body":"hi2"}}"#,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(res.get("mail"), Some(&s("m2")));
        assert_eq!(ctx.client.lock().unwrap().connects(), 2);

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mcp_articles_and_group_policy_send_one_wire_ask_each() {
        let dir = std::env::temp_dir().join(format!("roger-mcp-artpol-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("ap.store");

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        // The mock echoes each ask (minus `type` and `req`) under `ask`.
        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let lines = net::lines(Box::new(conn.try_clone().unwrap()));
            let w = net::writer(Box::new(conn));
            for line in lines {
                let req = line.unwrap();
                let r = req.get("req").and_then(Value::as_i64).unwrap();
                let kind = net::field(&req, "type").unwrap().to_string();
                let mut ask = req.clone();
                if let Value::Obj(pairs) = &mut ask {
                    pairs.retain(|(k, _)| k != "req");
                }
                net::send(
                    &w,
                    &obj(vec![
                        ("type", s(&kind)),
                        ("req", Value::Int(r)),
                        ("ask", ask),
                    ]),
                )
                .unwrap();
            }
        });

        let ctx = Ctx::new(target, "me-ap".into(), Inbox::open(&store).unwrap(), true);
        let call = |a: &str| call_tool(&ctx, &json::parse(a).unwrap());
        let ask = |a: &str| call(a).unwrap().get("ask").cloned().unwrap();

        assert_eq!(
            ask(r#"{"name":"roger_articles","arguments":{"group":"ops"}}"#),
            json::parse(r#"{"type":"articles","group":"ops","after":0,"limit":100}"#).unwrap()
        );
        assert_eq!(
            ask(r#"{"name":"roger_articles","arguments":{"group":"ops","after":3,"limit":500}}"#),
            json::parse(r#"{"type":"articles","group":"ops","after":3,"limit":500}"#).unwrap()
        );
        assert_eq!(
            call(r#"{"name":"roger_articles","arguments":{}}"#).unwrap_err(),
            "roger_articles: expected `group` to be a string, saw nothing"
        );
        assert_eq!(
            call(r#"{"name":"roger_articles","arguments":{"group":"ops","after":-1}}"#)
                .unwrap_err(),
            format!(
                "roger_articles: expected `after` to be an integer from 0 to {}, saw -1",
                i64::MAX
            )
        );
        assert_eq!(
            call(r#"{"name":"roger_articles","arguments":{"group":"ops","limit":501}}"#)
                .unwrap_err(),
            "roger_articles: expected `limit` to be an integer from 1 to 500, saw 501"
        );
        assert_eq!(
            call(r#"{"name":"roger_articles","arguments":{"group":"ops","limit":0}}"#).unwrap_err(),
            "roger_articles: expected `limit` to be an integer from 1 to 500, saw 0"
        );
        assert_eq!(
            call(r#"{"name":"roger_articles","arguments":{"group":"ops","after":"1"}}"#)
                .unwrap_err(),
            "roger_articles: expected `after` to be an integer, saw a string"
        );

        assert_eq!(
            ask(r#"{"name":"roger_group_policy","arguments":{"group":"ops"}}"#),
            json::parse(r#"{"type":"group_policy","group":"ops"}"#).unwrap()
        );
        assert_eq!(
            ask(
                r#"{"name":"roger_group_policy","arguments":{"group":"ops","keep_last":2,"ttl_ms":0,"retired":true}}"#
            ),
            json::parse(
                r#"{"type":"group_policy","group":"ops","keep_last":2,"ttl_ms":0,"retired":true}"#
            )
            .unwrap()
        );
        assert_eq!(
            ask(
                r#"{"name":"roger_group_policy","arguments":{"group":"ops","keep_last":null,"ttl_ms":null,"retired":false}}"#
            ),
            json::parse(
                r#"{"type":"group_policy","group":"ops","keep_last":null,"ttl_ms":null,"retired":false}"#
            )
            .unwrap()
        );
        assert_eq!(
            call(r#"{"name":"roger_group_policy","arguments":{"keep_last":1}}"#).unwrap_err(),
            "roger_group_policy: expected `group` to be a string, saw nothing"
        );
        assert_eq!(
            call(r#"{"name":"roger_group_policy","arguments":{"group":"ops","keep_last":-1}}"#)
                .unwrap_err(),
            "roger_group_policy: expected `keep_last` to be an integer of at least 0 or null, saw -1"
        );
        assert_eq!(
            call(r#"{"name":"roger_group_policy","arguments":{"group":"ops","ttl_ms":"5"}}"#)
                .unwrap_err(),
            "roger_group_policy: expected `ttl_ms` to be an integer or null, saw a string"
        );
        assert_eq!(
            call(r#"{"name":"roger_group_policy","arguments":{"group":"ops","retired":null}}"#)
                .unwrap_err(),
            "roger_group_policy: expected `retired` to be a boolean, saw null"
        );

        // One pooled connection carried every ask that reached the wire.
        assert_eq!(ctx.client.lock().unwrap().connects(), 1);
        drop(ctx);
        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

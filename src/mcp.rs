//! The MCP face: `roger mcp` is a session (it registers, writes each delivery
//! to its Store, then acks) that speaks the Model Context Protocol as
//! newline-delimited JSON-RPC on stdin and stdout. The session loop is the
//! adapter's (`session::serve`); this file adds only the protocol and the
//! tools, and gives the face no semantics of its own: every tool is one
//! request the socket protocol already has.

use crate::json::{self, obj, s, Value};
use crate::net;
use crate::session::{self, Face, McpHooks};
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
// no-poll: the one clock read is the bound on an inbox wait, never a scan.
use std::time::{Duration, Instant};

const VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_INBOX_LIMIT: i64 = 50;
const MAX_INBOX_LIMIT: i64 = 200;
const DEFAULT_WAIT_SECONDS: i64 = 10;
const MAX_WAIT_SECONDS: i64 = 60;
const DEFAULT_CLAIM_TTL_SECONDS: i64 = 900;
const DEFAULT_BOARD_WINDOW_SECONDS: i64 = 600;

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
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone())
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
    let bad = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
    let address = match flag(args, "--id") {
        Some(id) => id,
        None => match flag(args, "--id-env").and_then(|name| set(env, &name)) {
            Some(id) => id,
            None => match set(env, "ROGER_ADDRESS") {
                Some(id) => id,
                None => fresh()?,
            },
        },
    };
    let socket = match flag(args, "--socket").or_else(|| set(env, "ROGER_SOCKET")) {
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
    let (store, store_is_default) = match flag(args, "--store").or_else(|| set(env, "ROGER_STORE"))
    {
        Some(p) => (PathBuf::from(p), false),
        None => {
            let state = match set(env, "XDG_STATE_HOME") {
                Some(dir) => PathBuf::from(dir),
                None => match set(env, "HOME") {
                    Some(home) => PathBuf::from(home).join(".local").join("state"),
                    None => return Err(bad(
                        "expected --store, ROGER_STORE, XDG_STATE_HOME, or HOME, saw none of them"
                            .into(),
                    )),
                },
            };
            if address.is_empty()
                || address == "."
                || address == ".."
                || address.contains(['/', '\0'])
            {
                return Err(bad(format!(
                    "expected an Address usable as a file name for the default Store, saw {address:?}; pass --store"
                )));
            }
            (
                state
                    .join("roger")
                    .join("mcp")
                    .join(format!("{address}.store")),
                true,
            )
        }
    };
    Ok(Config {
        socket,
        address,
        store,
        store_is_default,
        label: flag(args, "--label"),
        declared: flag(args, "--declare"),
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
    Notification,
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
    match id {
        None => Parsed::Notification,
        Some(id @ (Value::Int(_) | Value::Str(_))) => Parsed::Request {
            id,
            method: method.to_string(),
            params: v.get("params").cloned().unwrap_or(Value::Obj(Vec::new())),
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

fn reply_ok(id: &Value, result: Value) -> Value {
    obj(vec![
        ("jsonrpc", s("2.0")),
        ("id", id.clone()),
        ("result", result),
    ])
}

fn reply_err(id: &Value, code: i64, message: &str) -> Value {
    obj(vec![
        ("jsonrpc", s("2.0")),
        ("id", id.clone()),
        (
            "error",
            obj(vec![("code", Value::Int(code)), ("message", s(message))]),
        ),
    ])
}

/// The protocol version to answer an `initialize` with.
pub fn negotiate(params: &Value) -> &'static str {
    let asked = params.get("protocolVersion").and_then(|v| v.as_str());
    VERSIONS
        .iter()
        .find(|v| Some(**v) == asked)
        .unwrap_or(&VERSIONS[0])
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

// ---- tools ----

/// (name, description, input schema)
const TOOLS: [(&str, &str, &str); 9] = [
    (
        "roger_send",
        "Send one Mail to an Address, or to every session with \"*\". Headers are opaque strings the sender attaches.",
        r#"{"type":"object","properties":{"to":{"type":"string"},"body":{"type":"string"},"mail":{"type":"string"},"headers":{"type":"object","additionalProperties":{"type":"string"}}},"required":["to","body"]}"#,
    ),
    (
        "roger_inbox",
        "Read the Mail in this session's Store after its read position, oldest first, and move the read position past what is returned. With none, wait up to wait_seconds for the next delivery.",
        r#"{"type":"object","properties":{"limit":{"type":"integer","minimum":1,"maximum":200},"wait_seconds":{"type":"integer","minimum":0,"maximum":60},"peek":{"type":"boolean"}}}"#,
    ),
    (
        "roger_board",
        "Read the Board: registered sessions and active claims. By default only sessions connected or seen within window_seconds.",
        r#"{"type":"object","properties":{"all":{"type":"boolean"},"window_seconds":{"type":"integer","minimum":0}}}"#,
    ),
    (
        "roger_label",
        "Set this session's display label. Its Address does not change.",
        r#"{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}"#,
    ),
    (
        "roger_claim",
        "Claim a key for this session, or refresh the claim it holds.",
        r#"{"type":"object","properties":{"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["key"]}"#,
    ),
    (
        "roger_renew",
        "Renew a claim this session holds.",
        r#"{"type":"object","properties":{"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["key","ttl_seconds"]}"#,
    ),
    (
        "roger_release",
        "Release a claim this session holds.",
        r#"{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}"#,
    ),
    (
        "roger_receipt",
        "Read one Mail's Receipt: its recipient, when it was Accepted, and the recipient's Record id once acked.",
        r#"{"type":"object","properties":{"mail":{"type":"string"}},"required":["mail"]}"#,
    ),
    (
        "roger_registered",
        "Say whether the coordinator confirmed this Register and no superseded notice arrived. Returns registered and sticky superseded (null or the notice). With wait_seconds, wait for confirmation or supersession. A superseded client should stop sending as this Address, tell its supervisor/user, and not automatically re-register; tools remain usable.",
        r#"{"type":"object","properties":{"wait_seconds":{"type":"integer","minimum":0,"maximum":60}}}"#,
    ),
];

fn tools_list() -> Value {
    let tools = TOOLS
        .iter()
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
    obj(vec![("tools", Value::Arr(tools))])
}

fn type_name(v: Option<&Value>) -> &'static str {
    match v {
        None => "nothing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "a boolean",
        Some(Value::Int(_)) => "an integer",
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

/// Everything a tool call needs.
struct Ctx {
    socket: PathBuf,
    me: String,
    inbox: Inbox,
    /// True once the coordinator's `registered` has arrived.
    confirmed: Mutex<(bool, Option<Value>)>,
    confirmed_cv: Condvar,
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
    let known: Vec<&str> = TOOLS.iter().map(|t| t.0).collect();
    if !known.contains(&name) {
        return Err(format!(
            "expected a tool among {}, saw {name}",
            known.join(", ")
        ));
    }
    let io = |e: io::Error| e.to_string();
    match name {
        "roger_send" => {
            let to = need_str(name, args, "to")?;
            let body = need_str(name, args, "body")?;
            let mail = match opt_str(name, args, "mail")? {
                Some(m) => m.to_string(),
                None => session::gen_uuid().map_err(io)?,
            };
            let mail =
                session::send_mail(&ctx.socket, &ctx.me, to, &mail, body, args.get("headers"))
                    .map_err(io)?;
            Ok(obj(vec![("mail", s(&mail))]))
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
            let reply = session::request(
                &ctx.socket,
                &obj(vec![
                    ("type", s("board")),
                    ("req", Value::Int(1)),
                    ("session", s(&ctx.me)),
                ]),
            )
            .map_err(io)?;
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
            let reply = session::request(
                &ctx.socket,
                &obj(vec![
                    ("type", s("relabel")),
                    ("req", Value::Int(1)),
                    ("session", s(&ctx.me)),
                    ("label", s(label)),
                ]),
            )
            .map_err(io)?;
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
            let reply = session::request(
                &ctx.socket,
                &obj(vec![
                    ("type", s("claim")),
                    ("req", Value::Int(1)),
                    ("op", s(op)),
                    ("key", s(key)),
                    ("owner", s(&ctx.me)),
                    ("ttl_ms", Value::Int(ttl_ms)),
                ]),
            )
            .map_err(io)?;
            Ok(without_envelope(reply))
        }
        _ => {
            let mail = need_str(name, args, "mail")?;
            let reply = session::request(
                &ctx.socket,
                &obj(vec![
                    ("type", s("receipt")),
                    ("req", Value::Int(1)),
                    ("mail", s(mail)),
                ]),
            )
            .map_err(io)?;
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

fn tool_result(text: &str, is_error: bool) -> Value {
    let mut pairs = vec![(
        "content",
        Value::Arr(vec![obj(vec![("type", s("text")), ("text", s(text))])]),
    )];
    if is_error {
        pairs.push(("isError", Value::Bool(true)));
    }
    obj(pairs)
}

// ---- the inbox ----

/// The reader's side of the Store: the entries after a read position kept in a file
/// beside it. A delivery's Store write makes `notify` fire, which is what
/// ends a wait for the next entry.
struct Inbox {
    store: PathBuf,
    pos_file: PathBuf,
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
            store: store.to_path_buf(),
            pos_file,
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
            let page = after(&entries(&self.store)?, consumed.as_deref(), limit);
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

/// The file beside a Store that holds how far its reader has consumed.
fn position_path(store: &Path) -> PathBuf {
    let mut p = store.as_os_str().to_owned();
    p.push(".cursor");
    PathBuf::from(p)
}

/// Every complete entry in the Store, oldest first. A trailing fragment with
/// no newline (a write in progress) is not an entry yet.
fn entries(store: &Path) -> io::Result<Vec<Value>> {
    let bytes = match std::fs::read(store) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("expected to read {}, saw {e}", store.display()),
            ))
        }
    };
    let end = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    Ok(String::from_utf8_lossy(&bytes[..end])
        .lines()
        .filter_map(|l| json::parse(l).ok())
        .filter(|e| e.get("record").is_some())
        .collect())
}

/// The entries after the one named `consumed`, at most `limit`, shaped as the
/// inbox returns them. A read position that names no entry (a Store that was
/// replaced) starts from the first. A `kind` or `headers` the entry lacks is
/// left out, never guessed.
fn after(all: &[Value], consumed: Option<&str>, limit: usize) -> Vec<Value> {
    let start = consumed
        .and_then(|c| {
            all.iter()
                .position(|e| e.get("record").and_then(|r| r.as_str()) == Some(c))
        })
        .map_or(0, |i| i + 1);
    all[start..]
        .iter()
        .take(limit)
        .map(|e| {
            Value::Obj(
                ["record", "mail", "from", "body", "kind", "headers"]
                    .iter()
                    .filter_map(|k| e.get(k).map(|v| (k.to_string(), v.clone())))
                    .collect(),
            )
        })
        .collect()
}

/// Write the read position so a crash leaves the old one or the new one: a temp
/// file, fsynced, renamed over, then the directory fsynced.
fn save_position(file: &Path, record: &str) -> io::Result<()> {
    use std::io::Write;
    let mut tmp = file.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(format!("{record}\n").as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, file)?;
    if let Some(dir) = file.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

// ---- the process ----

enum Event {
    Registered(net::Writer),
    Ended(io::Result<()>),
}

pub fn run(args: &[String]) -> io::Result<()> {
    let cfg = resolve(args, &|n| std::env::var(n).ok(), &session::gen_uuid)?;
    if !cfg.socket.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "expected a host daemon socket at {}, saw none",
                cfg.socket.display()
            ),
        ));
    }
    let ctx = Arc::new(Ctx {
        socket: cfg.socket.clone(),
        me: cfg.address.clone(),
        inbox: Inbox::open(&cfg.store)?,
        confirmed: Mutex::new((false, None)),
        confirmed_cv: Condvar::new(),
    });
    let gate = Arc::new(Mutex::new(false));
    let (tx, rx) = mpsc::channel::<Event>();

    let hooks = McpHooks {
        superseded: Box::new({
            let ctx = Arc::clone(&ctx);
            move |notice| {
                ctx.confirmed.lock().unwrap().1 = Some(notice);
                ctx.confirmed_cv.notify_all();
            }
        }),
        confirmed: Box::new({
            let ctx = Arc::clone(&ctx);
            move || {
                ctx.confirmed.lock().unwrap().0 = true;
                ctx.confirmed_cv.notify_all();
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
            move || ctx.inbox.notify()
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
        Ok(Event::Ended(Err(e))) => return Err(e),
        Ok(Event::Ended(Ok(()))) | Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "expected the host daemon to accept this session, saw its connection close",
            ))
        }
    };
    let cfg = Arc::new(cfg);
    let lost = {
        let (cfg, gate) = (Arc::clone(&cfg), Arc::clone(&gate));
        move || {
            let reason = match rx.recv() {
                Ok(Event::Ended(Err(e))) => e.to_string(),
                _ => "its connection closed".to_string(),
            };
            eprintln!("roger mcp: expected the host daemon connection to stay up, saw {reason}");
            finish(&cfg, &gate, None, 1)
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
    // Stdin closed: the client is gone, so this session is too. It leaves the
    // Board and later Broadcasts; Mail accepted for its Address stays pending.
    finish(&cfg, &gate, Some(&daemon), 0)
}

fn handle_line(ctx: &Arc<Ctx>, out: &net::Writer, line: &str) {
    match parse_message(line) {
        Parsed::Notification | Parsed::Ignore => {}
        Parsed::Bad { id, code, message } => {
            let _ = net::send(out, &reply_err(&id, code, &message));
        }
        Parsed::Request { id, method, params } => match method.as_str() {
            "initialize" => {
                let _ = net::send(out, &reply_ok(&id, initialize_result(&params)));
            }
            "ping" => {
                let _ = net::send(out, &reply_ok(&id, obj(vec![])));
            }
            "tools/list" => {
                let _ = net::send(out, &reply_ok(&id, tools_list()));
            }
            // Each call runs on its own thread, so a waiting inbox read never
            // holds up another call.
            "tools/call" => {
                let (ctx, out) = (Arc::clone(ctx), out.clone());
                std::thread::spawn(move || {
                    let result = match call_tool(&ctx, &params) {
                        Ok(v) => tool_result(&json::stringify(&v), false),
                        Err(reason) => tool_result(&reason, true),
                    };
                    let _ = net::send(&out, &reply_ok(&id, result));
                });
            }
            other => {
                let _ = net::send(
                    out,
                    &reply_err(
                        &id,
                        -32601,
                        &format!(
                            "expected initialize, ping, tools/list, or tools/call, saw {other}"
                        ),
                    ),
                );
            }
        },
    }
}

/// Leave. With a daemon writer, unregister first. A default Store with no
/// entries is removed with its position file: the gate is held so a delivery
/// cannot be written to a file about to go.
fn finish(cfg: &Config, gate: &Mutex<bool>, daemon: Option<&net::Writer>, code: i32) -> ! {
    let mut closing = gate.lock().unwrap();
    *closing = true;
    if let Some(w) = daemon {
        let _ = net::send(w, &obj(vec![("type", s("unregister"))]));
    }
    if cfg.store_is_default && entries(&cfg.store).is_ok_and(|e| e.is_empty()) {
        let _ = std::fs::remove_file(&cfg.store);
        let _ = std::fs::remove_file(position_path(&cfg.store));
    }
    std::process::exit(code)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            PathBuf::from(format!("/h/.local/state/roger/mcp/{c}.store"))
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
        assert_eq!(cfg.socket, PathBuf::from("/run/u/roger/daemon.sock"));
        assert_eq!(cfg.store, PathBuf::from("/st/roger/mcp/fresh-uuid.store"));
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
            Parsed::Notification
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
        let list = tools_list();
        let Some(Value::Arr(tools)) = list.get("tools") else {
            panic!("no tools");
        };
        assert_eq!(tools.len(), 9);
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
}

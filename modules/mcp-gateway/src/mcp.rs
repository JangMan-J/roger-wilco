//! Pure MCP `2026-07-28` JSON-RPC 2.0 protocol, SEP-2567 explicit session handles,
//! SEP-2575 per-request `_meta`, and SEP-3415 events (`events/list`, `events/poll`, `events/stream`).

use crate::daemon;
use crate::durlog::{blocking_io, gen_uuid, read_store_entries, shape_mail_entry};
use crate::session::{now_ms, GatewaySession, RegisterParams, SessionManager};
use serde_json::{Map, Value};
use std::io;
use std::sync::Arc;
use std::time::Duration;

pub const VERSION: &str = "2026-07-28";
pub const DISCOVER_TTL_MS: i64 = 300_000;
const DEFAULT_INBOX_LIMIT: i64 = 50;
const MAX_INBOX_LIMIT: i64 = 200;
const DEFAULT_WAIT_SECONDS: i64 = 10;
const MAX_WAIT_SECONDS: i64 = 60;
const DEFAULT_CLAIM_TTL_SECONDS: i64 = 900;
const DEFAULT_BOARD_WINDOW_SECONDS: i64 = 600;
pub const DEFAULT_POLL_MAX_EVENTS: usize = 100;
pub const MAX_POLL_MAX_EVENTS: usize = 500;
pub const DEFAULT_POLL_NEXT_MS: i64 = 5_000;
pub const DEFAULT_HEARTBEAT_MS: i64 = 30_000;
pub const MIN_HEARTBEAT_MS: i64 = 1_000;
pub const MAX_HEARTBEAT_MS: i64 = 60_000;

/// One parsed JSON-RPC 2.0 payload.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification,
    Ignore,
    Bad {
        id: Value,
        code: i64,
        message: String,
    },
}

pub fn parse_message(body: &str) -> Parsed {
    let bad = |id: Value, code: i64, message: String| Parsed::Bad { id, code, message };
    let v = match serde_json::from_str::<Value>(body) {
        Ok(v) => v,
        Err(e) => {
            return bad(
                Value::Null,
                -32700,
                format!("expected one JSON value per line, saw {e}"),
            )
        }
    };
    if !v.is_object() {
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
    let Some(method_str) = method.as_str() else {
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
        Some(id @ Value::String(_)) => Parsed::Request {
            id,
            method: method_str.to_string(),
            params: v
                .get("params")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
        },
        Some(Value::Number(n)) if n.is_i64() || n.is_u64() => Parsed::Request {
            id: Value::Number(n),
            method: method_str.to_string(),
            params: v
                .get("params")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
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

#[derive(Debug, Clone)]
pub struct RpcErr {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcErr {
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    pub fn header_mismatch(message: impl Into<String>) -> Self {
        Self {
            code: -32020,
            message: message.into(),
            data: None,
        }
    }

    pub fn unsupported_version(requested: &str) -> Self {
        Self {
            code: -32022,
            message: "Unsupported protocol version".into(),
            data: Some(serde_json::json!({
                "supported": [VERSION],
                "requested": requested,
            })),
        }
    }

    pub fn not_found(kind: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32023,
            message: message.into(),
            data: Some(serde_json::json!({ "kind": kind })),
        }
    }

    pub fn unsupported(feature: &str, value: &str, message: impl Into<String>) -> Self {
        Self {
            code: -32026,
            message: message.into(),
            data: Some(serde_json::json!({ "feature": feature, "value": value })),
        }
    }
}

pub fn reply_ok(id: &Value, result: Value) -> Value {
    let mut m = Map::new();
    m.insert("jsonrpc".into(), Value::String("2.0".into()));
    m.insert("id".into(), id.clone());
    m.insert("result".into(), result);
    Value::Object(m)
}

pub fn reply_err(id: &Value, code: i64, message: &str) -> Value {
    reply_err_data(id, code, message, None)
}

pub fn reply_err_data(id: &Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut err = Map::new();
    err.insert("code".into(), Value::Number(code.into()));
    err.insert("message".into(), Value::String(message.into()));
    if let Some(d) = data {
        err.insert("data".into(), d);
    }
    let mut m = Map::new();
    m.insert("jsonrpc".into(), Value::String("2.0".into()));
    m.insert("id".into(), id.clone());
    m.insert("error".into(), Value::Object(err));
    Value::Object(m)
}

pub fn server_info_meta() -> Value {
    serde_json::json!({
        "io.modelcontextprotocol/serverInfo": {
            "name": "roger-mcp-gateway",
            "version": env!("CARGO_PKG_VERSION"),
        }
    })
}

pub fn validate_meta(params: &Value) -> Result<&str, RpcErr> {
    if !params.is_object() {
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
    let Some(meta_obj) = meta.as_object() else {
        return Err(RpcErr::invalid_params(format!(
            "expected params._meta to be an object, saw {}",
            type_name(Some(meta))
        )));
    };
    let pv = match meta_obj.get("io.modelcontextprotocol/protocolVersion") {
        Some(Value::String(v)) => v.as_str(),
        other => {
            return Err(RpcErr::invalid_params(format!(
            "expected _meta[\"io.modelcontextprotocol/protocolVersion\"] to be a string, saw {}",
            type_name(other)
        )))
        }
    };
    match meta_obj.get("io.modelcontextprotocol/clientCapabilities") {
        Some(Value::Object(_)) => {}
        other => {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/clientCapabilities\"] to be an object, saw {}",
                type_name(other)
            )))
        }
    }
    if let Some(info) = meta_obj.get("io.modelcontextprotocol/clientInfo") {
        if !info.is_object() {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/clientInfo\"] to be an object, saw {}",
                type_name(Some(info))
            )));
        }
    }
    if let Some(level) = meta_obj.get("io.modelcontextprotocol/logLevel") {
        if !level.is_string() {
            return Err(RpcErr::invalid_params(format!(
                "expected _meta[\"io.modelcontextprotocol/logLevel\"] to be a string, saw {}",
                type_name(Some(level))
            )));
        }
    }
    if pv != VERSION {
        return Err(RpcErr::unsupported_version(pv));
    }
    Ok(pv)
}

pub fn discover_result() -> Value {
    serde_json::json!({
        "resultType": "complete",
        "supportedVersions": [VERSION],
        "capabilities": {
            "tools": { "listChanged": false },
            "extensions": {
                "io.modelcontextprotocol/events": { "listChanged": false }
            }
        },
        "_meta": server_info_meta(),
        "ttlMs": DISCOVER_TTL_MS,
        "cacheScope": "public",
    })
}

const SESSION_TOOLS: [(&str, &str, &str); 9] = [
    (
        "roger_send",
        "Send one Mail to an Address, or to every session with \"*\". Headers are opaque strings the sender attaches.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"to":{"type":"string"},"body":{"type":"string"},"mail":{"type":"string"},"headers":{"type":"object","additionalProperties":{"type":"string"}}},"required":["session","to","body"]}"#,
    ),
    (
        "roger_inbox",
        "Read the Mail in this session's Store after its read position, oldest first, and move the read position past what is returned. With none, wait up to wait_seconds for the next delivery.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":200},"wait_seconds":{"type":"integer","minimum":0,"maximum":60},"peek":{"type":"boolean"}},"required":["session"]}"#,
    ),
    (
        "roger_board",
        "Read the Board: registered sessions and active claims. By default only sessions connected or seen within window_seconds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"all":{"type":"boolean"},"window_seconds":{"type":"integer","minimum":0}},"required":["session"]}"#,
    ),
    (
        "roger_label",
        "Set this session's display label. Its Address does not change.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"label":{"type":"string"}},"required":["session","label"]}"#,
    ),
    (
        "roger_claim",
        "Claim a key for this session, or refresh the claim it holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["session","key"]}"#,
    ),
    (
        "roger_renew",
        "Renew a claim this session holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"},"ttl_seconds":{"type":"integer","minimum":1}},"required":["session","key","ttl_seconds"]}"#,
    ),
    (
        "roger_release",
        "Release a claim this session holds.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"key":{"type":"string"}},"required":["session","key"]}"#,
    ),
    (
        "roger_receipt",
        "Read one Mail's Receipt: its recipient, when it was Accepted, and the recipient's Record id once acked.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"mail":{"type":"string"}},"required":["session","mail"]}"#,
    ),
    (
        "roger_registered",
        "Say whether the coordinator confirmed this Register and no superseded notice arrived. Returns registered and sticky superseded (null or the notice). With wait_seconds, wait for confirmation or supersession. A superseded client should stop sending as this Address, tell its supervisor/user, and not automatically re-register; tools remain usable.",
        r#"{"type":"object","properties":{"session":{"type":"string"},"wait_seconds":{"type":"integer","minimum":0,"maximum":60}},"required":["session"]}"#,
    ),
];

pub fn tools_list(groups: bool) -> Value {
    let mut tools = vec![
        serde_json::json!({
            "name": "roger_register",
            "description": "Register or reattach a Roger session and return an opaque durable `session` handle and `address`. The handle survives across HTTP requests; call `roger_unregister` to close it, or re-register the same `id` to replace an earlier handle.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "label": {"type": "string"},
                    "declare": {"type": "string"},
                    "watch": {"type": "boolean"}
                },
                "additionalProperties": false
            }
        }),
        serde_json::json!({
            "name": "roger_unregister",
            "description": "Unregister and close an explicit Roger session handle.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session": {"type": "string"}
                },
                "required": ["session"],
                "additionalProperties": false
            }
        }),
    ];

    for (name, description, schema) in &SESSION_TOOLS {
        let mut t = Map::new();
        t.insert("name".into(), Value::String((*name).into()));
        t.insert("description".into(), Value::String((*description).into()));
        t.insert(
            "inputSchema".into(),
            serde_json::from_str::<Value>(schema).expect("a tool schema is valid JSON"),
        );
        tools.push(Value::Object(t));
    }

    if groups {
        // tools[2] is roger_send
        tools[2]["inputSchema"]["properties"]["group"] =
            serde_json::json!({"type":"string","minLength":1});
        tools[2]["inputSchema"]["required"] = serde_json::json!(["session", "body"]);
        tools[2]["inputSchema"]["oneOf"] = serde_json::json!([
            {"required":["to"],"not":{"required":["group"]}},
            {"required":["group"],"not":{"required":["to"]}}
        ]);
        for (name, description, schema) in [
            (
                "roger_subscribe",
                "Subscribe this session to a Group. Optional `after` sets the article position (>= 0); `latest` starts at the Group's current high-water mark.",
                serde_json::json!({"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string","minLength":1},"after":{"type":"integer","minimum":0},"latest":{"type":"boolean"}},"required":["session","group"]}),
            ),
            (
                "roger_unsubscribe",
                "Unsubscribe this session from a Group while keeping its durable position.",
                serde_json::json!({"type":"object","properties":{"session":{"type":"string"},"group":{"type":"string","minLength":1}},"required":["session","group"]}),
            ),
            (
                "roger_groups",
                "List known Groups with high-water marks, subscriber counts, and this session's subscription state and position.",
                serde_json::json!({"type":"object","properties":{"session":{"type":"string"}},"required":["session"],"additionalProperties":false}),
            ),
        ] {
            tools.push(serde_json::json!({
                "name": name,
                "description": description,
                "inputSchema": schema,
            }));
        }
    }

    serde_json::json!({
        "resultType": "complete",
        "tools": tools,
        "ttlMs": DISCOVER_TTL_MS,
        "cacheScope": "public",
        "_meta": server_info_meta(),
    })
}

pub fn tool_result(text: &str, is_error: bool) -> Value {
    let mut out = Map::new();
    out.insert("resultType".into(), Value::String("complete".into()));
    out.insert(
        "content".into(),
        serde_json::json!([{"type": "text", "text": text}]),
    );
    if is_error {
        out.insert("isError".into(), Value::Bool(true));
    }
    out.insert("_meta".into(), server_info_meta());
    Value::Object(out)
}

fn type_name(v: Option<&Value>) -> &'static str {
    match v {
        None => "nothing",
        Some(Value::Null) => "null",
        Some(Value::Bool(_)) => "a boolean",
        Some(Value::Number(n)) if n.is_i64() || n.is_u64() => "an integer",
        Some(Value::Number(_)) => "a number",
        Some(Value::String(_)) => "a string",
        Some(Value::Array(_)) => "an array",
        Some(Value::Object(_)) => "an object",
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
        Some(Value::String(t)) => Ok(t.as_str()),
        other => Err(wrong(tool, name, "a string", other)),
    }
}

fn opt_str<'a>(tool: &str, args: &'a Value, name: &str) -> Result<Option<&'a str>, String> {
    match args.get(name) {
        None => Ok(None),
        Some(Value::String(t)) => Ok(Some(t.as_str())),
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
        Some(Value::Number(num)) => {
            if let Some(n) = num.as_i64() {
                if n >= min && (n <= max || clamp) {
                    Ok(n.min(max))
                } else {
                    Err(format!(
                        "{tool}: expected `{name}` to be an integer from {min} to {max}, saw {n}"
                    ))
                }
            } else if let Some(u) = num.as_u64() {
                if clamp {
                    Ok(max)
                } else {
                    Err(format!(
                        "{tool}: expected `{name}` to be an integer from {min} to {max}, saw {u}"
                    ))
                }
            } else {
                Err(wrong(tool, name, "an integer", args.get(name)))
            }
        }
        other => Err(wrong(tool, name, "an integer", other)),
    }
}

fn seconds_to_ms(tool: &str, name: &str, secs: i64) -> Result<i64, String> {
    secs.checked_mul(1000)
        .ok_or_else(|| format!("{tool}: expected `{name}` to be a smaller number, saw {secs}"))
}

fn without_envelope(reply: Value) -> Value {
    match reply {
        Value::Object(mut pairs) => {
            pairs.shift_remove("type");
            pairs.shift_remove("req");
            Value::Object(pairs)
        }
        other => other,
    }
}

fn present(row: &Value, now: i64, window_ms: i64) -> bool {
    if row.get("connected") == Some(&Value::Bool(true)) {
        return true;
    }
    match row.get("last_seen_ms").and_then(Value::as_i64) {
        Some(seen) => seen > 0 && now.saturating_sub(seen) <= window_ms,
        None => false,
    }
}

fn check_no_extra_keys(
    tool: &str,
    args: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), String> {
    for k in args.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(format!("{tool}: unexpected argument `{k}`"));
        }
    }
    Ok(())
}

/// Dispatch one `tools/call` invocation concurrently via `SessionManager`.
pub async fn call_tool(sessions: &Arc<SessionManager>, params: &Value) -> Result<Value, String> {
    let name = match params.get("name") {
        Some(Value::String(n)) => n.as_str(),
        other => return Err(wrong("tools/call", "name", "a string", other)),
    };
    let empty = Value::Object(Map::new());
    let args = match params.get("arguments") {
        None => &empty,
        Some(a @ Value::Object(_)) => a,
        other => return Err(wrong(name, "arguments", "an object", other)),
    };
    let mut known: Vec<&str> = vec!["roger_register", "roger_unregister"];
    known.extend(SESSION_TOOLS.iter().map(|t| t.0));
    if sessions.groups() {
        known.extend(["roger_subscribe", "roger_unsubscribe", "roger_groups"]);
    }
    if !known.contains(&name) {
        return Err(format!(
            "expected a tool among {}, saw {name}",
            known.join(", ")
        ));
    }
    let io_err = |e: io::Error| e.to_string();

    if name == "roger_register" {
        if let Some(m) = args.as_object() {
            check_no_extra_keys(name, m, &["id", "label", "declare", "watch"])?;
        }
        let address = opt_str(name, args, "id")?.map(str::to_string);
        let label = opt_str(name, args, "label")?.map(str::to_string);
        let declared = opt_str(name, args, "declare")?.map(str::to_string);
        let watch = opt_bool(name, args, "watch")?;
        let sess = sessions
            .register(RegisterParams {
                address,
                label,
                declared,
                watch,
            })
            .await
            .map_err(io_err)?;
        return Ok(serde_json::json!({
            "session": sess.session_id,
            "address": sess.address,
        }));
    }

    if name == "roger_unregister" {
        if let Some(m) = args.as_object() {
            check_no_extra_keys(name, m, &["session"])?;
        }
        let handle = need_str(name, args, "session")?;
        let Some(address) = sessions.unregister(handle).await else {
            return Err(format!(
                "{name}: unknown or expired session handle {handle}; call roger_register"
            ));
        };
        return Ok(serde_json::json!({
            "ok": true,
            "address": address,
        }));
    }

    let handle = need_str(name, args, "session")?;
    let Some(session) = sessions.get(handle) else {
        return Err(format!(
            "{name}: unknown or expired session handle {handle}; call roger_register"
        ));
    };
    call_session_tool(&session, name, args).await
}

/// Dispatch one session-scoped tool against `session` (usable directly in tests too).
pub async fn call_session_tool(
    session: &GatewaySession,
    name: &str,
    args: &Value,
) -> Result<Value, String> {
    let io_err = |e: io::Error| e.to_string();
    match name {
        "roger_send" => {
            let group = if session.groups {
                opt_str(name, args, "group")?
            } else {
                None
            };
            if session.groups && (args.get("to").is_some() == args.get("group").is_some()) {
                return Err("roger_send: expected either to or group, saw both or neither".into());
            }
            if let Some(group) = group {
                if group.is_empty() {
                    return Err("expected non-empty group, saw empty string".into());
                }
                let body = need_str(name, args, "body")?;
                let mail = match opt_str(name, args, "mail")? {
                    Some(m) => m.to_owned(),
                    None => blocking_io(gen_uuid).map_err(io_err)?,
                };
                let mut req = serde_json::json!({"type":"send","req":1,"from":session.address,"group":group,"mail":mail,"body":body});
                if let Some(h) = args.get("headers") {
                    req["headers"] = h.clone();
                }
                return daemon::request(&session.socket, &req)
                    .await
                    .map(without_envelope)
                    .map_err(io_err);
            }
            let to = need_str(name, args, "to")?;
            let body = need_str(name, args, "body")?;
            let mail = match opt_str(name, args, "mail")? {
                Some(m) => m.to_string(),
                None => blocking_io(gen_uuid).map_err(io_err)?,
            };
            let accepted = daemon::send_mail(
                &session.socket,
                &session.address,
                to,
                &mail,
                body,
                args.get("headers"),
            )
            .await
            .map_err(io_err)?;
            let mut out = Map::new();
            out.insert("mail".into(), Value::String(accepted));
            Ok(Value::Object(out))
        }
        "roger_subscribe" | "roger_unsubscribe" | "roger_groups" => {
            if !session.groups {
                return Err(format!("tool {name} requires --groups"));
            }
            let op = name.strip_prefix("roger_").unwrap();
            let mut req = serde_json::json!({"type":op,"req":1,"session":session.address});
            if op != "groups" {
                let group = need_str(name, args, "group")?;
                if group.is_empty() {
                    return Err("expected non-empty group, saw empty string".into());
                }
                req["group"] = Value::String(group.into());
            } else if let Some(m) = args.as_object() {
                check_no_extra_keys(name, m, &["session"])?;
            }
            if op == "subscribe" {
                if args.get("after").is_some() {
                    req["after"] = opt_int(name, args, "after", 0, (0, i64::MAX), false)?.into();
                }
                if args.get("latest").is_some() {
                    req["latest"] = opt_bool(name, args, "latest")?.into();
                }
            }
            daemon::request(&session.socket, &req)
                .await
                .map(without_envelope)
                .map_err(io_err)
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
            let (mail, consumed) = session
                .inbox
                .read(limit as usize, Duration::from_secs(wait as u64), peek)
                .await
                .map_err(io_err)?;
            let mut out = Map::new();
            out.insert("mail".into(), Value::Array(mail));
            out.insert("cursor".into(), consumed.map_or(Value::Null, Value::String));
            Ok(Value::Object(out))
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
            let (registered, superseded) = session
                .registered_status(Duration::from_secs(wait as u64))
                .await;
            let mut out = Map::new();
            out.insert("registered".into(), Value::Bool(registered));
            out.insert("superseded".into(), superseded.unwrap_or(Value::Null));
            Ok(Value::Object(out))
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
            let mut req = Map::new();
            req.insert("type".into(), Value::String("board".into()));
            req.insert("req".into(), Value::Number(1.into()));
            req.insert("session".into(), Value::String(session.address.clone()));
            let reply = daemon::request(&session.socket, &Value::Object(req))
                .await
                .map_err(io_err)?;
            let now = reply.get("now_ms").and_then(Value::as_i64).unwrap_or(0);
            let sessions = match reply.get("sessions") {
                Some(Value::Array(rows)) => rows
                    .iter()
                    .filter(|r| all || present(r, now, window_ms))
                    .cloned()
                    .collect(),
                _ => Vec::new(),
            };
            let claims = match reply.get("claims") {
                Some(Value::Array(rows)) => rows.clone(),
                _ => Vec::new(),
            };
            let mut out = Map::new();
            out.insert("me".into(), Value::String(session.address.clone()));
            out.insert("sessions".into(), Value::Array(sessions));
            out.insert("claims".into(), Value::Array(claims));
            out.insert("now_ms".into(), Value::Number(now.into()));
            Ok(Value::Object(out))
        }
        "roger_label" => {
            let label = need_str(name, args, "label")?;
            let mut req = Map::new();
            req.insert("type".into(), Value::String("relabel".into()));
            req.insert("req".into(), Value::Number(1.into()));
            req.insert("session".into(), Value::String(session.address.clone()));
            req.insert("label".into(), Value::String(label.into()));
            let reply = daemon::request(&session.socket, &Value::Object(req))
                .await
                .map_err(io_err)?;
            let sess_out = daemon::field(&reply, "session")
                .map_err(io_err)?
                .to_string();
            let label_out = daemon::field(&reply, "label").map_err(io_err)?.to_string();
            let mut out = Map::new();
            out.insert("session".into(), Value::String(sess_out));
            out.insert("label".into(), Value::String(label_out));
            Ok(Value::Object(out))
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
            let mut req = Map::new();
            req.insert("type".into(), Value::String("claim".into()));
            req.insert("req".into(), Value::Number(1.into()));
            req.insert("op".into(), Value::String(op.into()));
            req.insert("key".into(), Value::String(key.into()));
            req.insert("owner".into(), Value::String(session.address.clone()));
            req.insert("ttl_ms".into(), Value::Number(ttl_ms.into()));
            let reply = daemon::request(&session.socket, &Value::Object(req))
                .await
                .map_err(io_err)?;
            Ok(without_envelope(reply))
        }
        "roger_receipt" => {
            let mail = need_str(name, args, "mail")?;
            let mut req = Map::new();
            req.insert("type".into(), Value::String("receipt".into()));
            req.insert("req".into(), Value::Number(1.into()));
            req.insert("mail".into(), Value::String(mail.into()));
            let reply = daemon::request(&session.socket, &Value::Object(req))
                .await
                .map_err(io_err)?;
            Ok(without_envelope(reply))
        }
        _ => Err(format!("unknown tool {name}")),
    }
}

// ---- SEP-3415 events ----

pub fn format_iso8601_utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let day_secs = secs.rem_euclid(86_400);
    let hour = day_secs / 3600;
    let min = (day_secs % 3600) / 60;
    let sec = day_secs % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

pub fn events_list_result() -> Value {
    serde_json::json!({
        "resultType": "complete",
        "events": [
            {
                "name": "roger.mail",
                "description": "Fires when Mail is durably stored in the session Store.",
                "delivery": ["push", "poll"],
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session": {"type": "string"},
                        "group": {"type": "string"},
                        "kind": {"type": "string", "enum": ["directed", "broadcast", "group"]}
                    },
                    "required": ["session"],
                    "additionalProperties": false
                },
                "payloadSchema": {
                    "type": "object",
                    "properties": {
                        "record": {"type": "string"},
                        "mail": {"type": "string"},
                        "from": {"type": "string"},
                        "group": {"type": "string"},
                        "article": {"type": "integer"},
                        "body": {"type": "string"},
                        "kind": {"type": "string", "enum": ["directed", "broadcast", "group"]},
                        "headers": {"type": "object", "additionalProperties": {"type": "string"}}
                    },
                    "required": ["record", "mail", "from", "body", "kind", "headers"]
                }
            },
            {
                "name": "roger.board",
                "description": "Fires on committed Board session and claim changes.",
                "delivery": ["push"],
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session": {"type": "string"}
                    },
                    "required": ["session"],
                    "additionalProperties": false
                },
                "payloadSchema": {
                    "type": "object",
                    "properties": {
                        "event": {"type": "string"},
                        "session": {"type": "string"},
                        "host": {"type": "string"},
                        "label": {"type": "string"},
                        "declared": {"type": "string"},
                        "key": {"type": "string"},
                        "owner": {"type": "string"},
                        "expires_ms": {"type": "integer"},
                        "at_ms": {"type": "integer"}
                    },
                    "required": ["event"]
                }
            },
            {
                "name": "roger.registration",
                "description": "Fires when the session Register is confirmed or superseded.",
                "delivery": ["push"],
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session": {"type": "string"}
                    },
                    "required": ["session"],
                    "additionalProperties": false
                },
                "payloadSchema": {
                    "type": "object",
                    "properties": {
                        "event": {"type": "string", "enum": ["registered", "superseded"]},
                        "session": {"type": "string"},
                        "host": {"type": "string"},
                        "at_ms": {"type": "integer"}
                    },
                    "required": ["event", "session"]
                }
            }
        ],
        "ttlMs": DISCOVER_TTL_MS,
        "cacheScope": "public",
        "_meta": server_info_meta(),
    })
}

#[derive(Clone, Default)]
pub struct MailFilter {
    pub group: Option<String>,
    pub kind: Option<String>,
}

impl MailFilter {
    pub fn matches(&self, entry: &Value) -> bool {
        if let Some(g) = &self.group {
            if entry.get("group").and_then(Value::as_str) != Some(g.as_str()) {
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

pub struct ValidatedEventReq {
    pub name: String,
    pub session: Arc<GatewaySession>,
    pub filter: MailFilter,
    pub cursor: Option<String>,
    pub max_age_ms: Option<i64>,
    pub max_events: usize,
    pub heartbeat_ms: i64,
}

fn parse_event_args(
    sessions: &Arc<SessionManager>,
    event_name: &str,
    params: &Value,
) -> Result<(Arc<GatewaySession>, MailFilter), RpcErr> {
    let args = match params.get("arguments") {
        Some(Value::Object(m)) => m,
        other => {
            return Err(RpcErr::invalid_params(format!(
                "expected arguments to be an object carrying session, saw {}",
                type_name(other)
            )))
        }
    };
    let mut handle: Option<&str> = None;
    let mut filter = MailFilter::default();
    for (k, v) in args {
        match (event_name, k.as_str()) {
            (_, "session") => {
                let Some(s) = v.as_str() else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.session to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                handle = Some(s);
            }
            ("roger.mail", "group") => {
                let Some(g) = v.as_str() else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.group to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                filter.group = Some(g.to_string());
            }
            ("roger.mail", "kind") => {
                let Some(kind) = v.as_str() else {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.kind to be a string, saw {}",
                        type_name(Some(v))
                    )));
                };
                if !matches!(kind, "directed" | "broadcast" | "group") {
                    return Err(RpcErr::invalid_params(format!(
                        "expected arguments.kind to be directed, broadcast, or group, saw {kind}"
                    )));
                }
                filter.kind = Some(kind.to_string());
            }
            (_, unknown) => {
                return Err(RpcErr::invalid_params(format!(
                    "unexpected argument {unknown} for event {event_name}"
                )));
            }
        }
    }
    let Some(handle) = handle else {
        return Err(RpcErr::invalid_params(
            "expected arguments.session to be a string, saw nothing",
        ));
    };
    let Some(session) = sessions.get(handle) else {
        return Err(RpcErr::not_found(
            "session",
            format!("unknown or expired session handle {handle}"),
        ));
    };
    Ok((session, filter))
}

pub fn validate_event_req(
    sessions: &Arc<SessionManager>,
    params: &Value,
    is_poll: bool,
) -> Result<ValidatedEventReq, RpcErr> {
    let name = match params.get("name") {
        Some(Value::String(n)) => n.clone(),
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
    let (session, filter) = parse_event_args(sessions, &name, params)?;
    if is_poll && name != "roger.mail" {
        return Err(RpcErr::unsupported(
            "deliveryMode",
            "poll",
            format!("event {name} supports only push delivery"),
        ));
    }
    let cursor = match params.get("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(c)) => Some(c.clone()),
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected cursor to be a string or null, saw {}",
                type_name(Some(other))
            )))
        }
    };
    let max_age_ms = match params.get("maxAgeMs") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) if n.as_i64().is_some_and(|v| v >= 0) => n.as_i64(),
        Some(Value::Number(n)) => {
            return Err(RpcErr::invalid_params(format!(
                "expected maxAgeMs >= 0, saw {n}"
            )))
        }
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected maxAgeMs to be an integer, saw {}",
                type_name(Some(other))
            )))
        }
    };
    let max_events = match params.get("maxEvents") {
        None | Some(Value::Null) => DEFAULT_POLL_MAX_EVENTS,
        Some(Value::Number(n)) if n.as_i64().is_some_and(|v| v >= 1) => {
            (n.as_i64().unwrap() as usize).min(MAX_POLL_MAX_EVENTS)
        }
        Some(Value::Number(n)) => {
            return Err(RpcErr::invalid_params(format!(
                "expected maxEvents >= 1, saw {n}"
            )))
        }
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected maxEvents to be an integer, saw {}",
                type_name(Some(other))
            )))
        }
    };
    let heartbeat_ms = match params.get("heartbeatMs") {
        None | Some(Value::Null) => DEFAULT_HEARTBEAT_MS,
        Some(Value::Number(n)) if n.as_i64().is_some_and(|v| v >= 1) => n
            .as_i64()
            .unwrap()
            .clamp(MIN_HEARTBEAT_MS, MAX_HEARTBEAT_MS),
        Some(Value::Number(n)) => {
            return Err(RpcErr::invalid_params(format!(
                "expected heartbeatMs >= 1, saw {n}"
            )))
        }
        Some(other) => {
            return Err(RpcErr::invalid_params(format!(
                "expected heartbeatMs to be an integer, saw {}",
                type_name(Some(other))
            )))
        }
    };
    Ok(ValidatedEventReq {
        name,
        session,
        filter,
        cursor,
        max_age_ms,
        max_events,
        heartbeat_ms,
    })
}

pub fn record_of(e: &Value) -> &str {
    e.get("record").and_then(Value::as_str).unwrap_or("")
}

pub fn tail_cursor(all: &[Value]) -> String {
    all.last()
        .map(|e| record_of(e).to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "r-0".to_string())
}

pub fn resolve_mail_window(
    all: &[Value],
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
    } else if let Some(idx) = all.iter().position(|e| record_of(e) == cur) {
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

pub fn mail_occurrence(e: &Value, include_cursor: bool) -> Value {
    let ts_ms = e.get("stored_at_ms").and_then(Value::as_i64).unwrap_or(0);
    let event_id = e
        .get("mail")
        .and_then(Value::as_str)
        .unwrap_or_else(|| record_of(e));
    let mut out = Map::new();
    out.insert("eventId".into(), Value::String(event_id.to_string()));
    out.insert("name".into(), Value::String("roger.mail".into()));
    out.insert("timestamp".into(), Value::String(format_iso8601_utc(ts_ms)));
    out.insert("data".into(), shape_mail_entry(e));
    if include_cursor {
        out.insert("cursor".into(), Value::String(record_of(e).to_string()));
    }
    Value::Object(out)
}

pub async fn events_poll(sessions: &Arc<SessionManager>, params: &Value) -> Result<Value, RpcErr> {
    let req = validate_event_req(sessions, params, true)?;
    let store_path = req.session.store_path.clone();
    let all = blocking_io(move || read_store_entries(&store_path)).map_err(|e| RpcErr {
        code: -32603,
        message: e.to_string(),
        data: None,
    })?;
    let now = now_ms();
    let (start, truncated, start_cursor) =
        resolve_mail_window(&all, req.cursor.as_deref(), req.max_age_ms, now);

    let mut matched = Vec::new();
    let mut last_idx = None;
    let mut has_more = false;
    for (offset, entry) in all[start..].iter().enumerate() {
        if !req.filter.matches(entry) {
            continue;
        }
        if matched.len() == req.max_events {
            has_more = true;
            break;
        }
        matched.push(mail_occurrence(entry, false));
        last_idx = Some(start + offset);
    }
    let next_cursor = if has_more {
        last_idx
            .map(|i| record_of(&all[i]).to_string())
            .unwrap_or(start_cursor)
    } else if start < all.len() {
        tail_cursor(&all)
    } else {
        start_cursor
    };

    Ok(serde_json::json!({
        "resultType": "complete",
        "events": matched,
        "cursor": next_cursor,
        "truncated": truncated,
        "hasMore": has_more,
        "nextPollMs": DEFAULT_POLL_NEXT_MS,
        "_meta": server_info_meta(),
    }))
}

pub fn format_notification(method: &str, mut params: Map<String, Value>, sub_id: &Value) -> String {
    let mut meta = Map::new();
    meta.insert(
        "io.modelcontextprotocol/subscriptionId".into(),
        sub_id.clone(),
    );
    params.insert("_meta".into(), Value::Object(meta));
    serde_json::to_string(&serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": Value::Object(params),
    }))
    .expect("JSON-RPC notification serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_validation_and_discover_follow_2026_07_28() {
        let d = discover_result();
        assert_eq!(d["resultType"], "complete");
        assert_eq!(d["supportedVersions"], serde_json::json!(["2026-07-28"]));
        assert_eq!(d["ttlMs"], DISCOVER_TTL_MS);
        assert_eq!(d["cacheScope"], "public");
        assert!(d["capabilities"]["extensions"]["io.modelcontextprotocol/events"].is_object());

        assert_eq!(
            validate_meta(&serde_json::json!({})).unwrap_err().code,
            -32602
        );
        let bad = validate_meta(&serde_json::json!({
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2025-11-25",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }))
        .unwrap_err();
        assert_eq!(bad.code, -32022);
        assert_eq!(
            bad.data.unwrap()["supported"],
            serde_json::json!(["2026-07-28"])
        );

        let valid_params = serde_json::json!({
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        });
        assert_eq!(validate_meta(&valid_params).unwrap(), "2026-07-28");
    }

    #[test]
    fn tools_list_includes_explicit_handles_and_required_session() {
        let list = tools_list(false);
        assert_eq!(list["resultType"], "complete");
        assert_eq!(list["ttlMs"], DISCOVER_TTL_MS);
        assert_eq!(list["cacheScope"], "public");
        let tools = list["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 11);
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
                "roger_register",
                "roger_unregister",
                "roger_send",
                "roger_inbox",
                "roger_board",
                "roger_label",
                "roger_claim",
                "roger_renew",
                "roger_release",
                "roger_receipt",
                "roger_registered",
            ]
        );
        for t in &tools[1..] {
            let req = t["inputSchema"]["required"].as_array().unwrap();
            assert!(req.contains(&Value::String("session".into())), "{t}");
        }

        let g_list = tools_list(true);
        let g_tools = g_list["tools"].as_array().unwrap();
        assert_eq!(g_tools.len(), 14);
    }

    #[test]
    fn message_classification_and_validation_errors() {
        assert!(matches!(
            parse_message(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#),
            Parsed::Notification
        ));
        match parse_message("{broken") {
            Parsed::Bad { code, .. } => assert_eq!(code, -32700),
            other => panic!("{other:?}"),
        }
        let args = serde_json::from_str::<Value>(r#"{"to":5,"limit":0,"n":999}"#).unwrap();
        assert_eq!(
            need_str("roger_send", &args, "to").unwrap_err(),
            "roger_send: expected `to` to be a string, saw an integer"
        );
        assert_eq!(opt_int("t", &args, "n", 1, (1, 200), true), Ok(200));
        assert!(opt_int("t", &args, "n", 1, (1, 200), false).is_err());
    }
}

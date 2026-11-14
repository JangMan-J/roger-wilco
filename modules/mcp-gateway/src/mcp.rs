//! MCP JSON-RPC 2.0 protocol negotiation, tool definitions, validation, and concurrent dispatch.
//!
//! All 9 tools (`roger_send`, `roger_inbox`, `roger_board`, `roger_label`, `roger_claim`,
//! `roger_renew`, `roger_release`, `roger_receipt`, `roger_registered`) preserve exact
//! schema, validation, bounds, clamping, error phrasing, and output parity with `roger mcp`.

use crate::daemon;
use crate::durlog::{blocking_io, gen_uuid};
use crate::session::GatewaySession;
use serde_json::{Map, Value};
use std::io;
use std::time::Duration;

pub const VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_INBOX_LIMIT: i64 = 50;
const MAX_INBOX_LIMIT: i64 = 200;
const DEFAULT_WAIT_SECONDS: i64 = 10;
const MAX_WAIT_SECONDS: i64 = 60;
const DEFAULT_CLAIM_TTL_SECONDS: i64 = 900;
const DEFAULT_BOARD_WINDOW_SECONDS: i64 = 600;

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

pub fn reply_ok(id: &Value, result: Value) -> Value {
    let mut m = Map::new();
    m.insert("jsonrpc".into(), Value::String("2.0".into()));
    m.insert("id".into(), id.clone());
    m.insert("result".into(), result);
    Value::Object(m)
}

pub fn reply_err(id: &Value, code: i64, message: &str) -> Value {
    let mut err = Map::new();
    err.insert("code".into(), Value::Number(code.into()));
    err.insert("message".into(), Value::String(message.into()));
    let mut m = Map::new();
    m.insert("jsonrpc".into(), Value::String("2.0".into()));
    m.insert("id".into(), id.clone());
    m.insert("error".into(), Value::Object(err));
    Value::Object(m)
}

pub fn negotiate(params: &Value) -> &'static str {
    let asked = params.get("protocolVersion").and_then(Value::as_str);
    VERSIONS
        .iter()
        .find(|v| Some(**v) == asked)
        .unwrap_or(&VERSIONS[0])
}

pub fn initialize_result(params: &Value) -> Value {
    let mut caps = Map::new();
    caps.insert("tools".into(), Value::Object(Map::new()));
    let mut info = Map::new();
    info.insert("name".into(), Value::String("roger-mcp-gateway".into()));
    info.insert(
        "version".into(),
        Value::String(env!("CARGO_PKG_VERSION").into()),
    );
    let mut res = Map::new();
    res.insert(
        "protocolVersion".into(),
        Value::String(negotiate(params).into()),
    );
    res.insert("capabilities".into(), Value::Object(caps));
    res.insert("serverInfo".into(), Value::Object(info));
    Value::Object(res)
}

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

pub fn tools_list() -> Value {
    let tools: Vec<Value> = TOOLS
        .iter()
        .map(|(name, description, schema)| {
            let mut t = Map::new();
            t.insert("name".into(), Value::String((*name).into()));
            t.insert("description".into(), Value::String((*description).into()));
            t.insert(
                "inputSchema".into(),
                serde_json::from_str::<Value>(schema).expect("a tool schema is valid JSON"),
            );
            Value::Object(t)
        })
        .collect();
    let mut out = Map::new();
    out.insert("tools".into(), Value::Array(tools));
    Value::Object(out)
}

pub fn tool_result(text: &str, is_error: bool) -> Value {
    let mut block = Map::new();
    block.insert("type".into(), Value::String("text".into()));
    block.insert("text".into(), Value::String(text.into()));
    let mut out = Map::new();
    out.insert("content".into(), Value::Array(vec![Value::Object(block)]));
    if is_error {
        out.insert("isError".into(), Value::Bool(true));
    }
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

/// Dispatch one `tools/call` invocation concurrently on `session`.
pub async fn call_tool(session: &GatewaySession, params: &Value) -> Result<Value, String> {
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
    let known: Vec<&str> = TOOLS.iter().map(|t| t.0).collect();
    if !known.contains(&name) {
        return Err(format!(
            "expected a tool among {}, saw {name}",
            known.join(", ")
        ));
    }
    let io_err = |e: io::Error| e.to_string();

    match name {
        "roger_send" => {
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
        _ => {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_negotiation_matches_roger_mcp() {
        let v = |s: &str| negotiate(&serde_json::from_str::<Value>(s).unwrap());
        assert_eq!(v(r#"{"protocolVersion":"2024-11-05"}"#), "2024-11-05");
        assert_eq!(v(r#"{"protocolVersion":"2025-03-26"}"#), "2025-03-26");
        assert_eq!(v(r#"{"protocolVersion":"2025-06-18"}"#), "2025-06-18");
        assert_eq!(v(r#"{"protocolVersion":"1999-01-01"}"#), "2025-06-18");
        assert_eq!(v(r#"{}"#), "2025-06-18");
    }

    #[test]
    fn tools_list_has_all_nine_tools_and_valid_schemas() {
        let list = tools_list();
        let tools = list["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 9);
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
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
    }

    #[test]
    fn message_classification_and_validation_errors_match_roger_mcp() {
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

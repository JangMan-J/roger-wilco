//! Roger-Wilco second-language client, written from `docs/contract.md`.
//!
//! Two wire surfaces, no other contact with the TypeScript core:
//! - coordinator HTTP: JSON outcomes behind a bearer credential;
//! - host daemon socket: newline-delimited JSON frames, protocol v1.
//!
//! The evidence module is the conformance half of this crate: it applies
//! the contract's delivery-evidence rules to a harness transcript and
//! rejects everything that is not an exact, durable, full-field record
//! of the expected message.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Coordinator HTTP API
// ---------------------------------------------------------------------------

pub struct Coordinator {
    base: String,
    credential: String,
}

impl Coordinator {
    pub fn new(base: &str, credential: &str) -> Coordinator {
        Coordinator { base: base.trim_end_matches('/').to_string(), credential: credential.to_string() }
    }

    /// One coordinator call. Returns the full outcome object.
    pub fn call(&self, method: &str, route: &str, body: Option<&Value>) -> Result<Value, String> {
        let url = format!("{}{}", self.base, route);
        let rest = url.strip_prefix("http://").ok_or("coordinator URL must be http://host:port")?;
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        let payload = body.map(Value::to_string).unwrap_or_default();
        let mut stream = TcpStream::connect(authority).map_err(|e| format!("connect: {e}"))?;
        stream.set_read_timeout(Some(Duration::from_secs(10))).map_err(|e| e.to_string())?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nhost: {authority}\r\nauthorization: Bearer {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
            self.credential,
            payload.len(),
        );
        stream.write_all(request.as_bytes()).map_err(|e| format!("write: {e}"))?;
        let mut raw = String::new();
        stream.read_to_string(&mut raw).map_err(|e| format!("read: {e}"))?;
        let body_text = decode_http_body(&raw)?;
        serde_json::from_str(&body_text).map_err(|e| format!("response is not JSON: {e}"))
    }
}

fn decode_http_body(raw: &str) -> Result<String, String> {
    let (head, body) = raw.split_once("\r\n\r\n").ok_or("malformed HTTP response")?;
    let lower = head.to_ascii_lowercase();
    if lower.contains("transfer-encoding: chunked") {
        let mut out = String::new();
        let mut rest = body;
        loop {
            let (line, after) = rest.split_once("\r\n").ok_or("bad chunk")?;
            let size = usize::from_str_radix(line.trim(), 16).map_err(|_| "bad chunk size")?;
            if size == 0 { return Ok(out); }
            out.push_str(&after[..size.min(after.len())]);
            rest = &after[size.min(after.len())..];
            rest = rest.strip_prefix("\r\n").unwrap_or(rest);
        }
    }
    Ok(body.to_string())
}

// ---------------------------------------------------------------------------
// Host daemon socket (local IPC v1)
// ---------------------------------------------------------------------------

pub struct Ipc {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Ipc {
    pub fn connect(socket_path: &str) -> Result<Ipc, String> {
        let stream = UnixStream::connect(socket_path).map_err(|e| format!("socket connect: {e}"))?;
        stream.set_read_timeout(Some(Duration::from_secs(70))).map_err(|e| e.to_string())?;
        let writer = stream.try_clone().map_err(|e| e.to_string())?;
        Ok(Ipc { reader: BufReader::new(stream), writer, next_id: 0 })
    }

    /// One IPC command; `changed` events are skipped while waiting.
    pub fn invoke(&mut self, method: &str, args: Value) -> Result<Value, String> {
        self.next_id += 1;
        let id = self.next_id;
        let frame = json!({ "v": 1, "id": id, "method": method, "args": args });
        self.writer
            .write_all(format!("{frame}\n").as_bytes())
            .map_err(|e| format!("socket write: {e}"))?;
        loop {
            let mut line = String::new();
            let read = self.reader.read_line(&mut line).map_err(|e| format!("socket read: {e}"))?;
            if read == 0 { return Err("socket closed".to_string()); }
            let value: Value = serde_json::from_str(line.trim()).map_err(|e| format!("frame is not JSON: {e}"))?;
            if value.get("event").is_some() { continue; }
            if value.get("id").and_then(Value::as_u64) != Some(id) { continue; }
            return value.get("result").cloned().ok_or_else(|| "reply without result".to_string());
        }
    }
}

// ---------------------------------------------------------------------------
// Delivery evidence conformance
// ---------------------------------------------------------------------------

/// The expected message fields a delivered record must match exactly.
pub struct ExpectedMessage {
    pub id: String,
    pub sequence: i64,
    pub timestamp: i64,
    pub sender: String,
    pub recipient: String,
    pub body: String,
    pub visibility: String,
    pub data_json: String,
}

/// Find the 1-based transcript line that durably records delivery of the
/// expected message, or `None`. Rules, all fail-closed:
///
/// - only complete (newline-terminated) lines are considered;
/// - if the transcript names a session through `stream` records and it is
///   not `session_id`, nothing in it counts;
/// - a record counts only when it is shaped
///   `{event:'delivered', message:{...}}`, at any nesting depth, including
///   inside JSON-encoded strings, and every message field matches;
/// - a record declaring a non-`durable` durability never counts;
/// - a bare mention of the message ID never counts.
pub fn find_delivery(transcript: &str, session_id: &str, expected: &ExpectedMessage) -> Option<usize> {
    let complete: Vec<&str> = transcript
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .map(|line| line.trim_end_matches(['\n', '\r']))
        .collect();
    let mut bound: Vec<String> = Vec::new();
    for line in &complete {
        if !line.contains("\"stream\"") { continue; }
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            collect_stream_ids(&value, 0, &mut bound);
        }
    }
    if !bound.is_empty() && !bound.iter().any(|id| id == session_id) { return None; }
    for (index, line) in complete.iter().enumerate() {
        if !line.contains(&expected.id) { continue; }
        let Ok(value) = serde_json::from_str::<Value>(line) else { continue; };
        if is_delivered_record(&value, expected, 0) { return Some(index + 1); }
    }
    None
}

fn collect_stream_ids(value: &Value, depth: usize, out: &mut Vec<String>) {
    if depth > 8 { return; }
    match value {
        Value::String(text) => {
            if text.contains("\"session\"") && text.len() <= 1024 * 1024 {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    collect_stream_ids(&parsed, depth + 1, out);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| collect_stream_ids(item, depth + 1, out)),
        Value::Object(map) => {
            if let Some(stream) = map.get("stream") {
                if stream.get("kind").and_then(Value::as_str) == Some("session") {
                    if let Some(id) = stream.get("id").and_then(Value::as_str) {
                        out.push(id.to_string());
                    }
                }
            }
            map.values().for_each(|item| collect_stream_ids(item, depth + 1, out));
        }
        _ => {}
    }
}

fn is_delivered_record(value: &Value, expected: &ExpectedMessage, depth: usize) -> bool {
    if depth > 8 { return false; }
    match value {
        Value::String(text) => {
            if !text.contains(&expected.id) || text.len() > 1024 * 1024 { return false; }
            serde_json::from_str::<Value>(text)
                .map(|parsed| is_delivered_record(&parsed, expected, depth + 1))
                .unwrap_or(false)
        }
        Value::Array(items) => items.iter().any(|item| is_delivered_record(item, expected, depth + 1)),
        Value::Object(map) => {
            if let Some(durability) = map.get("durability") {
                if durability.as_str() != Some("durable") { return false; }
            }
            if map.get("event").and_then(Value::as_str) == Some("delivered") {
                if let Some(message) = map.get("message") {
                    let matches = message.get("id").and_then(Value::as_str) == Some(expected.id.as_str())
                        && message.get("sequence").and_then(Value::as_i64) == Some(expected.sequence)
                        && message.get("timestamp").and_then(Value::as_i64) == Some(expected.timestamp)
                        && message.get("sender").and_then(Value::as_str) == Some(expected.sender.as_str())
                        && message.get("recipient").and_then(Value::as_str) == Some(expected.recipient.as_str())
                        && message.get("body").and_then(Value::as_str) == Some(expected.body.as_str())
                        && message.get("visibility").and_then(Value::as_str) == Some(expected.visibility.as_str())
                        && message.get("data").map(Value::to_string).unwrap_or_else(|| "null".to_string()) == expected.data_json;
                    if matches { return true; }
                }
            }
            map.values().any(|item| is_delivered_record(item, expected, depth + 1))
        }
        _ => false,
    }
}

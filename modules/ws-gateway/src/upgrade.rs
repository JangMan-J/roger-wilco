//! The WebSocket upgrade: Bearer check, query parsing, and opening the
//! Address's Store, all before the `101 Switching Protocols` answer.

use crate::cli::constant_time_eq;
use crate::durlog::{blocking_io, gen_uuid, store_path, DurLog};
use serde_json::Value;
use std::io;
use std::path::Path;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request};
use tokio_tungstenite::tungstenite::http::{Response, StatusCode};

/// Query keys accepted on `GET /`.
const KEYS: &str = "id, session, label, declare, registered, superseded, after";

/// One session's parameters and its open Store, ready for the session loop.
pub struct Opened {
    pub address: String,
    pub label: Option<String>,
    pub declared: Option<String>,
    pub confirm: bool,
    pub superseded: bool,
    /// `Some(record)`: replay Store entries after that record (`Some("")`: every entry).
    pub after: Option<String>,
    pub store: DurLog,
    /// Every complete Store entry, in Store line order.
    pub entries: Vec<Value>,
}

/// Session parameters parsed from the request URI.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Params {
    pub address: Option<String>,
    pub label: Option<String>,
    pub declared: Option<String>,
    pub confirm: bool,
    pub superseded: bool,
    pub after: Option<String>,
}

/// A refused upgrade: the HTTP status and a plain-text reason.
#[derive(Debug, PartialEq, Eq)]
pub struct Refusal {
    pub status: StatusCode,
    pub reason: String,
}

fn refuse(status: StatusCode, reason: String) -> Refusal {
    Refusal { status, reason }
}

/// The HTTP answer for a refused upgrade (`401` carries `WWW-Authenticate: Bearer`).
pub fn refusal_response(r: Refusal) -> ErrorResponse {
    let Refusal { status, reason } = r;
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8");
    if status == StatusCode::UNAUTHORIZED {
        builder = builder.header("www-authenticate", "Bearer");
    }
    builder
        .body(Some(reason))
        .expect("static status and headers build a response")
}

/// Check `Authorization: Bearer <token>` in constant time when a token is configured.
pub fn authorized(req: &Request, bearer: Option<&str>) -> bool {
    let Some(want) = bearer else {
        return true;
    };
    req.headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|got| constant_time_eq(got.as_bytes(), want.as_bytes()))
}

pub fn percent_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .ok_or_else(|| format!("expected two hex digits after '%', saw {:?}", &raw[i..]))?;
            let digit = |b: u8| (b as char).to_digit(16);
            let (Some(hi), Some(lo)) = (digit(hex[0]), digit(hex[1])) else {
                return Err(format!(
                    "expected hex digits in percent escape, saw {:?}",
                    String::from_utf8_lossy(&bytes[i..i + 3])
                ));
            };
            out.push(((hi << 4) | lo) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| "expected percent-decoded UTF-8, saw invalid UTF-8 bytes".to_string())
}

fn flag(key: &str, v: &str) -> Result<bool, String> {
    match v {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(format!("expected {key}=1 or {key}=0, saw {key}={other}")),
    }
}

/// Parse the query string of `GET /?...`. Values are percent-decoded and
/// otherwise passed through byte-identical; `+` is a literal plus.
pub fn parse_query(query: Option<&str>) -> Result<Params, String> {
    let mut p = Params::default();
    let mut seen: Vec<String> = Vec::new();
    for pair in query.unwrap_or("").split('&').filter(|s| !s.is_empty()) {
        let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(raw_k)?;
        let val = percent_decode(raw_v)?;
        let canonical = if key == "session" { "id" } else { key.as_str() };
        if seen.iter().any(|k| k == canonical) {
            return Err(format!(
                "expected query key {key} at most once, saw it again"
            ));
        }
        seen.push(canonical.to_string());
        match canonical {
            "id" => p.address = Some(val).filter(|v| !v.is_empty()),
            "label" => p.label = Some(val),
            "declare" => p.declared = Some(val),
            "registered" => p.confirm = flag("registered", &val)?,
            "superseded" => p.superseded = flag("superseded", &val)?,
            "after" => p.after = Some(val),
            _ => return Err(format!("expected query keys {KEYS}; saw {key}")),
        }
    }
    Ok(p)
}

/// Open `address`'s Store and read every complete entry, in line order.
pub fn open_store(store_dir: &Path, address: &str) -> io::Result<(DurLog, Vec<Value>)> {
    let path = store_path(store_dir, address);
    let (store, replay) = DurLog::open(&path)?;
    let mut entries = Vec::new();
    for line in replay {
        if let Ok(v) = serde_json::from_str::<Value>(&line?) {
            let has = |k: &str| v.get(k).and_then(Value::as_str).is_some();
            if has("record") && has("mail") {
                entries.push(v);
            }
        }
    }
    Ok((store, entries))
}

/// Everything decided before the upgrade answer: Bearer (401), path (404),
/// query (400), Store held by another connection (409), unknown `after` (400).
pub fn prepare(req: &Request, bearer: Option<&str>, store_dir: &Path) -> Result<Opened, Refusal> {
    if !authorized(req, bearer) {
        return Err(refuse(
            StatusCode::UNAUTHORIZED,
            "expected Authorization: Bearer <token> matching the gateway token, saw none or another\n".into(),
        ));
    }
    if req.uri().path() != "/" {
        return Err(refuse(
            StatusCode::NOT_FOUND,
            format!("expected path /, saw {}\n", req.uri().path()),
        ));
    }
    let p =
        parse_query(req.uri().query()).map_err(|e| refuse(StatusCode::BAD_REQUEST, e + "\n"))?;
    let address = match p.address {
        Some(a) => a,
        None => blocking_io(gen_uuid).map_err(|e| {
            refuse(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("expected a generated Address, saw {e}\n"),
            )
        })?,
    };
    let (store, entries) = blocking_io(|| open_store(store_dir, &address)).map_err(|e| {
        let status = if e.kind() == io::ErrorKind::WouldBlock {
            StatusCode::CONFLICT
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        refuse(
            status,
            format!("expected this Address's Store free to open, saw {e}\n"),
        )
    })?;
    if let Some(after) = p.after.as_deref().filter(|a| !a.is_empty()) {
        if replay_start(&entries, Some(after)).is_none() {
            return Err(refuse(
                StatusCode::BAD_REQUEST,
                format!("expected after=<record> naming an entry in this Address's Store, saw after={after}\n"),
            ));
        }
    }
    Ok(Opened {
        address,
        label: p.label,
        declared: p.declared,
        confirm: p.confirm,
        superseded: p.superseded,
        after: p.after,
        store,
        entries,
    })
}

/// Index of the first entry to replay: the line after the one holding
/// `after` (by Store line order), or 0 for an empty `after`. `None` when the
/// record is not in the Store.
pub fn replay_start(entries: &[Value], after: Option<&str>) -> Option<usize> {
    match after {
        None | Some("") => Some(0),
        Some(rec) => entries
            .iter()
            .position(|e| e.get("record").and_then(Value::as_str) == Some(rec))
            .map(|i| i + 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_decodes_composite_address_and_flags() {
        let p = parse_query(Some(
            "id=%5B%5B153%3AMeteor%20Shower%3A7%5D%5Dcaf%C3%A9&label=a+b&registered=1&superseded=0&after=r-2",
        ))
        .unwrap();
        assert_eq!(p.address.as_deref(), Some("[[153:Meteor Shower:7]]café"));
        assert_eq!(p.label.as_deref(), Some("a+b"));
        assert!(p.confirm);
        assert!(!p.superseded);
        assert_eq!(p.after.as_deref(), Some("r-2"));
    }

    #[test]
    fn query_accepts_session_alias_and_refuses_duplicates_and_unknown_keys() {
        let p = parse_query(Some("session=abc")).unwrap();
        assert_eq!(p.address.as_deref(), Some("abc"));
        assert!(parse_query(Some("id=a&session=b"))
            .unwrap_err()
            .contains("at most once"));
        assert!(parse_query(Some("nope=1"))
            .unwrap_err()
            .contains("saw nope"));
        assert!(parse_query(Some("registered=yes"))
            .unwrap_err()
            .contains("saw registered=yes"));
        assert!(percent_decode("%ZZ").is_err());
        assert!(percent_decode("%2").is_err());
        assert_eq!(parse_query(None).unwrap(), Params::default());
    }

    #[test]
    fn replay_start_follows_line_order_not_record_rank() {
        let entries: Vec<Value> = ["r-10", "r-2", "r-9"]
            .iter()
            .map(|r| serde_json::json!({ "record": r, "mail": "m" }))
            .collect();
        assert_eq!(replay_start(&entries, None), Some(0));
        assert_eq!(replay_start(&entries, Some("")), Some(0));
        assert_eq!(replay_start(&entries, Some("r-10")), Some(1));
        assert_eq!(replay_start(&entries, Some("r-2")), Some(2));
        assert_eq!(replay_start(&entries, Some("r-9")), Some(3));
        assert_eq!(replay_start(&entries, Some("r-1")), None);
    }
}

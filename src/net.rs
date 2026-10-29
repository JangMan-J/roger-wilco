//! Line-delimited JSON over any byte stream. One object per line.

use crate::json::{self, obj, s, Value};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};

pub type Writer = Arc<Mutex<Box<dyn Write + Send>>>;

/// The one shape a host daemon uses to forward a receipt upstream, both on
/// the ack itself and when replaying receipts after a reconnect.
pub fn ack_msg(mail: &str, session: &str, record: &str) -> Value {
    obj(vec![
        ("type", s("ack")),
        ("mail", s(mail)),
        ("session", s(session)),
        ("record", s(record)),
    ])
}

/// The Kind of a Mail sent to one Address.
pub const DIRECTED: &str = "directed";
/// The Kind of a copy of a Broadcast.
pub const BROADCAST: &str = "broadcast";

/// The one shape both the coordinator and a host daemon use to hand a Mail
/// to the next hop: its Kind and the sender's headers, carried as given.
pub fn deliver_msg(
    mail: &str,
    from: &str,
    to: &str,
    body: &str,
    kind: &str,
    headers: &Value,
) -> Value {
    obj(vec![
        ("type", s("deliver")),
        ("mail", s(mail)),
        ("from", s(from)),
        ("to", s(to)),
        ("body", s(body)),
        ("kind", s(kind)),
        ("headers", headers.clone()),
        legacy_field(kind), // legacy-wire: emit for the previous release's readers
    ])
}

/// The one field an earlier release's readers still look for on a delivery,
/// computed from the Kind. The release after this one deletes it, and every
/// caller of it.
pub fn legacy_field(kind: &str) -> (&'static str, Value) {
    ("wake", Value::Bool(kind != BROADCAST)) // legacy-wire: emit for the previous release's readers
}

/// A delivery's Kind. A line written before Kind existed says it in the
/// older field: false marks a Broadcast copy, anything else is directed.
pub fn kind_of(v: &Value) -> String {
    if let Some(kind) = v.get("kind").and_then(|x| x.as_str()) {
        return kind.to_string();
    }
    let older = v.get("wake"); // legacy-wire: read a log or peer from before Kind
    match older {
        Some(Value::Bool(false)) => BROADCAST.to_string(),
        _ => DIRECTED.to_string(),
    }
}

/// A delivery's headers, as the sender set them; the empty set when the line
/// has none (written before headers existed, or sent without any).
pub fn headers_of(v: &Value) -> Value {
    v.get("headers").cloned().unwrap_or(Value::Obj(Vec::new()))
}

/// A Register's declared text: opaque text a session states about itself,
/// stored and returned verbatim and never read by the core.
pub fn declared_of(v: &Value) -> Option<String> {
    v.get("declared")
        .and_then(|x| x.as_str())
        .map(str::to_string)
}

pub fn writer(w: Box<dyn Write + Send>) -> Writer {
    Arc::new(Mutex::new(w))
}

pub fn send(w: &Writer, v: &Value) -> io::Result<()> {
    let mut guard = w.lock().unwrap();
    guard.write_all(json::stringify(v).as_bytes())?;
    guard.write_all(b"\n")?;
    guard.flush()
}

pub struct Lines {
    inner: std::io::Lines<BufReader<Box<dyn Read + Send>>>,
}

pub fn lines(r: Box<dyn Read + Send>) -> Lines {
    Lines {
        inner: BufReader::new(r).lines(),
    }
}

impl Iterator for Lines {
    type Item = io::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Ok(line) if line.trim().is_empty() => continue,
                Ok(line) => {
                    return Some(
                        json::parse(&line)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
                    )
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

pub fn field<'a>(v: &'a Value, key: &str) -> io::Result<&'a str> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {key}")))
}

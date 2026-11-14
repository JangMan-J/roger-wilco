//! Line-delimited JSON transport to `roger daemon` over Unix socket or TCP (`tcp://`).

use crate::cli::DaemonSocket;
use serde_json::{Map, Value};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::Mutex;

pub struct DaemonReader {
    lines: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    buf: Vec<u8>,
}

pub struct DaemonWriter {
    inner: Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
}

/// Dial `target` (`Unix` socket or `TcpStream` with `TCP_NODELAY`) and split into reader/writer.
pub async fn connect(target: &DaemonSocket) -> io::Result<(DaemonReader, DaemonWriter)> {
    match target {
        DaemonSocket::Unix(path) => {
            let stream = UnixStream::connect(path).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at {}, saw {e}", path.display()),
                )
            })?;
            let (r, w) = stream.into_split();
            Ok((
                DaemonReader {
                    lines: BufReader::new(Box::new(r)),
                    buf: Vec::new(),
                },
                DaemonWriter {
                    inner: Mutex::new(Box::new(w)),
                },
            ))
        }
        DaemonSocket::Tcp(addr) => {
            let stream = TcpStream::connect(addr).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at tcp://{addr}, saw {e}"),
                )
            })?;
            stream.set_nodelay(true)?;
            let (r, w) = stream.into_split();
            Ok((
                DaemonReader {
                    lines: BufReader::new(Box::new(r)),
                    buf: Vec::new(),
                },
                DaemonWriter {
                    inner: Mutex::new(Box::new(w)),
                },
            ))
        }
    }
}

impl DaemonReader {
    pub async fn next_value(&mut self) -> io::Result<Option<Value>> {
        loop {
            self.buf.clear();
            let n = self.lines.read_until(b'\n', &mut self.buf).await?;
            if n == 0 {
                return Ok(None);
            }
            if self.buf.last() == Some(&b'\n') {
                self.buf.pop();
            }
            if self.buf.last() == Some(&b'\r') {
                self.buf.pop();
            }
            let text = std::str::from_utf8(&self.buf).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected a line of UTF-8, saw other bytes",
                )
            })?;
            if text.trim().is_empty() {
                continue;
            }
            let val = serde_json::from_str::<Value>(text)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            return Ok(Some(val));
        }
    }
}

impl DaemonWriter {
    pub async fn send(&self, value: &Value) -> io::Result<()> {
        let mut line = serde_json::to_string(value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        line.push('\n');
        let mut guard = self.inner.lock().await;
        guard.write_all(line.as_bytes()).await?;
        guard.flush().await
    }

    pub async fn shutdown(&self) -> io::Result<()> {
        let mut guard = self.inner.lock().await;
        guard.shutdown().await
    }
}

pub fn field<'a>(v: &'a Value, key: &str) -> io::Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("missing {key}")))
}

/// Extract a delivery's Kind (`"directed"` or `"broadcast"`), matching `roger`'s `net::kind_of`.
pub fn kind_of(v: &Value) -> String {
    if let Some(kind) = v.get("kind").and_then(Value::as_str) {
        return kind.to_string();
    }
    let older = v.get("wake"); // legacy-wire: read a log or peer from before Kind
    match older {
        Some(Value::Bool(false)) => "broadcast".to_string(),
        _ => "directed".to_string(),
    }
}

/// Extract a delivery's headers preserving exact sender key order.
pub fn headers_of(v: &Value) -> Value {
    v.get("headers")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()))
}

/// One short-lived request to the host daemon, answered by one line (identical to `session::request`).
pub async fn request(target: &DaemonSocket, req: &Value) -> io::Result<Value> {
    let (mut reader, writer) = connect(target).await?;
    writer.send(req).await?;
    let reply = reader.next_value().await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {target}, saw EOF"),
        )
    })?;
    if field(&reply, "type")? == "error" {
        return Err(io::Error::other(match field(&reply, "reason") {
            Ok(reason) => reason.to_string(),
            Err(_) => {
                let raw = serde_json::to_string(&reply).unwrap_or_default();
                format!("expected a reason in the error reply, saw {raw}")
            }
        }));
    }
    Ok(reply)
}

/// Short-lived `send` request to the host daemon (identical to `session::send_mail`).
pub async fn send_mail(
    target: &DaemonSocket,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let (mut reader, writer) = connect(target).await?;
    let mut ask = Map::new();
    ask.insert("type".into(), Value::String("send".into()));
    ask.insert("req".into(), Value::Number(1.into()));
    ask.insert("mail".into(), Value::String(mail.into()));
    ask.insert("from".into(), Value::String(from.into()));
    ask.insert("to".into(), Value::String(to.into()));
    ask.insert("body".into(), Value::String(body.into()));
    if let Some(h) = headers {
        ask.insert("headers".into(), h.clone());
    }
    writer.send(&Value::Object(ask)).await?;
    while let Some(v) = reader.next_value().await? {
        match field(&v, "type")? {
            "accepted" => return Ok(field(&v, "mail")?.to_string()),
            "error" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "send of {mail} refused: {}",
                        field(&v, "reason").unwrap_or("unexplained")
                    ),
                ))
            }
            _ => {}
        }
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no accepted"))
}

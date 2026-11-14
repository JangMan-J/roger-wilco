//! Line-delimited JSON transport to `roger daemon` over a Unix socket or TCP (`tcp://`).
//!
//! Every `tcp://` connection carrying a client token starts with
//! `{"type":"auth","token":"<value>"}`; Unix-socket connections never send it.

use crate::cli::{DaemonSocket, Dialer};
use serde_json::{Map, Value};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::sync::Mutex;

pub struct DaemonReader {
    lines: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    buf: Vec<u8>,
}

pub struct DaemonWriter {
    inner: Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
}

fn split_parts(
    r: Box<dyn AsyncRead + Unpin + Send>,
    w: Box<dyn AsyncWrite + Unpin + Send>,
) -> (DaemonReader, DaemonWriter) {
    (
        DaemonReader {
            lines: BufReader::new(r),
            buf: Vec::new(),
        },
        DaemonWriter {
            inner: Mutex::new(w),
        },
    )
}

/// Dial the host daemon and split the connection into reader and writer. On
/// `tcp://` with a client token, the auth line is written before anything else.
pub async fn connect(dialer: &Dialer) -> io::Result<(DaemonReader, DaemonWriter)> {
    match &dialer.socket {
        #[cfg(unix)]
        DaemonSocket::Unix(path) => {
            let stream = UnixStream::connect(path).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at {}, saw {e}", path.display()),
                )
            })?;
            let (r, w) = stream.into_split();
            Ok(split_parts(Box::new(r), Box::new(w)))
        }
        #[cfg(not(unix))]
        DaemonSocket::Unix(path) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "expected --socket tcp://<address:port> on this platform, saw {}",
                path.display()
            ),
        )),
        DaemonSocket::Tcp(addr) => {
            let stream = TcpStream::connect(addr).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at tcp://{addr}, saw {e}"),
                )
            })?;
            stream.set_nodelay(true)?;
            let (r, w) = stream.into_split();
            let (reader, writer) = split_parts(Box::new(r), Box::new(w));
            if let Some(token) = &dialer.client_token {
                let mut auth = Map::new();
                auth.insert("type".into(), Value::String("auth".into()));
                auth.insert("token".into(), Value::String(token.clone()));
                writer.send(&Value::Object(auth)).await?;
            }
            Ok((reader, writer))
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

/// A delivery's Kind (`"directed"` or `"broadcast"`), matching `roger`'s `net::kind_of`.
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

/// A delivery's headers, sender key order preserved.
pub fn headers_of(v: &Value) -> Value {
    v.get("headers")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()))
}

/// One short-lived request to the host daemon, answered by one line (as `session::request`).
pub async fn request(dialer: &Dialer, req: &Value) -> io::Result<Value> {
    let (mut reader, writer) = connect(dialer).await?;
    writer.send(req).await?;
    let reply = reader.next_value().await?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {}, saw EOF", dialer.socket),
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

/// Short-lived `send` request to the host daemon (as `session::send_mail`).
pub async fn send_mail(
    dialer: &Dialer,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let (mut reader, writer) = connect(dialer).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn first_line_seen(token: Option<&str>) -> Value {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dialer = Dialer {
            socket: DaemonSocket::Tcp(addr.to_string()),
            client_token: token.map(str::to_string),
        };
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(stream);
            let mut line = String::new();
            lines.read_line(&mut line).await.unwrap();
            serde_json::from_str::<Value>(&line).unwrap()
        });
        let (_reader, writer) = connect(&dialer).await.unwrap();
        let mut probe = Map::new();
        probe.insert("type".into(), Value::String("board".into()));
        writer.send(&Value::Object(probe)).await.unwrap();
        accept.await.unwrap()
    }

    #[tokio::test]
    async fn tcp_with_client_token_sends_auth_line_first() {
        let first = first_line_seen(Some("tok-1")).await;
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            r#"{"type":"auth","token":"tok-1"}"#
        );
    }

    #[tokio::test]
    async fn tcp_without_client_token_sends_request_first() {
        let first = first_line_seen(None).await;
        assert_eq!(
            serde_json::to_string(&first).unwrap(),
            r#"{"type":"board"}"#
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_socket_never_sends_auth_line() {
        let dir = std::env::temp_dir().join(format!("roger-ws-daemon-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let dialer = Dialer {
            socket: DaemonSocket::Unix(path),
            client_token: Some("tok-1".into()),
        };
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).await.unwrap();
            line
        });
        let (_reader, writer) = connect(&dialer).await.unwrap();
        let mut probe = Map::new();
        probe.insert("type".into(), Value::String("board".into()));
        writer.send(&Value::Object(probe)).await.unwrap();
        assert_eq!(accept.await.unwrap(), "{\"type\":\"board\"}\n");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

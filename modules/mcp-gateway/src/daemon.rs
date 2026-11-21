//! Line-delimited JSON transport to `roger daemon` over Unix socket or TCP (`tcp://`).

use crate::cli::DaemonSocket;
use serde_json::{Map, Value};
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
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

/// Dial `target` (`Unix` socket or `TcpStream` with `TCP_NODELAY`) and split into reader/writer.
pub async fn connect(target: &DaemonSocket) -> io::Result<(DaemonReader, DaemonWriter)> {
    match target {
        #[cfg(unix)]
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
            let writer = DaemonWriter {
                inner: Mutex::new(Box::new(w)),
            };
            // A daemon started with --client-token-env requires this first line over TCP.
            if let Some(token) = std::env::var("ROGER_CLIENT_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
            {
                writer
                    .send(&serde_json::json!({"type": "auth", "token": token}))
                    .await?;
            }
            Ok((
                DaemonReader {
                    lines: BufReader::new(Box::new(r)),
                    buf: Vec::new(),
                },
                writer,
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
        return Err(error_reply(&reply));
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
    writer
        .send(&send_ask(from, to, mail, body, headers))
        .await?;
    while let Some(v) = reader.next_value().await? {
        if matches!(field(&v, "type")?, "accepted" | "error") {
            return accepted_mail(&v, mail);
        }
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no accepted"))
}

fn is_reconnectable(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    )
}

fn error_reply(reply: &Value) -> io::Error {
    io::Error::other(match field(reply, "reason") {
        Ok(reason) => reason.to_string(),
        Err(_) => {
            let raw = serde_json::to_string(reply).unwrap_or_default();
            format!("expected a reason in the error reply, saw {raw}")
        }
    })
}

/// Persistent connection to the host daemon, reused across requests and redialed once
/// on EOF, broken pipe, or reset of a reused connection (matching `roger`'s `session::Client`).
pub struct DaemonClient {
    target: DaemonSocket,
    conn: Mutex<Option<(DaemonReader, DaemonWriter)>>,
    next_req: AtomicI64,
    #[cfg(test)]
    connects: std::sync::atomic::AtomicUsize,
}

impl DaemonClient {
    pub fn new(target: DaemonSocket) -> Self {
        Self {
            target,
            conn: Mutex::new(None),
            next_req: AtomicI64::new(0),
            #[cfg(test)]
            connects: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Send `msg` with a fresh `req` and return the first reply line answering it
    /// (a line naming another `req` is skipped). Replies of any `type` are returned.
    async fn exchange(&self, msg: &Value, accept: impl Fn(&Value) -> bool) -> io::Result<Value> {
        let req_id = self.next_req.fetch_add(1, Ordering::SeqCst) + 1;
        let mut msg = msg.clone();
        if let Value::Object(map) = &mut msg {
            map.insert("req".into(), Value::Number(req_id.into()));
        }
        let mut slot = self.conn.lock().await;
        // Cancellation safety: the connection lives only in this future while a reply is
        // outstanding; if the future is dropped, the connection drops with it and the
        // next request dials fresh instead of reading a stale reply.
        let reused = slot.take();
        let had_conn = reused.is_some();
        let mut conn = reused;
        for attempt in 0..2 {
            let pair = match conn.take() {
                Some(pair) => pair,
                None => {
                    let pair = connect(&self.target).await?;
                    #[cfg(test)]
                    self.connects.fetch_add(1, Ordering::SeqCst);
                    pair
                }
            };
            match self.once(pair, &msg, req_id, &accept).await {
                Ok((reply, pair)) => {
                    *slot = Some(pair);
                    return Ok(reply);
                }
                Err(e) if attempt == 0 && had_conn && is_reconnectable(&e) => continue,
                Err(e) => return Err(e),
            }
        }
        unreachable!("second attempt always returns")
    }

    async fn once(
        &self,
        (mut reader, writer): (DaemonReader, DaemonWriter),
        msg: &Value,
        req_id: i64,
        accept: &impl Fn(&Value) -> bool,
    ) -> io::Result<(Value, (DaemonReader, DaemonWriter))> {
        writer.send(msg).await?;
        while let Some(v) = reader.next_value().await? {
            if v.get("req")
                .and_then(Value::as_i64)
                .is_some_and(|r| r != req_id)
            {
                continue;
            }
            if accept(&v) {
                return Ok((v, (reader, writer)));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {}, saw EOF", self.target),
        ))
    }

    /// One request over the persistent connection, answered by one line (same contract as [`request`]).
    pub async fn request(&self, req: &Value) -> io::Result<Value> {
        let reply = self.exchange(req, |_| true).await?;
        if field(&reply, "type")? == "error" {
            return Err(error_reply(&reply));
        }
        Ok(reply)
    }

    /// `send` over the persistent connection (same contract as [`send_mail`]).
    pub async fn send_mail(
        &self,
        from: &str,
        to: &str,
        mail: &str,
        body: &str,
        headers: Option<&Value>,
    ) -> io::Result<String> {
        let ask = send_ask(from, to, mail, body, headers);
        let v = self
            .exchange(&ask, |v| {
                matches!(
                    v.get("type").and_then(Value::as_str),
                    Some("accepted" | "error")
                )
            })
            .await?;
        accepted_mail(&v, mail)
    }
}

fn send_ask(from: &str, to: &str, mail: &str, body: &str, headers: Option<&Value>) -> Value {
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
    Value::Object(ask)
}

fn accepted_mail(v: &Value, mail: &str) -> io::Result<String> {
    match field(v, "type")? {
        "accepted" => Ok(field(v, "mail")?.to_string()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "send of {mail} refused: {}",
                field(v, "reason").unwrap_or("unexplained")
            ),
        )),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Fake daemon: answers each line with `{"type":"accepted","req":N,"mail":..}`
    /// (or `ok`), closing each connection after `per_conn` replies.
    async fn fake_daemon(listener: UnixListener, per_conn: usize) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let (r, mut w) = stream.into_split();
                let mut lines = BufReader::new(r).lines();
                for _ in 0..per_conn {
                    let Ok(Some(line)) = lines.next_line().await else {
                        return;
                    };
                    let v: Value = serde_json::from_str(&line).unwrap();
                    let reply = if v["type"] == "send" {
                        serde_json::json!({"type":"accepted","req":v["req"],"mail":v["mail"]})
                    } else {
                        serde_json::json!({"type":"ok","req":v["req"]})
                    };
                    let mut out = serde_json::to_string(&reply).unwrap();
                    out.push('\n');
                    w.write_all(out.as_bytes()).await.unwrap();
                }
            });
        }
    }

    fn setup(name: &str, per_conn: usize) -> DaemonClient {
        let dir = std::env::temp_dir().join(format!("mcpgw-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.sock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(fake_daemon(listener, per_conn));
        DaemonClient::new(DaemonSocket::Unix(path))
    }

    #[tokio::test]
    async fn reuses_one_connection_with_monotonic_reqs() {
        let client = setup("reuse", usize::MAX);
        let a = client
            .request(&serde_json::json!({"type":"x","req":1}))
            .await
            .unwrap();
        let b = client
            .request(&serde_json::json!({"type":"x","req":1}))
            .await
            .unwrap();
        let m = client.send_mail("a", "b", "m1", "hi", None).await.unwrap();
        assert_eq!((a["req"].as_i64(), b["req"].as_i64()), (Some(1), Some(2)));
        assert_eq!(m, "m1");
        assert_eq!(client.connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reconnects_once_after_daemon_closes() {
        let client = setup("reconnect", 1);
        client
            .request(&serde_json::json!({"type":"x"}))
            .await
            .unwrap();
        let m = client.send_mail("a", "b", "m2", "hi", None).await.unwrap();
        assert_eq!(m, "m2");
        client
            .request(&serde_json::json!({"type":"x"}))
            .await
            .unwrap();
        assert_eq!(client.connects.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancelled_request_drops_its_connection() {
        let client = setup("cancel", usize::MAX);
        client
            .request(&serde_json::json!({"type":"x"}))
            .await
            .unwrap();
        // Dropped before its reply is read: the next request must not read the stale line.
        let _ = tokio::time::timeout(
            std::time::Duration::ZERO,
            client.request(&serde_json::json!({"type":"x"})),
        )
        .await;
        let r = client
            .request(&serde_json::json!({"type":"x"}))
            .await
            .unwrap();
        assert_eq!(r["req"].as_i64(), Some(3));
    }
}

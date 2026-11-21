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

fn reply_error(reply: &Value) -> io::Error {
    io::Error::other(match field(reply, "reason") {
        Ok(reason) => reason.to_string(),
        Err(_) => {
            let raw = serde_json::to_string(reply).unwrap_or_default();
            format!("expected a reason in the error reply, saw {raw}")
        }
    })
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
        return Err(reply_error(&reply));
    }
    Ok(reply)
}

enum SendTarget<'a> {
    Address(&'a str),
    Group(&'a str),
}

fn build_send_ask(
    from: &str,
    target: SendTarget<'_>,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
    expected_article: Option<i64>,
) -> Value {
    let mut ask = Map::new();
    ask.insert("type".into(), Value::String("send".into()));
    ask.insert("req".into(), Value::Number(1.into()));
    ask.insert("mail".into(), Value::String(mail.into()));
    ask.insert("from".into(), Value::String(from.into()));
    match target {
        SendTarget::Address(to) => {
            ask.insert("to".into(), Value::String(to.into()));
        }
        SendTarget::Group(group) => {
            ask.insert("group".into(), Value::String(group.into()));
        }
    }
    ask.insert("body".into(), Value::String(body.into()));
    if let Some(h) = headers {
        ask.insert("headers".into(), h.clone());
    }
    if let Some(exp) = expected_article {
        ask.insert("expected_article".into(), Value::Number(exp.into()));
    }
    Value::Object(ask)
}

fn parse_group_accepted(accepted: &Value) -> io::Result<(String, i64)> {
    let mail = field(accepted, "mail")?.to_string();
    let article = accepted
        .get("article")
        .and_then(Value::as_i64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing article"))?;
    Ok((mail, article))
}

async fn send(
    dialer: &Dialer,
    from: &str,
    target: SendTarget<'_>,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<Value> {
    let (mut reader, writer) = connect(dialer).await?;
    let ask = build_send_ask(from, target, mail, body, headers, None);
    writer.send(&ask).await?;
    while let Some(v) = reader.next_value().await? {
        match field(&v, "type")? {
            "accepted" => return Ok(v),
            "error" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "send of {mail} refused: {}",
                        field(&v, "reason").unwrap_or("unexplained")
                    ),
                ));
            }
            _ => {}
        }
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no accepted"))
}

/// Short-lived directed or broadcast `send` request to the host daemon.
pub async fn send_mail(
    dialer: &Dialer,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let accepted = send(dialer, from, SendTarget::Address(to), mail, body, headers).await?;
    Ok(field(&accepted, "mail")?.to_string())
}

/// Short-lived Group `send`; returns the accepted Mail id and article position.
pub async fn send_group(
    dialer: &Dialer,
    from: &str,
    group: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<(String, i64)> {
    let accepted = send(dialer, from, SendTarget::Group(group), mail, body, headers).await?;
    parse_group_accepted(&accepted)
}

struct ClientState {
    conn: Option<(DaemonReader, DaemonWriter)>,
    next_req: i64,
    #[cfg(test)]
    connects: usize,
}

/// Reusable client connection to the host daemon, reconnecting once on EOF,
/// broken pipe, or connection reset before a complete reply.
pub struct DaemonClient {
    dialer: Dialer,
    state: Mutex<ClientState>,
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

async fn exchange(
    conn: &mut (DaemonReader, DaemonWriter),
    msg: &Value,
    req_id: i64,
    socket: &DaemonSocket,
) -> io::Result<Value> {
    let (reader, writer) = conn;
    writer.send(msg).await?;
    while let Some(v) = reader.next_value().await? {
        if let Some(r) = v.get("req").and_then(Value::as_i64) {
            if r != req_id {
                continue;
            }
        }
        return Ok(v);
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("expected one reply line from {socket}, saw EOF"),
    ))
}

impl DaemonClient {
    pub fn new(dialer: Dialer) -> Self {
        Self {
            dialer,
            state: Mutex::new(ClientState {
                conn: None,
                next_req: 0,
                #[cfg(test)]
                connects: 0,
            }),
        }
    }

    #[cfg(test)]
    async fn connects(&self) -> usize {
        self.state.lock().await.connects
    }

    async fn raw_request(&self, req: &Value) -> io::Result<Value> {
        let mut state = self.state.lock().await;
        state.next_req += 1;
        let req_id = state.next_req;
        let mut msg = req.clone();
        if let Value::Object(map) = &mut msg {
            map.insert("req".into(), Value::Number(req_id.into()));
        }

        // Take the cached connection out before awaiting I/O so dropping this
        // future mid-exchange drops the in-flight socket instead of leaving a
        // partial request or unread reply on the shared connection.
        if let Some(mut conn) = state.conn.take() {
            match exchange(&mut conn, &msg, req_id, &self.dialer.socket).await {
                Ok(reply) => {
                    state.conn = Some(conn);
                    return Ok(reply);
                }
                Err(e) if is_reconnectable(&e) => {
                    drop(conn);
                }
                Err(e) => return Err(e),
            }
        }

        let mut conn = connect(&self.dialer).await?;
        #[cfg(test)]
        {
            state.connects += 1;
        }
        let reply = exchange(&mut conn, &msg, req_id, &self.dialer.socket).await?;
        state.conn = Some(conn);
        Ok(reply)
    }

    /// Send one request on the persistent connection and return its reply line.
    pub async fn request(&self, req: &Value) -> io::Result<Value> {
        let reply = self.raw_request(req).await?;
        if field(&reply, "type")? == "error" {
            return Err(reply_error(&reply));
        }
        Ok(reply)
    }

    async fn send(
        &self,
        from: &str,
        target: SendTarget<'_>,
        mail: &str,
        body: &str,
        headers: Option<&Value>,
        expected_article: Option<i64>,
    ) -> io::Result<Value> {
        let ask = build_send_ask(from, target, mail, body, headers, expected_article);
        let reply = self.raw_request(&ask).await?;
        match field(&reply, "type")? {
            "accepted" => Ok(reply),
            "error" => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "send of {mail} refused: {}",
                    field(&reply, "reason").unwrap_or("unexplained")
                ),
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected accepted or error, saw {other}"),
            )),
        }
    }

    /// Directed or broadcast `send` on the persistent connection.
    pub async fn send_mail(
        &self,
        from: &str,
        to: &str,
        mail: &str,
        body: &str,
        headers: Option<&Value>,
    ) -> io::Result<String> {
        let accepted = self
            .send(from, SendTarget::Address(to), mail, body, headers, None)
            .await?;
        Ok(field(&accepted, "mail")?.to_string())
    }

    /// Group `send` on the persistent connection; returns the accepted Mail id
    /// and article position.
    pub async fn send_group(
        &self,
        from: &str,
        group: &str,
        mail: &str,
        body: &str,
        headers: Option<&Value>,
        expected_article: Option<i64>,
    ) -> io::Result<(String, i64)> {
        let accepted = self
            .send(
                from,
                SendTarget::Group(group),
                mail,
                body,
                headers,
                expected_article,
            )
            .await?;
        parse_group_accepted(&accepted)
    }
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

    async fn read_json_line(lines: &mut BufReader<tokio::net::TcpStream>) -> Option<Value> {
        let mut line = String::new();
        let n = lines.read_line(&mut line).await.unwrap();
        if n == 0 {
            return None;
        }
        Some(serde_json::from_str::<Value>(&line).unwrap())
    }

    async fn write_json_line(lines: &mut BufReader<tokio::net::TcpStream>, v: &Value) {
        let mut raw = serde_json::to_string(v).unwrap();
        raw.push('\n');
        lines.get_mut().write_all(raw.as_bytes()).await.unwrap();
        lines.get_mut().flush().await.unwrap();
    }

    #[tokio::test]
    async fn daemon_client_reuses_connection_auth_and_monotonic_reqs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dialer = Dialer {
            socket: DaemonSocket::Tcp(addr.to_string()),
            client_token: Some("tok-reuse".into()),
        };

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut io = BufReader::new(stream);

            let auth = read_json_line(&mut io).await.unwrap();
            assert_eq!(auth["type"], "auth");
            assert_eq!(auth["token"], "tok-reuse");

            let r1 = read_json_line(&mut io).await.unwrap();
            assert_eq!(r1["type"], "board");
            assert_eq!(r1["req"], 1);
            write_json_line(&mut io, &serde_json::json!({"type": "board", "req": 1})).await;

            let r2 = read_json_line(&mut io).await.unwrap();
            assert_eq!(r2["type"], "board");
            assert_eq!(r2["req"], 2);
            write_json_line(
                &mut io,
                &serde_json::json!({"type": "error", "req": 2, "reason": "refused"}),
            )
            .await;

            let r3 = read_json_line(&mut io).await.unwrap();
            assert_eq!(r3["type"], "send");
            assert_eq!(r3["req"], 3);
            assert_eq!(r3["from"], "alice");
            assert_eq!(r3["to"], "bob");
            assert_eq!(r3["mail"], "m-1");
            assert_eq!(r3["body"], "hello");
            assert_eq!(r3["headers"], serde_json::json!({"x": "1"}));
            write_json_line(
                &mut io,
                &serde_json::json!({"type": "accepted", "req": 3, "mail": "m-1"}),
            )
            .await;

            let r4 = read_json_line(&mut io).await.unwrap();
            assert_eq!(r4["type"], "send");
            assert_eq!(r4["req"], 4);
            assert_eq!(r4["group"], "ops");
            assert_eq!(r4["expected_article"], 4);
            write_json_line(
                &mut io,
                &serde_json::json!({"type": "accepted", "req": 4, "mail": "m-2", "article": 5}),
            )
            .await;

            let r5 = read_json_line(&mut io).await.unwrap();
            assert_eq!(r5["type"], "send");
            assert_eq!(r5["req"], 5);
            assert_eq!(r5["group"], "ops");
            assert!(r5.get("expected_article").is_none());
            write_json_line(
                &mut io,
                &serde_json::json!({"type": "accepted", "req": 5, "mail": "m-3", "article": 6}),
            )
            .await;
        });

        let client = DaemonClient::new(dialer);

        let reply = client
            .request(&serde_json::json!({"type": "board", "req": 99}))
            .await
            .unwrap();
        assert_eq!(reply["type"], "board");
        assert_eq!(client.connects().await, 1);

        let err = client
            .request(&serde_json::json!({"type": "board"}))
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "refused");
        assert_eq!(client.connects().await, 1, "error reply keeps socket open");

        let headers = serde_json::json!({"x": "1"});
        let mail = client
            .send_mail("alice", "bob", "m-1", "hello", Some(&headers))
            .await
            .unwrap();
        assert_eq!(mail, "m-1");
        assert_eq!(client.connects().await, 1);

        let (mail, article) = client
            .send_group("alice", "ops", "m-2", "post", None, Some(4))
            .await
            .unwrap();
        assert_eq!(mail, "m-2");
        assert_eq!(article, 5);
        assert_eq!(client.connects().await, 1);

        let (mail, article) = client
            .send_group("alice", "ops", "m-3", "post2", None, None)
            .await
            .unwrap();
        assert_eq!(mail, "m-3");
        assert_eq!(article, 6);
        assert_eq!(client.connects().await, 1);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn daemon_client_reconnects_once_on_eof_and_reauthenticates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dialer = Dialer {
            socket: DaemonSocket::Tcp(addr.to_string()),
            client_token: Some("tok-reconn".into()),
        };

        let server = tokio::spawn(async move {
            // Connection 1: answer req 1, then close so req 2 hits EOF/reset.
            {
                let (stream, _) = listener.accept().await.unwrap();
                let mut io = BufReader::new(stream);
                let auth = read_json_line(&mut io).await.unwrap();
                assert_eq!(auth["type"], "auth");
                assert_eq!(auth["token"], "tok-reconn");

                let r1 = read_json_line(&mut io).await.unwrap();
                assert_eq!(r1["req"], 1);
                write_json_line(&mut io, &serde_json::json!({"type": "board", "req": 1})).await;
            }

            // Connection 2: client retries req 2 once with fresh auth, succeeds, then server closes.
            {
                let (stream, _) = listener.accept().await.unwrap();
                let mut io = BufReader::new(stream);
                let auth = read_json_line(&mut io).await.unwrap();
                assert_eq!(auth["type"], "auth");
                assert_eq!(auth["token"], "tok-reconn");

                let r2 = read_json_line(&mut io).await.unwrap();
                assert_eq!(r2["type"], "send");
                assert_eq!(r2["req"], 2);
                write_json_line(
                    &mut io,
                    &serde_json::json!({"type": "accepted", "req": 2, "mail": "m-reconn"}),
                )
                .await;
            }

            // Connection 3: client retries req 3 once, server drops immediately without replying.
            {
                let (stream, _) = listener.accept().await.unwrap();
                let mut io = BufReader::new(stream);
                let _ = read_json_line(&mut io).await;
                let _ = read_json_line(&mut io).await;
            }
        });

        let client = DaemonClient::new(dialer);

        let reply = client
            .request(&serde_json::json!({"type": "board"}))
            .await
            .unwrap();
        assert_eq!(reply["type"], "board");
        assert_eq!(client.connects().await, 1);

        let mail = client
            .send_mail("a", "b", "m-reconn", "hi", None)
            .await
            .unwrap();
        assert_eq!(mail, "m-reconn");
        assert_eq!(client.connects().await, 2);

        // Conn 2 closed and conn 3 also closes on retry: must fail after one retry.
        let err = client
            .request(&serde_json::json!({"type": "board"}))
            .await
            .unwrap_err();
        assert!(
            is_reconnectable(&err),
            "expected reconnectable error, saw {err}"
        );
        assert_eq!(client.connects().await, 3);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn daemon_client_is_cancellation_safe() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dialer = Dialer {
            socket: DaemonSocket::Tcp(addr.to_string()),
            client_token: None,
        };
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            // Connection 1: answer req 1, then hold req 2 until caller cancels.
            let (stream1, _) = listener.accept().await.unwrap();
            let mut io1 = BufReader::new(stream1);
            let r1 = read_json_line(&mut io1).await.unwrap();
            assert_eq!(r1["req"], 1);
            write_json_line(
                &mut io1,
                &serde_json::json!({"type": "board", "req": 1, "tag": "first"}),
            )
            .await;

            let r2 = read_json_line(&mut io1).await.unwrap();
            assert_eq!(r2["req"], 2);
            let _ = seen_tx.send(());
            let _ = release_rx.await;
            // Even if a delayed reply were written here, the client dropped conn 1.
            let _ = io1
                .get_mut()
                .write_all(b"{\"type\":\"board\",\"req\":2,\"tag\":\"stale\"}\n")
                .await;

            // Connection 2: clean recovery for req 3.
            let (stream2, _) = listener.accept().await.unwrap();
            let mut io2 = BufReader::new(stream2);
            let r3 = read_json_line(&mut io2).await.unwrap();
            assert_eq!(r3["req"], 3);
            write_json_line(
                &mut io2,
                &serde_json::json!({"type": "board", "req": 3, "tag": "fresh"}),
            )
            .await;
        });

        let client = DaemonClient::new(dialer);
        let board = serde_json::json!({"type": "board"});

        let first = client.request(&board).await.unwrap();
        assert_eq!(first["tag"], "first");
        assert_eq!(client.connects().await, 1);

        // Cancel req 2 after the server has read it but before it replies.
        tokio::select! {
            _ = client.request(&board) => panic!("req 2 should have been cancelled"),
            _ = seen_rx => {}
        }
        let _ = release_tx.send(());

        // Subsequent request dials a fresh connection and never sees req 2's stale reply.
        let fresh = client.request(&board).await.unwrap();
        assert_eq!(fresh["tag"], "fresh");
        assert_eq!(client.connects().await, 2);

        server.await.unwrap();
    }
}

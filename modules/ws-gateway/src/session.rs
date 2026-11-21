//! One WebSocket connection as one `roger adapter` session: register with the
//! host daemon, write each delivery to the Store, tell the client, then Ack.
//! One text frame carries one JSON line in either direction.

use crate::cli::Dialer;
use crate::daemon::{self, DaemonClient, DaemonReader, DaemonWriter};
use crate::durlog::{blocking_io, gen_uuid, DurLog};
use crate::upgrade::{replay_start, Opened};
use futures_util::stream::{SplitSink, StreamExt};
use futures_util::SinkExt;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write as _};
#[cfg(feature = "test-hooks")]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

type Sink = SplitSink<WebSocketStream<TcpStream>, Message>;
type Out = Arc<tokio::sync::Mutex<Sink>>;
/// The Store, held while open; `None` once the session is closing, after
/// which a delivery is left unstored and unacked so it stays pending.
type Gate = Arc<Mutex<Option<DurLog>>>;

/// What every session in this process shares.
pub struct Ctx {
    pub dialer: Dialer,
    #[cfg(feature = "test-hooks")]
    pub hold_before_ack: Option<PathBuf>,
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

fn warn(msg: &str) {
    let _ = writeln!(io::stderr().lock(), "roger-ws-gateway: {msg}");
}

/// Send one JSON line as one text frame; returns once the frame is written.
async fn send_line(out: &Out, v: &Value) -> io::Result<()> {
    let text = serde_json::to_string(v).map_err(io::Error::other)?;
    out.lock()
        .await
        .send(Message::text(text))
        .await
        .map_err(io::Error::other)
}

async fn close_with(out: &Out, code: CloseCode, reason: &str) {
    let mut reason = reason.to_string();
    while reason.len() > 120 {
        reason.pop();
    }
    let mut sink = out.lock().await;
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
    let _ = sink.close().await;
}

fn push_group_fields<'a>(v: &'a Value, pairs: &mut Vec<(&'a str, Value)>) {
    if let Some(group) = v.get("group").and_then(Value::as_str) {
        pairs.push(("group", s(group)));
    }
    if let Some(article) = v.get("article").and_then(Value::as_i64) {
        pairs.push(("article", Value::Number(article.into())));
    }
}

fn mail_line(v: &Value, record: &str) -> Value {
    let get = |key: &str| v.get(key).and_then(Value::as_str).unwrap_or_default();
    let mut pairs = vec![
        ("type", s("mail")),
        ("mail", s(get("mail"))),
        ("from", s(get("from"))),
    ];
    push_group_fields(v, &mut pairs);
    pairs.extend([
        ("body", s(get("body"))),
        ("record", s(record)),
        ("kind", s(&daemon::kind_of(v))),
        ("headers", daemon::headers_of(v)),
    ]);
    obj(pairs)
}

/// Serve one upgraded connection until the client or the host daemon ends it.
pub async fn run(ctx: Arc<Ctx>, ws: WebSocketStream<TcpStream>, opened: Opened) {
    let Opened {
        address,
        label,
        declared,
        confirm,
        superseded,
        groups,
        watch,
        after,
        store,
        entries,
    } = opened;
    let (sink, mut stream) = ws.split();
    let out: Out = Arc::new(tokio::sync::Mutex::new(sink));

    let (dreader, dwriter) = match daemon::connect(&ctx.dialer).await {
        Ok(pair) => pair,
        Err(e) => {
            warn(&e.to_string());
            close_with(&out, CloseCode::Error, &e.to_string()).await;
            return;
        }
    };
    let dwriter = Arc::new(dwriter);

    let mut register = vec![("type", s("register")), ("session", s(&address))];
    if let Some(label) = &label {
        register.push(("label", s(label)));
    }
    if let Some(d) = &declared {
        register.push(("declared", s(d)));
    }
    if confirm {
        register.push(("confirm_register", Value::Bool(true)));
    }
    register.push(("notify_superseded", Value::Bool(true)));
    if watch {
        register.push(("watch", Value::Bool(true)));
    }
    if let Err(e) = dwriter.send(&obj(register)).await {
        warn(&format!(
            "expected to register with the host daemon, saw {e}"
        ));
        close_with(&out, CloseCode::Error, "host daemon connection failed").await;
        return;
    }
    if send_line(
        &out,
        &obj(vec![("type", s("ready")), ("session", s(&address))]),
    )
    .await
    .is_err()
    {
        unregister(&dwriter).await;
        return;
    }

    // Resume: Store entries after `after`, in Store line order, before anything live.
    let mut replayed: HashSet<String> = HashSet::new();
    if let Some(start) = after
        .as_deref()
        .and_then(|a| replay_start(&entries, Some(a)))
    {
        for e in &entries[start..] {
            let get = |k: &str| e.get(k).and_then(Value::as_str).unwrap_or_default();
            let line = mail_line(e, get("record"));
            if send_line(&out, &line).await.is_err() {
                unregister(&dwriter).await;
                return;
            }
            replayed.insert(get("mail").to_string());
        }
    }
    let records: HashMap<String, String> = entries
        .iter()
        .filter_map(|e| {
            Some((
                e.get("mail")?.as_str()?.to_string(),
                e.get("record")?.as_str()?.to_string(),
            ))
        })
        .collect();

    let gate: Gate = Arc::new(Mutex::new(Some(store)));
    let mut reader = tokio::spawn(reader_loop(
        Arc::clone(&ctx),
        Arc::clone(&out),
        dreader,
        Arc::clone(&dwriter),
        Arc::clone(&gate),
        records,
        replayed,
        address.clone(),
        confirm,
        superseded,
        watch,
    ));

    // Requests run one at a time, in order, so replies pair with requests by order;
    // frames keep being read meanwhile so the client's close is seen at once.
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<String, String>>();
    let worker_out = Arc::clone(&out);
    let worker_ctx = Arc::clone(&ctx);
    let me = address.clone();
    tokio::spawn(async move {
        let mut client = DaemonClient::new(worker_ctx.dialer.clone());
        while let Some(frame) = rx.recv().await {
            let reply = match frame {
                Ok(text) => client_request(&mut client, &me, groups, &text).await,
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
            };
            let reply = reply
                .unwrap_or_else(|e| obj(vec![("type", s("error")), ("error", s(&e.to_string()))]));
            if send_line(&worker_out, &reply).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            frame = stream.next() => {
                match frame {
                    Some(Ok(Message::Text(t))) => {
                        if !t.trim().is_empty() && tx.send(Ok(t.to_string())).is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        let _ = tx.send(Err(
                            "expected a text frame holding one JSON line, saw a binary frame".into(),
                        ));
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            _ = &mut reader => {
                // The host daemon connection ended: so does this session.
                close_with(&out, CloseCode::Away, "host daemon connection ended").await;
                return;
            }
        }
    }

    // The client is gone: so is this session. It leaves the Board now; Mail
    // already accepted for its Address stays pending at the coordinator.
    let _ = gate.lock().map(|mut g| g.take());
    unregister(&dwriter).await;
    reader.abort();
    let _ = out.lock().await.close().await;
}

async fn unregister(dwriter: &DaemonWriter) {
    let _ = dwriter.send(&obj(vec![("type", s("unregister"))])).await;
    let _ = dwriter.shutdown().await;
}

#[allow(clippy::too_many_arguments)]
async fn reader_loop(
    ctx: Arc<Ctx>,
    out: Out,
    mut dreader: DaemonReader,
    dwriter: Arc<DaemonWriter>,
    gate: Gate,
    mut records: HashMap<String, String>,
    mut replayed: HashSet<String>,
    address: String,
    confirm: bool,
    superseded: bool,
    watch: bool,
) {
    let _ = &ctx;
    let mut next_record = records
        .values()
        .filter_map(|r| r.strip_prefix("r-")?.parse::<u64>().ok())
        .max()
        .unwrap_or(0)
        + 1;
    let mut req = 0i64;
    loop {
        let v = match dreader.next_value().await {
            Ok(Some(v)) => v,
            Ok(None) => return,
            Err(e) => {
                warn(&format!("host daemon stream for {address}: {e}"));
                return;
            }
        };
        let Ok(kind_of_line) = daemon::field(&v, "type") else {
            return;
        };
        match kind_of_line {
            "deliver" => {
                let (Ok(mail), Ok(from), Ok(body)) = (
                    daemon::field(&v, "mail"),
                    daemon::field(&v, "from"),
                    daemon::field(&v, "body"),
                ) else {
                    return;
                };
                let kind = daemon::kind_of(&v);
                let headers = daemon::headers_of(&v);
                let record = match records.get(mail) {
                    Some(existing) => existing.clone(),
                    None => {
                        let candidate = format!("r-{next_record}");
                        let mut fields = vec![
                            ("record", s(&candidate)),
                            ("mail", s(mail)),
                            ("from", s(from)),
                        ];
                        push_group_fields(&v, &mut fields);
                        fields.extend([
                            ("body", s(body)),
                            ("kind", s(&kind)),
                            ("headers", headers.clone()),
                        ]);
                        let entry = obj(fields);
                        let Ok(line) = serde_json::to_string(&entry) else {
                            return;
                        };
                        let committed = blocking_io(|| {
                            let mut held = gate.lock().map_err(|_| {
                                io::Error::other("expected the Store gate intact, saw it poisoned")
                            })?;
                            match held.as_mut() {
                                None => Ok(false),
                                Some(store) => store.append(&line).map(|()| true),
                            }
                        });
                        match committed {
                            Ok(true) => {
                                next_record += 1;
                                records.insert(mail.to_string(), candidate.clone());
                                candidate
                            }
                            Ok(false) => continue,
                            Err(e) => {
                                warn(&format!("Store append failed for {address}: {e}"));
                                return;
                            }
                        }
                    }
                };
                if !replayed.remove(mail) {
                    let line = mail_line(&v, &record);
                    if send_line(&out, &line).await.is_err() {
                        return;
                    }
                }
                #[cfg(feature = "test-hooks")]
                if let Some(marker) = &ctx.hold_before_ack {
                    if !marker.exists() {
                        let marker = marker.clone();
                        let wrote = blocking_io(move || -> io::Result<()> {
                            let mut f = std::fs::File::create(&marker)?;
                            f.write_all(b"held\n")?;
                            f.sync_all()
                        });
                        if wrote.is_ok() {
                            std::future::pending::<()>().await;
                        }
                    }
                }
                req += 1;
                let ack = obj(vec![
                    ("type", s("ack")),
                    ("req", Value::Number(req.into())),
                    ("mail", s(mail)),
                    ("record", s(&record)),
                ]);
                if dwriter.send(&ack).await.is_err() {
                    return;
                }
            }
            "superseded" => {
                let (Ok(sess), Ok(host)) =
                    (daemon::field(&v, "session"), daemon::field(&v, "host"))
                else {
                    continue;
                };
                let notice = obj(vec![
                    ("type", s("superseded")),
                    ("session", s(sess)),
                    ("host", s(host)),
                    ("at_ms", v.get("at_ms").cloned().unwrap_or(Value::Null)),
                ]);
                let _ = writeln!(
                    io::stderr().lock(),
                    "SUPERSEDED {}: client should stop sending as this Address, notify its supervisor/user, and not automatically re-register",
                    serde_json::to_string(&notice).unwrap_or_default()
                );
                if superseded {
                    let _ = send_line(&out, &notice).await;
                }
            }
            "board_event" if watch => {
                if send_line(&out, &v).await.is_err() {
                    return;
                }
            }
            "registered" if confirm => {
                let line = obj(vec![("type", s("registered")), ("session", s(&address))]);
                if send_line(&out, &line).await.is_err() {
                    return;
                }
            }
            "error" => warn(&format!(
                "host daemon refused the session connection: {}",
                daemon::field(&v, "reason").unwrap_or("unexplained")
            )),
            _ => {}
        }
    }
}

fn without_req(mut reply: Value) -> Value {
    if let Some(fields) = reply.as_object_mut() {
        fields.remove("req");
    }
    reply
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// One client request, answered by exactly one reply line.
async fn client_request(
    client: &mut DaemonClient,
    me: &str,
    groups: bool,
    text: &str,
) -> io::Result<Value> {
    let v: Value = serde_json::from_str(text)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    let one = Value::Number(1.into());
    match daemon::field(&v, "type")? {
        "send" => client_send(client, me, &v, groups).await,
        "board" => {
            client
                .request(&obj(vec![
                    ("type", s("board")),
                    ("req", one),
                    ("session", s(me)),
                ]))
                .await
        }
        "receipt" => client
            .request(&obj(vec![
                ("type", s("receipt")),
                ("req", one),
                ("mail", s(daemon::field(&v, "mail")?)),
            ]))
            .await
            .map(without_req),
        "claim" => {
            let ttl = v.get("ttl_ms").and_then(Value::as_i64).unwrap_or(0);
            client
                .request(&obj(vec![
                    ("type", s("claim")),
                    ("req", one),
                    ("op", s(daemon::field(&v, "op")?)),
                    ("key", s(daemon::field(&v, "key")?)),
                    ("owner", s(me)),
                    ("ttl_ms", Value::Number(ttl.into())),
                ]))
                .await
        }
        "relabel" => {
            let session = v.get("session").and_then(Value::as_str).unwrap_or(me);
            client
                .request(&obj(vec![
                    ("type", s("relabel")),
                    ("req", one),
                    ("session", s(session)),
                    ("label", s(daemon::field(&v, "label")?)),
                ]))
                .await
        }
        "subscribe" if groups => {
            let mut request = vec![
                ("type", s("subscribe")),
                ("req", one),
                ("session", s(me)),
                ("group", s(daemon::field(&v, "group")?)),
            ];
            match v.get("after") {
                None | Some(Value::Null) => {}
                Some(after) => {
                    let after = after
                        .as_i64()
                        .ok_or_else(|| invalid("after must be an integer"))?;
                    request.push(("after", Value::Number(after.into())));
                }
            }
            match v.get("latest") {
                None | Some(Value::Null) | Some(Value::Bool(false)) => {}
                Some(Value::Bool(true)) => request.push(("latest", Value::Bool(true))),
                Some(_) => return Err(invalid("latest must be a boolean")),
            }
            Ok(without_req(client.request(&obj(request)).await?))
        }
        "unsubscribe" if groups => Ok(without_req(
            client
                .request(&obj(vec![
                    ("type", s("unsubscribe")),
                    ("req", one),
                    ("session", s(me)),
                    ("group", s(daemon::field(&v, "group")?)),
                ]))
                .await?,
        )),
        "groups" if groups => {
            let session = v.get("session").and_then(Value::as_str).unwrap_or(me);
            let mut request = vec![("type", s("groups")), ("req", one), ("session", s(session))];
            if let Some(prefix) = v.get("prefix").and_then(Value::as_str) {
                request.push(("prefix", s(prefix)));
            }
            if v.get("include_retired") == Some(&Value::Bool(true)) {
                request.push(("include_retired", Value::Bool(true)));
            }
            Ok(without_req(client.request(&obj(request)).await?))
        }
        "articles" if groups => {
            let after = v.get("after").and_then(Value::as_i64).unwrap_or(0);
            let limit = v.get("limit").and_then(Value::as_i64).unwrap_or(100);
            Ok(without_req(
                client
                    .request(&obj(vec![
                        ("type", s("articles")),
                        ("req", one),
                        ("group", s(daemon::field(&v, "group")?)),
                        ("after", Value::Number(after.into())),
                        ("limit", Value::Number(limit.into())),
                    ]))
                    .await?,
            ))
        }
        "group_policy" if groups => {
            let mut request = vec![
                ("type", s("group_policy")),
                ("req", one),
                ("group", s(daemon::field(&v, "group")?)),
            ];
            for key in ["keep_last", "ttl_ms", "retired"] {
                if let Some(val) = v.get(key) {
                    request.push((key, val.clone()));
                }
            }
            Ok(without_req(client.request(&obj(request)).await?))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            if groups {
                format!(
                    "expected request send, board, relabel, claim, subscribe, unsubscribe, or groups; saw {other}"
                )
            } else {
                format!("expected request send, board, relabel, or claim; saw {other}")
            },
        )),
    }
}

async fn client_send(
    client: &mut DaemonClient,
    me: &str,
    v: &Value,
    groups: bool,
) -> io::Result<Value> {
    // A missing mail id is generated; a present one of the wrong type is a
    // malformed send, not a request to pick another id.
    let mail = match v.get("mail") {
        None => blocking_io(gen_uuid)?,
        Some(m) => m
            .as_str()
            .ok_or_else(|| invalid("mail must be a string"))?
            .to_string(),
    };
    let to = match (groups, v.get("to"), v.get("group")) {
        (false, _, _) => daemon::field(v, "to")?,
        (true, Some(_), Some(_)) => {
            return Err(invalid("expected either to or group on send, saw both"))
        }
        (true, None, None) => return Err(invalid("expected to or group on send, saw neither")),
        (true, Some(to), None) => to.as_str().ok_or_else(|| invalid("to must be a string"))?,
        (true, None, Some(group)) => {
            let group = group
                .as_str()
                .ok_or_else(|| invalid("group must be a string"))?;
            let expected_article =
                match v.get("expected_article") {
                    None | Some(Value::Null) => None,
                    Some(x) => Some(x.as_i64().filter(|n| *n >= 0).ok_or_else(|| {
                        invalid("expected_article must be a non-negative integer")
                    })?),
                };
            let (mail, article) = client
                .send_group(
                    me,
                    group,
                    &mail,
                    daemon::field(v, "body")?,
                    v.get("headers"),
                    expected_article,
                )
                .await?;
            return Ok(obj(vec![
                ("type", s("accepted")),
                ("mail", s(&mail)),
                ("group", s(group)),
                ("article", Value::Number(article.into())),
            ]));
        }
    };
    let mail = client
        .send_mail(me, to, &mail, daemon::field(v, "body")?, v.get("headers"))
        .await?;
    Ok(obj(vec![("type", s("accepted")), ("mail", s(&mail))]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DaemonSocket;
    use std::path::PathBuf;

    #[tokio::test]
    async fn default_connection_refuses_group_requests_and_still_requires_to() {
        let dialer = Dialer {
            socket: DaemonSocket::Unix(PathBuf::from("unused-daemon.socket")),
            client_token: None,
        };
        let mut client = DaemonClient::new(dialer);
        let send = client_request(
            &mut client,
            "reader-1",
            false,
            r#"{"type":"send","group":"staff","body":"hello"}"#,
        )
        .await
        .unwrap_err();
        assert_eq!(send.to_string(), "missing to");
        for request in ["subscribe", "unsubscribe", "groups"] {
            let error = client_request(
                &mut client,
                "reader-1",
                false,
                &format!(r#"{{"type":"{request}","group":"staff"}}"#),
            )
            .await
            .unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("expected request send, board, relabel, or claim; saw {request}")
            );
        }
    }
}

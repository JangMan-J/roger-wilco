//! A contract-shaped session: registers with its host daemon,
//! writes delivered mail to its own durable Store, and acks naming the
//! Store record. It knows nothing about the core beyond the socket.

use crate::claims::Op;
use crate::durlog::DurLog;
use crate::json::{self, obj, s, Value};
use crate::net;
use std::collections::HashMap;
use std::io::{self, Read as _, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Test hook (feature `test-hooks` only): when set, the first delivery is
/// written to the Store, the marker file is created, and the session parks
/// before its ack until killed. On restart the marker exists and it acks.
#[cfg(feature = "test-hooks")]
pub static HOLD_BEFORE_ACK: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// What the MCP face hooks into the session loop; the loop itself is shared.
pub struct McpHooks {
    /// Called once the coordinator holds the Register (`registered`).
    pub confirmed: Box<dyn Fn() + Send>,
    /// Called once after Register is sent, with the writer to the host daemon.
    pub registered: Option<Box<dyn FnOnce(net::Writer) + Send>>,
    /// Called after each delivery has reached the Store, never before.
    pub stored: Box<dyn Fn() + Send>,
    /// Held across a Store write; true once the face is closing, after which
    /// a delivery is left unstored and unacked so it stays pending.
    pub gate: Arc<Mutex<bool>>,
}

/// How a client reaches this session: the Tracer's own stdout, the adapter's
/// client stream, or the MCP server's hooks.
pub enum Face {
    Tracer,
    /// The client stream, and whether the client asked for `registered`.
    Adapter(net::Writer, bool),
    Mcp(McpHooks),
}

/// The Tracer: a session with no harness, reporting on its own stdout.
pub fn run(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
) -> io::Result<()> {
    serve(socket, id, label, store_path, declared, Face::Tracer)
}

/// The adapter: the same session, fronted by one line-delimited JSON stream
/// on stdin/stdout for a client. The client sends `{"type":"send","to","body"}`
/// (optional `"mail"` and `"headers"`, an object of string values), `{"type":"board"}` (read as this session),
/// `{"type":"relabel","label"}` (optional `"session"`, default this one), or
/// `{"type":"claim","op","key"}` (optional `"ttl_ms"`, owned by this session),
/// and is answered in order with `accepted`, `board`, `relabeled`,
/// `claim_result`, or `error`; it is also told `ready` once and `mail` per delivery,
/// which carries the delivery's `kind` and `headers`. A `mail`
/// line follows the Store write and never gates the ack: the client renders
/// what the adapter already holds and acknowledges nothing.
/// `declared` is the client's opaque self-description, sent with Register.
/// `ready` means the Register was written to the host daemon. With
/// `confirm` (the `--registered` flag) the client is also sent
/// `{"type":"registered","session"}` once, after `ready`, when the coordinator
/// holds the Register; without it that line is never sent.
/// When the client's stdin closes, the session unregisters and the process exits.
pub fn adapter(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    confirm: bool,
) -> io::Result<()> {
    let out = net::writer(Box::new(io::stdout()));
    serve(
        socket,
        id,
        label,
        store_path,
        declared,
        Face::Adapter(out, confirm),
    )
}

/// The adapter's client side, started once the session has registered.
fn client_io(socket: PathBuf, me: String, to_client: net::Writer, daemon: net::Writer) {
    // A send blocks until the core answers, so sends run on a worker, one at a
    // time and in order (a client pairs replies to sends by order). The reader
    // below only reads stdin, so the client's EOF ends this process at once even
    // while a send waits.
    let (tx, rx) = std::sync::mpsc::channel::<io::Result<Value>>();
    std::thread::spawn(move || {
        for line in rx {
            let reply = line.and_then(|v| client_request(&socket, &me, &v));
            let _ = net::send(
                &to_client,
                &reply.unwrap_or_else(|e| {
                    obj(vec![("type", s("error")), ("error", s(&e.to_string()))])
                }),
            );
        }
    });
    std::thread::spawn(move || {
        for line in net::lines(Box::new(io::stdin())) {
            if tx.send(line).is_err() {
                break;
            }
        }
        // The client's stdin closed: its harness is gone, so this session is
        // too. It leaves the Board and every later Broadcast now; Mail already
        // accepted for its Address stays pending at the coordinator.
        let _ = net::send(&daemon, &obj(vec![("type", s("unregister"))]));
        std::process::exit(0);
    });
}

fn client_request(socket: &PathBuf, me: &str, v: &Value) -> io::Result<Value> {
    match net::field(v, "type")? {
        "send" => client_send(socket, me, v),
        "board" => request(
            socket,
            &obj(vec![
                ("type", s("board")),
                ("req", Value::Int(1)),
                ("session", s(me)),
            ]),
        ),
        "claim" => request(
            socket,
            &obj(vec![
                ("type", s("claim")),
                ("req", Value::Int(1)),
                ("op", s(net::field(v, "op")?)),
                ("key", s(net::field(v, "key")?)),
                ("owner", s(me)),
                (
                    "ttl_ms",
                    Value::Int(v.get("ttl_ms").and_then(|x| x.as_i64()).unwrap_or(0)),
                ),
            ]),
        ),
        "relabel" => request(
            socket,
            &obj(vec![
                ("type", s("relabel")),
                ("req", Value::Int(1)),
                (
                    "session",
                    s(v.get("session").and_then(|x| x.as_str()).unwrap_or(me)),
                ),
                ("label", s(net::field(v, "label")?)),
            ]),
        ),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected request send, board, relabel, or claim; saw {other}"),
        )),
    }
}

fn client_send(socket: &PathBuf, from: &str, v: &Value) -> io::Result<Value> {
    // A missing mail id is generated; a present one of the wrong type is a
    // malformed send, not a request to pick another id.
    let mail = match v.get("mail") {
        None => gen_uuid()?,
        Some(m) => m
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mail must be a string"))?
            .to_string(),
    };
    let mail = send_mail(
        socket,
        from,
        net::field(v, "to")?,
        &mail,
        net::field(v, "body")?,
        v.get("headers"),
    )?;
    Ok(obj(vec![("type", s("accepted")), ("mail", s(&mail))]))
}

/// The session loop all three faces share: register, write each delivery to
/// the Store, then ack naming its Record id. Returns when the host daemon
/// connection ends.
pub fn serve(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    face: Face,
) -> io::Result<()> {
    let (client, mut mcp, confirm) = match face {
        Face::Tracer => (None, None, true),
        Face::Adapter(w, c) => (Some(w), None, c),
        Face::Mcp(h) => (None, Some(h), true),
    };
    let tracer = client.is_none() && mcp.is_none();
    let (mut store, prior) = DurLog::open(&store_path)?;
    let mut records: HashMap<String, String> = HashMap::new(); // mail -> record
    for line in &prior {
        if let Ok(v) = json::parse(line) {
            if let (Ok(mail), Ok(record)) = (net::field(&v, "mail"), net::field(&v, "record")) {
                records.insert(mail.to_string(), record.to_string());
            }
        }
    }
    let mut next_record = records.len() as u64 + 1;

    let conn = UnixStream::connect(&socket).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("expected a host daemon at {}, saw {e}", socket.display()),
        )
    })?;
    let writer = net::writer(Box::new(conn.try_clone()?));
    let lines = net::lines(Box::new(conn));
    let mut register = vec![("type", s("register")), ("session", s(&id))];
    if let Some(label) = &label {
        register.push(("label", s(label)));
    }
    if let Some(d) = &declared {
        register.push(("declared", s(d)));
    }
    if confirm {
        register.push(("confirm_register", Value::Bool(true)));
    }
    net::send(&writer, &obj(register))?;
    match &client {
        Some(w) => {
            net::send(w, &obj(vec![("type", s("ready")), ("session", s(&id))]))?;
            client_io(socket, id.clone(), w.clone(), writer.clone());
        }
        None if tracer => {
            println!("READY session={id}");
            io::stdout().flush()?;
        }
        None => {}
    }
    if let Some(registered) = mcp.as_mut().and_then(|h| h.registered.take()) {
        registered(writer.clone());
    }

    let mut req = 0i64;
    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "deliver" => {
                let mail = net::field(&v, "mail")?.to_string();
                let record = match records.get(&mail) {
                    Some(existing) => existing.clone(),
                    None => {
                        let held = match &mcp {
                            Some(h) => {
                                let closing = h.gate.lock().unwrap();
                                if *closing {
                                    continue;
                                }
                                Some(closing)
                            }
                            None => None,
                        };
                        let record = format!("r-{next_record}");
                        next_record += 1;
                        let entry = obj(vec![
                            ("record", s(&record)),
                            ("mail", s(&mail)),
                            ("from", s(net::field(&v, "from")?)),
                            ("body", s(net::field(&v, "body")?)),
                            ("kind", s(&net::kind_of(&v))),
                            ("headers", net::headers_of(&v)),
                        ]);
                        store.append(&json::stringify(&entry))?;
                        records.insert(mail.clone(), record.clone());
                        drop(held);
                        if let Some(h) = &mcp {
                            (h.stored)();
                        }
                        if tracer {
                            println!(
                                "STORED mail={mail} record={record} kind={}",
                                net::kind_of(&v)
                            );
                            print_headers(&mail, &net::headers_of(&v));
                            io::stdout().flush()?;
                        }
                        record
                    }
                };
                if let Some(w) = &client {
                    let kind = net::kind_of(&v);
                    net::send(
                        w,
                        &obj(vec![
                            ("type", s("mail")),
                            ("mail", s(&mail)),
                            ("from", s(net::field(&v, "from")?)),
                            ("body", s(net::field(&v, "body")?)),
                            ("record", s(&record)),
                            ("kind", s(&kind)),
                            ("headers", net::headers_of(&v)),
                        ]),
                    )?;
                }
                #[cfg(feature = "test-hooks")]
                if let Some(marker) = HOLD_BEFORE_ACK.get() {
                    if !marker.exists() {
                        std::fs::write(marker, "held\n")?;
                        loop {
                            std::thread::park();
                        }
                    }
                }
                req += 1;
                net::send(
                    &writer,
                    &obj(vec![
                        ("type", s("ack")),
                        ("req", Value::Int(req)),
                        ("mail", s(&mail)),
                        ("record", s(&record)),
                    ]),
                )?;
            }
            "registered" if confirm => {
                if let Some(w) = &client {
                    net::send(
                        w,
                        &obj(vec![("type", s("registered")), ("session", s(&id))]),
                    )?;
                } else if let Some(h) = &mcp {
                    (h.confirmed)();
                } else {
                    println!("REGISTERED session={id}");
                    io::stdout().flush()?;
                }
            }
            "acked" if tracer => {
                println!(
                    "DELIVERED mail={} record={}",
                    net::field(&v, "mail")?,
                    net::field(&v, "record")?
                );
                io::stdout().flush()?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// The client: one send, reporting acceptance on stdout.
pub fn send(
    socket: PathBuf,
    from: String,
    to: String,
    mail: String,
    body: String,
    headers: Option<Value>,
) -> io::Result<()> {
    let mail = send_mail(&socket, &from, &to, &mail, &body, headers.as_ref())?;
    println!("ACCEPTED mail={mail}");
    Ok(())
}

/// The client: give a session a new label. Its Address and mailbox stay.
pub fn relabel(socket: PathBuf, session: String, label: String) -> io::Result<()> {
    let reply = request(
        &socket,
        &obj(vec![
            ("type", s("relabel")),
            ("req", Value::Int(1)),
            ("session", s(&session)),
            ("label", s(&label)),
        ]),
    )?;
    println!("RELABELED {}", json::stringify(&reply));
    Ok(())
}

pub fn send_mail(
    socket: &PathBuf,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let conn = UnixStream::connect(socket)?;
    let writer = net::writer(Box::new(conn.try_clone()?));
    let mut lines = net::lines(Box::new(conn));
    let mut ask = vec![
        ("type", s("send")),
        ("req", Value::Int(1)),
        ("mail", s(mail)),
        ("from", s(from)),
        ("to", s(to)),
        ("body", s(body)),
    ];
    if let Some(h) = headers {
        ask.push(("headers", h.clone()));
    }
    net::send(&writer, &obj(ask))?;
    for line in &mut lines {
        let v = line?;
        match net::field(&v, "type")? {
            "accepted" => return Ok(net::field(&v, "mail")?.to_string()),
            "error" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "send of {mail} refused: {}",
                        net::field(&v, "reason").unwrap_or("unexplained")
                    ),
                ))
            }
            _ => {}
        }
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no accepted"))
}

/// The line that follows a Mail's own line when it has headers: the set as
/// one JSON object, key order as the sender gave it.
fn print_headers(mail: &str, headers: &Value) {
    if matches!(headers, Value::Obj(pairs) if !pairs.is_empty()) {
        println!("HEADERS mail={mail} {}", json::stringify(headers));
    }
}

/// One request to the host daemon, answered by exactly one line.
pub fn request(socket: &PathBuf, v: &Value) -> io::Result<Value> {
    let conn = UnixStream::connect(socket)?;
    let writer = net::writer(Box::new(conn.try_clone()?));
    net::send(&writer, v)?;
    let reply = net::lines(Box::new(conn)).next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {}, saw EOF", socket.display()),
        )
    })??;
    if net::field(&reply, "type")? == "error" {
        return Err(io::Error::other(match net::field(&reply, "reason") {
            Ok(reason) => reason.to_string(),
            Err(_) => format!(
                "expected a reason in the error reply, saw {}",
                json::stringify(&reply)
            ),
        }));
    }
    Ok(reply)
}

fn holder_text(v: &Value) -> String {
    match (
        net::field(v, "holder"),
        v.get("remaining_ms").and_then(|x| x.as_i64()),
    ) {
        (Ok(h), Some(r)) => format!("holder={h} remaining_ms={r} "),
        _ => String::new(),
    }
}

/// The client: one Mail's lifecycle at the coordinator. Prints `RECEIPT` once
/// the recipient has acked, else `PENDING`. Both stamps are on the
/// coordinator's clock, so `latency_ms` is their difference; a stamp the
/// coordinator's log never held prints `absent`, never zero.
pub fn receipt(socket: PathBuf, mail: String) -> io::Result<()> {
    let reply = request(
        &socket,
        &obj(vec![
            ("type", s("receipt")),
            ("req", Value::Int(1)),
            ("mail", s(&mail)),
        ]),
    )?;
    let accepted = reply.get("accepted_at_ms").and_then(|x| x.as_i64());
    let shown = |t: Option<i64>| t.map_or_else(|| "absent".to_string(), |t| t.to_string());
    let to = net::field(&reply, "to")?;
    let kind = net::field(&reply, "kind")
        .map(|k| format!(" kind={k}"))
        .unwrap_or_default();
    match net::field(&reply, "record") {
        Ok(record) => {
            let receipted = reply.get("receipted_at_ms").and_then(|x| x.as_i64());
            let latency = accepted.zip(receipted).map(|(a, r)| r - a);
            println!(
                "RECEIPT mail={mail} to={to} record={record} accepted_at_ms={} receipted_at_ms={} latency_ms={}{kind}",
                shown(accepted),
                shown(receipted),
                shown(latency)
            );
        }
        Err(_) => println!(
            "PENDING mail={mail} to={to} accepted_at_ms={}{kind}",
            shown(accepted)
        ),
    }
    print_headers(&mail, &net::headers_of(&reply));
    Ok(())
}

/// The client: one claim operation (`acquire`, `renew`, or `release`) for
/// `owner` on `key`. Prints the outcome; returns false when refused.
pub fn claim(
    socket: PathBuf,
    op: &Op,
    owner: String,
    key: String,
    ttl_ms: i64,
) -> io::Result<bool> {
    let reply = request(
        &socket,
        &obj(vec![
            ("type", s("claim")),
            ("req", Value::Int(1)),
            ("op", s(op.name())),
            ("key", s(&key)),
            ("owner", s(&owner)),
            ("ttl_ms", Value::Int(ttl_ms)),
        ]),
    )?;
    let ok = matches!(reply.get("ok"), Some(Value::Bool(true)));
    let word = match (ok, op) {
        (false, _) => "REFUSED",
        (true, Op::Acquire) => "CLAIMED",
        (true, Op::Renew) => "RENEWED",
        (true, Op::Release) => "RELEASED",
    };
    let reason = match net::field(&reply, "reason") {
        Ok(r) => format!("reason={r:?} "),
        Err(_) => String::new(),
    };
    println!(
        "{word} op={} {reason}{}key={key}",
        op.name(),
        holder_text(&reply)
    );
    Ok(ok)
}

/// The client: one Board read, one line per registered session and per live
/// claim. `from` names the reading session, which counts as its activity.
pub fn board(socket: PathBuf, from: Option<String>) -> io::Result<()> {
    let mut ask = vec![("type", s("board")), ("req", Value::Int(1))];
    if let Some(from) = &from {
        ask.push(("session", s(from)));
    }
    let reply = request(&socket, &obj(ask))?;
    let list = |name: &str| match reply.get(name) {
        Some(Value::Arr(items)) => items.clone(),
        _ => Vec::new(),
    };
    let (sessions, claims) = (list("sessions"), list("claims"));
    for v in &sessions {
        println!(
            "SESSION address={} host={} connected={} last_seen_ms={} label={}",
            net::field(v, "session")?,
            net::field(v, "host")?,
            v.get("connected") == Some(&Value::Bool(true)),
            v.get("last_seen_ms").and_then(|x| x.as_i64()).unwrap_or(0),
            net::field(v, "label")?
        );
        // Its own line: label and declared text are both free to the end.
        if let Ok(d) = net::field(v, "declared") {
            println!(
                "DECLARED address={} declared={d}",
                net::field(v, "session")?
            );
        }
    }
    for v in &claims {
        println!("CLAIM {}key={}", holder_text(v), net::field(v, "key")?);
    }
    println!("BOARD sessions={} claims={}", sessions.len(), claims.len());
    Ok(())
}

pub fn gen_uuid() -> io::Result<String> {
    let mut b = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    ))
}

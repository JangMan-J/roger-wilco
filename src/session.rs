//! A contract-shaped session: registers with its host daemon,
//! writes delivered mail to its own durable Store, and acks naming the
//! Store record. It knows nothing about the core beyond the socket.

use crate::claims::Op;
use crate::durlog::{DurLog, Policy};
use crate::json::{self, obj, s, Value};
use crate::net;
use std::collections::HashMap;
use std::io::{self, BufRead, Read as _, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// A session's Store: the file it writes each delivery to, when that file
/// rotates (`--rotate-bytes`), and whether a rotation culls (`--cull`).
pub struct Store {
    pub path: PathBuf,
    pub policy: Policy,
    pub cull: bool,
}

impl Store {
    /// The Store at `path` with the rotation flags on a session's command
    /// line: `--rotate-bytes <n>`, and `--cull`, which needs it. A Store has
    /// no clock, so the coordinator's `--cull-after-ms` is refused here.
    pub fn from_args(path: PathBuf, args: &[String]) -> io::Result<Store> {
        let (policy, cull) = rotation(args)?;
        Ok(Store { path, policy, cull })
    }
}

/// `--rotate-bytes <n>` and `--cull` from a session's command line.
pub fn rotation(args: &[String]) -> io::Result<(Policy, bool)> {
    let bad = |m: String| io::Error::new(io::ErrorKind::InvalidInput, m);
    if args.iter().any(|a| a == "--cull-after-ms") {
        return Err(bad(
            "expected --cull to cull a Store at its rotations (a Store has no clock; --cull-after-ms is the coordinator's), saw --cull-after-ms"
                .into(),
        ));
    }
    let policy = Policy::from_args(args)?;
    let cull = args.iter().any(|a| a == "--cull");
    if cull && policy.rotate_bytes == 0 {
        let saw = if args.iter().any(|a| a == "--rotate-bytes") {
            "--rotate-bytes 0"
        } else {
            "none"
        };
        return Err(bad(format!(
            "expected --rotate-bytes above 0 with --cull (culling happens when a Store rotates), saw {saw}"
        )));
    }
    Ok((policy, cull))
}

/// Test hook (feature `test-hooks` only): when set, the first delivery is
/// written to the Store, the marker file is created, and the session parks
/// before its ack until killed. On restart the marker exists and it acks.
#[cfg(feature = "test-hooks")]
pub static HOLD_BEFORE_ACK: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// What the MCP face hooks into the session loop; the loop itself is shared.
pub struct McpHooks {
    /// Called once the coordinator holds the Register (`registered`).
    pub confirmed: Box<dyn Fn() + Send>,
    pub superseded: Box<dyn Fn(Value) + Send>,
    /// Called once after Register is sent, with the writer to the host daemon.
    pub registered: Option<Box<dyn FnOnce(net::Writer) + Send>>,
    /// Called after each delivery has reached the Store, never before.
    pub stored: Box<dyn Fn() + Send>,
    /// The Record id the inbox has returned through, None before it has
    /// returned any. Asked when the Store rotates: no entry after it is culled.
    pub position: Box<dyn Fn() -> Option<String> + Send>,
    /// Held across a Store write and a rotation; true once the face is
    /// closing, after which a delivery is left unstored and unacked so it
    /// stays pending.
    pub gate: Arc<Mutex<bool>>,
}

/// How a client reaches this session: the Tracer's own stdout, the adapter's
/// client stream, or the MCP server's hooks.
pub enum Face {
    Tracer,
    /// The client stream, and independent `registered` / `superseded` opt-ins.
    Adapter(net::Writer, bool, bool),
    Mcp(McpHooks),
}

/// The Tracer: a session with no harness, reporting on its own stdout.
pub fn run(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store: Store,
    declared: Option<String>,
) -> io::Result<()> {
    serve(socket, id, label, store, declared, Face::Tracer)
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
/// holds the Register; without it that line is never sent. `superseded`
/// independently enables replacement notices on stdout; stderr always warns.
/// When the client's stdin closes, the session unregisters and the process exits.
pub fn adapter(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store: Store,
    declared: Option<String>,
    confirm: bool,
    superseded: bool,
) -> io::Result<()> {
    let out = net::writer(Box::new(io::stdout()));
    serve(
        socket,
        id,
        label,
        store,
        declared,
        Face::Adapter(out, confirm, superseded),
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
    store: Store,
    declared: Option<String>,
    face: Face,
) -> io::Result<()> {
    let (client, mut mcp, confirm, superseded) = match face {
        Face::Tracer => (None, None, true, true),
        Face::Adapter(w, c, n) => (Some(w), None, c, n),
        Face::Mcp(h) => (None, Some(h), true, true),
    };
    let tracer = client.is_none() && mcp.is_none();
    let (mut log, prior) = DurLog::open(&store.path)?;
    let mut records: HashMap<String, String> = HashMap::new(); // mail -> record
    let mut next_record = replay(prior, &mut records)?;
    {
        let _held = mcp.as_ref().map(|h| h.gate.lock().unwrap());
        rotate_if_due(&mut log, &store, next_record, &records, mcp.as_ref());
    }

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
    register.push(("notify_superseded", Value::Bool(true)));
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
                        log.append(&json::stringify(&entry))?;
                        records.insert(mail.clone(), record.clone());
                        rotate_if_due(&mut log, &store, next_record, &records, mcp.as_ref());
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
            "superseded" => {
                let notice = obj(vec![
                    ("type", s("superseded")),
                    ("session", s(net::field(&v, "session")?)),
                    ("host", s(net::field(&v, "host")?)),
                    ("at_ms", v.get("at_ms").cloned().unwrap_or(Value::Null)),
                ]);
                if let Some(h) = &mcp {
                    (h.superseded)(notice.clone());
                }
                let _ = writeln!(io::stderr().lock(), "SUPERSEDED {}: client should stop sending as this Address, notify its supervisor/user, and not automatically re-register", json::stringify(&notice));
                if let Some(w) = &client {
                    if superseded {
                        let _ = net::send(w, &notice);
                    }
                } else if tracer {
                    let mut out = io::stdout().lock();
                    let _ = writeln!(out, "SUPERSEDED {}", json::stringify(&notice));
                    let _ = out.flush();
                }
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

/// Read a Store's replay into `records` (Mail id to Record id, from entries
/// and tombstones alike) and return the next Record number. A file that a
/// rotation started holds a snapshot block whose `snapshot` line carries
/// `next_record`, the number that was next when it was written; the entries
/// appended after its `snapshot_end` each took one more. A file with no
/// snapshot numbers from 1, one per entry. The lines inside a snapshot block
/// are never counted: a cull or a hand edit may have left fewer of them than
/// the Record ids they once took. And the next number is always past the
/// highest `r-<n>` the file shows anywhere, so a block that lost its
/// `snapshot_end`, or a `next_record` that was lowered, brings no id back.
fn replay(
    prior: impl Iterator<Item = io::Result<String>>,
    records: &mut HashMap<String, String>,
) -> io::Result<u64> {
    let mut snapshot_next: Option<u64> = None;
    let mut inside = false;
    let mut since = 0u64;
    let mut highest = 0u64;
    for line in prior {
        let Ok(v) = json::parse(&line?) else {
            continue;
        };
        if v.get("snapshot").is_some() {
            let next = v
                .get("next_record")
                .and_then(Value::as_i64)
                .and_then(|n| u64::try_from(n).ok())
                .filter(|n| *n >= 1)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "expected the Store's snapshot line to carry next_record, a whole number from 1, saw {}",
                            json::stringify(&v)
                        ),
                    )
                })?;
            snapshot_next = Some(next);
            inside = true;
            since = 0;
            continue;
        }
        if v.get("snapshot_end").is_some() {
            inside = false;
            continue;
        }
        if let (Ok(mail), Ok(record)) = (net::field(&v, "mail"), net::field(&v, "record")) {
            let fresh = records
                .insert(mail.to_string(), record.to_string())
                .is_none();
            if fresh && !inside {
                since += 1;
            }
            // This session's own ids: `r-` and the number it counted to.
            if let Some(n) = record
                .strip_prefix("r-")
                .and_then(|n| n.parse::<u64>().ok())
            {
                highest = highest.max(n);
            }
        }
    }
    Ok((snapshot_next.unwrap_or(1) + since).max(highest.saturating_add(1)))
}

/// Rotate the Store once it is due under its policy: a fresh file restates
/// the current one (see `restate`), its `snapshot` line carrying
/// `next_record`, and the current file stays whole beside it. With `--cull`,
/// an entry an earlier rotation restated is culled, so a Store keeps its
/// most recent Mail whole through one rotation. On the MCP face (`mcp`) an
/// entry the inbox has not returned is never culled, so with no read
/// position, or one this Store does not hold, nothing is. A rotation that
/// fails leaves the Store on its old file; the failure goes to stderr and the
/// session carries on.
fn rotate_if_due(
    log: &mut DurLog,
    store: &Store,
    next_record: u64,
    records: &HashMap<String, String>,
    mcp: Option<&McpHooks>,
) {
    if !log.due(&store.policy) {
        return;
    }
    let (cull, through) = match mcp.map(|h| (h.position)()) {
        None => (store.cull, None),
        Some(Some(p)) if records.values().any(|r| *r == p) => (store.cull, Some(p)),
        Some(_) => (false, None),
    };
    let lines = std::fs::File::open(&store.path)
        .and_then(|f| restate(io::BufReader::new(f), cull, through.as_deref()));
    let rotated = match lines {
        Ok(lines) => log
            .rotate(
                vec![(
                    "next_record",
                    Value::Int(i64::try_from(next_record).unwrap_or(i64::MAX)),
                )],
                &lines,
            )
            .map(|_| ()),
        // Not tried again on every append: only after more growth.
        Err(e) => {
            log.put_off();
            Err(e)
        }
    };
    if let Err(e) = rotated {
        eprintln!(
            "Store {} not rotated, kept on its current file: {e}",
            store.path.display()
        );
    }
}

/// The lines a rotation restates, read from the Store file in order: each
/// tombstone as it is, and each entry as it is unless it is culled. With
/// `cull`, an entry inside the file's snapshot block, which an earlier
/// rotation already restated, becomes a tombstone that keeps its Record id
/// and Mail id; an entry after the block, or in a file with no block, is
/// kept. `through` is the Record id a reader has returned through: no entry
/// after it is culled. Snapshot lines and anything that is neither an entry
/// nor a tombstone are left to the archive.
fn restate(mut file: impl BufRead, cull: bool, through: Option<&str>) -> io::Result<Vec<String>> {
    /// One entry or tombstone of the file.
    struct Held {
        line: String,
        record: String,
        mail: String,
        tombstone: bool,
        inside: bool,
    }
    let mut held: Vec<Held> = Vec::new();
    let mut inside = false;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        // Only complete lines: a fragment at the end is no entry yet.
        if file.read_until(b'\n', &mut buf)? == 0 || buf.last() != Some(&b'\n') {
            break;
        }
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        // Read as the replay reads it, so a line the session counts is a
        // line this restates: bytes that are not UTF-8 do not lose the line.
        let line = String::from_utf8_lossy(&buf).into_owned();
        let Ok(v) = json::parse(&line) else {
            continue;
        };
        if v.get("snapshot").is_some() {
            inside = true;
            continue;
        }
        if v.get("snapshot_end").is_some() {
            inside = false;
            continue;
        }
        let (Ok(record), Ok(mail)) = (net::field(&v, "record"), net::field(&v, "mail")) else {
            continue;
        };
        held.push(Held {
            record: record.to_string(),
            mail: mail.to_string(),
            tombstone: v.get("culled") == Some(&Value::Bool(true)),
            inside,
            line,
        });
    }
    // Nothing is culled from a file that does not say plainly what an
    // earlier rotation restated: a block with no `snapshot_end` may run into
    // entries written since. Nor when the reader's position is not among the
    // lines: then nothing says which entries it has been given.
    let known = through.is_none_or(|t| held.iter().any(|h| h.record == t));
    let mut reach = cull && !inside && known;
    let mut kept = Vec::with_capacity(held.len());
    for h in held {
        if reach && h.inside && !h.tombstone {
            kept.push(json::stringify(&obj(vec![
                ("record", s(&h.record)),
                ("mail", s(&h.mail)),
                ("culled", Value::Bool(true)),
            ])));
        } else {
            kept.push(h.line);
        }
        if through == Some(h.record.as_str()) {
            reach = false;
        }
    }
    Ok(kept)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn replayed(text: &str) -> io::Result<(u64, HashMap<String, String>)> {
        let mut records = HashMap::new();
        let next = replay(text.lines().map(|l| Ok(l.to_string())), &mut records)?;
        Ok((next, records))
    }

    #[test]
    fn the_next_record_is_past_every_record_the_file_shows_whatever_its_block_says() {
        // A block that lost its end line: the two entries after it still count.
        let (next, records) = replayed(concat!(
            "{\"snapshot\":1,\"prior\":\"s.1\",\"next_record\":3}\n",
            "{\"record\":\"r-1\",\"mail\":\"a\",\"from\":\"x\",\"body\":\"\"}\n",
            "{\"record\":\"r-2\",\"mail\":\"b\",\"culled\":true}\n",
            "{\"record\":\"r-3\",\"mail\":\"c\",\"from\":\"x\",\"body\":\"\"}\n",
            "{\"record\":\"r-4\",\"mail\":\"d\",\"from\":\"x\",\"body\":\"\"}\n",
        ))
        .unwrap();
        assert_eq!(next, 5);
        assert_eq!(records.len(), 4);
        // A `next_record` lowered by hand below what the file holds.
        let (next, _) = replayed(concat!(
            "{\"snapshot\":1,\"prior\":\"s.1\",\"next_record\":1}\n",
            "{\"record\":\"r-7\",\"mail\":\"a\",\"culled\":true}\n",
            "{\"snapshot_end\":1}\n",
        ))
        .unwrap();
        assert_eq!(next, 8);
    }

    fn restated(text: &[u8], cull: bool, through: Option<&str>) -> Vec<String> {
        restate(io::Cursor::new(text.to_vec()), cull, through).unwrap()
    }

    #[test]
    fn a_line_that_is_not_utf8_is_restated_as_the_replay_reads_it() {
        let mut file = b"{\"record\":\"r-1\",\"mail\":\"a\",\"from\":\"x\",\"body\":\"".to_vec();
        file.extend_from_slice(b"\xff\"}\n");
        file.extend_from_slice(
            b"{\"record\":\"r-2\",\"mail\":\"b\",\"from\":\"x\",\"body\":\"ok\"}\n",
        );
        let kept = restated(&file, true, None);
        assert_eq!(kept.len(), 2, "{kept:?}");
        assert!(
            kept[0].starts_with("{\"record\":\"r-1\",\"mail\":\"a\""),
            "{kept:?}"
        );
        // The same two Mail the replay of that file knows.
        let (next, records) = replayed(&String::from_utf8_lossy(&file)).unwrap();
        assert_eq!((next, records.len()), (3, 2));
    }

    #[test]
    fn nothing_is_culled_from_a_block_with_no_end_or_past_a_position_the_file_lacks() {
        let entry = |n: u32| {
            format!("{{\"record\":\"r-{n}\",\"mail\":\"m{n}\",\"from\":\"x\",\"body\":\"b\"}}\n")
        };
        let head = "{\"snapshot\":1,\"prior\":\"s.1\",\"next_record\":3}\n";
        let closed = format!(
            "{head}{}{}{{\"snapshot_end\":1}}\n{}",
            entry(1),
            entry(2),
            entry(3)
        );
        let open = format!("{head}{}{}{}", entry(1), entry(2), entry(3));
        let culled = |kept: &[String]| {
            kept.iter()
                .filter(|l| l.contains("\"culled\":true"))
                .count()
        };
        // The ordinary case: what the block restated is culled, the rest kept.
        assert_eq!(culled(&restated(closed.as_bytes(), true, None)), 2);
        // No end line: the block may run into entries written since.
        let kept = restated(open.as_bytes(), true, None);
        assert_eq!((kept.len(), culled(&kept)), (3, 0));
        // A reader's position that names no line of the file.
        let kept = restated(closed.as_bytes(), true, Some("r-9"));
        assert_eq!((kept.len(), culled(&kept)), (3, 0));
        // A position inside the block: only up to it.
        assert_eq!(culled(&restated(closed.as_bytes(), true, Some("r-1"))), 1);
    }

    #[test]
    fn the_next_record_counts_entries_only_after_a_snapshot_block() {
        // No snapshot: one Record number per entry, as before rotation.
        let (next, records) =
            replayed("{\"record\":\"r-1\",\"mail\":\"a\"}\n{\"record\":\"r-2\",\"mail\":\"b\"}")
                .unwrap();
        assert_eq!(next, 3);
        assert_eq!(records["b"], "r-2");
        // A block that holds fewer lines than its next_record implies, then
        // two entries appended after it.
        let (next, records) = replayed(concat!(
            "{\"snapshot\":2,\"prior\":\"x.store.2\",\"next_record\":9}\n",
            "{\"record\":\"r-4\",\"mail\":\"a\",\"culled\":true}\n",
            "{\"record\":\"r-7\",\"mail\":\"b\",\"from\":\"f\",\"body\":\"x\"}\n",
            "{\"snapshot_end\":2}\n",
            "{\"record\":\"r-9\",\"mail\":\"c\"}\n",
            "{\"record\":\"r-10\",\"mail\":\"d\"}",
        ))
        .unwrap();
        assert_eq!(next, 11);
        assert_eq!(records["a"], "r-4", "a tombstone keeps its Record id");
        assert_eq!(records.len(), 4);
        let (next, _) =
            replayed("{\"snapshot\":1,\"prior\":\"x.1\",\"next_record\":5}\n{\"snapshot_end\":1}")
                .unwrap();
        assert_eq!(next, 5);
        // A snapshot line without its high-water mark is refused, not guessed.
        for bad in [
            "{\"snapshot\":1,\"prior\":\"x.1\"}",
            "{\"snapshot\":1,\"prior\":\"x.1\",\"next_record\":0}",
        ] {
            let e = replayed(bad).unwrap_err().to_string();
            assert!(e.contains("expected") && e.contains("saw"), "{e}");
        }
    }

    #[test]
    fn a_rotation_culls_what_an_earlier_one_restated_and_keeps_what_came_after() {
        let text = concat!(
            "{\"snapshot\":2,\"prior\":\"s.2\",\"next_record\":5}\n",
            "{\"record\":\"r-1\",\"mail\":\"a\",\"culled\":true}\n",
            "{\"record\":\"r-2\",\"mail\":\"b\",\"from\":\"f\",\"body\":\"restated once\"}\n",
            "not a line of JSON\n",
            "{\"record\":\"r-3\",\"mail\":\"c\",\"from\":\"f\",\"body\":\"restated once\"}\n",
            "{\"note\":\"neither an entry nor a tombstone\"}\n",
            "{\"record\":\"r-4\",\"mail\":\"d\",\"from\":\"f\",\"body\":\"restated once\"}\n",
            "{\"snapshot_end\":2}\n",
            "{\"record\":\"r-5\",\"mail\":\"e\",\"from\":\"f\",\"body\":\"since\"}\n",
            "{\"record\":\"r-6\",\"mail\":\"f\",\"body\":\"torn",
        );
        let lines: Vec<&str> = text.lines().collect();
        let tomb =
            |r: &str, m: &str| format!("{{\"record\":\"{r}\",\"mail\":\"{m}\",\"culled\":true}}");
        let restated =
            |cull, through| restate(text.as_bytes(), cull, through).expect("an in-memory read");
        assert_eq!(
            restated(false, None),
            [lines[1], lines[2], lines[4], lines[6], lines[8]],
            "without a cull every entry and tombstone, verbatim"
        );
        assert_eq!(
            restated(true, None),
            [
                lines[1].to_string(),
                tomb("r-2", "b"),
                tomb("r-3", "c"),
                tomb("r-4", "d"),
                lines[8].to_string()
            ],
            "the block's entries are culled, the entry after it is kept"
        );
        assert_eq!(
            restated(true, Some("r-3")),
            [
                lines[1].to_string(),
                tomb("r-2", "b"),
                tomb("r-3", "c"),
                lines[6].to_string(),
                lines[8].to_string()
            ],
            "nothing after the read position is culled"
        );
        // A file no rotation started has no block: its first rotation culls nothing.
        let first = concat!(
            "{\"record\":\"r-1\",\"mail\":\"a\",\"from\":\"f\",\"body\":\"x\"}\n",
            "{\"record\":\"r-2\",\"mail\":\"b\",\"from\":\"f\",\"body\":\"y\"}\n",
        );
        assert_eq!(
            restate(first.as_bytes(), true, None).unwrap(),
            first.lines().collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_store_culls_on_a_switch_that_needs_rotation_and_has_no_clock() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        let (policy, cull) = rotation(&args(&[])).unwrap();
        assert_eq!((policy.rotate_bytes, cull), (0, false));
        let (policy, cull) = rotation(&args(&["--rotate-bytes", "9", "--cull"])).unwrap();
        assert_eq!(
            (policy.rotate_bytes, policy.cull_after_ms, cull),
            (9, None, true)
        );
        for (bad, says) in [
            (
                &["--cull"][..],
                "expected --rotate-bytes above 0 with --cull (culling happens when a Store rotates), saw none",
            ),
            (
                &["--cull", "--rotate-bytes", "0"][..],
                "saw --rotate-bytes 0",
            ),
            (
                &["--rotate-bytes", "9", "--cull-after-ms", "5"][..],
                "expected --cull to cull a Store at its rotations (a Store has no clock; --cull-after-ms is the coordinator's), saw --cull-after-ms",
            ),
            (
                &["--rotate-bytes", "lots"][..],
                "--rotate-bytes expects a whole number, saw lots",
            ),
        ] {
            let e = rotation(&args(bad)).unwrap_err().to_string();
            assert!(e.contains(says), "{bad:?}: {e}");
        }
    }
}

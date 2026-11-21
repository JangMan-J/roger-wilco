//! A contract-shaped session: registers with its host daemon,
//! writes delivered mail to its own durable Store, and acks naming the
//! Store record. It knows nothing about the core beyond the socket.

use crate::claims::Op;
use crate::durlog::DurLog;
use crate::json::{self, obj, s, Value};
use crate::net;
use std::collections::HashMap;
use std::io::{self, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Dial a host daemon over a Unix socket path or `tcp://<address:port>`.
pub fn connect(target: &Path) -> io::Result<(net::Writer, net::Lines)> {
    match target.to_str().and_then(|s| s.strip_prefix("tcp://")) {
        Some("") => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected tcp://<address:port>, saw tcp://",
        )),
        Some(addr) => {
            let conn = TcpStream::connect(addr).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at {}, saw {e}", target.display()),
                )
            })?;
            conn.set_nodelay(true)?;
            let writer = net::writer(Box::new(conn.try_clone()?));
            if let Some(auth) = net::client_auth_line() {
                net::send(&writer, &auth)?;
            }
            Ok((writer, net::lines(Box::new(conn))))
        }
        #[cfg(unix)]
        None => {
            let conn = UnixStream::connect(target).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("expected a host daemon at {}, saw {e}", target.display()),
                )
            })?;
            Ok((
                net::writer(Box::new(conn.try_clone()?)),
                net::lines(Box::new(conn)),
            ))
        }
        #[cfg(not(unix))]
        None => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "expected tcp://<address:port> on this platform, saw {}",
                target.display()
            ),
        )),
    }
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
    /// Called when a `board_event` arrives (after `watch` is enabled).
    pub board_event: Box<dyn Fn(Value) + Send>,
    /// Held across a Store write; true once the face is closing, after which
    /// a delivery is left unstored and unacked so it stays pending.
    pub gate: Arc<Mutex<bool>>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// How a client reaches this session: the Tracer's own stdout, the adapter's
/// client stream, or the MCP server's hooks.
pub enum Face {
    Tracer(Vec<String>),
    /// The client stream, and independent `registered` / `superseded` /
    /// `board_event` / `groups` opt-ins.
    Adapter(net::Writer, bool, bool, bool, bool),
    Mcp(McpHooks),
}

enum InnerFace {
    Tracer {
        out: Box<dyn Write + Send>,
        subscriptions: Vec<String>,
    },
    Adapter {
        to_client: net::Writer,
        from_client: net::Lines,
        exit_on_eof: bool,
        confirm: bool,
        superseded: bool,
        watch: bool,
        groups_enabled: bool,
    },
    Mcp(McpHooks),
}

/// The Tracer: a session with no harness, reporting on its own stdout.
#[allow(dead_code)]
pub fn run(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
) -> io::Result<()> {
    run_with_groups(socket, id, label, store_path, declared, Vec::new())
}

/// The Tracer with optional startup Group subscriptions sent after Register.
pub fn run_with_groups(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    subscriptions: Vec<String>,
) -> io::Result<()> {
    serve(
        socket,
        id,
        label,
        store_path,
        declared,
        Face::Tracer(subscriptions),
    )
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
/// `watch` (the `--watch` flag) asks for committed Board changes and passes
/// each on as the `board_event` line the host daemon sent; without it that
/// line is never sent.
/// When the client's stdin closes, the session unregisters and the process exits.
#[allow(dead_code)]
pub fn adapter(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    (confirm, superseded, watch): (bool, bool, bool),
) -> io::Result<()> {
    adapter_with_groups(
        socket,
        id,
        label,
        store_path,
        declared,
        (confirm, superseded, watch, false),
    )
}

/// The adapter with optional `--groups` client request support.
pub fn adapter_with_groups(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    (confirm, superseded, watch, groups_enabled): (bool, bool, bool, bool),
) -> io::Result<()> {
    let out = net::writer(Box::new(io::stdout()));
    serve(
        socket,
        id,
        label,
        store_path,
        declared,
        Face::Adapter(out, confirm, superseded, watch, groups_enabled),
    )
}

/// The adapter's client side, started once the session has registered.
fn client_io(
    socket: PathBuf,
    me: String,
    from_client: net::Lines,
    to_client: net::Writer,
    daemon: net::Writer,
    groups_enabled: bool,
    exit_on_eof: bool,
) {
    // A send blocks until the core answers, so sends run on a worker, one at a
    // time and in order (a client pairs replies to sends by order). The reader
    // below only reads stdin, so the client's EOF ends this process at once even
    // while a send waits.
    let (tx, rx) = std::sync::mpsc::channel::<io::Result<Value>>();
    std::thread::spawn(move || {
        let mut client = Client::new(socket);
        for line in rx {
            let reply = line.and_then(|v| client_request(&mut client, &me, &v, groups_enabled));
            let _ = net::send(
                &to_client,
                &reply.unwrap_or_else(|e| {
                    obj(vec![("type", s("error")), ("error", s(&e.to_string()))])
                }),
            );
        }
    });
    std::thread::spawn(move || {
        for line in from_client {
            if tx.send(line).is_err() {
                break;
            }
        }
        // The client's stdin closed: its harness is gone, so this session is
        // too. It leaves the Board and every later Broadcast now; Mail already
        // accepted for its Address stays pending at the coordinator.
        let _ = net::send(&daemon, &obj(vec![("type", s("unregister"))]));
        if exit_on_eof {
            std::process::exit(0);
        }
    });
}

fn client_request(
    client: &mut Client,
    me: &str,
    v: &Value,
    groups_enabled: bool,
) -> io::Result<Value> {
    match net::field(v, "type")? {
        "send" => client_send(client, me, v, groups_enabled),
        "board" => client.request(obj(vec![("type", s("board")), ("session", s(me))])),
        "claim" => client.request(obj(vec![
            ("type", s("claim")),
            ("op", s(net::field(v, "op")?)),
            ("key", s(net::field(v, "key")?)),
            ("owner", s(me)),
            (
                "ttl_ms",
                Value::Int(v.get("ttl_ms").and_then(|x| x.as_i64()).unwrap_or(0)),
            ),
        ])),
        "relabel" => client.request(obj(vec![
            ("type", s("relabel")),
            (
                "session",
                s(v.get("session").and_then(|x| x.as_str()).unwrap_or(me)),
            ),
            ("label", s(net::field(v, "label")?)),
        ])),
        "receipt" => client.request(obj(vec![
            ("type", s("receipt")),
            ("mail", s(net::field(v, "mail")?)),
        ])),
        "subscribe" if groups_enabled => {
            let group = net::field(v, "group")?;
            let after = match v.get("after") {
                None | Some(Value::Null) => None,
                Some(Value::Int(n)) => Some(*n),
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "after must be an integer",
                    ))
                }
            };
            let latest = match v.get("latest") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "latest must be a boolean",
                    ))
                }
            };
            subscribe_req_on(client, me, group, after, latest)
        }
        "unsubscribe" if groups_enabled => unsubscribe_req_on(client, me, net::field(v, "group")?),
        "groups" if groups_enabled => {
            let sess = v.get("session").and_then(Value::as_str).unwrap_or(me);
            let prefix = v.get("prefix").and_then(Value::as_str);
            let include_retired = v.get("include_retired") == Some(&Value::Bool(true));
            groups_req_filtered_on(client, Some(sess), prefix, include_retired)
        }
        "articles" if groups_enabled => {
            let group = net::field(v, "group")?;
            let after = v.get("after").and_then(Value::as_i64).unwrap_or(0);
            let limit = v.get("limit").and_then(Value::as_i64).unwrap_or(100) as usize;
            client.request(obj(vec![
                ("type", s("articles")),
                ("group", s(group)),
                ("after", Value::Int(after)),
                ("limit", Value::Int(limit as i64)),
            ]))
        }
        "group_policy" if groups_enabled => {
            let group = net::field(v, "group")?;
            let mut ask = vec![("type", s("group_policy")), ("group", s(group))];
            for key in ["keep_last", "ttl_ms", "retired"] {
                if let Some(val) = v.get(key) {
                    ask.push((key, val.clone()));
                }
            }
            client.request(obj(ask))
        }
        other if groups_enabled => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "expected request send, board, relabel, claim, receipt, subscribe, unsubscribe, groups, articles, or group_policy; saw {other}"
            ),
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected request send, board, relabel, or claim; saw {other}"),
        )),
    }
}

fn client_send(
    client: &mut Client,
    from: &str,
    v: &Value,
    groups_enabled: bool,
) -> io::Result<Value> {
    // A missing mail id is generated; a present one of the wrong type is a
    // malformed send, not a request to pick another id.
    let mail = match v.get("mail") {
        None => gen_uuid()?,
        Some(m) => m
            .as_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mail must be a string"))?
            .to_string(),
    };
    if !groups_enabled {
        let mail = send_mail_on(
            client,
            from,
            net::field(v, "to")?,
            &mail,
            net::field(v, "body")?,
            v.get("headers"),
        )?;
        return Ok(obj(vec![("type", s("accepted")), ("mail", s(&mail))]));
    }
    match (v.get("to"), v.get("group")) {
        (Some(_), Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected either to or group on send, saw both",
        )),
        (None, None) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected to or group on send, saw neither",
        )),
        (Some(to_val), None) => {
            let to = to_val.as_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "to must be a string")
            })?;
            let mail = send_mail_on(
                client,
                from,
                to,
                &mail,
                net::field(v, "body")?,
                v.get("headers"),
            )?;
            Ok(obj(vec![("type", s("accepted")), ("mail", s(&mail))]))
        }
        (None, Some(grp_val)) => {
            let group = grp_val.as_str().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "group must be a string")
            })?;
            let expected_article = match v.get("expected_article") {
                None | Some(Value::Null) => None,
                Some(x) => Some(x.as_i64().filter(|n| *n >= 0).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "expected_article must be a non-negative integer",
                    )
                })?),
            };
            let (mail, grp, article) = send_group_mail_on(
                client,
                from,
                group,
                &mail,
                net::field(v, "body")?,
                v.get("headers"),
                expected_article,
            )?;
            Ok(obj(vec![
                ("type", s("accepted")),
                ("mail", s(&mail)),
                ("group", s(&grp)),
                ("article", Value::Int(article)),
            ]))
        }
    }
}

/// The session loop all three faces share: register, write each delivery to
/// the Store, then ack naming its Record id. Returns when the host daemon
/// connection ends.
fn next_record_after(records: &HashMap<String, String>) -> u64 {
    records
        .values()
        .filter_map(|r| r.strip_prefix("r-"))
        .filter(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        .filter_map(|n| n.parse::<u64>().ok())
        .max()
        .map_or(1, |n| n + 1)
}

pub fn serve(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    face: Face,
) -> io::Result<()> {
    let inner = match face {
        Face::Tracer(subscriptions) => InnerFace::Tracer {
            out: Box::new(io::stdout()),
            subscriptions,
        },
        Face::Adapter(to_client, confirm, superseded, watch, groups_enabled) => {
            InnerFace::Adapter {
                to_client,
                from_client: net::lines(Box::new(io::stdin())),
                exit_on_eof: true,
                confirm,
                superseded,
                watch,
                groups_enabled,
            }
        }
        Face::Mcp(h) => InnerFace::Mcp(h),
    };
    serve_inner(socket, id, label, store_path, declared, inner)
}

fn serve_inner(
    socket: PathBuf,
    id: String,
    label: Option<String>,
    store_path: PathBuf,
    declared: Option<String>,
    face: InnerFace,
) -> io::Result<()> {
    let (client, mut tracer_out, mut mcp, confirm, superseded, watch, subscriptions) = match face {
        InnerFace::Tracer { out, subscriptions } => {
            (None, Some(out), None, true, true, false, subscriptions)
        }
        InnerFace::Adapter {
            to_client,
            from_client,
            exit_on_eof,
            confirm,
            superseded,
            watch,
            groups_enabled,
        } => (
            Some((to_client, from_client, exit_on_eof, groups_enabled)),
            None,
            None,
            confirm,
            superseded,
            watch,
            Vec::new(),
        ),
        InnerFace::Mcp(h) => (None, None, Some(h), true, true, false, Vec::new()),
    };
    let (mut store, prior) = DurLog::open(&store_path)?;
    let mut records: HashMap<String, String> = HashMap::new(); // mail -> record
    for line in prior {
        if let Ok(v) = json::parse(&line?) {
            if let (Ok(mail), Ok(record)) = (net::field(&v, "mail"), net::field(&v, "record")) {
                records.insert(mail.to_string(), record.to_string());
            }
        }
    }
    let mut next_record = next_record_after(&records);

    let (writer, lines) = connect(&socket)?;
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
    if watch {
        register.push(("watch", Value::Bool(true)));
    }
    net::send(&writer, &obj(register))?;

    let mut req = 0i64;
    let client = match client {
        Some((w, from_client, exit_on_eof, groups_enabled)) => {
            net::send(&w, &obj(vec![("type", s("ready")), ("session", s(&id))]))?;
            client_io(
                socket,
                id.clone(),
                from_client,
                w.clone(),
                writer.clone(),
                groups_enabled,
                exit_on_eof,
            );
            Some(w)
        }
        None => {
            if let Some(out) = tracer_out.as_mut() {
                writeln!(out, "READY session={id}")?;
                out.flush()?;
                for group in &subscriptions {
                    req += 1;
                    net::send(
                        &writer,
                        &obj(vec![
                            ("type", s("subscribe")),
                            ("req", Value::Int(req)),
                            ("session", s(&id)),
                            ("group", s(group)),
                        ]),
                    )?;
                }
            }
            None
        }
    };
    if let Some(registered) = mcp.as_mut().and_then(|h| h.registered.take()) {
        registered(writer.clone());
    }

    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "deliver" => {
                let mail = net::field(&v, "mail")?.to_string();
                let kind = net::kind_of(&v);
                let headers = net::headers_of(&v);
                let group_article = match (
                    v.get("group").and_then(Value::as_str),
                    v.get("article").and_then(Value::as_i64),
                ) {
                    (Some(g), Some(a)) => Some((g.to_string(), a)),
                    _ => None,
                };
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
                        let mut entry_pairs = vec![
                            ("record", s(&record)),
                            ("mail", s(&mail)),
                            ("from", s(net::field(&v, "from")?)),
                        ];
                        if let Some((ref grp, art)) = group_article {
                            entry_pairs.push(("group", s(grp)));
                            entry_pairs.push(("article", Value::Int(art)));
                        }
                        entry_pairs.push(("body", s(net::field(&v, "body")?)));
                        entry_pairs.push(("kind", s(&kind)));
                        entry_pairs.push(("headers", headers.clone()));
                        if mcp.is_some() {
                            entry_pairs.push(("stored_at_ms", Value::Int(now_ms())));
                        }
                        let entry = obj(entry_pairs);
                        store.append(&json::stringify(&entry))?;
                        records.insert(mail.clone(), record.clone());
                        drop(held);
                        if let Some(h) = &mcp {
                            (h.stored)();
                        }
                        if let Some(out) = tracer_out.as_mut() {
                            match (&group_article, kind.as_str()) {
                                (Some((grp, art)), "group") => {
                                    writeln!(
                                        out,
                                        "STORED mail={mail} record={record} kind=group group={grp} article={art}"
                                    )?;
                                }
                                _ => {
                                    writeln!(
                                        out,
                                        "STORED mail={mail} record={record} kind={kind}"
                                    )?;
                                }
                            }
                            write_headers(out, &mail, &headers)?;
                            out.flush()?;
                        }
                        record
                    }
                };
                if let Some(w) = &client {
                    let mut mail_pairs = vec![
                        ("type", s("mail")),
                        ("mail", s(&mail)),
                        ("from", s(net::field(&v, "from")?)),
                    ];
                    if let Some((ref grp, art)) = group_article {
                        mail_pairs.push(("group", s(grp)));
                        mail_pairs.push(("article", Value::Int(art)));
                    }
                    mail_pairs.push(("body", s(net::field(&v, "body")?)));
                    mail_pairs.push(("record", s(&record)));
                    mail_pairs.push(("kind", s(&kind)));
                    mail_pairs.push(("headers", headers));
                    net::send(w, &obj(mail_pairs))?;
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
                } else if let Some(out) = tracer_out.as_mut() {
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
                } else if let Some(out) = tracer_out.as_mut() {
                    writeln!(out, "REGISTERED session={id}")?;
                    out.flush()?;
                }
            }
            "subscribed" => {
                if let Some(out) = tracer_out.as_mut() {
                    if let (Ok(grp), Ok(sess), Some(pos), Some(hw)) = (
                        net::field(&v, "group"),
                        net::field(&v, "session"),
                        v.get("position").and_then(Value::as_i64),
                        v.get("high_water").and_then(Value::as_i64),
                    ) {
                        writeln!(
                            out,
                            "SUBSCRIBED group={grp} session={sess} position={pos} high_water={hw}"
                        )?;
                        out.flush()?;
                    }
                }
            }
            "board_event" => {
                if let Some(w) = &client {
                    if watch {
                        let _ = net::send(w, &v);
                    }
                } else if let Some(h) = &mcp {
                    (h.board_event)(v);
                }
            }
            "acked" => {
                if let Some(out) = tracer_out.as_mut() {
                    writeln!(
                        out,
                        "DELIVERED mail={} record={}",
                        net::field(&v, "mail")?,
                        net::field(&v, "record")?
                    )?;
                    out.flush()?;
                }
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

/// The client: one Group send, reporting acceptance and article number on stdout.
#[allow(dead_code)]
pub fn send_group(
    socket: PathBuf,
    from: String,
    group: String,
    mail: String,
    body: String,
    headers: Option<Value>,
    expected_article: Option<i64>,
) -> io::Result<()> {
    send_group_to(
        &mut io::stdout().lock(),
        &socket,
        &from,
        &group,
        &mail,
        &body,
        headers.as_ref(),
        expected_article,
    )
}

#[allow(clippy::too_many_arguments)]
fn send_group_to(
    out: &mut dyn Write,
    socket: &Path,
    from: &str,
    group: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
    expected_article: Option<i64>,
) -> io::Result<()> {
    let (mail, grp, article) =
        send_group_mail_with(socket, from, group, mail, body, headers, expected_article)?;
    writeln!(out, "ACCEPTED mail={mail} group={grp} article={article}")?;
    out.flush()
}

/// The client: subscribe `session` to `group` on stdout.
#[allow(dead_code)]
pub fn subscribe(
    socket: PathBuf,
    session: String,
    group: String,
    after: Option<i64>,
    latest: bool,
) -> io::Result<()> {
    subscribe_to(
        &mut io::stdout().lock(),
        &socket,
        &session,
        &group,
        after,
        latest,
    )
}

fn subscribe_to(
    out: &mut dyn Write,
    socket: &Path,
    session: &str,
    group: &str,
    after: Option<i64>,
    latest: bool,
) -> io::Result<()> {
    let reply = subscribe_req(socket, session, group, after, latest)?;
    let grp = net::field(&reply, "group")?;
    let sess = net::field(&reply, "session")?;
    let position = reply
        .get("position")
        .and_then(Value::as_i64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing position"))?;
    let high_water = reply
        .get("high_water")
        .and_then(Value::as_i64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing high_water"))?;
    writeln!(
        out,
        "SUBSCRIBED group={grp} session={sess} position={position} high_water={high_water}"
    )?;
    out.flush()
}

/// The client: unsubscribe `session` from `group` on stdout.
#[allow(dead_code)]
pub fn unsubscribe(socket: PathBuf, session: String, group: String) -> io::Result<()> {
    unsubscribe_to(&mut io::stdout().lock(), &socket, &session, &group)
}

fn unsubscribe_to(
    out: &mut dyn Write,
    socket: &Path,
    session: &str,
    group: &str,
) -> io::Result<()> {
    let reply = unsubscribe_req(socket, session, group)?;
    let grp = net::field(&reply, "group")?;
    let sess = net::field(&reply, "session")?;
    let position = reply
        .get("position")
        .and_then(Value::as_i64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing position"))?;
    writeln!(
        out,
        "UNSUBSCRIBED group={grp} session={sess} position={position}"
    )?;
    out.flush()
}

/// The client: list known Groups and optional per-session position state on stdout.
#[allow(dead_code)]
pub fn groups(
    socket: PathBuf,
    session: Option<String>,
    prefix: Option<String>,
    include_retired: bool,
) -> io::Result<()> {
    groups_to(
        &mut io::stdout().lock(),
        &socket,
        session.as_deref(),
        prefix.as_deref(),
        include_retired,
    )
}

fn groups_to(
    out: &mut dyn Write,
    socket: &Path,
    session: Option<&str>,
    prefix: Option<&str>,
    include_retired: bool,
) -> io::Result<()> {
    let reply = groups_req_filtered(socket, session, prefix, include_retired)?;
    let items = match reply.get("groups") {
        Some(Value::Arr(items)) => items.clone(),
        _ => Vec::new(),
    };
    for g in &items {
        let grp = net::field(g, "group")?;
        let hw = g.get("high_water").and_then(Value::as_i64).unwrap_or(0);
        let subs = g.get("subscribers").and_then(Value::as_i64).unwrap_or(0);
        let mut extra = String::new();
        if matches!(g.get("retired"), Some(Value::Bool(true))) {
            extra.push_str(" retired=true");
        }
        if let Some(kl) = g.get("keep_last").and_then(Value::as_i64) {
            extra.push_str(&format!(" keep_last={kl}"));
        }
        if let Some(ttl) = g.get("ttl_ms").and_then(Value::as_i64) {
            extra.push_str(&format!(" ttl_ms={ttl}"));
        }
        if session.is_some()
            || g.get("subscribed").is_some()
            || g.get("position").is_some()
            || g.get("record").is_some()
        {
            let subscribed = matches!(g.get("subscribed"), Some(Value::Bool(true)));
            let pos = g.get("position").and_then(Value::as_i64).unwrap_or(0);
            let rec = g.get("record").and_then(Value::as_str).unwrap_or("absent");
            writeln!(
                out,
                "GROUP group={grp} high_water={hw} subscribers={subs} subscribed={subscribed} position={pos} record={rec}{extra}"
            )?;
        } else {
            writeln!(
                out,
                "GROUP group={grp} high_water={hw} subscribers={subs}{extra}"
            )?;
        }
    }
    writeln!(out, "GROUPS count={}", items.len())?;
    out.flush()
}

#[allow(dead_code)]
pub fn articles(
    socket: PathBuf,
    group: String,
    after: i64,
    limit: usize,
    follow: bool,
    json_out: bool,
) -> io::Result<()> {
    if after < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected after >= 0, saw {after}"),
        ));
    }
    if !(1..=500).contains(&limit) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected limit in 1..=500, saw {limit}"),
        ));
    }
    let (writer, mut lines) = connect(&socket)?;
    net::send(
        &writer,
        &obj(vec![
            ("type", s("articles")),
            ("req", Value::Int(1)),
            ("group", s(&group)),
            ("after", Value::Int(after)),
            ("limit", Value::Int(limit as i64)),
            ("follow", Value::Bool(follow)),
        ]),
    )?;
    let reply = lines.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {}, saw EOF", socket.display()),
        )
    })??;
    if net::field(&reply, "type")? == "error" {
        return Err(io::Error::other(
            net::field(&reply, "reason").unwrap_or("request refused"),
        ));
    }
    let mut out = io::stdout().lock();
    if json_out {
        writeln!(out, "{}", json::stringify(&reply))?;
        out.flush()?;
        if follow {
            for line in lines {
                let v = line?;
                writeln!(out, "{}", json::stringify(&v))?;
                out.flush()?;
            }
        }
        return Ok(());
    }
    let hw = reply.get("high_water").and_then(Value::as_i64).unwrap_or(0);
    let items = match reply.get("articles") {
        Some(Value::Arr(items)) => items.clone(),
        _ => Vec::new(),
    };
    let mut last_seen = after;
    for a in &items {
        let art = a.get("article").and_then(Value::as_i64).unwrap_or(0);
        last_seen = last_seen.max(art);
        let mail = a.get("mail").and_then(Value::as_str).unwrap_or("");
        let from = a.get("from").and_then(Value::as_str).unwrap_or("");
        let body = a.get("body").and_then(Value::as_str).unwrap_or("");
        writeln!(
            out,
            "ARTICLE group={group} article={art} mail={mail} from={from} body={body}"
        )?;
        if let Some(h) = a.get("headers") {
            write_headers(&mut out, mail, h)?;
        }
    }
    writeln!(
        out,
        "ARTICLES group={group} high_water={hw} count={}",
        items.len()
    )?;
    out.flush()?;
    if follow {
        for line in lines {
            let v = line?;
            if net::field(&v, "type")? != "article" {
                continue;
            }
            let art = v.get("article").and_then(Value::as_i64).unwrap_or(0);
            if art <= last_seen {
                continue;
            }
            last_seen = art;
            let mail = v.get("mail").and_then(Value::as_str).unwrap_or("");
            let from = v.get("from").and_then(Value::as_str).unwrap_or("");
            let body = v.get("body").and_then(Value::as_str).unwrap_or("");
            writeln!(
                out,
                "ARTICLE group={group} article={art} mail={mail} from={from} body={body}"
            )?;
            if let Some(h) = v.get("headers") {
                write_headers(&mut out, mail, h)?;
            }
            out.flush()?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub fn group_policy(
    socket: PathBuf,
    group: String,
    keep_last: Option<Option<i64>>,
    ttl_ms: Option<Option<i64>>,
    retired: Option<bool>,
) -> io::Result<()> {
    let mut ask = vec![
        ("type", s("group_policy")),
        ("req", Value::Int(1)),
        ("group", s(&group)),
    ];
    if let Some(kl) = keep_last {
        ask.push(("keep_last", kl.map_or(Value::Null, Value::Int)));
    }
    if let Some(ttl) = ttl_ms {
        ask.push(("ttl_ms", ttl.map_or(Value::Null, Value::Int)));
    }
    if let Some(r) = retired {
        ask.push(("retired", Value::Bool(r)));
    }
    let reply = request(&socket, &obj(ask))?;
    let hw = reply.get("high_water").and_then(Value::as_i64).unwrap_or(0);
    let ret = matches!(reply.get("retired"), Some(Value::Bool(true)));
    let kl = reply
        .get("keep_last")
        .and_then(Value::as_i64)
        .map_or_else(|| "none".to_string(), |n| n.to_string());
    let ttl = reply
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .map_or_else(|| "none".to_string(), |n| n.to_string());
    println!(
        "GROUP_POLICY group={group} high_water={hw} retired={ret} keep_last={kl} ttl_ms={ttl}"
    );
    Ok(())
}

/// The client: watch committed Board changes on a connection that is no
/// session. Prints `WATCHING` once the host daemon has taken the request,
/// then `EVENT <json>` per `board_event` line, until the connection ends.
pub fn watch(socket: PathBuf) -> io::Result<()> {
    let (writer, lines) = connect(&socket)?;
    net::send(
        &writer,
        &obj(vec![("type", s("watch")), ("req", Value::Int(1))]),
    )?;
    let mut out = io::stdout().lock();
    for line in lines {
        let v = line?;
        match net::field(&v, "type")? {
            "watching" => writeln!(out, "WATCHING")?,
            "board_event" => writeln!(out, "EVENT {}", json::stringify(&v))?,
            "error" => {
                return Err(io::Error::other(format!(
                    "expected watching, saw error: {}",
                    net::field(&v, "reason").unwrap_or("unexplained")
                )))
            }
            _ => continue,
        }
        out.flush()?;
    }
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

/// Reusable client connection to a host daemon, reconnecting once on EOF or
/// broken pipe before a complete reply.
pub struct Client {
    socket: PathBuf,
    conn: Option<(net::Writer, net::Lines)>,
    next_req: i64,
    #[cfg(test)]
    connects: usize,
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

impl Client {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            conn: None,
            next_req: 0,
            #[cfg(test)]
            connects: 0,
        }
    }

    #[cfg(test)]
    pub fn connects(&self) -> usize {
        self.connects
    }

    fn ensure_conn(&mut self) -> io::Result<&mut (net::Writer, net::Lines)> {
        if self.conn.is_none() {
            let pair = connect(&self.socket)?;
            #[cfg(test)]
            {
                self.connects += 1;
            }
            self.conn = Some(pair);
        }
        Ok(self.conn.as_mut().expect("connection initialized"))
    }

    fn raw_request_once(&mut self, msg: &Value, req_id: i64) -> io::Result<Value> {
        let socket_display = self.socket.display().to_string();
        let (writer, lines) = self.ensure_conn()?;
        net::send(writer, msg)?;
        for item in lines {
            let v = item?;
            if let Some(r) = v.get("req").and_then(Value::as_i64) {
                if r != req_id {
                    continue;
                }
            }
            return Ok(v);
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected one reply line from {socket_display}, saw EOF"),
        ))
    }

    fn raw_request(&mut self, mut msg: Value) -> io::Result<Value> {
        self.next_req += 1;
        let req_id = self.next_req;
        if let Value::Obj(pairs) = &mut msg {
            pairs.retain(|(k, _)| k != "req");
            pairs.push(("req".to_string(), Value::Int(req_id)));
        }
        let had_conn = self.conn.is_some();
        match self.raw_request_once(&msg, req_id) {
            Ok(v) => Ok(v),
            Err(e) => {
                self.conn = None;
                if had_conn && is_reconnectable(&e) {
                    self.raw_request_once(&msg, req_id)
                        .inspect_err(|_| self.conn = None)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn request(&mut self, msg: Value) -> io::Result<Value> {
        let reply = self.raw_request(msg)?;
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
}

pub fn request_on(client: &mut Client, v: Value) -> io::Result<Value> {
    client.request(v)
}

pub fn send_mail_on(
    client: &mut Client,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let mut ask = vec![
        ("type", s("send")),
        ("mail", s(mail)),
        ("from", s(from)),
        ("to", s(to)),
        ("body", s(body)),
    ];
    if let Some(h) = headers {
        ask.push(("headers", h.clone()));
    }
    let v = client.raw_request(obj(ask))?;
    match net::field(&v, "type")? {
        "accepted" => Ok(net::field(&v, "mail")?.to_string()),
        "error" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "send of {mail} refused: {}",
                net::field(&v, "reason").unwrap_or("unexplained")
            ),
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected accepted or error, saw {other}"),
        )),
    }
}

pub fn send_group_mail_on(
    client: &mut Client,
    from: &str,
    group: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
    expected_article: Option<i64>,
) -> io::Result<(String, String, i64)> {
    let mut ask = vec![
        ("type", s("send")),
        ("mail", s(mail)),
        ("from", s(from)),
        ("group", s(group)),
        ("body", s(body)),
    ];
    if let Some(h) = headers {
        ask.push(("headers", h.clone()));
    }
    if let Some(exp) = expected_article {
        ask.push(("expected_article", Value::Int(exp)));
    }
    let v = client.raw_request(obj(ask))?;
    match net::field(&v, "type")? {
        "accepted" => {
            let mail_id = net::field(&v, "mail")?.to_string();
            let grp = v
                .get("group")
                .and_then(Value::as_str)
                .unwrap_or(group)
                .to_string();
            let article = v
                .get("article")
                .and_then(Value::as_i64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing article"))?;
            Ok((mail_id, grp, article))
        }
        "error" => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "send of {mail} refused: {}",
                net::field(&v, "reason").unwrap_or("unexplained")
            ),
        )),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected accepted or error, saw {other}"),
        )),
    }
}

pub fn subscribe_req_on(
    client: &mut Client,
    session: &str,
    group: &str,
    after: Option<i64>,
    latest: bool,
) -> io::Result<Value> {
    if after.is_some() && latest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected after or latest, saw both",
        ));
    }
    if let Some(n) = after {
        if n < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("expected after >= 0, saw {n}"),
            ));
        }
    }
    let mut ask = vec![
        ("type", s("subscribe")),
        ("session", s(session)),
        ("group", s(group)),
    ];
    if let Some(n) = after {
        ask.push(("after", Value::Int(n)));
    }
    if latest {
        ask.push(("latest", Value::Bool(true)));
    }
    client.request(obj(ask))
}

pub fn unsubscribe_req_on(client: &mut Client, session: &str, group: &str) -> io::Result<Value> {
    client.request(obj(vec![
        ("type", s("unsubscribe")),
        ("session", s(session)),
        ("group", s(group)),
    ]))
}

pub fn groups_req_filtered_on(
    client: &mut Client,
    session: Option<&str>,
    prefix: Option<&str>,
    include_retired: bool,
) -> io::Result<Value> {
    let mut ask = vec![("type", s("groups"))];
    if let Some(sess) = session {
        ask.push(("session", s(sess)));
    }
    if let Some(p) = prefix.filter(|p| !p.is_empty()) {
        ask.push(("prefix", s(p)));
    }
    if include_retired {
        ask.push(("include_retired", Value::Bool(true)));
    }
    client.request(obj(ask))
}

pub fn send_mail(
    socket: &Path,
    from: &str,
    to: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
) -> io::Result<String> {
    let mut client = Client::new(socket.to_path_buf());
    send_mail_on(&mut client, from, to, mail, body, headers)
}

pub fn send_group_mail_with(
    socket: &Path,
    from: &str,
    group: &str,
    mail: &str,
    body: &str,
    headers: Option<&Value>,
    expected_article: Option<i64>,
) -> io::Result<(String, String, i64)> {
    let mut client = Client::new(socket.to_path_buf());
    send_group_mail_on(
        &mut client,
        from,
        group,
        mail,
        body,
        headers,
        expected_article,
    )
}

pub fn subscribe_req(
    socket: &Path,
    session: &str,
    group: &str,
    after: Option<i64>,
    latest: bool,
) -> io::Result<Value> {
    let mut client = Client::new(socket.to_path_buf());
    subscribe_req_on(&mut client, session, group, after, latest)
}

pub fn unsubscribe_req(socket: &Path, session: &str, group: &str) -> io::Result<Value> {
    let mut client = Client::new(socket.to_path_buf());
    unsubscribe_req_on(&mut client, session, group)
}

pub fn groups_req_filtered(
    socket: &Path,
    session: Option<&str>,
    prefix: Option<&str>,
    include_retired: bool,
) -> io::Result<Value> {
    let mut client = Client::new(socket.to_path_buf());
    groups_req_filtered_on(&mut client, session, prefix, include_retired)
}

/// The line that follows a Mail's own line when it has headers: the set as
/// one JSON object, key order as the sender gave it.
fn write_headers(out: &mut dyn Write, mail: &str, headers: &Value) -> io::Result<()> {
    if matches!(headers, Value::Obj(pairs) if !pairs.is_empty()) {
        writeln!(out, "HEADERS mail={mail} {}", json::stringify(headers))?;
    }
    Ok(())
}

fn print_headers(mail: &str, headers: &Value) {
    let _ = write_headers(&mut io::stdout().lock(), mail, headers);
}

/// One request to the host daemon, answered by exactly one line.
pub fn request(socket: &Path, v: &Value) -> io::Result<Value> {
    let mut client = Client::new(socket.to_path_buf());
    client.request(v.clone())
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

pub use roger_wire::wire::gen_uuid;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::sync::mpsc;

    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl SharedBuf {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn mock_one_shot_daemon(
        expected: usize,
        responder: impl Fn(Value) -> Value + Send + 'static,
    ) -> (PathBuf, mpsc::Receiver<Value>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let (tx, rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            for _ in 0..expected {
                let (conn, _) = listener.accept().unwrap();
                let mut lines = net::lines(Box::new(conn.try_clone().unwrap()));
                let w = net::writer(Box::new(conn));
                let req = lines.next().unwrap().unwrap();
                tx.send(req.clone()).unwrap();
                let resp = responder(req);
                net::send(&w, &resp).unwrap();
            }
        });
        (target, rx, handle)
    }

    #[test]
    fn cli_group_commands_send_subscribe_unsubscribe_and_list_groups() {
        let (target, seen, handle) = mock_one_shot_daemon(7, |req| {
            let r = req.get("req").and_then(Value::as_i64).unwrap_or(1);
            match net::field(&req, "type").unwrap() {
                "send" => obj(vec![
                    ("type", s("accepted")),
                    ("req", Value::Int(r)),
                    ("mail", s(net::field(&req, "mail").unwrap())),
                    ("group", s(net::field(&req, "group").unwrap())),
                    ("article", Value::Int(7)),
                ]),
                "subscribe" => {
                    let pos = if req.get("latest") == Some(&Value::Bool(true)) {
                        9
                    } else {
                        req.get("after").and_then(Value::as_i64).unwrap_or(0)
                    };
                    obj(vec![
                        ("type", s("subscribed")),
                        ("req", Value::Int(r)),
                        ("session", s(net::field(&req, "session").unwrap())),
                        ("group", s(net::field(&req, "group").unwrap())),
                        ("position", Value::Int(pos)),
                        ("high_water", Value::Int(9)),
                    ])
                }
                "unsubscribe" => obj(vec![
                    ("type", s("unsubscribed")),
                    ("req", Value::Int(r)),
                    ("session", s(net::field(&req, "session").unwrap())),
                    ("group", s(net::field(&req, "group").unwrap())),
                    ("position", Value::Int(3)),
                ]),
                "groups" => {
                    if req.get("session").is_some() {
                        obj(vec![
                            ("type", s("groups")),
                            ("req", Value::Int(r)),
                            (
                                "groups",
                                Value::Arr(vec![
                                    obj(vec![
                                        ("group", s("ops")),
                                        ("high_water", Value::Int(9)),
                                        ("subscribers", Value::Int(2)),
                                        ("subscribed", Value::Bool(true)),
                                        ("position", Value::Int(4)),
                                        ("record", s("r-4")),
                                    ]),
                                    obj(vec![
                                        ("group", s("dev")),
                                        ("high_water", Value::Int(2)),
                                        ("subscribers", Value::Int(1)),
                                        ("subscribed", Value::Bool(false)),
                                        ("position", Value::Int(0)),
                                        ("record", Value::Null),
                                    ]),
                                ]),
                            ),
                        ])
                    } else {
                        obj(vec![
                            ("type", s("groups")),
                            ("req", Value::Int(r)),
                            (
                                "groups",
                                Value::Arr(vec![obj(vec![
                                    ("group", s("ops")),
                                    ("high_water", Value::Int(9)),
                                    ("subscribers", Value::Int(2)),
                                ])]),
                            ),
                        ])
                    }
                }
                other => panic!("unexpected type {other}"),
            }
        });

        let mut out = Vec::new();
        let hdrs = obj(vec![("x-k", s("1"))]);
        send_group_to(
            &mut out,
            &target,
            "s1",
            "ops",
            "m-g1",
            "post body",
            Some(&hdrs),
            None,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "ACCEPTED mail=m-g1 group=ops article=7\n"
        );
        let req = seen.recv().unwrap();
        assert!(req.get("to").is_none());
        assert_eq!(net::field(&req, "group").unwrap(), "ops");
        assert_eq!(req.get("headers"), Some(&hdrs));

        let mut out = Vec::new();
        subscribe_to(&mut out, &target, "s1", "ops", None, false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "SUBSCRIBED group=ops session=s1 position=0 high_water=9\n"
        );
        let req = seen.recv().unwrap();
        assert!(req.get("after").is_none() && req.get("latest").is_none());

        let mut out = Vec::new();
        subscribe_to(&mut out, &target, "s1", "ops", Some(3), false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "SUBSCRIBED group=ops session=s1 position=3 high_water=9\n"
        );
        assert_eq!(seen.recv().unwrap().get("after"), Some(&Value::Int(3)));

        let mut out = Vec::new();
        subscribe_to(&mut out, &target, "s1", "ops", None, true).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "SUBSCRIBED group=ops session=s1 position=9 high_water=9\n"
        );
        assert_eq!(seen.recv().unwrap().get("latest"), Some(&Value::Bool(true)));

        assert!(subscribe_req(&target, "s1", "ops", Some(1), true).is_err());
        assert!(subscribe_req(&target, "s1", "ops", Some(-1), false).is_err());

        let mut out = Vec::new();
        unsubscribe_to(&mut out, &target, "s1", "ops").unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "UNSUBSCRIBED group=ops session=s1 position=3\n"
        );
        let req = seen.recv().unwrap();
        assert_eq!(net::field(&req, "group").unwrap(), "ops");

        let mut out = Vec::new();
        groups_to(&mut out, &target, None, None, false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "GROUP group=ops high_water=9 subscribers=2\nGROUPS count=1\n"
        );
        assert!(seen.recv().unwrap().get("session").is_none());

        let mut out = Vec::new();
        groups_to(&mut out, &target, Some("s1"), None, false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "GROUP group=ops high_water=9 subscribers=2 subscribed=true position=4 record=r-4\nGROUP group=dev high_water=2 subscribers=1 subscribed=false position=0 record=absent\nGROUPS count=2\n"
        );
        assert_eq!(seen.recv().unwrap().get("session"), Some(&s("s1")));

        handle.join().unwrap();
    }

    #[test]
    fn tracer_with_groups_subscribes_after_register_and_stores_group_and_directed_deliveries() {
        let dir = std::env::temp_dir().join(format!(
            "roger-session-tracer-groups-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("t.store");

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));

        let daemon_thread = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let mut lines = net::lines(Box::new(conn.try_clone().unwrap()));
            let w = net::writer(Box::new(conn));

            let reg = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&reg, "type").unwrap(), "register");
            assert_eq!(net::field(&reg, "session").unwrap(), "reader-1");

            let sub1 = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&sub1, "type").unwrap(), "subscribe");
            assert_eq!(net::field(&sub1, "group").unwrap(), "ops");
            let sub2 = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&sub2, "type").unwrap(), "subscribe");
            assert_eq!(net::field(&sub2, "group").unwrap(), "alerts");

            net::send(
                &w,
                &obj(vec![
                    ("type", s("subscribed")),
                    ("req", Value::Int(1)),
                    ("session", s("reader-1")),
                    ("group", s("ops")),
                    ("position", Value::Int(0)),
                    ("high_water", Value::Int(12)),
                ]),
            )
            .unwrap();
            net::send(
                &w,
                &obj(vec![
                    ("type", s("deliver")),
                    ("mail", s("d-1")),
                    ("from", s("sender-a")),
                    ("to", s("reader-1")),
                    ("body", s("direct msg")),
                    ("kind", s("directed")),
                    ("headers", obj(vec![])),
                ]),
            )
            .unwrap();
            net::send(
                &w,
                &obj(vec![
                    ("type", s("deliver")),
                    ("mail", s("g-12")),
                    ("from", s("sender-b")),
                    ("to", s("reader-1")),
                    ("group", s("ops")),
                    ("article", Value::Int(12)),
                    ("body", s("group post")),
                    ("kind", s("group")),
                    ("headers", obj(vec![("x-trace", s("ok"))])),
                ]),
            )
            .unwrap();

            let ack1 = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&ack1, "type").unwrap(), "ack");
            assert_eq!(net::field(&ack1, "mail").unwrap(), "d-1");
            assert_eq!(net::field(&ack1, "record").unwrap(), "r-1");

            let ack2 = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&ack2, "type").unwrap(), "ack");
            assert_eq!(net::field(&ack2, "mail").unwrap(), "g-12");
            assert_eq!(net::field(&ack2, "record").unwrap(), "r-2");
        });

        let out = SharedBuf::new();
        serve_inner(
            target,
            "reader-1".into(),
            None,
            store_path.clone(),
            None,
            InnerFace::Tracer {
                out: Box::new(out.clone()),
                subscriptions: vec!["ops".into(), "alerts".into()],
            },
        )
        .unwrap();
        daemon_thread.join().unwrap();

        let store_lines: Vec<String> = std::fs::read_to_string(&store_path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(
            store_lines,
            vec![
                r#"{"record":"r-1","mail":"d-1","from":"sender-a","body":"direct msg","kind":"directed","headers":{}}"#,
                r#"{"record":"r-2","mail":"g-12","from":"sender-b","group":"ops","article":12,"body":"group post","kind":"group","headers":{"x-trace":"ok"}}"#,
            ]
        );

        let text = out.text();
        let printed: Vec<&str> = text.lines().collect();
        assert_eq!(
            printed,
            vec![
                "READY session=reader-1",
                "SUBSCRIBED group=ops session=reader-1 position=0 high_water=12",
                "STORED mail=d-1 record=r-1 kind=directed",
                "STORED mail=g-12 record=r-2 kind=group group=ops article=12",
                r#"HEADERS mail=g-12 {"x-trace":"ok"}"#,
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn adapter_groups_gate_refuses_when_disabled_and_serves_group_ops_and_mail_when_enabled() {
        let unused = PathBuf::from("tcp://127.0.0.1:1");
        for kind in ["subscribe", "unsubscribe", "groups"] {
            let err = client_request(
                &mut Client::new(unused.clone()),
                "me-1",
                &obj(vec![("type", s(kind)), ("group", s("ops"))]),
                false,
            )
            .unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("expected request send, board, relabel, or claim; saw {kind}")
            );
        }
        let unknown_groups = client_request(
            &mut Client::new(unused.clone()),
            "me-1",
            &obj(vec![("type", s("bogus"))]),
            true,
        )
        .unwrap_err();
        assert_eq!(
            unknown_groups.to_string(),
            "expected request send, board, relabel, claim, receipt, subscribe, unsubscribe, groups, articles, or group_policy; saw bogus"
        );
        let send_no_groups = client_request(
            &mut Client::new(unused.clone()),
            "me-1",
            &obj(vec![
                ("type", s("send")),
                ("group", s("ops")),
                ("body", s("hi")),
            ]),
            false,
        )
        .unwrap_err();
        assert!(send_no_groups.to_string().contains("missing to"));

        let both = client_request(
            &mut Client::new(unused.clone()),
            "me-1",
            &obj(vec![
                ("type", s("send")),
                ("to", s("peer")),
                ("group", s("ops")),
                ("body", s("hi")),
            ]),
            true,
        )
        .unwrap_err();
        assert!(both.to_string().contains("expected either to or group"));
        let neither = client_request(
            &mut Client::new(unused),
            "me-1",
            &obj(vec![("type", s("send")), ("body", s("hi"))]),
            true,
        )
        .unwrap_err();
        assert!(neither.to_string().contains("expected to or group"));

        let dir = std::env::temp_dir().join(format!(
            "roger-session-adapter-groups-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store_path = dir.join("a.store");

        let (req_target, seen_reqs, req_daemon) = mock_one_shot_daemon(6, |req| {
            let r = req.get("req").and_then(Value::as_i64).unwrap_or(1);
            match net::field(&req, "type").unwrap() {
                "send" if req.get("group").is_some() => obj(vec![
                    ("type", s("accepted")),
                    ("req", Value::Int(r)),
                    ("mail", s(net::field(&req, "mail").unwrap())),
                    ("group", s(net::field(&req, "group").unwrap())),
                    ("article", Value::Int(6)),
                ]),
                "send" => obj(vec![
                    ("type", s("accepted")),
                    ("req", Value::Int(r)),
                    ("mail", s(net::field(&req, "mail").unwrap())),
                ]),
                "subscribe" => obj(vec![
                    ("type", s("subscribed")),
                    ("req", Value::Int(r)),
                    ("session", s(net::field(&req, "session").unwrap())),
                    ("group", s(net::field(&req, "group").unwrap())),
                    (
                        "position",
                        Value::Int(req.get("after").and_then(Value::as_i64).unwrap_or(6)),
                    ),
                    ("high_water", Value::Int(6)),
                ]),
                "unsubscribe" => obj(vec![
                    ("type", s("unsubscribed")),
                    ("req", Value::Int(r)),
                    ("session", s(net::field(&req, "session").unwrap())),
                    ("group", s(net::field(&req, "group").unwrap())),
                    ("position", Value::Int(6)),
                ]),
                "groups" => obj(vec![
                    ("type", s("groups")),
                    ("req", Value::Int(r)),
                    (
                        "groups",
                        Value::Arr(vec![obj(vec![
                            ("group", s("ops")),
                            ("high_water", Value::Int(6)),
                            ("subscribers", Value::Int(1)),
                            ("subscribed", Value::Bool(true)),
                            ("position", Value::Int(6)),
                            ("record", s("r-1")),
                        ])]),
                    ),
                ]),
                other => panic!("unexpected {other}"),
            }
        });

        assert_eq!(
            client_request(
                &mut Client::new(req_target.clone()),
                "me-1",
                &json::parse(r#"{"type":"send","group":"ops","mail":"mg1","body":"hello"}"#)
                    .unwrap(),
                true,
            )
            .unwrap(),
            json::parse(r#"{"type":"accepted","mail":"mg1","group":"ops","article":6}"#).unwrap()
        );
        assert_eq!(
            client_request(
                &mut Client::new(req_target.clone()),
                "me-1",
                &json::parse(r#"{"type":"send","to":"peer","mail":"md1","body":"direct"}"#)
                    .unwrap(),
                true,
            )
            .unwrap(),
            json::parse(r#"{"type":"accepted","mail":"md1"}"#).unwrap()
        );
        let sub_after = client_request(
            &mut Client::new(req_target.clone()),
            "me-1",
            &json::parse(r#"{"type":"subscribe","group":"ops","after":2}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(sub_after.get("position"), Some(&Value::Int(2)));
        let sub_latest = client_request(
            &mut Client::new(req_target.clone()),
            "me-1",
            &json::parse(r#"{"type":"subscribe","group":"ops","latest":true}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(sub_latest.get("position"), Some(&Value::Int(6)));
        let unsub = client_request(
            &mut Client::new(req_target.clone()),
            "me-1",
            &json::parse(r#"{"type":"unsubscribe","group":"ops"}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(net::field(&unsub, "type").unwrap(), "unsubscribed");
        let grps = client_request(
            &mut Client::new(req_target),
            "me-1",
            &json::parse(r#"{"type":"groups"}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(net::field(&grps, "type").unwrap(), "groups");
        for _ in 0..6 {
            let _ = seen_reqs.recv().unwrap();
        }
        req_daemon.join().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let sess_target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let sess_daemon = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let mut lines = net::lines(Box::new(conn.try_clone().unwrap()));
            let w = net::writer(Box::new(conn));
            let reg = lines.next().unwrap().unwrap();
            assert_eq!(net::field(&reg, "type").unwrap(), "register");
            net::send(
                &w,
                &obj(vec![
                    ("type", s("deliver")),
                    ("mail", s("ga-5")),
                    ("from", s("author")),
                    ("to", s("me-1")),
                    ("group", s("ops")),
                    ("article", Value::Int(5)),
                    ("body", s("adapter article")),
                    ("kind", s("group")),
                    ("headers", obj(vec![])),
                ]),
            )
            .unwrap();
            for msg in lines {
                let msg = msg.unwrap();
                if net::field(&msg, "type").unwrap() == "ack" {
                    assert_eq!(net::field(&msg, "mail").unwrap(), "ga-5");
                    break;
                }
            }
        });

        let client_out = SharedBuf::new();
        let (hold_tx, hold_rx) = mpsc::channel::<u8>();
        struct HoldRead(mpsc::Receiver<u8>);
        impl io::Read for HoldRead {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                let _ = self.0.recv();
                Ok(0)
            }
        }
        serve_inner(
            sess_target,
            "me-1".into(),
            None,
            store_path.clone(),
            None,
            InnerFace::Adapter {
                to_client: net::writer(Box::new(client_out.clone())),
                from_client: net::lines(Box::new(HoldRead(hold_rx))),
                exit_on_eof: false,
                confirm: false,
                superseded: false,
                watch: false,
                groups_enabled: true,
            },
        )
        .unwrap();
        drop(hold_tx);
        sess_daemon.join().unwrap();

        let emitted: Vec<Value> = client_out
            .text()
            .lines()
            .map(|l| json::parse(l).unwrap())
            .collect();
        assert_eq!(emitted.len(), 2);
        assert_eq!(net::field(&emitted[0], "type").unwrap(), "ready");
        assert_eq!(
            emitted[1],
            json::parse(
                r#"{"type":"mail","mail":"ga-5","from":"author","group":"ops","article":5,"body":"adapter article","record":"r-1","kind":"group","headers":{}}"#
            )
            .unwrap()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn client_reuses_connection_keeps_open_on_error_and_reconnects_on_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let server = std::thread::spawn(move || {
            // Connection 1: serves 3 requests (including an error reply), then drops.
            let (conn1, _) = listener.accept().unwrap();
            let mut lines1 = net::lines(Box::new(conn1.try_clone().unwrap()));
            let w1 = net::writer(Box::new(conn1));
            for expected_req in 1..=3 {
                let req = lines1.next().unwrap().unwrap();
                assert_eq!(req.get("req").and_then(Value::as_i64), Some(expected_req));
                let resp = if expected_req == 2 {
                    obj(vec![
                        ("type", s("error")),
                        ("req", Value::Int(expected_req)),
                        ("reason", s("refused")),
                    ])
                } else {
                    obj(vec![
                        ("type", s("board")),
                        ("req", Value::Int(expected_req)),
                        ("sessions", Value::Arr(vec![])),
                        ("claims", Value::Arr(vec![])),
                    ])
                };
                net::send(&w1, &resp).unwrap();
            }
            drop((lines1, w1));

            // Connection 2: accepts reconnect after EOF and serves req 4.
            let (conn2, _) = listener.accept().unwrap();
            let mut lines2 = net::lines(Box::new(conn2.try_clone().unwrap()));
            let w2 = net::writer(Box::new(conn2));
            let req = lines2.next().unwrap().unwrap();
            assert_eq!(req.get("req").and_then(Value::as_i64), Some(4));
            net::send(
                &w2,
                &obj(vec![
                    ("type", s("accepted")),
                    ("req", Value::Int(4)),
                    ("mail", s("m-4")),
                ]),
            )
            .unwrap();
        });

        let mut client = Client::new(target);
        assert!(client.request(obj(vec![("type", s("board"))])).is_ok());
        assert_eq!(client.connects(), 1);
        let err = client
            .request(obj(vec![("type", s("board"))]))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "refused");
        assert_eq!(client.connects(), 1, "error reply keeps socket open");
        assert!(client.request(obj(vec![("type", s("board"))])).is_ok());
        assert_eq!(client.connects(), 1);

        // Server closed conn1; next call reconnects once cleanly.
        let mail = send_mail_on(&mut client, "a", "b", "m-4", "hi", None).unwrap();
        assert_eq!(mail, "m-4");
        assert_eq!(client.connects(), 2);
        server.join().unwrap();
    }

    #[test]
    fn next_record_after_handles_gaps_empty_and_junk_ids() {
        assert_eq!(next_record_after(&HashMap::new()), 1);

        let mut with_gaps = HashMap::new();
        with_gaps.insert("m1".into(), "r-1".into());
        with_gaps.insert("m2".into(), "r-5".into());
        assert_eq!(next_record_after(&with_gaps), 6);

        let mut junk_only = HashMap::new();
        junk_only.insert("m1".into(), "bad".into());
        junk_only.insert("m2".into(), "r-abc".into());
        junk_only.insert("m3".into(), "r-".into());
        junk_only.insert("m4".into(), "x-9".into());
        junk_only.insert("m5".into(), "r-+4".into());
        assert_eq!(next_record_after(&junk_only), 1);

        junk_only.insert("m6".into(), "r-3".into());
        assert_eq!(next_record_after(&junk_only), 4);
    }

    #[test]
    fn client_request_receipt_works_with_and_without_groups_and_pools_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let target = PathBuf::from(format!("tcp://{}", listener.local_addr().unwrap()));
        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().unwrap();
            let mut lines = net::lines(Box::new(conn.try_clone().unwrap()));
            let w = net::writer(Box::new(conn));
            for expected_req in 1..=4 {
                let req = lines.next().unwrap().unwrap();
                assert_eq!(req.get("req").and_then(Value::as_i64), Some(expected_req));
                let resp = match net::field(&req, "type").unwrap() {
                    "receipt" => obj(vec![
                        ("type", s("receipt")),
                        ("req", Value::Int(expected_req)),
                        ("mail", s(net::field(&req, "mail").unwrap())),
                        ("to", s("peer")),
                        ("record", s("r-2")),
                    ]),
                    "board" => obj(vec![
                        ("type", s("board")),
                        ("req", Value::Int(expected_req)),
                        ("sessions", Value::Arr(vec![])),
                        ("claims", Value::Arr(vec![])),
                    ]),
                    "articles" => obj(vec![
                        ("type", s("articles")),
                        ("req", Value::Int(expected_req)),
                        ("group", s(net::field(&req, "group").unwrap())),
                        ("articles", Value::Arr(vec![])),
                    ]),
                    other => panic!("unexpected {other}"),
                };
                net::send(&w, &resp).unwrap();
            }
        });

        let mut client = Client::new(target);
        let r_off = client_request(
            &mut client,
            "me-1",
            &json::parse(r#"{"type":"receipt","mail":"m-1"}"#).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(net::field(&r_off, "type").unwrap(), "receipt");
        assert_eq!(net::field(&r_off, "mail").unwrap(), "m-1");
        assert_eq!(net::field(&r_off, "record").unwrap(), "r-2");

        let r_on = client_request(
            &mut client,
            "me-1",
            &json::parse(r#"{"type":"receipt","mail":"m-2"}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(net::field(&r_on, "type").unwrap(), "receipt");
        assert_eq!(net::field(&r_on, "mail").unwrap(), "m-2");

        let b = client_request(
            &mut client,
            "me-1",
            &json::parse(r#"{"type":"board"}"#).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(net::field(&b, "type").unwrap(), "board");

        let a = client_request(
            &mut client,
            "me-1",
            &json::parse(r#"{"type":"articles","group":"ops","after":1,"limit":10}"#).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(net::field(&a, "type").unwrap(), "articles");

        assert_eq!(client.connects(), 1);
        server.join().unwrap();
    }
}

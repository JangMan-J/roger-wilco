//! `roger audit`: a read-only listing of what the coordinator log, host daemon
//! logs and Stores hold for one Address or all, in log order, with findings.
//!
//! Every input is concurrent read-only: a coordinator's slice databases and a
//! host daemon's database open read-only (one snapshot each, never a write
//! lock), with bodies moved to cold day files read from those sealed files; a
//! plain log or Store is read as bytes, never opened as a `DurLog`. So any
//! number of audits read what a running coordinator, host daemon or session
//! holds, beside its writer. A line a generation never stamped reads `absent`, never
//! zero. Every opaque text (an Address, a label, a declared text, a header
//! set, a body) is copied byte-identical; nothing here reads its meaning, and
//! the only comparisons are equality of an Address, a mail id, a record id or
//! a claim key against another of the same field. Findings are listed, not
//! judged: the exit status says only whether every input could be read.

use crate::json::{self, s, Value};
use crate::net;
use crate::sql::audit_ro::{self, Coord, Daemon};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::Path;

const USAGE: &str = "roger audit [--coordinator-log|--coordinator <path>] \
[--daemon-log|--daemon <path>]... [--store <file>]... [--address <address>] [--bodies] [--json] \
(concurrent read-only)";

#[derive(Clone, Copy, PartialEq)]
enum Source {
    Coordinator,
    Daemon,
    Store,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Source::Coordinator => "coordinator",
            Source::Daemon => "daemon",
            Source::Store => "store",
        }
    }
}

struct Opts {
    /// Every input in argument order.
    inputs: Vec<(Source, String)>,
    address: Option<String>,
    bodies: bool,
    json: bool,
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

fn parse_args(args: &[String]) -> io::Result<Opts> {
    let mut opts = Opts {
        inputs: Vec::new(),
        address: None,
        bodies: false,
        json: false,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut value = || {
            it.next()
                .cloned()
                .ok_or_else(|| bad(format!("expected a value after {flag}, saw none")))
        };
        match flag.as_str() {
            "--coordinator-log" | "--coordinator" => {
                let path = value()?;
                if opts.inputs.iter().any(|(k, _)| *k == Source::Coordinator) {
                    return Err(bad(format!(
                        "expected --coordinator-log once, saw it again with {path}"
                    )));
                }
                opts.inputs.push((Source::Coordinator, path));
            }
            "--daemon-log" | "--daemon" => opts.inputs.push((Source::Daemon, value()?)),
            "--store" => opts.inputs.push((Source::Store, value()?)),
            "--address" => opts.address = Some(value()?),
            "--bodies" => opts.bodies = true,
            "--json" => opts.json = true,
            other => return Err(bad(format!("expected a flag of `{USAGE}`, saw {other}"))),
        }
    }
    if opts.inputs.is_empty() {
        return Err(bad(format!(
            "expected at least one input (`{USAGE}`), saw none"
        )));
    }
    Ok(opts)
}

/// What one input holds: a plain file's complete lines, or a read-only
/// snapshot of a coordinator's or host daemon's database.
enum Records {
    Lines {
        /// Each complete line's 1-based number and its object, when it is one.
        lines: Vec<(usize, Option<Value>)>,
        /// The trailing fragment's byte length; 0 when the file ends in `\n`.
        torn: usize,
    },
    Coord(Coord),
    Daemon(Daemon),
}

/// One input, read whole.
struct Input {
    source: Source,
    path: String,
    records: Records,
}

impl Input {
    fn lines(&self) -> &[(usize, Option<Value>)] {
        match &self.records {
            Records::Lines { lines, .. } => lines,
            _ => &[],
        }
    }

    fn count(&self) -> usize {
        match &self.records {
            Records::Lines { lines, .. } => lines.len(),
            Records::Coord(c) => c.record_count,
            Records::Daemon(d) => d.record_count,
        }
    }
}

fn read_input(source: Source, path: &str, bodies: bool) -> io::Result<Input> {
    let at = Path::new(path);
    let records = match source {
        Source::Coordinator if audit_ro::has_coord(at) => {
            Some(Records::Coord(audit_ro::coord(at, bodies)?))
        }
        Source::Daemon if audit_ro::has_daemon(at) => Some(Records::Daemon(audit_ro::daemon(at)?)),
        _ => None,
    };
    if let Some(records) = records {
        return Ok(Input {
            source,
            path: path.to_string(),
            records,
        });
    }
    let bytes = std::fs::read(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "expected a readable {} file at {path}, saw {e}",
                source.name()
            ),
        )
    })?;
    let end = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let lines = bytes[..end]
        .split_inclusive(|&b| b == b'\n')
        .enumerate()
        .map(|(i, raw)| {
            let parsed = std::str::from_utf8(&raw[..raw.len() - 1])
                .ok()
                .and_then(|text| json::parse(text).ok())
                .filter(|v| matches!(v, Value::Obj(_)));
            (i + 1, parsed)
        })
        .collect();
    Ok(Input {
        source,
        path: path.to_string(),
        records: Records::Lines {
            lines,
            torn: bytes.len() - end,
        },
    })
}

/// One output record: the word, its keyed fields in order, and an optional
/// JSON tail (a header set or a body) shown bare in the text form.
#[derive(Clone)]
struct Line {
    word: &'static str,
    fields: Vec<(&'static str, Value)>,
    tail: Option<(&'static str, Value)>,
}

fn line(word: &'static str, fields: Vec<(&'static str, Value)>) -> Line {
    Line {
        word,
        fields,
        tail: None,
    }
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Null => "absent".to_string(),
        Value::Str(t) => t.clone(),
        Value::Int(n) => n.to_string(),
        Value::Arr(xs) => xs.iter().map(text_of).collect::<Vec<_>>().join(","),
        other => json::stringify(other),
    }
}

/// `WORD key=value ...`, the free text last; `absent` for what a line never held.
fn text(l: &Line) -> String {
    let mut out = String::from(l.word);
    for (k, v) in &l.fields {
        out.push(' ');
        out.push_str(k);
        out.push('=');
        out.push_str(&text_of(v));
    }
    if let Some((_, v)) = &l.tail {
        out.push(' ');
        out.push_str(&if matches!(v, Value::Null) {
            "absent".to_string()
        } else {
            json::stringify(v)
        });
    }
    out
}

/// `{"type":"WORD",...}` with the same fields; `absent` is `null`.
fn json_line(l: &Line) -> String {
    let mut pairs = vec![("type".to_string(), s(l.word))];
    pairs.extend(l.fields.iter().map(|(k, v)| (k.to_string(), v.clone())));
    if let Some((k, v)) = &l.tail {
        pairs.push((k.to_string(), v.clone()));
    }
    json::stringify(&Value::Obj(pairs))
}

fn int(n: usize) -> Value {
    Value::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

fn opt_int(v: Option<i64>) -> Value {
    v.map_or(Value::Null, Value::Int)
}

fn opt_str(v: Option<&str>) -> Value {
    v.map_or(Value::Null, s)
}

fn stamp_of(v: &Value, key: &str) -> Option<i64> {
    v.get(key).and_then(|x| x.as_i64())
}

fn audit_headers(v: &Value) -> Value {
    v.get("headers").cloned().unwrap_or(Value::Null)
}

/// Missing headers remain absent; an explicitly empty set has no extra line.
/// Nonempty sets keep their key order.
fn headers_line(mail: &str, headers: &Value) -> Option<Line> {
    (!matches!(headers, Value::Obj(pairs) if pairs.is_empty())).then(|| Line {
        word: "HEADERS",
        fields: vec![("mail", s(mail))],
        tail: Some(("headers", headers.clone())),
    })
}

/// A body as stored; `absent` when a cold day file no longer holds it.
fn body_line(mail: &str, body: Option<&str>) -> Line {
    Line {
        word: "BODY",
        fields: vec![("mail", s(mail))],
        tail: Some(("body", opt_str(body))),
    }
}

/// A stored header set's text as its JSON value; absent when never stored.
fn stored_headers(text: Option<&str>) -> Value {
    text.map_or(Value::Null, |t| json::parse(t).unwrap_or_else(|_| s(t)))
}

/// A Store entry's Kind: `kind` when the entry has one, the Kind the older
/// field gives when that is what the entry has, else absent.
fn store_kind(v: &Value) -> Value {
    let has_older = v.get("wake").is_some(); // legacy-wire: a Store entry from before Kind says it here
    if v.get("kind").is_some() || has_older {
        s(&net::kind_of(v))
    } else {
        Value::Null
    }
}

/// One Mail the coordinator accepted: a `mail` line, one recipient's copy of
/// a `broadcast` line, or an envelope row, joined to its receipts (from a
/// log, only the first `receipt` line for its id).
struct MailRow {
    id: String,
    kind: String,
    from: String,
    /// The recipient's Address, or the Group's name for a Group article.
    to: String,
    article: Option<i64>,
    /// None when a cold day file no longer holds it.
    body: Option<String>,
    body_bytes: Value,
    headers: Value,
    accepted: Option<i64>,
    line: Value,
    receipts: Vec<ReceiptRow>,
    /// Readers holding a position on a Group article's Group.
    readers: Vec<String>,
}

struct ReceiptRow {
    session: String,
    record: String,
    at: Option<i64>,
    line: Value,
}

/// One timeline entry of the coordinator records, held until every receipt
/// is known. `names` are the Addresses `--address` may match on the line.
enum Item {
    Plain {
        line: Line,
        names: Vec<String>,
    },
    Mail(usize),
    Stray {
        line: Line,
        session: String,
        record: String,
        mail: String,
    },
}

fn session_item(
    word: &'static str,
    session: &str,
    host: Option<&str>,
    at: Option<i64>,
    n: Value,
    free: Option<(&'static str, &str)>,
) -> Item {
    let mut fields = vec![("address", s(session))];
    if let Some(host) = host {
        fields.push(("host", s(host)));
    }
    fields.push(("at_ms", opt_int(at)));
    fields.push(("line", n));
    if let Some((k, t)) = free {
        fields.push((k, s(t)));
    }
    Item::Plain {
        line: line(word, fields),
        names: vec![session.to_string()],
    }
}

fn takeover_item(session: &str, old_host: &str, host: &str, at: Option<i64>, n: Value) -> Item {
    Item::Plain {
        line: line(
            "TAKEOVER",
            vec![
                ("address", s(session)),
                ("old_host", s(old_host)),
                ("host", s(host)),
                ("at_ms", opt_int(at)),
                ("line", n),
            ],
        ),
        names: vec![session.to_string()],
    }
}

fn claim_item(
    key: &str,
    owner: &str,
    expires: i64,
    n: Value,
    last_owner: &mut HashMap<String, String>,
) -> Item {
    last_owner.insert(key.to_string(), owner.to_string());
    Item::Plain {
        line: line(
            "CLAIM",
            vec![
                ("key", s(key)),
                ("owner", s(owner)),
                ("expires_ms", Value::Int(expires)),
                ("line", n),
            ],
        ),
        names: vec![owner.to_string()],
    }
}

fn release_item(key: &str, n: Value, last_owner: &HashMap<String, String>) -> Item {
    Item::Plain {
        line: line("RELEASE", vec![("key", s(key)), ("line", n)]),
        names: last_owner.get(key).cloned().into_iter().collect(),
    }
}

fn stray_item(mail: &str, session: &str, record: &str, n: Value) -> Item {
    Item::Stray {
        line: line(
            "STRAY_RECEIPT",
            vec![
                ("mail", s(mail)),
                ("session", s(session)),
                ("record", s(record)),
                ("line", n),
            ],
        ),
        session: session.to_string(),
        record: record.to_string(),
        mail: mail.to_string(),
    }
}

/// The timeline and Mail of a coordinator log, in line order.
fn coordinator_lines(input: &Input) -> (Vec<Item>, Vec<MailRow>) {
    let mut items: Vec<Item> = Vec::new();
    let mut mails: Vec<MailRow> = Vec::new();
    let mut by_id: HashMap<String, usize> = HashMap::new();
    let mut last_owner: HashMap<String, String> = HashMap::new();
    let session_event =
        |v: &Value, word: &'static str, n: usize, free: Option<(&'static str, &str)>| {
            let session = net::field(v, "session").ok()?;
            let host = if word == "REGISTER" {
                Some(net::field(v, "host").ok()?)
            } else {
                None
            };
            Some(session_item(
                word,
                session,
                host,
                stamp_of(v, "at_ms"),
                int(n),
                free,
            ))
        };
    let mail_row =
        |id: String, kind: &str, from: &str, to: &str, body: &str, v: &Value, n| MailRow {
            id,
            kind: kind.to_string(),
            from: from.to_string(),
            to: to.to_string(),
            article: None,
            body: Some(body.to_string()),
            body_bytes: int(body.len()),
            headers: audit_headers(v),
            accepted: stamp_of(v, "accepted_at_ms"),
            line: int(n),
            receipts: Vec::new(),
            readers: Vec::new(),
        };
    for (n, v) in input
        .lines()
        .iter()
        .filter_map(|(n, v)| Some((*n, v.as_ref()?)))
    {
        let item = match v.get("ev").and_then(|x| x.as_str()) {
            Some("takeover") => match (
                net::field(v, "session"),
                net::field(v, "old_host"),
                net::field(v, "host"),
            ) {
                (Ok(session), Ok(old_host), Ok(host)) => Some(takeover_item(
                    session,
                    old_host,
                    host,
                    stamp_of(v, "at_ms"),
                    int(n),
                )),
                _ => None,
            },
            Some("session") => session_event(v, "REGISTER", n, None),
            Some("unregister") => session_event(v, "UNREGISTER", n, None),
            Some("label") => net::field(v, "label")
                .ok()
                .and_then(|l| session_event(v, "LABEL", n, Some(("label", l)))),
            Some("declared") => match net::declared_of(v) {
                Some(d) => session_event(v, "DECLARED", n, Some(("declared", &d))),
                None => session_event(v, "UNDECLARED", n, None),
            },
            Some("mail") => match (
                net::field(v, "mail"),
                net::field(v, "from"),
                net::field(v, "to"),
                net::field(v, "body"),
            ) {
                (Ok(mail), Ok(from), Ok(to), Ok(body)) => {
                    mails.push(mail_row(
                        mail.to_string(),
                        net::DIRECTED,
                        from,
                        to,
                        body,
                        v,
                        n,
                    ));
                    by_id.entry(mail.to_string()).or_insert(mails.len() - 1);
                    Some(Item::Mail(mails.len() - 1))
                }
                _ => None,
            },
            Some("broadcast") => {
                if let (Ok(mail), Ok(from), Ok(body), Some(Value::Arr(to))) = (
                    net::field(v, "mail"),
                    net::field(v, "from"),
                    net::field(v, "body"),
                    v.get("to"),
                ) {
                    for addr in to.iter().filter_map(|x| x.as_str()) {
                        let id = format!("{mail}/{addr}");
                        mails.push(mail_row(id.clone(), net::BROADCAST, from, addr, body, v, n));
                        by_id.entry(id).or_insert(mails.len() - 1);
                        items.push(Item::Mail(mails.len() - 1));
                    }
                }
                None
            }
            Some("receipt") => match (net::field(v, "mail"), net::field(v, "record")) {
                (Ok(mail), Ok(record)) => match by_id.get(mail) {
                    Some(&i) => {
                        let row = &mut mails[i];
                        if row.receipts.is_empty() {
                            let session = v
                                .get("session")
                                .and_then(|x| x.as_str())
                                .unwrap_or(&row.to)
                                .to_string();
                            row.receipts.push(ReceiptRow {
                                session,
                                record: record.to_string(),
                                at: stamp_of(v, "receipted_at_ms"),
                                line: int(n),
                            });
                        }
                        None
                    }
                    None => net::field(v, "session")
                        .ok()
                        .map(|session| stray_item(mail, session, record, int(n))),
                },
                _ => None,
            },
            Some("claim") => match (
                net::field(v, "key"),
                net::field(v, "owner"),
                stamp_of(v, "expires_ms"),
            ) {
                (Ok(key), Ok(owner), Some(expires)) => {
                    Some(claim_item(key, owner, expires, int(n), &mut last_owner))
                }
                _ => None,
            },
            Some("release") => net::field(v, "key")
                .ok()
                .map(|key| release_item(key, int(n), &last_owner)),
            _ => None,
        };
        if let Some(item) = item {
            items.push(item);
        }
    }
    (items, mails)
}

/// The timeline and Mail of a coordinator's slice databases, in `seq` order.
fn coordinator_snapshot(snapshot: &Coord) -> (Vec<Item>, Vec<MailRow>) {
    let mut items: Vec<Item> = Vec::new();
    let mut mails: Vec<MailRow> = Vec::new();
    let mut last_owner: HashMap<String, String> = HashMap::new();
    for entry in &snapshot.items {
        let t = match entry {
            audit_ro::Item::Envelope(e) => {
                mails.push(MailRow {
                    id: e.mail_id.clone(),
                    kind: e.kind.clone(),
                    from: e.sender.clone(),
                    to: e.recipient.clone(),
                    article: (e.kind == net::GROUP).then_some(e.article_num),
                    body: e.body.clone(),
                    body_bytes: Value::Int(e.body_len),
                    headers: stored_headers(e.headers_json.as_deref()),
                    accepted: e.accepted_at_ms,
                    line: Value::Int(e.line),
                    receipts: e
                        .receipts
                        .iter()
                        .map(|r| ReceiptRow {
                            session: r.reader.clone(),
                            record: r.record_id.clone(),
                            at: r.receipted_at_ms,
                            line: Value::Int(r.receipt_line),
                        })
                        .collect(),
                    readers: e.subscribers.clone(),
                });
                items.push(Item::Mail(mails.len() - 1));
                continue;
            }
            audit_ro::Item::Timeline(t) => t,
        };
        let n = Value::Int(t.line);
        let addr = t.addr.as_deref();
        let item = match t.kind.as_str() {
            "session" | "register" => addr
                .zip(t.host.as_deref())
                .map(|(a, h)| session_item("REGISTER", a, Some(h), t.at_ms, n, None)),
            "unregister" => addr.map(|a| session_item("UNREGISTER", a, None, t.at_ms, n, None)),
            "label" => addr
                .zip(t.label.as_deref())
                .map(|(a, l)| session_item("LABEL", a, None, t.at_ms, n, Some(("label", l)))),
            "declared" | "undeclared" => {
                addr.map(
                    |a| match t.declared.as_deref().filter(|_| t.kind == "declared") {
                        Some(d) => {
                            session_item("DECLARED", a, None, t.at_ms, n, Some(("declared", d)))
                        }
                        None => session_item("UNDECLARED", a, None, t.at_ms, n, None),
                    },
                )
            }
            "takeover" => match (addr, t.old_host.as_deref(), t.host.as_deref()) {
                (Some(a), Some(old), Some(h)) => Some(takeover_item(a, old, h, t.at_ms, n)),
                _ => None,
            },
            "claim" => match (t.claim_key.as_deref(), addr, t.expires_ms) {
                (Some(key), Some(owner), Some(expires)) => {
                    Some(claim_item(key, owner, expires, n, &mut last_owner))
                }
                _ => None,
            },
            "release" => t
                .claim_key
                .as_deref()
                .map(|key| release_item(key, n, &last_owner)),
            "receipt" | "stray_receipt" => match (
                t.mail_id.as_deref(),
                addr,
                t.label.as_deref().or(t.registration.as_deref()),
            ) {
                (Some(mail), Some(session), Some(record)) => {
                    Some(stray_item(mail, session, record, n))
                }
                _ => None,
            },
            _ => None,
        };
        items.extend(item);
    }
    (items, mails)
}

struct Audit<'a> {
    opts: &'a Opts,
    kept_mail: HashSet<String>,
    /// Mail ids of Group articles: one Record id per reader, so their record
    /// evidence is compared only within one reader's view (`--address`).
    group_mail: HashSet<String>,
    out: Vec<Line>,
    unreceipted: Vec<Line>,
    duplicates: Vec<Line>,
    /// Record ids per mail from the coordinator, a daemon log and a Store, in
    /// the order each mail was first listed.
    records: Vec<(String, Vec<RecordEvidence>)>,
    current_source: usize,
    record_index: HashMap<String, usize>,
    registers: usize,
    unregisters: usize,
    mails: usize,
    receipted: usize,
    torn: usize,
    skipped: usize,
}

struct RecordEvidence {
    source: usize,
    kind: Source,
    id: String,
}

/// Record ids within one source, keyed by `key`, in order of first mention,
/// so one id naming more than one mail is a `DUPLICATE_RECORD`.
#[derive(Default)]
struct RecordsSeen {
    order: Vec<(String, String)>,
    mails: HashMap<(String, String), Vec<String>>,
}

impl RecordsSeen {
    fn note(&mut self, key: (String, String), mail: &str) {
        let mails = self.mails.entry(key.clone()).or_default();
        if mails.is_empty() {
            self.order.push(key);
        }
        if !mails.iter().any(|m| m == mail) {
            mails.push(mail.to_string());
        }
    }

    fn duplicates(&self, path: &str) -> Vec<Line> {
        self.order
            .iter()
            .filter_map(|key| {
                let mails = &self.mails[key];
                (mails.len() > 1).then(|| {
                    line(
                        "DUPLICATE_RECORD",
                        vec![
                            ("path", s(path)),
                            ("record", s(&key.1)),
                            ("mails", Value::Arr(mails.iter().map(|m| s(m)).collect())),
                        ],
                    )
                })
            })
            .collect()
    }
}

impl Audit<'_> {
    /// True when the line is listed: every line without `--address`, else
    /// one naming that Address in one of `names`.
    fn keep(&self, names: &[&str]) -> bool {
        match &self.opts.address {
            None => true,
            Some(a) => names.iter().any(|n| n == a),
        }
    }

    /// True when a record id for `mail` read for `session` (None: a Store's,
    /// which names no reader) is comparable evidence: always, except for a
    /// Group article outside one reader's view.
    fn comparable(&self, mail: &str, session: Option<&str>) -> bool {
        if !self.group_mail.contains(mail) {
            return true;
        }
        match (self.opts.address.as_deref(), session) {
            (Some(a), Some(session)) => a == session,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    fn record(&mut self, mail: &str, which: Source, id: &str) {
        let i = match self.record_index.get(mail) {
            Some(&i) => i,
            None => {
                self.records.push((mail.to_string(), Vec::new()));
                self.record_index
                    .insert(mail.to_string(), self.records.len() - 1);
                self.records.len() - 1
            }
        };
        let records = &mut self.records[i].1;
        if !records
            .iter()
            .any(|r| r.source == self.current_source && r.id == id)
        {
            records.push(RecordEvidence {
                source: self.current_source,
                kind: which,
                id: id.to_string(),
            });
        }
    }

    fn sources(&mut self, inputs: &[Input]) {
        for input in inputs {
            self.out.push(line(
                "SOURCE",
                vec![
                    ("kind", s(input.source.name())),
                    ("path", s(&input.path)),
                    ("lines", int(input.count())),
                ],
            ));
            let Records::Lines { lines, torn } = &input.records else {
                continue;
            };
            if *torn > 0 {
                self.torn += 1;
                self.out.push(line(
                    "TORN",
                    vec![("path", s(&input.path)), ("bytes", int(*torn))],
                ));
            }
            for (n, parsed) in lines {
                if parsed.is_none() {
                    self.skipped += 1;
                    self.out.push(line(
                        "SKIPPED",
                        vec![("path", s(&input.path)), ("line", int(*n))],
                    ));
                }
            }
        }
    }

    fn coordinator(&mut self, input: &Input) {
        let (items, mails) = match &input.records {
            Records::Coord(snapshot) => coordinator_snapshot(snapshot),
            _ => coordinator_lines(input),
        };
        let mut seen = RecordsSeen::default();
        for item in items {
            match item {
                Item::Plain { line, names } => {
                    let names: Vec<&str> = names.iter().map(String::as_str).collect();
                    if self.keep(&names) {
                        match line.word {
                            "REGISTER" => self.registers += 1,
                            "UNREGISTER" => self.unregisters += 1,
                            _ => {}
                        }
                        if line.word == "TAKEOVER" {
                            let mut finding = line.clone();
                            finding.word = "ADDRESS_TAKEOVER";
                            self.out.push(line);
                            self.out.push(finding);
                        } else {
                            self.out.push(line);
                        }
                    }
                }
                Item::Stray {
                    line,
                    session,
                    record,
                    mail,
                } => {
                    if self.keep(&[&session]) || self.kept_mail.contains(&mail) {
                        if self.comparable(&mail, Some(&session)) {
                            self.record(&mail, Source::Coordinator, &record);
                        }
                        seen.note((session, record), &mail);
                        self.out.push(line);
                    }
                }
                Item::Mail(i) => self.mail(&mails[i], &mut seen),
            }
        }
        self.duplicates.extend(seen.duplicates(&input.path));
    }

    /// One MAIL line, its HEADERS and BODY right after it, and its findings.
    /// A Group article keeps the Group as `to`; with `--address` it shows
    /// that reader's own receipt.
    fn mail(&mut self, row: &MailRow, seen: &mut RecordsSeen) {
        let group = row.kind == net::GROUP;
        let address = self.opts.address.clone();
        let mut names: Vec<&str> = vec![&row.from, &row.to];
        if group {
            names.extend(row.receipts.iter().map(|r| r.session.as_str()));
            names.extend(row.readers.iter().map(String::as_str));
        }
        if !self.keep(&names) {
            return;
        }
        let receipt = match address.as_deref() {
            Some(a) if group => row.receipts.iter().find(|r| r.session == a),
            _ => row.receipts.first(),
        };
        self.mails += 1;
        let (receipted_at, record, receipt_line) = match receipt {
            Some(r) => (r.at, s(&r.record), r.line.clone()),
            None => (None, s("pending"), Value::Null),
        };
        let latency = match (row.accepted, receipted_at) {
            (Some(a), Some(r)) => Some(r - a),
            _ => None,
        };
        let head = |extra: Vec<(&'static str, Value)>| {
            let mut fields = vec![
                ("mail", s(&row.id)),
                ("kind", s(&row.kind)),
                ("from", s(&row.from)),
                ("to", s(&row.to)),
            ];
            if let Some(article) = row.article {
                fields.push(("article", Value::Int(article)));
            }
            fields.push(("accepted_at_ms", opt_int(row.accepted)));
            fields.extend(extra);
            fields
        };
        self.out.push(line(
            "MAIL",
            head(vec![
                ("receipted_at_ms", opt_int(receipted_at)),
                ("latency_ms", opt_int(latency)),
                ("record", record),
                ("receipt_line", receipt_line),
                ("body_bytes", row.body_bytes.clone()),
                ("line", row.line.clone()),
            ]),
        ));
        self.out.extend(headers_line(&row.id, &row.headers));
        if self.opts.bodies {
            self.out.push(body_line(&row.id, row.body.as_deref()));
        }
        match receipt {
            Some(r) => {
                self.receipted += 1;
                if group {
                    for other in row
                        .receipts
                        .iter()
                        .filter(|o| address.as_deref().is_none_or(|a| a == o.session))
                    {
                        seen.note((other.session.clone(), other.record.clone()), &row.id);
                    }
                } else {
                    seen.note((r.session.clone(), r.record.clone()), &row.id);
                }
                if self.comparable(&row.id, Some(&r.session)) {
                    self.record(&row.id, Source::Coordinator, &r.record);
                }
            }
            None => self
                .unreceipted
                .push(line("UNRECEIPTED", head(vec![("line", row.line.clone())]))),
        }
    }

    fn daemon_receipt(
        &mut self,
        path: &str,
        (mail, session, record): (&str, &str, &str),
        n: Value,
        seen: &mut RecordsSeen,
    ) {
        if self.keep(&[session]) || self.kept_mail.contains(mail) {
            self.out.push(line(
                "DAEMON_RECEIPT",
                vec![
                    ("path", s(path)),
                    ("mail", s(mail)),
                    ("session", s(session)),
                    ("record", s(record)),
                    ("line", n),
                ],
            ));
            seen.note((session.to_string(), record.to_string()), mail);
            if self.comparable(mail, Some(session)) {
                self.record(mail, Source::Daemon, record);
            }
        }
    }

    fn daemon_deliver(&mut self, path: &str, fields: [&str; 4], headers: &Value, n: Value) {
        let [mail, kind, from, to] = fields;
        if self.keep(&[from, to]) {
            self.out.push(line(
                "DAEMON_DELIVER",
                vec![
                    ("path", s(path)),
                    ("mail", s(mail)),
                    ("kind", s(kind)),
                    ("from", s(from)),
                    ("to", s(to)),
                    ("line", n),
                ],
            ));
            self.out.extend(headers_line(mail, headers));
        }
    }

    fn daemon(&mut self, input: &Input) {
        let mut seen = RecordsSeen::default();
        if let Records::Daemon(snapshot) = &input.records {
            for (seq, row) in &snapshot.pending {
                let fields = [
                    row.mail_id.as_str(),
                    row.kind.as_str(),
                    row.from.as_str(),
                    row.to.as_str(),
                ];
                self.daemon_deliver(&input.path, fields, &row.headers, Value::Int(*seq));
            }
            for (seq, r) in &snapshot.unconfirmed_receipts {
                let ids = (r.mail_id.as_str(), r.session.as_str(), r.record_id.as_str());
                self.daemon_receipt(&input.path, ids, Value::Int(*seq), &mut seen);
            }
        }
        for (n, v) in input
            .lines()
            .iter()
            .filter_map(|(n, v)| Some((*n, v.as_ref()?)))
        {
            match v.get("ev").and_then(|x| x.as_str()) {
                Some("deliver") => {
                    if let (Ok(mail), Ok(from), Ok(to), Ok(_)) = (
                        net::field(v, "mail"),
                        net::field(v, "from"),
                        net::field(v, "to"),
                        net::field(v, "body"),
                    ) {
                        let kind = net::kind_of(v);
                        let fields = [mail, kind.as_str(), from, to];
                        self.daemon_deliver(&input.path, fields, &audit_headers(v), int(n));
                    }
                }
                Some("receipt") => {
                    if let (Ok(mail), Ok(session), Ok(record)) = (
                        net::field(v, "mail"),
                        net::field(v, "session"),
                        net::field(v, "record"),
                    ) {
                        self.daemon_receipt(
                            &input.path,
                            (mail, session, record),
                            int(n),
                            &mut seen,
                        );
                    }
                }
                _ => {}
            }
        }
        self.duplicates.extend(seen.duplicates(&input.path));
    }

    /// A Store names no Address, so `--address` filters nothing here.
    fn store(&mut self, input: &Input) {
        let mut seen = RecordsSeen::default();
        for (n, v) in input
            .lines()
            .iter()
            .filter_map(|(n, v)| Some((*n, v.as_ref()?)))
        {
            if let (Ok(record), Ok(mail), Ok(from), Ok(body)) = (
                net::field(v, "record"),
                net::field(v, "mail"),
                net::field(v, "from"),
                net::field(v, "body"),
            ) {
                self.out.push(line(
                    "STORE",
                    vec![
                        ("path", s(&input.path)),
                        ("record", s(record)),
                        ("mail", s(mail)),
                        ("from", s(from)),
                        ("kind", store_kind(v)),
                        ("line", int(n)),
                    ],
                ));
                self.out.extend(headers_line(mail, &audit_headers(v)));
                if self.opts.bodies {
                    self.out.push(body_line(mail, Some(body)));
                }
                seen.note((String::new(), record.to_string()), mail);
                if self.comparable(mail, None) {
                    self.record(mail, Source::Store, record);
                }
            }
        }
        self.duplicates.extend(seen.duplicates(&input.path));
    }

    fn findings(&mut self, sources: usize) {
        let unreceipted = std::mem::take(&mut self.unreceipted);
        let duplicates = std::mem::take(&mut self.duplicates);
        let mismatches: Vec<Line> = self
            .records
            .iter()
            .filter_map(|(mail, records)| {
                let (reference, conflict) = records.iter().find_map(|reference| {
                    records
                        .iter()
                        .find(|r| r.source != reference.source && r.id != reference.id)
                        .map(|conflict| (reference, conflict))
                })?;
                // Show the disagreeing pair where the kinds differ; a single
                // kind has one field, and its full evidence stays in the timeline.
                let id = |kind| {
                    [conflict, reference]
                        .into_iter()
                        .chain(records.iter())
                        .find(|r| r.kind == kind)
                        .map(|r| r.id.as_str())
                };
                Some(line(
                    "RECORD_MISMATCH",
                    vec![
                        ("mail", s(mail)),
                        ("coordinator", opt_str(id(Source::Coordinator))),
                        ("daemon", opt_str(id(Source::Daemon))),
                        ("store", opt_str(id(Source::Store))),
                    ],
                ))
            })
            .collect();
        let summary = line(
            "AUDIT",
            vec![
                ("sources", int(sources)),
                ("registers", int(self.registers)),
                ("unregisters", int(self.unregisters)),
                ("mails", int(self.mails)),
                ("receipted", int(self.receipted)),
                ("unreceipted", int(unreceipted.len())),
                ("duplicates", int(duplicates.len())),
                ("mismatches", int(mismatches.len())),
                ("torn", int(self.torn)),
                ("skipped", int(self.skipped)),
            ],
        );
        self.out.extend(unreceipted);
        self.out.extend(duplicates);
        self.out.extend(mismatches);
        self.out.push(summary);
    }
}

/// The whole listing for the inputs, in output order: sources, the
/// coordinator timeline, each daemon log, each Store, then the findings.
fn listing(inputs: &[Input], opts: &Opts) -> Vec<Line> {
    // Join receipt-only evidence to Addresses wherever the supplied records
    // name them, independent of input order. Store rows themselves stay
    // unfiltered. A Group article joins by its sender only: each reader's
    // receipt names that reader.
    let keep = |name: &str| opts.address.as_deref().is_none_or(|a| a == name);
    let mut kept_mail = HashSet::new();
    let mut group_mail = HashSet::new();
    let note = |kept_mail: &mut HashSet<String>,
                group_mail: &mut HashSet<String>,
                (mail, kind, from, to): (&str, &str, &str, &str)| {
        if kind == net::GROUP {
            group_mail.insert(mail.to_string());
            if keep(from) {
                kept_mail.insert(mail.to_string());
            }
        } else if keep(from) || keep(to) {
            kept_mail.insert(mail.to_string());
        }
    };
    for input in inputs {
        match &input.records {
            Records::Coord(snapshot) => {
                for item in &snapshot.items {
                    if let audit_ro::Item::Envelope(e) = item {
                        note(
                            &mut kept_mail,
                            &mut group_mail,
                            (&e.mail_id, &e.kind, &e.sender, &e.recipient),
                        );
                    }
                }
            }
            Records::Daemon(snapshot) => {
                for (_, d) in &snapshot.pending {
                    note(
                        &mut kept_mail,
                        &mut group_mail,
                        (&d.mail_id, &d.kind, &d.from, &d.to),
                    );
                }
            }
            Records::Lines { lines, .. } => {
                for (_, v) in lines {
                    let Some(v) = v else { continue };
                    let Some(mail) = v.get("mail").and_then(Value::as_str) else {
                        continue;
                    };
                    if v.get("kind").and_then(Value::as_str) == Some(net::GROUP) {
                        let from = v.get("from").and_then(Value::as_str).unwrap_or("");
                        note(
                            &mut kept_mail,
                            &mut group_mail,
                            (mail, net::GROUP, from, ""),
                        );
                        continue;
                    }
                    let from = v.get("from").and_then(Value::as_str).is_some_and(keep);
                    if let Some(Value::Arr(to)) = v.get("to") {
                        for addr in to.iter().filter_map(Value::as_str) {
                            if from || keep(addr) {
                                kept_mail.insert(format!("{mail}/{addr}"));
                            }
                        }
                    } else if from || v.get("to").and_then(Value::as_str).is_some_and(keep) {
                        kept_mail.insert(mail.to_string());
                    }
                }
            }
        }
    }
    let mut audit = Audit {
        opts,
        kept_mail,
        group_mail,
        out: Vec::new(),
        unreceipted: Vec::new(),
        duplicates: Vec::new(),
        records: Vec::new(),
        current_source: 0,
        record_index: HashMap::new(),
        registers: 0,
        unregisters: 0,
        mails: 0,
        receipted: 0,
        torn: 0,
        skipped: 0,
    };
    audit.sources(inputs);
    for source in [Source::Coordinator, Source::Daemon, Source::Store] {
        for (index, input) in inputs
            .iter()
            .enumerate()
            .filter(|(_, i)| i.source == source)
        {
            audit.current_source = index;
            match source {
                Source::Coordinator => audit.coordinator(input),
                Source::Daemon => audit.daemon(input),
                Source::Store => audit.store(input),
            }
        }
    }
    audit.findings(inputs.len());
    audit.out
}

/// `roger audit`: `args` is the whole argv after the subcommand.
pub fn run(args: &[String]) -> io::Result<()> {
    let opts = parse_args(args)?;
    let inputs = opts
        .inputs
        .iter()
        .map(|(source, path)| read_input(*source, path, opts.bodies))
        .collect::<io::Result<Vec<Input>>>()?;
    let lines = listing(&inputs, &opts);
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    for l in &lines {
        let rendered = if opts.json { json_line(l) } else { text(l) };
        out.write_all(rendered.as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::audit_ro::write_test_fixture;

    /// The text listing of the SQLite fixture: a hot body, a body sealed in
    /// a cold day file, and a Group article read by two of three readers.
    fn audit_fixture(tag: &str, address: Option<&str>) -> (Vec<String>, String, String) {
        let dir = std::env::temp_dir().join(format!("roger-audit-ro-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (c, d) = write_test_fixture(&dir);
        let (c, d) = (
            c.to_str().unwrap().to_string(),
            d.to_str().unwrap().to_string(),
        );
        let opts = Opts {
            inputs: vec![
                (Source::Coordinator, c.clone()),
                (Source::Daemon, d.clone()),
            ],
            address: address.map(str::to_string),
            bodies: true,
            json: false,
        };
        let inputs: Vec<Input> = opts
            .inputs
            .iter()
            .map(|(source, path)| read_input(*source, path, true).unwrap())
            .collect();
        let out = listing(&inputs, &opts).iter().map(text).collect();
        let _ = std::fs::remove_dir_all(&dir);
        (out, c, d)
    }

    #[test]
    fn sqlite_audit_reads_cold_bodies_and_group_articles() {
        let (out, c, d) = audit_fixture("all", None);
        assert_eq!(
            out,
            [
                format!("SOURCE kind=coordinator path={c} lines=8"),
                format!("SOURCE kind=daemon path={d} lines=2"),
                "REGISTER address=r1 host=h at_ms=10 line=1".into(),
                "MAIL mail=hot-1 kind=directed from=s to=r1 accepted_at_ms=100 receipted_at_ms=150 latency_ms=50 record=r-1 receipt_line=3 body_bytes=3 line=2".into(),
                "HEADERS mail=hot-1 absent".into(),
                "BODY mail=hot-1 \"hot\"".into(),
                "MAIL mail=cold-1 kind=directed from=s to=r1 accepted_at_ms=200 receipted_at_ms=absent latency_ms=absent record=pending receipt_line=absent body_bytes=4 line=4".into(),
                "BODY mail=cold-1 \"cold\"".into(),
                "MAIL mail=g-1 kind=group from=s to=news article=1 accepted_at_ms=300 receipted_at_ms=310 latency_ms=10 record=r-2 receipt_line=6 body_bytes=7 line=5".into(),
                "HEADERS mail=g-1 {\"k\":\"v\"}".into(),
                "BODY mail=g-1 \"article\"".into(),
                "STRAY_RECEIPT mail=lost session=r1 record=r-9 line=8".into(),
                format!("DAEMON_DELIVER path={d} mail=g-1 kind=group from=s to=r3 line=1"),
                "HEADERS mail=g-1 {\"k\":\"v\"}".into(),
                format!("DAEMON_RECEIPT path={d} mail=g-1 session=r1 record=r-2 line=1"),
                "UNRECEIPTED mail=cold-1 kind=directed from=s to=r1 accepted_at_ms=200 line=4".into(),
                "AUDIT sources=2 registers=1 unregisters=0 mails=3 receipted=2 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0".into(),
            ]
        );
    }

    #[test]
    fn sqlite_audit_group_address_shows_that_readers_own_receipt() {
        let (out, c, d) = audit_fixture("reader", Some("r2"));
        assert_eq!(
            out,
            [
                format!("SOURCE kind=coordinator path={c} lines=8"),
                format!("SOURCE kind=daemon path={d} lines=2"),
                "MAIL mail=g-1 kind=group from=s to=news article=1 accepted_at_ms=300 receipted_at_ms=320 latency_ms=20 record=r-5 receipt_line=7 body_bytes=7 line=5".into(),
                "HEADERS mail=g-1 {\"k\":\"v\"}".into(),
                "BODY mail=g-1 \"article\"".into(),
                "AUDIT sources=2 registers=0 unregisters=0 mails=1 receipted=1 unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0".into(),
            ]
        );
        let (out, _, d) = audit_fixture("subscriber", Some("r3"));
        assert_eq!(
            out[2..],
            [
                "MAIL mail=g-1 kind=group from=s to=news article=1 accepted_at_ms=300 receipted_at_ms=absent latency_ms=absent record=pending receipt_line=absent body_bytes=7 line=5".to_string(),
                "HEADERS mail=g-1 {\"k\":\"v\"}".into(),
                "BODY mail=g-1 \"article\"".into(),
                format!("DAEMON_DELIVER path={d} mail=g-1 kind=group from=s to=r3 line=1"),
                "HEADERS mail=g-1 {\"k\":\"v\"}".into(),
                "UNRECEIPTED mail=g-1 kind=group from=s to=news article=1 accepted_at_ms=300 line=5".into(),
                "AUDIT sources=2 registers=0 unregisters=0 mails=1 receipted=0 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0".into(),
            ]
        );
    }
}

//! `roger audit`: a read-only listing of what the coordinator log, host daemon
//! logs and Stores hold for one Address or all, in log order, with findings.
//!
//! Each input is read as plain bytes, never opened as a `DurLog`: no lock, no
//! truncation, no write, so it reads a file a running coordinator, host daemon
//! or session holds. A line a generation never stamped reads `absent`, never
//! zero. Every opaque text (an Address, a label, a declared text, a header
//! set, a body) is copied byte-identical; nothing here reads its meaning, and
//! the only comparisons are equality of an Address, a mail id, a record id or
//! a claim key against another of the same field. Findings are listed, not
//! judged: the exit status says only whether every input could be read.

use crate::json::{self, s, Value};
use crate::net;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};

const USAGE: &str = "roger audit [--coordinator-log <file>] [--daemon-log <file>]... \
[--store <file>]... [--address <address>] [--bodies] [--json]";

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
            "--coordinator-log" => {
                let path = value()?;
                if opts.inputs.iter().any(|(k, _)| *k == Source::Coordinator) {
                    return Err(bad(format!(
                        "expected --coordinator-log once, saw it again with {path}"
                    )));
                }
                opts.inputs.push((Source::Coordinator, path));
            }
            "--daemon-log" => opts.inputs.push((Source::Daemon, value()?)),
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

/// One input, read whole and split into its complete lines.
struct Input {
    source: Source,
    path: String,
    /// Each complete line's 1-based number and its object, when it is one.
    lines: Vec<(usize, Option<Value>)>,
    /// The trailing fragment's byte length; 0 when the file ends in `\n`.
    torn: usize,
}

fn read_input(source: Source, path: &str) -> io::Result<Input> {
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
        lines,
        torn: bytes.len() - end,
    })
}

/// One output record: the word, its keyed fields in order, and an optional
/// JSON tail (a header set or a body) shown bare in the text form.
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

fn body_line(mail: &str, body: &str) -> Line {
    Line {
        word: "BODY",
        fields: vec![("mail", s(mail))],
        tail: Some(("body", s(body))),
    }
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

/// One Mail the coordinator accepted: a `mail` line, or one recipient's copy
/// of a `broadcast` line, joined to the first `receipt` line for its id.
struct MailRow {
    id: String,
    kind: &'static str,
    from: String,
    to: String,
    body: String,
    headers: Value,
    accepted: Option<i64>,
    line: usize,
    receipt: Option<ReceiptRow>,
}

struct ReceiptRow {
    session: String,
    record: String,
    at: Option<i64>,
    line: usize,
}

/// One timeline entry of the coordinator log, held until every receipt is
/// known. `names` are the Addresses `--address` may match on the line.
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

struct Audit<'a> {
    opts: &'a Opts,
    kept_mail: HashSet<String>,
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
                    ("lines", int(input.lines.len())),
                ],
            ));
            if input.torn > 0 {
                self.torn += 1;
                self.out.push(line(
                    "TORN",
                    vec![("path", s(&input.path)), ("bytes", int(input.torn))],
                ));
            }
            for (n, parsed) in &input.lines {
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
        let mut items: Vec<Item> = Vec::new();
        let mut mails: Vec<MailRow> = Vec::new();
        let mut by_id: HashMap<String, usize> = HashMap::new();
        let mut last_owner: HashMap<String, String> = HashMap::new();
        let session_event =
            |v: &Value, word: &'static str, n: usize, free: Option<(&'static str, &str)>| {
                let session = net::field(v, "session")?;
                let mut fields = vec![("address", s(session))];
                if word == "REGISTER" {
                    fields.push(("host", s(net::field(v, "host")?)));
                }
                fields.push(("at_ms", opt_int(stamp_of(v, "at_ms"))));
                fields.push(("line", int(n)));
                if let Some((k, t)) = free {
                    fields.push((k, s(t)));
                }
                Ok::<Item, io::Error>(Item::Plain {
                    line: line(word, fields),
                    names: vec![session.to_string()],
                })
            };
        for (n, v) in input
            .lines
            .iter()
            .filter_map(|(n, v)| Some((*n, v.as_ref()?)))
        {
            let item = match v.get("ev").and_then(|x| x.as_str()) {
                Some("session") => session_event(v, "REGISTER", n, None).ok(),
                Some("unregister") => session_event(v, "UNREGISTER", n, None).ok(),
                Some("label") => net::field(v, "label")
                    .ok()
                    .and_then(|l| session_event(v, "LABEL", n, Some(("label", l))).ok()),
                Some("declared") => match net::declared_of(v) {
                    Some(d) => session_event(v, "DECLARED", n, Some(("declared", &d))).ok(),
                    None => session_event(v, "UNDECLARED", n, None).ok(),
                },
                Some("mail") => match (
                    net::field(v, "mail"),
                    net::field(v, "from"),
                    net::field(v, "to"),
                    net::field(v, "body"),
                ) {
                    (Ok(mail), Ok(from), Ok(to), Ok(body)) => {
                        mails.push(MailRow {
                            id: mail.to_string(),
                            kind: net::DIRECTED,
                            from: from.to_string(),
                            to: to.to_string(),
                            body: body.to_string(),
                            headers: audit_headers(v),
                            accepted: stamp_of(v, "accepted_at_ms"),
                            line: n,
                            receipt: None,
                        });
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
                            mails.push(MailRow {
                                id: id.clone(),
                                kind: net::BROADCAST,
                                from: from.to_string(),
                                to: addr.to_string(),
                                body: body.to_string(),
                                headers: audit_headers(v),
                                accepted: stamp_of(v, "accepted_at_ms"),
                                line: n,
                                receipt: None,
                            });
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
                            let session = v
                                .get("session")
                                .and_then(|x| x.as_str())
                                .unwrap_or(&row.to)
                                .to_string();
                            row.receipt.get_or_insert(ReceiptRow {
                                session,
                                record: record.to_string(),
                                at: stamp_of(v, "receipted_at_ms"),
                                line: n,
                            });
                            None
                        }
                        None => net::field(v, "session").ok().map(|session| Item::Stray {
                            line: line(
                                "STRAY_RECEIPT",
                                vec![
                                    ("mail", s(mail)),
                                    ("session", s(session)),
                                    ("record", s(record)),
                                    ("line", int(n)),
                                ],
                            ),
                            session: session.to_string(),
                            record: record.to_string(),
                            mail: mail.to_string(),
                        }),
                    },
                    _ => None,
                },
                Some("claim") => match (
                    net::field(v, "key"),
                    net::field(v, "owner"),
                    stamp_of(v, "expires_ms"),
                ) {
                    (Ok(key), Ok(owner), Some(expires)) => {
                        last_owner.insert(key.to_string(), owner.to_string());
                        Some(Item::Plain {
                            line: line(
                                "CLAIM",
                                vec![
                                    ("key", s(key)),
                                    ("owner", s(owner)),
                                    ("expires_ms", Value::Int(expires)),
                                    ("line", int(n)),
                                ],
                            ),
                            names: vec![owner.to_string()],
                        })
                    }
                    _ => None,
                },
                Some("release") => net::field(v, "key").ok().map(|key| Item::Plain {
                    line: line("RELEASE", vec![("key", s(key)), ("line", int(n))]),
                    names: last_owner.get(key).cloned().into_iter().collect(),
                }),
                _ => None,
            };
            if let Some(item) = item {
                items.push(item);
            }
        }

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
                        self.out.push(line);
                    }
                }
                Item::Stray {
                    line,
                    session,
                    record,
                    mail,
                } => {
                    if self.keep(&[&session]) || self.kept_mail.contains(&mail) {
                        self.record(&mail, Source::Coordinator, &record);
                        seen.note((session, record), &mail);
                        self.out.push(line);
                    }
                }
                Item::Mail(i) => {
                    let row = &mails[i];
                    if !self.keep(&[&row.from, &row.to]) {
                        continue;
                    }
                    self.mails += 1;
                    let (receipted_at, record, receipt_line) = match &row.receipt {
                        Some(r) => (r.at, s(&r.record), int(r.line)),
                        None => (None, s("pending"), Value::Null),
                    };
                    let latency = match (row.accepted, receipted_at) {
                        (Some(a), Some(r)) => Some(r - a),
                        _ => None,
                    };
                    self.out.push(line(
                        "MAIL",
                        vec![
                            ("mail", s(&row.id)),
                            ("kind", s(row.kind)),
                            ("from", s(&row.from)),
                            ("to", s(&row.to)),
                            ("accepted_at_ms", opt_int(row.accepted)),
                            ("receipted_at_ms", opt_int(receipted_at)),
                            ("latency_ms", opt_int(latency)),
                            ("record", record),
                            ("receipt_line", receipt_line),
                            ("body_bytes", int(row.body.len())),
                            ("line", int(row.line)),
                        ],
                    ));
                    self.out.extend(headers_line(&row.id, &row.headers));
                    if self.opts.bodies {
                        self.out.push(body_line(&row.id, &row.body));
                    }
                    match &row.receipt {
                        Some(r) => {
                            self.receipted += 1;
                            seen.note((r.session.clone(), r.record.clone()), &row.id);
                            self.record(&row.id, Source::Coordinator, &r.record);
                        }
                        None => self.unreceipted.push(line(
                            "UNRECEIPTED",
                            vec![
                                ("mail", s(&row.id)),
                                ("kind", s(row.kind)),
                                ("from", s(&row.from)),
                                ("to", s(&row.to)),
                                ("accepted_at_ms", opt_int(row.accepted)),
                                ("line", int(row.line)),
                            ],
                        )),
                    }
                }
            }
        }
        self.duplicates.extend(seen.duplicates(&input.path));
    }

    fn daemon(&mut self, input: &Input) {
        let mut seen = RecordsSeen::default();
        for (n, v) in input
            .lines
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
                        if self.keep(&[from, to]) {
                            self.out.push(line(
                                "DAEMON_DELIVER",
                                vec![
                                    ("path", s(&input.path)),
                                    ("mail", s(mail)),
                                    ("kind", s(&net::kind_of(v))),
                                    ("from", s(from)),
                                    ("to", s(to)),
                                    ("line", int(n)),
                                ],
                            ));
                            self.out.extend(headers_line(mail, &audit_headers(v)));
                        }
                    }
                }
                Some("receipt") => {
                    if let (Ok(mail), Ok(session), Ok(record)) = (
                        net::field(v, "mail"),
                        net::field(v, "session"),
                        net::field(v, "record"),
                    ) {
                        if self.keep(&[session]) || self.kept_mail.contains(mail) {
                            self.out.push(line(
                                "DAEMON_RECEIPT",
                                vec![
                                    ("path", s(&input.path)),
                                    ("mail", s(mail)),
                                    ("session", s(session)),
                                    ("record", s(record)),
                                    ("line", int(n)),
                                ],
                            ));
                            seen.note((session.to_string(), record.to_string()), mail);
                            self.record(mail, Source::Daemon, record);
                        }
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
            .lines
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
                    self.out.push(body_line(mail, body));
                }
                seen.note((String::new(), record.to_string()), mail);
                self.record(mail, Source::Store, record);
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
    // name them, independent of input order. Store rows themselves stay unfiltered.
    let mut kept_mail = HashSet::new();
    for input in inputs {
        for (_, v) in &input.lines {
            let Some(v) = v else { continue };
            let Some(mail) = v.get("mail").and_then(Value::as_str) else {
                continue;
            };
            let keep = |name: &str| opts.address.as_deref().is_none_or(|a| a == name);
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
    let mut audit = Audit {
        opts,
        kept_mail,
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
        .map(|(source, path)| read_input(*source, path))
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

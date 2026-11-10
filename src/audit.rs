//! `roger audit`: a read-only listing of what the coordinator log, host daemon
//! logs and Stores hold for one Address or all, in log order, with findings.
//!
//! Each input is read as plain bytes, never opened as a `DurLog`: no lock, no
//! truncation, no write, so it reads a file a running coordinator, host daemon
//! or session holds. A line a generation never stamped reads `absent`, never
//! zero. Every opaque text (an Address, a label, a declared text, a header
//! set, a body) is copied byte-identical; nothing here reads its meaning, and
//! the only comparisons are equality of an Address, a mail id, a record id, a
//! claim key, a claim deadline or a registration id against another of the
//! same field, and of a snapshot's `prior` against an input's file. Findings
//! are listed, not judged: the exit status says only whether every input
//! could be read.
//!
//! A rotated log is several files: archives `<name>.<n>` and the file at the
//! path, which opens with a snapshot block restating what its owner still
//! needs from the archive it names as `prior`. Every coordinator file given
//! forms one timeline in argument order. A block whose `prior` is another
//! input of the same kind (see `supplied`) is skipped, since that file holds
//! the lines it restates. Any other block is read, and a line it restates
//! adds nothing when the same source already listed what it says, so a moved,
//! renamed or left-out archive never lists a Mail twice. An input that is the
//! same file as an earlier one of its kind is listed as a source and read once.

use crate::json::{self, s, Value};
use crate::net;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

const USAGE: &str = "roger audit [--coordinator-log <file>]... [--daemon-log <file>]... \
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
            "--coordinator-log" => opts.inputs.push((Source::Coordinator, value()?)),
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
    /// The file's device and inode, to know an archive named by a snapshot
    /// block when it is also an input, whatever path it was given by.
    id: Option<(u64, u64)>,
}

fn read_input(source: Source, path: &str) -> io::Result<Input> {
    let failed = |e: io::Error| {
        io::Error::new(
            e.kind(),
            format!(
                "expected a readable {} file at {path}, saw {e}",
                source.name()
            ),
        )
    };
    let mut file = std::fs::File::open(path).map_err(failed)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(failed)?;
    let id = file.metadata().ok().map(|m| (m.dev(), m.ino()));
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
        id,
    })
}

/// One step through an input: a line with its number, true when it is
/// restated inside a snapshot block that is read, or the `SNAPSHOT` record
/// for a block's opening marker.
enum Step<'i> {
    Line(usize, &'i Value, bool),
    Snapshot(Line),
}

/// A block opens with a `snapshot` key on a line without `ev`.
fn opens(v: &Value) -> bool {
    v.get("snapshot").is_some() && v.get("ev").is_none()
}

/// A block closes with a `snapshot_end` key on a line that opens none.
fn closes(v: &Value) -> bool {
    v.get("snapshot_end").is_some() && v.get("ev").is_none() && !opens(v)
}

/// True when a block's `prior` names another input of the same kind: the
/// file read beside `inputs[index]` under that name (same device and inode),
/// or, for the coordinator only, an input with that file name wherever it
/// lives. A coordinator archive counts only before the file in argument
/// order. Host daemon logs and Stores are matched by the file alone: every
/// host names its log alike, so a name would match another host's archive.
fn supplied(inputs: &[Input], index: usize, prior: &str) -> bool {
    let input = &inputs[index];
    let beside = Path::new(&input.path)
        .parent()
        .unwrap_or(Path::new(""))
        .join(prior);
    let id = std::fs::metadata(beside).ok().map(|m| (m.dev(), m.ino()));
    let timeline = input.source == Source::Coordinator;
    inputs.iter().enumerate().any(|(i, other)| {
        let placed = if timeline { i < index } else { i != index };
        let named = timeline && Path::new(&other.path).file_name() == Some(OsStr::new(prior));
        other.source == input.source && placed && (named || (id.is_some() && other.id == id))
    })
}

/// True when `inputs[index]` is the same file (device and inode) as an
/// earlier input of the same kind. It is listed as a `SOURCE` and its lines
/// are read once, under the earlier input.
fn repeated(inputs: &[Input], index: usize) -> bool {
    let input = &inputs[index];
    input.id.is_some()
        && inputs[..index]
            .iter()
            .any(|other| other.source == input.source && other.id == input.id)
}

/// The object lines of `inputs[index]` in order, each snapshot block
/// resolved: a `SNAPSHOT` record at its marker, then its lines, marked
/// restated, unless its `prior` is another input of the same kind. A block
/// runs to its `snapshot_end`, to the next `snapshot`, or to the end of the
/// file; a `snapshot_end` outside a block is passed over. A repeated input
/// yields nothing.
fn steps(inputs: &[Input], index: usize) -> Vec<Step<'_>> {
    if repeated(inputs, index) {
        return Vec::new();
    }
    let input = &inputs[index];
    let lines = &input.lines;
    let is = |i: usize, test: fn(&Value) -> bool| {
        lines.get(i).and_then(|(_, v)| v.as_ref()).is_some_and(test)
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let (n, v) = (lines[i].0, lines[i].1.as_ref());
        i += 1;
        let Some(v) = v else { continue };
        if opens(v) {
            let start = i;
            while i < lines.len() && !is(i, opens) && !is(i, closes) {
                i += 1;
            }
            let prior = v.get("prior").and_then(Value::as_str);
            let read = !prior.is_some_and(|p| supplied(inputs, index, p));
            out.push(Step::Snapshot(line(
                "SNAPSHOT",
                vec![
                    ("path", s(&input.path)),
                    ("seq", opt_int(stamp_of(v, "snapshot"))),
                    ("prior", opt_str(prior)),
                    ("at_ms", opt_int(stamp_of(v, "at_ms"))),
                    ("next_record", opt_int(stamp_of(v, "next_record"))),
                    ("lines", int(i - start)),
                    ("read", Value::Bool(read)),
                ],
            )));
            if read {
                out.extend(
                    lines[start..i]
                        .iter()
                        .filter_map(|(n, v)| Some(Step::Line(*n, v.as_ref()?, true))),
                );
            }
            if is(i, closes) {
                i += 1;
            }
        } else if !closes(v) {
            out.push(Step::Line(n, v, false));
        }
    }
    out
}

/// A coordinator record's `line=`, and before it `path=` when several
/// coordinator files form the timeline; `keys` names the pair, and `at` is
/// absent for what has no line yet.
fn place(
    multi: bool,
    keys: [&'static str; 2],
    at: Option<(&str, usize)>,
) -> Vec<(&'static str, Value)> {
    let mut out = Vec::new();
    if multi {
        out.push((keys[0], at.map_or(Value::Null, |(p, _)| s(p))));
    }
    out.push((keys[1], at.map_or(Value::Null, |(_, n)| int(n))));
    out
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
    /// None when the line is settled: its body is gone.
    body: Option<String>,
    body_bytes: Value,
    headers: Value,
    accepted: Option<i64>,
    /// The input index of the file holding the line, and its line number.
    at: (usize, usize),
    receipt: Option<ReceiptRow>,
}

struct ReceiptRow {
    session: String,
    record: String,
    stamp: Option<i64>,
    at: (usize, usize),
}

/// The body and its UTF-8 length of a `mail` or `broadcast` line; a settled
/// line keeps only the length, under `settled.bytes`.
fn body_of(v: &Value, body: &str) -> (Option<String>, Value) {
    match v.get("settled") {
        Some(settled) => (None, opt_int(stamp_of(settled, "bytes"))),
        None => (Some(body.to_string()), int(body.len())),
    }
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
        input: usize,
    },
    /// Listed whatever `--address` is: it describes a file, not an Address.
    Snapshot(Line),
    Culled {
        line: Line,
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
    snapshots: usize,
    culled: usize,
    /// What host daemon logs and Stores have listed so far, by mail id,
    /// receipt and record: a line a snapshot block restates that repeats
    /// one of these adds nothing.
    delivered: HashSet<String>,
    daemon_receipts: HashSet<(String, String, String)>,
    stored: HashSet<(String, String)>,
}

/// What the coordinator timeline shows so far of what a snapshot restates,
/// so a restated line that repeats it adds nothing. Only Addresses, claim
/// keys, claim owners and registration ids are compared, each against its
/// own field; never a label or declared text.
#[derive(Default)]
struct Shown {
    registered: HashSet<String>,
    labelled: HashSet<String>,
    declared: HashSet<String>,
    claims: HashMap<String, (String, i64)>,
    takeovers: HashSet<String>,
}

impl Shown {
    /// True when a restated `ev` line repeats what the timeline shows: a
    /// registered session, a label or declared text already shown for it
    /// while registered, the claim a key holds, a takeover already listed.
    fn repeats(&self, ev: &str, v: &Value) -> bool {
        let session = net::field(v, "session").unwrap_or_default();
        match ev {
            "session" => self.registered.contains(session),
            "label" => self.labelled.contains(session),
            "declared" => self.declared.contains(session) == net::declared_of(v).is_some(),
            "claim" => match (
                net::field(v, "key"),
                net::field(v, "owner"),
                stamp_of(v, "expires_ms"),
            ) {
                (Ok(key), Ok(owner), Some(expires)) => self
                    .claims
                    .get(key)
                    .is_some_and(|(o, e)| o == owner && *e == expires),
                _ => false,
            },
            "takeover" => net::field(v, "registration").is_ok_and(|r| self.takeovers.contains(r)),
            _ => false,
        }
    }

    /// Note an event the timeline lists.
    fn note(&mut self, ev: &str, v: &Value) {
        let session = net::field(v, "session").unwrap_or_default().to_string();
        match ev {
            "session" => {
                if self.registered.insert(session.clone()) {
                    self.labelled.remove(&session);
                    self.declared.remove(&session);
                }
            }
            "unregister" => {
                self.registered.remove(&session);
                self.labelled.remove(&session);
                self.declared.remove(&session);
            }
            "label" => {
                self.labelled.insert(session);
            }
            "declared" => {
                if net::declared_of(v).is_some() {
                    self.declared.insert(session);
                } else {
                    self.declared.remove(&session);
                }
            }
            "claim" => {
                if let (Ok(key), Ok(owner), Some(expires)) = (
                    net::field(v, "key"),
                    net::field(v, "owner"),
                    stamp_of(v, "expires_ms"),
                ) {
                    self.claims
                        .insert(key.to_string(), (owner.to_string(), expires));
                }
            }
            "release" => {
                if let Ok(key) = net::field(v, "key") {
                    self.claims.remove(key);
                }
            }
            "takeover" => {
                if let Ok(r) = net::field(v, "registration") {
                    self.takeovers.insert(r.to_string());
                }
            }
            _ => {}
        }
    }
}

struct RecordEvidence {
    source: usize,
    kind: Source,
    id: String,
}

/// Record ids within one source, keyed by `key`, in order of first mention,
/// so one id naming more than one mail is a `DUPLICATE_RECORD`. The finding
/// names the file whose line gave the id its second mail: the input itself,
/// or one file of the coordinator timeline.
#[derive(Default)]
struct RecordsSeen {
    order: Vec<(String, String)>,
    mails: HashMap<(String, String), (Vec<String>, String)>,
}

impl RecordsSeen {
    fn note(&mut self, key: (String, String), mail: &str, path: &str) {
        let (mails, at) = self.mails.entry(key.clone()).or_default();
        if mails.is_empty() {
            self.order.push(key);
        }
        if !mails.iter().any(|m| m == mail) {
            mails.push(mail.to_string());
            if mails.len() == 2 {
                *at = path.to_string();
            }
        }
    }

    fn duplicates(&self) -> Vec<Line> {
        self.order
            .iter()
            .filter_map(|key| {
                let (mails, path) = &self.mails[key];
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

    /// Each input's `SOURCE`, then its `TORN` and `SKIPPED` records; a
    /// repeated input has those under its first listing only.
    fn sources(&mut self, inputs: &[Input]) {
        for (index, input) in inputs.iter().enumerate() {
            self.out.push(line(
                "SOURCE",
                vec![
                    ("kind", s(input.source.name())),
                    ("path", s(&input.path)),
                    ("lines", int(input.lines.len())),
                ],
            ));
            if repeated(inputs, index) {
                continue;
            }
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

    /// The coordinator timeline: every coordinator input, `files` in argument
    /// order, read as one log. `line` stays each file's own line number;
    /// with more than one file, `path` says which file it is in. A line a
    /// read snapshot block restates adds nothing when it repeats what the
    /// timeline shows: a Mail it already has, a Receipt that Mail already
    /// has, a culled id already settled, or a session, label, declared text,
    /// claim or takeover already shown (see `Shown`).
    fn coordinator(&mut self, inputs: &[Input], files: &[usize]) {
        let multi = files.len() > 1;
        let mut items: Vec<Item> = Vec::new();
        let mut mails: Vec<MailRow> = Vec::new();
        let mut by_id: HashMap<String, usize> = HashMap::new();
        let mut last_owner: HashMap<String, String> = HashMap::new();
        let mut culled: HashSet<String> = HashSet::new();
        let mut shown = Shown::default();
        let session_event = |v: &Value,
                             word: &'static str,
                             here: Vec<(&'static str, Value)>,
                             free: Option<(&'static str, &str)>| {
            let session = net::field(v, "session")?;
            let mut fields = vec![("address", s(session))];
            if word == "REGISTER" {
                fields.push(("host", s(net::field(v, "host")?)));
            }
            fields.push(("at_ms", opt_int(stamp_of(v, "at_ms"))));
            fields.extend(here);
            if let Some((k, t)) = free {
                fields.push((k, s(t)));
            }
            Ok::<Item, io::Error>(Item::Plain {
                line: line(word, fields),
                names: vec![session.to_string()],
            })
        };
        for &input in files {
            let path = inputs[input].path.as_str();
            for step in steps(inputs, input) {
                let (n, v, restated) = match step {
                    Step::Snapshot(l) => {
                        items.push(Item::Snapshot(l));
                        continue;
                    }
                    Step::Line(n, v, restated) => (n, v, restated),
                };
                let ev = v.get("ev").and_then(|x| x.as_str());
                let receipted =
                    |mail: &str| by_id.get(mail).is_some_and(|&i| mails[i].receipt.is_some());
                let repeat = restated
                    && match ev {
                        Some("mail") => net::field(v, "mail").is_ok_and(|m| by_id.contains_key(m)),
                        Some("receipt") => net::field(v, "mail").is_ok_and(receipted),
                        Some("culled") => {
                            net::field(v, "mail").is_ok_and(|m| culled.contains(m) || receipted(m))
                        }
                        Some(ev) => shown.repeats(ev, v),
                        None => false,
                    };
                if repeat {
                    continue;
                }
                let here = || place(multi, ["path", "line"], Some((path, n)));
                let item = match ev {
                    Some("takeover") => {
                        match (
                            net::field(v, "session"),
                            net::field(v, "old_host"),
                            net::field(v, "host"),
                        ) {
                            (Ok(session), Ok(old_host), Ok(host)) => {
                                let mut fields = vec![
                                    ("address", s(session)),
                                    ("old_host", s(old_host)),
                                    ("host", s(host)),
                                    ("at_ms", opt_int(stamp_of(v, "at_ms"))),
                                ];
                                fields.extend(here());
                                Some(Item::Plain {
                                    line: line("TAKEOVER", fields),
                                    names: vec![session.to_string()],
                                })
                            }
                            _ => None,
                        }
                    }
                    Some("session") => session_event(v, "REGISTER", here(), None).ok(),
                    Some("unregister") => session_event(v, "UNREGISTER", here(), None).ok(),
                    Some("label") => net::field(v, "label")
                        .ok()
                        .and_then(|l| session_event(v, "LABEL", here(), Some(("label", l))).ok()),
                    Some("declared") => match net::declared_of(v) {
                        Some(d) => {
                            session_event(v, "DECLARED", here(), Some(("declared", &d))).ok()
                        }
                        None => session_event(v, "UNDECLARED", here(), None).ok(),
                    },
                    Some("mail") => match (
                        net::field(v, "mail"),
                        net::field(v, "from"),
                        net::field(v, "to"),
                        net::field(v, "body"),
                    ) {
                        (Ok(mail), Ok(from), Ok(to), Ok(body)) => {
                            let (body, body_bytes) = body_of(v, body);
                            let broadcast =
                                v.get("kind").and_then(Value::as_str) == Some(net::BROADCAST);
                            mails.push(MailRow {
                                id: mail.to_string(),
                                kind: if broadcast {
                                    net::BROADCAST
                                } else {
                                    net::DIRECTED
                                },
                                from: from.to_string(),
                                to: to.to_string(),
                                body,
                                body_bytes,
                                headers: audit_headers(v),
                                accepted: stamp_of(v, "accepted_at_ms"),
                                at: (input, n),
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
                            let (body, body_bytes) = body_of(v, body);
                            for addr in to.iter().filter_map(|x| x.as_str()) {
                                let id = format!("{mail}/{addr}");
                                if restated && by_id.contains_key(&id) {
                                    continue;
                                }
                                mails.push(MailRow {
                                    id: id.clone(),
                                    kind: net::BROADCAST,
                                    from: from.to_string(),
                                    to: addr.to_string(),
                                    body: body.clone(),
                                    body_bytes: body_bytes.clone(),
                                    headers: audit_headers(v),
                                    accepted: stamp_of(v, "accepted_at_ms"),
                                    at: (input, n),
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
                                    stamp: stamp_of(v, "receipted_at_ms"),
                                    at: (input, n),
                                });
                                None
                            }
                            None => net::field(v, "session").ok().map(|session| {
                                let mut fields = vec![
                                    ("mail", s(mail)),
                                    ("session", s(session)),
                                    ("record", s(record)),
                                ];
                                fields.extend(here());
                                Item::Stray {
                                    line: line("STRAY_RECEIPT", fields),
                                    session: session.to_string(),
                                    record: record.to_string(),
                                    mail: mail.to_string(),
                                    input,
                                }
                            }),
                        },
                        _ => None,
                    },
                    Some("culled") => net::field(v, "mail").ok().map(|mail| {
                        culled.insert(mail.to_string());
                        let mut fields = vec![("mail", s(mail))];
                        fields.extend(here());
                        Item::Culled {
                            line: line("CULLED", fields),
                            mail: mail.to_string(),
                        }
                    }),
                    Some("claim") => match (
                        net::field(v, "key"),
                        net::field(v, "owner"),
                        stamp_of(v, "expires_ms"),
                    ) {
                        (Ok(key), Ok(owner), Some(expires)) => {
                            last_owner.insert(key.to_string(), owner.to_string());
                            let mut fields = vec![
                                ("key", s(key)),
                                ("owner", s(owner)),
                                ("expires_ms", Value::Int(expires)),
                            ];
                            fields.extend(here());
                            Some(Item::Plain {
                                line: line("CLAIM", fields),
                                names: vec![owner.to_string()],
                            })
                        }
                        _ => None,
                    },
                    Some("release") => net::field(v, "key").ok().map(|key| {
                        let mut fields = vec![("key", s(key))];
                        fields.extend(here());
                        Item::Plain {
                            line: line("RELEASE", fields),
                            names: last_owner.get(key).cloned().into_iter().collect(),
                        }
                    }),
                    _ => None,
                };
                if let Some(item) = item {
                    if let Some(ev) = ev {
                        shown.note(ev, v);
                    }
                    items.push(item);
                }
            }
        }

        let place_of = |(input, n): (usize, usize), keys| {
            place(multi, keys, Some((inputs[input].path.as_str(), n)))
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
                Item::Snapshot(line) => self.snapshot(line),
                // A culled Mail names no Address: it is listed without
                // `--address`, or when supplied records join it to that one.
                Item::Culled { line, mail } => {
                    if self.opts.address.is_none() || self.kept_mail.contains(&mail) {
                        self.culled += 1;
                        self.out.push(line);
                    }
                }
                // A receipt for a culled Mail had its acceptance in a file
                // the rotation left behind: it is no stray.
                Item::Stray { mail, .. } if culled.contains(&mail) => {}
                Item::Stray {
                    line,
                    session,
                    record,
                    mail,
                    input,
                } => {
                    if self.keep(&[&session]) || self.kept_mail.contains(&mail) {
                        self.record(&mail, Source::Coordinator, &record);
                        seen.note((session, record), &mail, &inputs[input].path);
                        self.out.push(line);
                    }
                }
                Item::Mail(i) => {
                    let row = &mails[i];
                    if !self.keep(&[&row.from, &row.to]) {
                        continue;
                    }
                    self.mails += 1;
                    let (receipted_at, record, receipt_at) = match &row.receipt {
                        Some(r) => (r.stamp, s(&r.record), Some(r.at)),
                        None => (None, s("pending"), None),
                    };
                    let latency = match (row.accepted, receipted_at) {
                        (Some(a), Some(r)) => Some(r - a),
                        _ => None,
                    };
                    let mut fields = vec![
                        ("mail", s(&row.id)),
                        ("kind", s(row.kind)),
                        ("from", s(&row.from)),
                        ("to", s(&row.to)),
                        ("accepted_at_ms", opt_int(row.accepted)),
                        ("receipted_at_ms", opt_int(receipted_at)),
                        ("latency_ms", opt_int(latency)),
                        ("record", record),
                    ];
                    fields.extend(match receipt_at {
                        Some(at) => place_of(at, ["receipt_path", "receipt_line"]),
                        None => place(multi, ["receipt_path", "receipt_line"], None),
                    });
                    fields.push(("body_bytes", row.body_bytes.clone()));
                    fields.extend(place_of(row.at, ["path", "line"]));
                    self.out.push(line("MAIL", fields));
                    self.out.extend(headers_line(&row.id, &row.headers));
                    if let (true, Some(body)) = (self.opts.bodies, &row.body) {
                        self.out.push(body_line(&row.id, body));
                    }
                    match &row.receipt {
                        Some(r) => {
                            self.receipted += 1;
                            seen.note(
                                (r.session.clone(), r.record.clone()),
                                &row.id,
                                &inputs[r.at.0].path,
                            );
                            self.record(&row.id, Source::Coordinator, &r.record);
                        }
                        None if culled.contains(&row.id) => {}
                        None => {
                            let mut fields = vec![
                                ("mail", s(&row.id)),
                                ("kind", s(row.kind)),
                                ("from", s(&row.from)),
                                ("to", s(&row.to)),
                                ("accepted_at_ms", opt_int(row.accepted)),
                            ];
                            fields.extend(place_of(row.at, ["path", "line"]));
                            self.unreceipted.push(line("UNRECEIPTED", fields));
                        }
                    }
                }
            }
        }
        self.duplicates.extend(seen.duplicates());
    }

    /// A `SNAPSHOT` record: listed whatever `--address` is, since it
    /// describes a file, not an Address.
    fn snapshot(&mut self, l: Line) {
        self.snapshots += 1;
        self.out.push(l);
    }

    /// One host daemon log. A delivery or receipt a read snapshot block
    /// restates adds nothing when this file or an earlier host daemon log
    /// has already listed it.
    fn daemon(&mut self, inputs: &[Input], index: usize) {
        let input = &inputs[index];
        let mut seen = RecordsSeen::default();
        for step in steps(inputs, index) {
            let (n, v, restated) = match step {
                Step::Snapshot(l) => {
                    self.snapshot(l);
                    continue;
                }
                Step::Line(n, v, restated) => (n, v, restated),
            };
            match v.get("ev").and_then(|x| x.as_str()) {
                Some("deliver") => {
                    if let (Ok(mail), Ok(from), Ok(to), Ok(_)) = (
                        net::field(v, "mail"),
                        net::field(v, "from"),
                        net::field(v, "to"),
                        net::field(v, "body"),
                    ) {
                        let fresh = self.delivered.insert(mail.to_string());
                        if restated && !fresh {
                            continue;
                        }
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
                        let fresh = self.daemon_receipts.insert((
                            mail.to_string(),
                            session.to_string(),
                            record.to_string(),
                        ));
                        if restated && !fresh {
                            continue;
                        }
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
                            seen.note((session.to_string(), record.to_string()), mail, &input.path);
                            self.record(mail, Source::Daemon, record);
                        }
                    }
                }
                _ => {}
            }
        }
        self.duplicates.extend(seen.duplicates());
    }

    /// A Store names no Address, so `--address` filters nothing here. A
    /// tombstone (`culled: true`, no body) is record evidence for its Mail
    /// as an entry is. An entry or tombstone a read snapshot block restates
    /// adds nothing when this file or an earlier Store has already listed
    /// that record for that Mail.
    fn store(&mut self, inputs: &[Input], index: usize) {
        let input = &inputs[index];
        let mut seen = RecordsSeen::default();
        for step in steps(inputs, index) {
            let (n, v, restated) = match step {
                Step::Snapshot(l) => {
                    self.snapshot(l);
                    continue;
                }
                Step::Line(n, v, restated) => (n, v, restated),
            };
            let (Ok(record), Ok(mail)) = (net::field(v, "record"), net::field(v, "mail")) else {
                continue;
            };
            let entry = net::field(v, "from").ok().zip(net::field(v, "body").ok());
            if entry.is_none() && v.get("culled") != Some(&Value::Bool(true)) {
                continue;
            }
            let fresh = self.stored.insert((record.to_string(), mail.to_string()));
            if restated && !fresh {
                continue;
            }
            match entry {
                Some((from, body)) => {
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
                }
                None => {
                    self.culled += 1;
                    self.out.push(line(
                        "STORE_CULLED",
                        vec![
                            ("path", s(&input.path)),
                            ("record", s(record)),
                            ("mail", s(mail)),
                            ("line", int(n)),
                        ],
                    ));
                }
            }
            seen.note((String::new(), record.to_string()), mail, &input.path);
            self.record(mail, Source::Store, record);
        }
        self.duplicates.extend(seen.duplicates());
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
                ("snapshots", int(self.snapshots)),
                ("culled", int(self.culled)),
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
        snapshots: 0,
        culled: 0,
        delivered: HashSet::new(),
        daemon_receipts: HashSet::new(),
        stored: HashSet::new(),
    };
    audit.sources(inputs);
    let of = |source: Source| -> Vec<usize> {
        (0..inputs.len())
            .filter(|&i| inputs[i].source == source)
            .collect()
    };
    // The coordinator timeline is one source of record evidence.
    let coordinators = of(Source::Coordinator);
    if let Some(&first) = coordinators.first() {
        audit.current_source = first;
        audit.coordinator(inputs, &coordinators);
    }
    for index in of(Source::Daemon) {
        audit.current_source = index;
        audit.daemon(inputs, index);
    }
    for index in of(Source::Store) {
        audit.current_source = index;
        audit.store(inputs, index);
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

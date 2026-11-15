//! Reads `roger audit --bodies --json` lines in order and checks every Mail and
//! every declared text against the roster, as docs/CONVENTIONS.md describes.
//!
//! Only the audit's own timeline is read: `MAIL` with the `HEADERS` and `BODY`
//! lines right after it, `DECLARED` and `UNDECLARED`. `POSITION`, `RECEIPT`,
//! daemon and Store lines are passed over. Addresses and Group names are
//! compared as whole strings only.

use crate::canon;
use crate::roster::Roster;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signature, VerifyingKey};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

/// Header names of the convention. The verifier reads them because reading
/// them is what the convention asks a consumer to do.
const H_SIG: &str = "sig";
const H_KID: &str = "kid";
const H_SEQ: &str = "seq";
const H_SENT: &str = "sent_at_ms";
const H_BODY: &str = "body_sha256";

/// Failure classes on a Mail.
pub const MISSING_SIGNATURE: &str = "missing_signature";
pub const MALFORMED_HEADERS: &str = "malformed_headers";
pub const UNKNOWN_SIGNER: &str = "unknown_signer";
pub const BODY_HASH_MISMATCH: &str = "body_hash_mismatch";
pub const BODY_UNAVAILABLE: &str = "body_unavailable";
pub const INVALID_SIGNATURE: &str = "invalid_signature";
pub const REPLAYED_SEQ: &str = "replayed_seq";
pub const SENDER_UNDECLARED: &str = "sender_undeclared";
pub const SENDER_DECLARATION_INVALID: &str = "sender_declaration_invalid";

/// Failure classes on a declared text.
pub const DECLARE_MALFORMED: &str = "malformed";
pub const DECLARE_UNKNOWN_SCHEME: &str = "unknown_scheme";
pub const DECLARE_ADDRESS_MISMATCH: &str = "address_mismatch";
pub const DECLARE_UNKNOWN_KEY: &str = "unknown_key";
pub const DECLARE_INVALID_SIGNATURE: &str = "invalid_signature";
pub const DECLARE_REPLAYED_NONCE: &str = "replayed_nonce";

#[derive(Clone, Copy, Default)]
pub struct Options {
    /// Skip the check that a Mail's `from` holds a verified declaration.
    pub allow_undeclared: bool,
}

/// One output record: a word and its fields in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub word: &'static str,
    pub fields: Vec<(&'static str, Value)>,
}

impl Report {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// The failure classes this record names (empty when none).
    pub fn failures(&self) -> Vec<String> {
        match self.get("failures") {
            Some(Value::Array(xs)) => xs
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        }
    }

    /// `WORD key=value ...`; arrays comma-joined, `null` as `absent`.
    pub fn text(&self) -> String {
        let mut out = String::from(self.word);
        for (k, v) in &self.fields {
            let shown = match v {
                Value::Null => "absent".to_string(),
                Value::String(s) => s.clone(),
                Value::Array(xs) if xs.is_empty() => "none".to_string(),
                Value::Array(xs) => xs
                    .iter()
                    .map(|x| x.as_str().map_or_else(|| x.to_string(), str::to_string))
                    .collect::<Vec<_>>()
                    .join(","),
                other => other.to_string(),
            };
            out.push(' ');
            out.push_str(k);
            out.push('=');
            out.push_str(&shown);
        }
        out
    }

    /// `{"type":"WORD",...}` with the same fields.
    pub fn json(&self) -> String {
        // Built by hand to keep field order (`Map` is key-ordered).
        let mut out = String::from("{\"type\":");
        out.push_str(&Value::from(self.word).to_string());
        for (k, v) in &self.fields {
            out.push(',');
            out.push_str(&Value::from(*k).to_string());
            out.push(':');
            out.push_str(&v.to_string());
        }
        out.push('}');
        out
    }
}

/// A Mail's headers as the audit listed them.
enum Headers {
    /// `HEADERS ... absent` (`headers:null`): the Mail carried no set.
    Absent,
    /// No HEADERS line: an explicitly empty set.
    Empty,
    Set(Map<String, Value>),
}

struct PendingMail {
    mail: String,
    kind: String,
    from: String,
    to: String,
    group: Option<String>,
    line: Value,
    headers: Headers,
    body: Option<String>,
}

enum DeclState {
    Verified,
    Failed,
    Cleared,
}

pub struct Verifier<'a> {
    roster: &'a Roster,
    opts: Options,
    pending: Option<PendingMail>,
    declared: HashMap<String, DeclState>,
    nonces: HashMap<String, (String, Value)>,
    /// Per `from`: seq -> the Mail id (Broadcast copies and multi-reader Group
    /// rows share one).
    seqs: BTreeMap<String, BTreeMap<u64, String>>,
    pub out: Vec<Report>,
    mails: usize,
    mails_failed: usize,
    decls: usize,
    decls_failed: usize,
    gaps: usize,
}

fn str_field<'v>(v: &'v Value, key: &str) -> Result<&'v str, String> {
    v.get(key).and_then(Value::as_str).ok_or_else(|| {
        format!(
            "expected a string {key} on a {} line, saw {}",
            v.get("type").map_or("untyped".into(), Value::to_string),
            v.get(key).map_or("none".into(), Value::to_string)
        )
    })
}

fn opt_str_field<'v>(v: &'v Value, key: &str) -> Result<Option<&'v str>, String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.as_str())),
        Some(other) => Err(format!(
            "expected a string {key} on a {} line, saw {other}",
            v.get("type").map_or("untyped".into(), Value::to_string)
        )),
    }
}

fn check_opt_int_field(v: &Value, key: &str) -> Result<(), String> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Number(n)) if n.is_i64() || n.is_u64() => Ok(()),
        Some(other) => Err(format!(
            "expected an integer {key} on a {} line, saw {other}",
            v.get("type").map_or("untyped".into(), Value::to_string)
        )),
    }
}

/// Canonical `(mail_id, to)` for `roger-sig/1`:
/// - `broadcast`: `<mail>/<to>` strips `/<to>`, and `to` is `*`.
/// - `group`: `mail` verbatim, and `to` is the opaque Group name (`MAIL.group`
///   when present and non-empty, otherwise `MAIL.to`).
/// - `directed` (and any other kind): `mail` and `to` verbatim.
fn signed_target(kind: &str, mail: &str, to: &str, group: Option<&str>) -> (String, String) {
    match kind {
        "broadcast" => {
            let suffix = format!("/{to}");
            (
                mail.strip_suffix(&suffix).unwrap_or(mail).to_string(),
                "*".to_string(),
            )
        }
        "group" => (
            mail.to_string(),
            group.filter(|g| !g.is_empty()).unwrap_or(to).to_string(),
        ),
        _ => (mail.to_string(), to.to_string()),
    }
}

fn decimal(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn signature(text: &str) -> Option<Signature> {
    let bytes = STANDARD.decode(text).ok()?;
    let arr: [u8; 64] = bytes.as_slice().try_into().ok()?;
    Some(Signature::from_bytes(&arr))
}

fn verifies(key: &VerifyingKey, msg: &[u8], sig: &Signature) -> bool {
    key.verify_strict(msg, sig).is_ok()
}

fn failures_value(f: &[&'static str]) -> Value {
    Value::Array(f.iter().map(|x| Value::from(*x)).collect())
}

impl<'a> Verifier<'a> {
    pub fn new(roster: &'a Roster, opts: Options) -> Self {
        Verifier {
            roster,
            opts,
            pending: None,
            declared: HashMap::new(),
            nonces: HashMap::new(),
            seqs: BTreeMap::new(),
            out: Vec::new(),
            mails: 0,
            mails_failed: 0,
            decls: 0,
            decls_failed: 0,
            gaps: 0,
        }
    }

    /// One audit JSON line, in the audit's order.
    pub fn feed(&mut self, v: &Value) -> Result<(), String> {
        let word = v
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("expected an audit JSON object with a type, saw {v}"))?;
        let attaches = match (&self.pending, word) {
            (Some(p), "HEADERS" | "BODY") => v.get("mail").and_then(Value::as_str) == Some(&p.mail),
            _ => false,
        };
        if attaches {
            let p = self.pending.as_mut().expect("pending checked above");
            if word == "HEADERS" {
                p.headers = match v.get("headers") {
                    Some(Value::Object(m)) => Headers::Set(m.clone()),
                    _ => Headers::Absent,
                };
            } else {
                p.body = Some(str_field(v, "body")?.to_string());
            }
            return Ok(());
        }
        self.flush();
        match word {
            "MAIL" => {
                let mail = str_field(v, "mail")?.to_string();
                let kind = str_field(v, "kind")?.to_string();
                let from = str_field(v, "from")?.to_string();
                let group = opt_str_field(v, "group")?.map(str::to_string);
                check_opt_int_field(v, "article")?;
                let to = match opt_str_field(v, "to")? {
                    Some(t) => t.to_string(),
                    None if kind == "group" => group
                        .clone()
                        .ok_or_else(|| str_field(v, "to").unwrap_err())?,
                    None => str_field(v, "to")?.to_string(),
                };
                self.pending = Some(PendingMail {
                    mail,
                    kind,
                    from,
                    to,
                    group,
                    line: v.get("line").cloned().unwrap_or(Value::Null),
                    headers: Headers::Empty,
                    body: None,
                });
            }
            "DECLARED" => {
                let address = str_field(v, "address")?.to_string();
                let text = str_field(v, "declared")?.to_string();
                let line = v.get("line").cloned().unwrap_or(Value::Null);
                self.declaration(&address, &text, line);
            }
            "UNDECLARED" => {
                let address = str_field(v, "address")?.to_string();
                self.declared.insert(address.clone(), DeclState::Cleared);
                self.out.push(Report {
                    word: "DECLARATION",
                    fields: vec![
                        ("address", Value::from(address)),
                        ("line", v.get("line").cloned().unwrap_or(Value::Null)),
                        ("status", Value::from("cleared")),
                        ("failures", Value::Array(Vec::new())),
                    ],
                });
            }
            // POSITION, RECEIPT, STORE, daemon and summary records outside MAIL -> HEADERS -> BODY.
            _ => {}
        }
        Ok(())
    }

    fn declaration(&mut self, address: &str, text: &str, line: Value) {
        self.decls += 1;
        let mut failures: Vec<&'static str> = Vec::new();
        let mut detail: Vec<String> = Vec::new();
        let mut kid_v = Value::Null;
        let mut nonce_v = Value::Null;
        match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(obj)) => {
                let s = |k: &str| obj.get(k).and_then(Value::as_str);
                kid_v = s("kid").map_or(Value::Null, Value::from);
                nonce_v = s("nonce").map_or(Value::Null, Value::from);
                let form = canon::declare_form(text);
                if let Err(e) = &form {
                    push(&mut failures, DECLARE_MALFORMED);
                    detail.push(e.clone());
                }
                for (k, ok) in [
                    ("scheme", s("scheme").is_some()),
                    ("address", s("address").is_some()),
                    ("facts", obj.get("facts").is_some_and(Value::is_object)),
                    ("nonce", s("nonce").is_some()),
                    (
                        "issued_at_ms",
                        obj.get("issued_at_ms")
                            .and_then(Value::as_number)
                            .is_some_and(|n| n.is_u64() || n.is_i64()),
                    ),
                    ("kid", s("kid").is_some()),
                    ("sig", s("sig").is_some()),
                ] {
                    if !ok {
                        push(&mut failures, DECLARE_MALFORMED);
                        detail.push(format!(
                            "expected field {k} of the right type, saw {}",
                            obj.get(k).map_or("none".into(), Value::to_string)
                        ));
                    }
                }
                if let Some(scheme) = s("scheme") {
                    if scheme != canon::DECLARE_SCHEME {
                        failures.push(DECLARE_UNKNOWN_SCHEME);
                        detail.push(format!(
                            "expected scheme {}, saw {scheme}",
                            canon::DECLARE_SCHEME
                        ));
                    }
                }
                if let Some(a) = s("address") {
                    if a != address {
                        failures.push(DECLARE_ADDRESS_MISMATCH);
                        detail.push(format!("expected address {address}, saw {a}"));
                    }
                }
                if let Some(nonce) = s("nonce") {
                    match self.nonces.get(nonce) {
                        Some((a, l)) => {
                            failures.push(DECLARE_REPLAYED_NONCE);
                            detail.push(format!(
                                "expected a fresh nonce, saw the one declared for {a} at line {l}"
                            ));
                        }
                        None => {
                            self.nonces
                                .insert(nonce.to_string(), (address.to_string(), line.clone()));
                        }
                    }
                }
                if let (Some(kid), Some(sig)) = (s("kid"), s("sig")) {
                    match self.roster.authorities.get(kid) {
                        None => {
                            failures.push(DECLARE_UNKNOWN_KEY);
                            detail.push(format!("expected kid of a roster authority, saw {kid}"));
                        }
                        Some(key) => {
                            // A text without a canonical form is already `malformed`.
                            let ok = form.as_ref().map_or(true, |f| {
                                signature(sig).is_some_and(|g| verifies(key, f, &g))
                            });
                            if !ok {
                                failures.push(DECLARE_INVALID_SIGNATURE);
                                detail.push(format!(
                                    "expected an Ed25519 signature by {kid} over the canonical form"
                                ));
                            }
                        }
                    }
                }
            }
            Ok(other) => {
                failures.push(DECLARE_MALFORMED);
                detail.push(format!("expected a JSON object, saw {other}"));
            }
            Err(e) => {
                failures.push(DECLARE_MALFORMED);
                detail.push(format!("expected a JSON object, saw {e}"));
            }
        }
        let ok = failures.is_empty();
        if !ok {
            self.decls_failed += 1;
        }
        self.declared.insert(
            address.to_string(),
            if ok {
                DeclState::Verified
            } else {
                DeclState::Failed
            },
        );
        self.out.push(Report {
            word: "DECLARATION",
            fields: vec![
                ("address", Value::from(address)),
                ("line", line),
                ("kid", kid_v),
                ("nonce", nonce_v),
                (
                    "status",
                    Value::from(if ok { "verified" } else { "failed" }),
                ),
                ("failures", failures_value(&failures)),
                ("detail", Value::from(detail.join("; "))),
            ],
        });
    }

    fn flush(&mut self) {
        let Some(p) = self.pending.take() else {
            return;
        };
        self.mails += 1;
        let mut failures: Vec<&'static str> = Vec::new();
        let mut detail: Vec<String> = Vec::new();
        let mut seq_v = Value::Null;
        let mut kid_v = Value::Null;

        if !self.opts.allow_undeclared {
            match self.declared.get(&p.from) {
                Some(DeclState::Verified) => {}
                Some(DeclState::Failed) => {
                    failures.push(SENDER_DECLARATION_INVALID);
                    detail.push(format!(
                        "expected {} to hold a verified declaration, saw a failed one",
                        p.from
                    ));
                }
                Some(DeclState::Cleared) | None => {
                    failures.push(SENDER_UNDECLARED);
                    detail.push(format!(
                        "expected {} to hold a declaration, saw none",
                        p.from
                    ));
                }
            }
        }

        let (mail_id, to) = signed_target(&p.kind, &p.mail, &p.to, p.group.as_deref());

        let headers = match &p.headers {
            Headers::Set(m) => Some(m),
            Headers::Absent | Headers::Empty => None,
        };
        let h = |k: &str| headers.and_then(|m| m.get(k));
        match h(H_SIG) {
            None => {
                failures.push(MISSING_SIGNATURE);
                detail.push("expected a sig header, saw none".to_string());
            }
            Some(_) => {
                let mut parts: HashMap<&str, &str> = HashMap::new();
                for k in [H_SIG, H_KID, H_SEQ, H_SENT, H_BODY] {
                    match h(k).map(|x| x.as_str()) {
                        Some(Some(s)) => {
                            parts.insert(k, s);
                        }
                        Some(None) => {
                            push(&mut failures, MALFORMED_HEADERS);
                            detail.push(format!("expected header {k} to be a string"));
                        }
                        None => {
                            push(&mut failures, MALFORMED_HEADERS);
                            detail.push(format!("expected header {k}, saw none"));
                        }
                    }
                }
                kid_v = parts.get(H_KID).map_or(Value::Null, |s| Value::from(*s));
                seq_v = parts.get(H_SEQ).map_or(Value::Null, |s| Value::from(*s));
                let seq_n = parts
                    .get(H_SEQ)
                    .filter(|s| decimal(s))
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|n| *n >= 1);
                if let Some(s) = parts.get(H_SEQ) {
                    if seq_n.is_none() {
                        push(&mut failures, MALFORMED_HEADERS);
                        detail.push(format!("expected seq a decimal from 1, saw {s}"));
                    }
                }
                if let Some(s) = parts.get(H_SENT) {
                    if !decimal(s) {
                        push(&mut failures, MALFORMED_HEADERS);
                        detail.push(format!("expected sent_at_ms decimal, saw {s}"));
                    }
                }
                if let Some(s) = parts.get(H_BODY) {
                    if !(s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())) {
                        push(&mut failures, MALFORMED_HEADERS);
                        detail.push(format!("expected body_sha256 64 hex digits, saw {s}"));
                    }
                }
                let sig = parts.get(H_SIG).and_then(|s| signature(s));
                if parts.contains_key(H_SIG) && sig.is_none() {
                    push(&mut failures, MALFORMED_HEADERS);
                    detail.push("expected sig base64 of 64 bytes".to_string());
                }

                // Body integrity, independent of the signature.
                if let Some(claimed) = parts.get(H_BODY) {
                    match &p.body {
                        Some(body) => {
                            let actual = canon::sha256_hex(body.as_bytes());
                            if !claimed.eq_ignore_ascii_case(&actual) {
                                failures.push(BODY_HASH_MISMATCH);
                                detail
                                    .push(format!("expected body_sha256 {actual}, saw {claimed}"));
                            }
                        }
                        None => {
                            failures.push(BODY_UNAVAILABLE);
                            detail.push(
                                "expected a BODY line (roger audit --bodies), saw none".to_string(),
                            );
                        }
                    }
                }

                // Key: the roster's, for this exact `from`, under this kid.
                let key = parts.get(H_KID).and_then(|kid| {
                    let key = self.roster.signers.get(&p.from).and_then(|ks| ks.get(*kid));
                    if key.is_none() {
                        failures.push(UNKNOWN_SIGNER);
                        detail.push(format!(
                            "expected a roster key {kid} for {}, saw none",
                            p.from
                        ));
                    }
                    key
                });

                if let (Some(key), Some(sig), Some(seq), Some(sent), Some(bh)) = (
                    key,
                    sig,
                    parts.get(H_SEQ),
                    parts.get(H_SENT),
                    parts.get(H_BODY),
                ) {
                    let form = canon::sig_form(&p.from, &to, &mail_id, seq, sent, bh);
                    if verifies(key, &form, &sig) {
                        if let Some(n) = seq_n {
                            let seen = self.seqs.entry(p.from.clone()).or_default();
                            match seen.get(&n) {
                                Some(prior) if *prior != mail_id => {
                                    failures.push(REPLAYED_SEQ);
                                    detail.push(format!(
                                        "expected a fresh seq from {}, saw {n} again (first on {prior})",
                                        p.from
                                    ));
                                }
                                Some(_) => {}
                                None => {
                                    seen.insert(n, mail_id.clone());
                                }
                            }
                        }
                    } else {
                        failures.push(INVALID_SIGNATURE);
                        detail.push(format!(
                            "expected an Ed25519 signature by {} over the roger-sig/1 form",
                            kid_v.as_str().unwrap_or("")
                        ));
                    }
                }
            }
        }

        let ok = failures.is_empty();
        if !ok {
            self.mails_failed += 1;
        }
        self.out.push(Report {
            word: "MAIL",
            fields: vec![
                ("mail", Value::from(p.mail)),
                ("kind", Value::from(p.kind)),
                ("from", Value::from(p.from)),
                ("to", Value::from(p.to)),
                ("line", p.line),
                ("kid", kid_v),
                ("seq", seq_v),
                (
                    "status",
                    Value::from(if ok { "verified" } else { "failed" }),
                ),
                ("failures", failures_value(&failures)),
                ("detail", Value::from(detail.join("; "))),
            ],
        });
    }

    /// Close the input: the last Mail, sequence gaps per signer, the summary.
    /// Returns the exit status (0 when nothing failed, 1 otherwise).
    pub fn finish(&mut self) -> u8 {
        self.flush();
        let mut gaps = Vec::new();
        for (from, seen) in &self.seqs {
            let mut expect = 1u64;
            for &n in seen.keys() {
                if n > expect {
                    gaps.push(Report {
                        word: "SEQ_GAP",
                        fields: vec![
                            ("from", Value::from(from.as_str())),
                            ("missing_first", json!(expect)),
                            ("missing_last", json!(n - 1)),
                        ],
                    });
                }
                expect = n.saturating_add(1);
            }
        }
        self.gaps = gaps.len();
        self.out.extend(gaps);
        let failed = self.mails_failed + self.decls_failed + self.gaps;
        let status = u8::from(failed > 0);
        self.out.push(Report {
            word: "VERIFY",
            fields: vec![
                ("mails", json!(self.mails)),
                ("mails_verified", json!(self.mails - self.mails_failed)),
                ("mails_failed", json!(self.mails_failed)),
                ("declarations", json!(self.decls)),
                (
                    "declarations_verified",
                    json!(self.decls - self.decls_failed),
                ),
                ("declarations_failed", json!(self.decls_failed)),
                ("seq_gaps", json!(self.gaps)),
                ("exit", json!(status)),
            ],
        });
        status
    }
}

fn push(f: &mut Vec<&'static str>, class: &'static str) {
    if !f.contains(&class) {
        f.push(class);
    }
}

/// Verify a whole audit JSON text. Blank lines are ignored; any other line
/// that is not a JSON object is an input error.
pub fn verify_text(
    text: &str,
    roster: &Roster,
    opts: Options,
) -> Result<(Vec<Report>, u8), String> {
    let mut v = Verifier::new(roster, opts);
    for (i, raw) in text.lines().enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(raw).map_err(|e| {
            format!(
                "expected roger audit --json output, saw line {} that is not JSON: {e}",
                i + 1
            )
        })?;
        v.feed(&value)
            .map_err(|e| format!("audit line {}: {e}", i + 1))?;
    }
    let status = v.finish();
    Ok((v.out, status))
}

//! Builds signed coordinator logs and a Store as fixtures, lists them with the
//! real core `roger audit --bodies --json` (built from the repository root),
//! and runs the real `roger-verify` binary over that output.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use roger_verify::canon;
use serde_json::{json, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;

const VERIFY_BIN: &str = env!("CARGO_BIN_EXE_roger-verify");
const A: &str = "[[1:LEAD:0]]9e2a0000-0000-4000-8000-00000000000a";
const B: &str = "[[3:BUILDER:1]]3b0d5b6e-7e3d-4a0e-9b1c-2a7f1c9d0e11";
const C: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const MALLORY: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";

/// `cargo build` at the repository root once; the binary is `target/debug/roger`.
fn roger_bin() -> &'static Path {
    static ROGER: OnceLock<PathBuf> = OnceLock::new();
    ROGER.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("modules/verify lives two levels under the repository root")
            .to_path_buf();
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("target"));
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .status()
            .expect("failed to invoke cargo build for roger");
        assert!(status.success(), "building roger failed");
        let bin = target_dir
            .join("debug")
            .join(format!("roger{}", std::env::consts::EXE_SUFFIX));
        assert!(bin.exists(), "expected built roger at {}", bin.display());
        bin
    })
}

fn tmpdir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("roger-verify-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sk(n: u8) -> SigningKey {
    SigningKey::from_bytes(&[n; 32])
}

fn pk(n: u8) -> String {
    STANDARD.encode(sk(n).verifying_key().as_bytes())
}

/// The authority (key 1) signs declarations; A, B and C sign Mail with keys
/// 2, 3, 4. MALLORY is not in the roster.
fn write_roster(dir: &Path) -> PathBuf {
    let path = dir.join("roster.json");
    let roster = json!({
        "scheme": "roger-roster/1",
        "authorities": {"authority-2026": pk(1)},
        "signers": {A: {"a-1": pk(2)}, B: {"b-1": pk(3)}, C: {"c-1": pk(4)}},
    });
    std::fs::write(&path, roster.to_string()).unwrap();
    path
}

fn declare(address: &str, nonce: &str, role: &str) -> String {
    let mut v = json!({
        "scheme": "roger-declare/1",
        "address": address,
        "facts": {"parent": A, "role": role, "launch": "L-4417"},
        "nonce": nonce,
        "issued_at_ms": 1793545146038u64,
        "kid": "authority-2026",
    });
    let form = canon::declare_form(&v.to_string()).unwrap();
    v["sig"] = Value::from(STANDARD.encode(sk(1).sign(&form).to_bytes()));
    v.to_string()
}

#[allow(clippy::too_many_arguments)]
fn headers(key: u8, kid: &str, from: &str, to: &str, mail: &str, seq: u64, body: &str) -> Value {
    let bh = canon::sha256_hex(body.as_bytes());
    let sent = (1793545147000 + seq).to_string();
    let form = canon::sig_form(from, to, mail, &seq.to_string(), &sent, &bh);
    json!({
        "sent_at_ms": sent,
        "seq": seq.to_string(),
        "body_sha256": bh,
        "kid": kid,
        "sig": STANDARD.encode(sk(key).sign(&form).to_bytes()),
    })
}

/// A coordinator log in the core's own line format.
struct Log {
    lines: Vec<Value>,
    at: i64,
}

impl Log {
    fn new() -> Self {
        Log {
            lines: Vec::new(),
            at: 1793545146000,
        }
    }

    fn tick(&mut self) -> i64 {
        self.at += 3;
        self.at
    }

    fn register(&mut self, address: &str, declared: Option<&str>) {
        let at = self.tick();
        self.lines
            .push(json!({"ev":"session","session":address,"host":"h1","at_ms":at}));
        let mut ev = json!({"ev":"declared","session":address});
        if let Some(d) = declared {
            ev["declared"] = Value::from(d);
        }
        ev["at_ms"] = Value::from(at);
        self.lines.push(ev);
    }

    fn mail(&mut self, id: &str, from: &str, to: &str, body: &str, headers: Value) {
        let at = self.tick();
        self.lines.push(
            json!({"ev":"mail","mail":id,"from":from,"to":to,"body":body,
            "headers":headers,"accepted_at_ms":at}),
        );
        let at = self.tick();
        self.lines
            .push(json!({"ev":"receipt","mail":id,"session":to,
            "record":format!("r-{id}"),"receipted_at_ms":at}));
    }

    fn broadcast(&mut self, id: &str, from: &str, to: &[&str], body: &str, headers: Value) {
        let at = self.tick();
        self.lines
            .push(json!({"ev":"broadcast","mail":id,"from":from,"body":body,
            "to":to,"headers":headers,"accepted_at_ms":at}));
        for t in to {
            let at = self.tick();
            self.lines
                .push(json!({"ev":"receipt","mail":format!("{id}/{t}"),
                "session":t,"record":format!("r-{id}-{t}"),"receipted_at_ms":at}));
        }
    }

    fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join("coordinator.log");
        let text: String = self.lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(&path, text).unwrap();
        path
    }
}

fn audit(args: &[&str]) -> Vec<u8> {
    let out = Command::new(roger_bin())
        .arg("audit")
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "roger audit failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn verify(roster: &Path, extra: &[&str], stdin: &[u8]) -> Output {
    let mut child = Command::new(VERIFY_BIN)
        .arg("--roster")
        .arg(roster)
        .args(extra)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

fn records(out: &Output) -> Vec<Value> {
    String::from_utf8(out.stdout.clone())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn mail<'v>(rs: &'v [Value], id: &str) -> &'v Value {
    rs.iter()
        .find(|r| r["type"] == "MAIL" && r["mail"] == id)
        .unwrap_or_else(|| panic!("no MAIL {id} in {rs:?}"))
}

fn failures(r: &Value) -> Vec<&str> {
    r["failures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap())
        .collect()
}

/// A, B, C registered with authority-signed declarations.
fn fleet() -> Log {
    let mut log = Log::new();
    log.register(A, Some(&declare(A, "nonce-a-1", "lead")));
    log.register(B, Some(&declare(B, "nonce-b-1", "builder")));
    log.register(C, Some(&declare(C, "nonce-c-1", "builder")));
    log
}

#[test]
fn a_signed_fleet_verifies_end_to_end() {
    let dir = tmpdir("good");
    let roster = write_roster(&dir);
    let mut log = fleet();
    let body = "multi-line\nbody with \"quotes\" and é";
    log.mail("m1", A, B, body, headers(2, "a-1", A, B, "m1", 1, body));
    log.mail("m2", B, A, "ack", headers(3, "b-1", B, A, "m2", 1, "ack"));
    log.broadcast(
        "all-1",
        A,
        &[A, B, C],
        "to everyone",
        headers(2, "a-1", A, "*", "all-1", 2, "to everyone"),
    );
    log.mail("m3", A, C, "", headers(2, "a-1", A, C, "m3", 3, ""));
    // A Register with the same declared text again commits no new line in
    // the core; a repeated session line alone changes nothing here.
    log.lines
        .push(json!({"ev":"session","session":B,"host":"h2","at_ms":1}));
    let coord = log.write(&dir);
    let store = dir.join("b.store");
    std::fs::write(
        &store,
        format!(
            "{}\n",
            json!({"record":"r-m1","mail":"m1","from":A,"body":body,"kind":"directed",
                   "headers":headers(2, "a-1", A, B, "m1", 1, body)})
        ),
    )
    .unwrap();

    let listing = audit(&[
        "--coordinator-log",
        coord.to_str().unwrap(),
        "--store",
        store.to_str().unwrap(),
        "--bodies",
        "--json",
    ]);
    let out = verify(&roster, &["--json"], &listing);
    let rs = records(&out);
    assert_eq!(out.status.code(), Some(0), "{rs:#?}");
    let mails: Vec<_> = rs.iter().filter(|r| r["type"] == "MAIL").collect();
    assert_eq!(mails.len(), 6, "m1, m2, three broadcast copies, m3");
    assert!(mails.iter().all(|m| m["status"] == "verified"));
    let decls: Vec<_> = rs.iter().filter(|r| r["type"] == "DECLARATION").collect();
    assert_eq!(decls.len(), 3);
    assert!(decls.iter().all(|d| d["status"] == "verified"));
    assert!(!rs.iter().any(|r| r["type"] == "SEQ_GAP"));
    let summary = rs.last().unwrap();
    assert_eq!(summary["type"], "VERIFY");
    assert_eq!(summary["mails_verified"], 6);
    assert_eq!(summary["exit"], 0);

    // Text output, from a file instead of stdin.
    let audit_file = dir.join("audit.jsonl");
    std::fs::write(&audit_file, &listing).unwrap();
    let out = verify(&roster, &["--audit", audit_file.to_str().unwrap()], b"");
    assert_eq!(out.status.code(), Some(0));
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains(&format!(
        "MAIL mail=all-1/{C} kind=broadcast from={A} to={C}"
    )));
    assert!(text.lines().last().unwrap().starts_with("VERIFY mails=6 "));

    // Without --bodies the hashes cannot be checked.
    let listing = audit(&["--coordinator-log", coord.to_str().unwrap(), "--json"]);
    let out = verify(&roster, &["--json"], &listing);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        failures(mail(&records(&out), "m1")),
        vec!["body_unavailable"]
    );
}

#[test]
fn every_failure_class_is_reported_from_a_real_audit() {
    let dir = tmpdir("bad");
    let roster = write_roster(&dir);
    let mut log = fleet();
    // seq 1 fine.
    log.mail(
        "ok-1",
        A,
        B,
        "one",
        headers(2, "a-1", A, B, "ok-1", 1, "one"),
    );
    // Body changed after signing.
    log.mail(
        "tampered",
        A,
        B,
        "two!",
        headers(2, "a-1", A, B, "tampered", 2, "two"),
    );
    // Signed for ok-1, replayed under a new id.
    log.mail(
        "replayed-id",
        A,
        B,
        "one",
        headers(2, "a-1", A, B, "ok-1", 1, "one"),
    );
    // A second Mail signed with seq 2: two processes sending as A.
    log.mail(
        "dup-seq",
        A,
        B,
        "dup",
        headers(2, "a-1", A, B, "dup-seq", 2, "dup"),
    );
    // seq 5: 3 and 4 never arrive.
    log.mail(
        "after-gap",
        A,
        B,
        "five",
        headers(2, "a-1", A, B, "after-gap", 5, "five"),
    );
    // No headers at all.
    log.mail("unsigned", A, B, "plain", json!({}));
    // A key the roster does not hold for A.
    log.mail(
        "wrong-kid",
        A,
        B,
        "k",
        headers(3, "b-1", A, B, "wrong-kid", 6, "k"),
    );
    // MALLORY self-declares with a forged signature and signs with its own key.
    log.register(
        MALLORY,
        Some(&declare(MALLORY, "nonce-m", "lead").replace("lead", "root")),
    );
    log.mail(
        "from-mallory",
        MALLORY,
        B,
        "hi",
        headers(9, "m-1", MALLORY, B, "from-mallory", 1, "hi"),
    );
    // C re-registers replaying A's declaration nonce, then clears its declaration.
    log.register(C, Some(&declare(C, "nonce-a-1", "builder")));
    log.register(C, None);
    log.mail(
        "from-cleared",
        C,
        B,
        "c",
        headers(4, "c-1", C, B, "from-cleared", 1, "c"),
    );
    let coord = log.write(&dir);

    let listing = audit(&[
        "--coordinator-log",
        coord.to_str().unwrap(),
        "--bodies",
        "--json",
    ]);
    let out = verify(&roster, &["--json"], &listing);
    let rs = records(&out);
    assert_eq!(out.status.code(), Some(1), "{rs:#?}");

    assert!(failures(mail(&rs, "ok-1")).is_empty());
    assert_eq!(failures(mail(&rs, "tampered")), vec!["body_hash_mismatch"]);
    assert_eq!(
        failures(mail(&rs, "replayed-id")),
        vec!["invalid_signature"]
    );
    assert_eq!(failures(mail(&rs, "dup-seq")), vec!["replayed_seq"]);
    assert!(failures(mail(&rs, "after-gap")).is_empty());
    assert_eq!(failures(mail(&rs, "unsigned")), vec!["missing_signature"]);
    assert_eq!(failures(mail(&rs, "wrong-kid")), vec!["unknown_signer"]);
    assert_eq!(
        failures(mail(&rs, "from-mallory")),
        vec!["sender_declaration_invalid", "unknown_signer"]
    );
    assert_eq!(
        failures(mail(&rs, "from-cleared")),
        vec!["sender_undeclared"]
    );

    let decls: Vec<_> = rs.iter().filter(|r| r["type"] == "DECLARATION").collect();
    let of = |addr: &str| -> Vec<&Value> {
        decls
            .iter()
            .copied()
            .filter(|d| d["address"] == addr)
            .collect()
    };
    assert_eq!(failures(of(MALLORY)[0]), vec!["invalid_signature"]);
    assert_eq!(failures(of(C)[1]), vec!["replayed_nonce"]);
    assert_eq!(of(C)[2]["status"], "cleared");

    let gaps: Vec<_> = rs.iter().filter(|r| r["type"] == "SEQ_GAP").collect();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert_eq!(gaps[0]["from"], A);
    assert_eq!(gaps[0]["missing_first"], 3);
    assert_eq!(gaps[0]["missing_last"], 4);

    // --allow-undeclared drops only the declaration requirement.
    let out = verify(&roster, &["--json", "--allow-undeclared"], &listing);
    assert!(failures(mail(&records(&out), "from-cleared")).is_empty());
}

#[test]
fn usage_and_input_errors_exit_two() {
    let dir = tmpdir("usage");
    let roster = write_roster(&dir);
    let out = Command::new(VERIFY_BIN).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let bad = dir.join("bad-roster.json");
    std::fs::write(&bad, "{}").unwrap();
    assert_eq!(verify(&bad, &[], b"").status.code(), Some(2));
    // Text audit output is not the JSON this reads.
    let out = verify(&roster, &[], b"AUDIT sources=1\n");
    assert_eq!(out.status.code(), Some(2));
    let help = Command::new(VERIFY_BIN).arg("--help").output().unwrap();
    assert_eq!(help.status.code(), Some(0));
}

#[test]
fn group_articles_positions_and_multi_reader_audits_verify_and_catch_tampering() {
    let dir = tmpdir("groups");
    let roster = write_roster(&dir);
    let mut log = fleet();
    log.mail(
        "d-1",
        A,
        B,
        "hello",
        headers(2, "a-1", A, B, "d-1", 1, "hello"),
    );
    let coord = log.write(&dir);

    // Start from real `roger audit --bodies --json` output (declarations + seq 1 directed mail),
    // and append Group articles, multi-reader rows, POSITION records, and Group STORE records.
    let base_listing = String::from_utf8(audit(&[
        "--coordinator-log",
        coord.to_str().unwrap(),
        "--bodies",
        "--json",
    ]))
    .unwrap();
    let mut valid_lines: Vec<Value> = base_listing
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|v| v["type"] != "AUDIT")
        .collect();

    let g1_headers = headers(2, "a-1", A, "desk.intake", "g-art-1", 2, "dispatch one");
    valid_lines.extend([
        json!({
            "type": "POSITION",
            "group": "desk.intake",
            "session": B,
            "subscribed": true,
            "position": 1,
            "record": "r-b-g1",
            "receipted_at_ms": 1793545147010i64
        }),
        // Reader B's audit row (`to` = Group name, `article` = 1).
        json!({
            "type": "MAIL",
            "mail": "g-art-1",
            "kind": "group",
            "from": A,
            "to": "desk.intake",
            "article": 1,
            "accepted_at_ms": 1793545147002i64,
            "receipted_at_ms": 1793545147010i64,
            "latency_ms": 8,
            "record": "r-b-g1",
            "receipt_line": 12,
            "body_bytes": 12,
            "line": 9
        }),
        json!({"type": "HEADERS", "mail": "g-art-1", "headers": g1_headers.clone()}),
        json!({"type": "BODY", "mail": "g-art-1", "body": "dispatch one"}),
        // Reader C's concatenated audit row for the same `(from, mail_id, seq)`.
        json!({
            "type": "MAIL",
            "mail": "g-art-1",
            "kind": "group",
            "from": A,
            "to": C,
            "group": "desk.intake",
            "article": 1,
            "accepted_at_ms": 1793545147002i64,
            "receipted_at_ms": 1793545147014i64,
            "latency_ms": 12,
            "record": "r-c-g1",
            "receipt_line": 13,
            "body_bytes": 12,
            "line": 9
        }),
        json!({"type": "HEADERS", "mail": "g-art-1", "headers": g1_headers}),
        json!({"type": "BODY", "mail": "g-art-1", "body": "dispatch one"}),
        json!({
            "type": "POSITION",
            "group": "desk.intake",
            "session": C,
            "subscribed": true,
            "position": 1,
            "record": "r-c-g1",
            "receipted_at_ms": 1793545147014i64
        }),
        // Second Group article (`seq` = 3).
        json!({
            "type": "MAIL",
            "mail": "g-art-2",
            "kind": "group",
            "from": A,
            "to": "desk.intake",
            "group": "desk.intake",
            "article": 2,
            "accepted_at_ms": 1793545147020i64,
            "receipted_at_ms": 1793545147025i64,
            "latency_ms": 5,
            "record": "r-b-g2",
            "receipt_line": 15,
            "body_bytes": 12,
            "line": 14
        }),
        json!({
            "type": "HEADERS",
            "mail": "g-art-2",
            "headers": headers(2, "a-1", A, "desk.intake", "g-art-2", 3, "dispatch two")
        }),
        json!({"type": "BODY", "mail": "g-art-2", "body": "dispatch two"}),
        json!({
            "type": "STORE",
            "path": "b.store",
            "record": "r-b-g1",
            "mail": "g-art-1",
            "from": A,
            "kind": "group",
            "group": "desk.intake",
            "article": 1,
            "line": 1
        }),
        json!({"type": "AUDIT", "sources": 1, "mails": 4}),
    ]);

    let valid_text: String = valid_lines.iter().map(|v| format!("{v}\n")).collect();
    let out = verify(&roster, &["--json"], valid_text.as_bytes());
    let rs = records(&out);
    assert_eq!(out.status.code(), Some(0), "{rs:#?}");
    let mail_rs: Vec<_> = rs.iter().filter(|r| r["type"] == "MAIL").collect();
    assert_eq!(mail_rs.len(), 4);
    assert!(mail_rs.iter().all(|r| r["status"] == "verified"));
    assert!(!rs.iter().any(|r| r["type"] == "SEQ_GAP"));

    // Text mode via `--audit <file>` also succeeds.
    let audit_path = dir.join("group-audit.jsonl");
    std::fs::write(&audit_path, &valid_text).unwrap();
    let text_out = verify(&roster, &["--audit", audit_path.to_str().unwrap()], b"");
    assert_eq!(text_out.status.code(), Some(0));
    let rendered = String::from_utf8(text_out.stdout).unwrap();
    assert!(rendered.contains(&format!(
        "MAIL mail=g-art-1 kind=group from={A} to=desk.intake"
    )));
    assert!(rendered
        .lines()
        .last()
        .unwrap()
        .starts_with("VERIFY mails=4 mails_verified=4 mails_failed=0 "));

    // Failure cases: tampered group name, tampered body, and distinct mail_id reusing seq 2.
    let mut bad_lines = valid_lines;
    bad_lines.pop(); // drop trailing AUDIT summary before appending
    bad_lines.extend([
        json!({
            "type": "MAIL",
            "mail": "g-tampered-group",
            "kind": "group",
            "from": A,
            "to": "wrong.group",
            "article": 3,
            "line": 20
        }),
        json!({
            "type": "HEADERS",
            "mail": "g-tampered-group",
            "headers": headers(2, "a-1", A, "desk.intake", "g-tampered-group", 4, "clean")
        }),
        json!({"type": "BODY", "mail": "g-tampered-group", "body": "clean"}),
        json!({
            "type": "MAIL",
            "mail": "g-tampered-body",
            "kind": "group",
            "from": A,
            "to": "desk.intake",
            "article": 4,
            "line": 21
        }),
        json!({
            "type": "HEADERS",
            "mail": "g-tampered-body",
            "headers": headers(2, "a-1", A, "desk.intake", "g-tampered-body", 5, "clean")
        }),
        json!({"type": "BODY", "mail": "g-tampered-body", "body": "altered"}),
        json!({
            "type": "MAIL",
            "mail": "g-replayed-seq",
            "kind": "group",
            "from": A,
            "to": "desk.intake",
            "article": 5,
            "line": 22
        }),
        json!({
            "type": "HEADERS",
            "mail": "g-replayed-seq",
            "headers": headers(2, "a-1", A, "desk.intake", "g-replayed-seq", 2, "dup seq")
        }),
        json!({"type": "BODY", "mail": "g-replayed-seq", "body": "dup seq"}),
    ]);
    let bad_text: String = bad_lines.iter().map(|v| format!("{v}\n")).collect();
    let bad_out = verify(&roster, &["--json"], bad_text.as_bytes());
    let bad_rs = records(&bad_out);
    assert_eq!(bad_out.status.code(), Some(1), "{bad_rs:#?}");
    assert_eq!(
        failures(mail(&bad_rs, "g-tampered-group")),
        vec!["invalid_signature"]
    );
    assert_eq!(
        failures(mail(&bad_rs, "g-tampered-body")),
        vec!["body_hash_mismatch"]
    );
    assert_eq!(
        failures(mail(&bad_rs, "g-replayed-seq")),
        vec!["replayed_seq"]
    );
}

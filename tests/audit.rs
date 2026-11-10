//! `roger audit` over fixture files of every line generation: exact text and
//! `--json` output, `--address`, findings, bodies, rotated logs with and
//! without their archives, and a read of logs a live coordinator, host daemon
//! and Tracer hold locked. Expected values are the literals chosen here, from
//! the brief's line formats.

#[cfg(not(feature = "test-hooks"))]
compile_error!("tests/audit.rs needs the test hooks: cargo test --features test-hooks");

#[allow(dead_code)]
#[path = "../src/json.rs"]
mod json;

use json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_roger");
const A: &str = "[[1a:Meteor Shower:zz]]aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const C: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const HEADERS: &str = r#"{"note":"ü v","empty":""}"#;
const DECLARED: &str = r#"{"scheme":"roger-declare/1","n":"1"}"#;

/// A fresh directory per test, named by the checkout so sibling worktrees
/// never share one; removed when the test passes, kept when it fails.
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Dir {
        let tree = env!("CARGO_MANIFEST_DIR")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        let dir = std::env::temp_dir().join(format!("roger-audit-{tree:016x}-{tag}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }

    fn write(&self, name: &str, text: &str) -> String {
        let p = self.0.join(name);
        fs::write(&p, text).unwrap();
        p.to_str().unwrap().to_string()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

#[test]
fn takeover_is_an_event_and_a_finding_with_verbatim_address() {
    let dir = Dir::new("takeover");
    let path = dir.write("coordinator", &format!("{{\"ev\":\"takeover\",\"session\":\"{A}\",\"old_host\":\"a\",\"host\":\"b\",\"at_ms\":123}}\n"));
    let output = audit_ok(&["--coordinator-log", &path, "--json", "--address", A]);
    let rows: Vec<_> = output.lines().map(|l| json::parse(l).unwrap()).collect();
    for kind in ["TAKEOVER", "ADDRESS_TAKEOVER"] {
        let matches: Vec<_> = rows
            .iter()
            .filter(|r| r.get("type").unwrap().as_str() == Some(kind))
            .collect();
        assert_eq!(matches.len(), 1, "expected one {kind}");
        let row = matches[0];
        assert_eq!(row.get("address").unwrap().as_str(), Some(A));
        assert_eq!(row.get("old_host").unwrap().as_str(), Some("a"));
        assert_eq!(row.get("host").unwrap().as_str(), Some("b"));
        assert_eq!(row.get("at_ms").unwrap().as_i64(), Some(123));
    }
    let text = audit_ok(&["--coordinator-log", &path, "--address", A]);
    for word in ["TAKEOVER", "ADDRESS_TAKEOVER"] {
        assert!(text
            .lines()
            .any(|l| l == format!("{word} address={A} old_host=a host=b at_ms=123 line=1")));
    }
    let filtered = audit_ok(&["--coordinator-log", &path, "--json", "--address", B]);
    assert!(!filtered.contains("TAKEOVER"));
}

fn audit(args: &[&str]) -> Output {
    Command::new(BIN).arg("audit").args(args).output().unwrap()
}

fn audit_ok(args: &[&str]) -> String {
    let out = audit(args);
    assert!(
        out.status.success(),
        "audit {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

// Convert the literal expected text records to their JSON contract shape.
// Field boundaries are known here; this is not a parser for command output.
fn expected_json(text: &str) -> Value {
    let (word, rest) = text.split_once(' ').unwrap();
    let mut fields = vec![("type".to_string(), json::s(word))];
    if word == "HEADERS" || word == "BODY" {
        let rest = rest.strip_prefix("mail=").unwrap();
        let end = rest
            .find(" {")
            .or_else(|| rest.find(" \""))
            .or_else(|| rest.find(" absent"))
            .unwrap();
        let (mail, tail) = (&rest[..end], &rest[end + 1..]);
        fields.push(("mail".into(), json::s(mail)));
        fields.push((
            if word == "HEADERS" { "headers" } else { "body" }.into(),
            if tail == "absent" {
                Value::Null
            } else {
                json::parse(tail).unwrap()
            },
        ));
    } else {
        let keys = [
            "kind",
            "path",
            "lines",
            "bytes",
            "line",
            "address",
            "host",
            "at_ms",
            "label",
            "declared",
            "mail",
            "from",
            "to",
            "accepted_at_ms",
            "receipted_at_ms",
            "latency_ms",
            "record",
            "receipt_line",
            "body_bytes",
            "session",
            "coordinator",
            "daemon",
            "store",
            "key",
            "owner",
            "expires_ms",
            "sources",
            "registers",
            "unregisters",
            "mails",
            "receipted",
            "unreceipted",
            "duplicates",
            "mismatches",
            "torn",
            "skipped",
            "snapshots",
            "culled",
            "seq",
            "prior",
            "next_record",
            "read",
            "receipt_path",
        ];
        let mut remaining = rest;
        while !remaining.is_empty() {
            let (key, value) = remaining.split_once('=').unwrap();
            let end = if key == "label" || key == "declared" {
                value.len()
            } else {
                keys.iter()
                    .filter_map(|k| value.find(&format!(" {k}=")))
                    .min()
                    .unwrap_or(value.len())
            };
            let raw = &value[..end];
            let numeric = [
                "lines",
                "bytes",
                "line",
                "at_ms",
                "accepted_at_ms",
                "receipted_at_ms",
                "latency_ms",
                "receipt_line",
                "body_bytes",
                "expires_ms",
                "sources",
                "registers",
                "unregisters",
                "mails",
                "receipted",
                "unreceipted",
                "duplicates",
                "mismatches",
                "torn",
                "skipped",
                "snapshots",
                "culled",
                "seq",
                "next_record",
            ];
            let v = if raw == "absent" {
                Value::Null
            } else if key == "read" {
                Value::Bool(raw.parse().unwrap())
            } else if numeric.contains(&key) {
                Value::Int(raw.parse().unwrap())
            } else {
                json::s(raw)
            };
            fields.push((key.into(), v));
            remaining = value[end..].strip_prefix(' ').unwrap_or("");
        }
    }
    Value::Obj(fields)
}

#[test]
fn audit_json_and_address_exact_output() {
    let dir = Dir::new("json");
    let (c, d, s) = fixture(&dir);
    let expected = expected_full(&c, &d, &s);
    let out = audit_ok(&[
        "--coordinator-log",
        &c,
        "--daemon-log",
        &d,
        "--store",
        &s,
        "--json",
    ]);
    let got: Vec<Value> = out.lines().map(|l| json::parse(l).unwrap()).collect();
    assert_eq!(
        got,
        expected
            .iter()
            .map(|l| expected_json(l))
            .collect::<Vec<_>>()
    );
    let mut filtered: Vec<String> = expected
        .into_iter()
        .filter(|l| {
            !l.starts_with(&format!("REGISTER address={B} "))
                && !l.starts_with(&format!("REGISTER address={C} "))
                && !l.starts_with(&format!("LABEL address={B} "))
                && !l.starts_with("MAIL mail=m2 ")
                && !l.starts_with("STRAY_RECEIPT ")
                && !l.starts_with("AUDIT ")
        })
        .collect();
    filtered.push("AUDIT sources=3 registers=1 unregisters=1 mails=5 receipted=3 unreceipted=2 duplicates=0 mismatches=0 torn=1 skipped=1 snapshots=0 culled=0".into());
    let out = audit_ok(&[
        "--coordinator-log",
        &c,
        "--daemon-log",
        &d,
        "--store",
        &s,
        "--address",
        A,
    ]);
    assert_eq!(out, filtered.join("\n") + "\n");
    let out = audit_ok(&[
        "--coordinator-log",
        &c,
        "--daemon-log",
        &d,
        "--store",
        &s,
        "--address",
        A,
        "--json",
    ]);
    assert_eq!(
        out.lines()
            .map(|l| json::parse(l).unwrap())
            .collect::<Vec<_>>(),
        filtered
            .iter()
            .map(|l| expected_json(l))
            .collect::<Vec<_>>()
    );
}

#[test]
fn audit_bodies_and_usage() {
    let dir = Dir::new("body");
    let s = dir.write("s", "{\"record\":\"r-1\",\"mail\":\"m\",\"from\":\"a\",\"body\":\"é\\n\\\"\\\\\",\"headers\":{}}\n");
    let base = format!("SOURCE kind=store path={s} lines=1\nSTORE path={s} record=r-1 mail=m from=a kind=absent line=1\n");
    let summary = "AUDIT sources=1 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=0 culled=0\n";
    assert_eq!(audit_ok(&["--store", &s]), base.clone() + summary);
    assert_eq!(
        audit_ok(&["--store", &s, "--bodies"]),
        base + "BODY mail=m \"é\\n\\\"\\\\\"\n" + summary
    );
    for args in [
        vec![],
        vec!["--store"],
        vec!["--unknown"],
        vec!["--store", "/no/such/audit-input"],
    ] {
        let out = audit(&args);
        assert!(!out.status.success());
        assert!(String::from_utf8(out.stderr).unwrap().contains("expected"));
    }
}

#[test]
fn audit_duplicate_and_mismatched_records() {
    let dir = Dir::new("findings");
    let c = dir.write(
        "c",
        concat!(
            "{\"ev\":\"mail\",\"mail\":\"m\",\"from\":\"a\",\"to\":\"b\",\"body\":\"\"}\n",
            "{\"ev\":\"receipt\",\"mail\":\"m\",\"session\":\"b\",\"record\":\"r-1\"}\n",
            "{\"ev\":\"receipt\",\"mail\":\"m\",\"session\":\"b\",\"record\":\"ignored\"}\n"
        ),
    );
    let d = dir.write(
        "d",
        "{\"ev\":\"receipt\",\"mail\":\"m\",\"session\":\"b\",\"record\":\"r-1\"}\n",
    );
    let s = dir.write(
        "s",
        concat!(
            "{\"record\":\"r-3\",\"mail\":\"m\",\"from\":\"a\",\"body\":\"\"}\n",
            "{\"record\":\"r-2\",\"mail\":\"x\",\"from\":\"a\",\"body\":\"\"}\n",
            "{\"record\":\"r-2\",\"mail\":\"y\",\"from\":\"a\",\"body\":\"\"}\n"
        ),
    );
    let out = audit_ok(&["--coordinator-log", &c, "--daemon-log", &d, "--store", &s]);
    let mail = "MAIL mail=m kind=directed from=a to=b accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=r-1 receipt_line=2 body_bytes=0 line=1";
    assert_eq!(out.lines().find(|l| l.starts_with("MAIL ")), Some(mail));
    let json_out = audit_ok(&[
        "--coordinator-log",
        &c,
        "--daemon-log",
        &d,
        "--store",
        &s,
        "--json",
    ]);
    assert_eq!(
        json_out
            .lines()
            .map(|l| json::parse(l).unwrap())
            .find(|v| v.get("type").and_then(Value::as_str) == Some("MAIL")),
        Some(expected_json(mail))
    );
    let findings: Vec<_> = out
        .lines()
        .filter(|l| l.starts_with("DUPLICATE") || l.starts_with("RECORD_MISMATCH"))
        .collect();
    assert_eq!(
        findings,
        [
            format!("DUPLICATE_RECORD path={s} record=r-2 mails=x,y"),
            "RECORD_MISMATCH mail=m coordinator=r-1 daemon=r-1 store=r-3".into()
        ]
    );
    let equal = dir.write(
        "equal",
        "{\"record\":\"r-1\",\"mail\":\"m\",\"from\":\"a\",\"body\":\"\"}\n",
    );
    assert!(!audit_ok(&["--coordinator-log", &c, "--store", &equal]).contains("RECORD_MISMATCH"));
}

struct Processes(Vec<Child>);
impl Processes {
    fn spawn(&mut self, args: &[&str]) -> BufReader<std::process::ChildStdout> {
        let mut child = Command::new(BIN)
            .args(args)
            .env("ROGER_TOKEN", "audit-test")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let out = BufReader::new(child.stdout.take().unwrap());
        self.0.push(child);
        out
    }
}
impl Drop for Processes {
    fn drop(&mut self) {
        for child in self.0.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn wait_until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < end, "process observation timed out");
        std::thread::sleep(Duration::from_millis(10)); // no-poll: bounded test observation, not delivery
    }
}
#[test]
fn audit_live_locked_logs_are_read_only() {
    let dir = Dir::new("live");
    let p = |name: &str| dir.0.join(name).to_str().unwrap().to_owned();
    let (c, d, s, port, sock) = (p("c"), p("d"), p("s"), p("port"), p("sock"));
    let mut processes = Processes(Vec::new());
    let _coord = processes.spawn(&[
        "coordinator",
        "--bind",
        "127.0.0.1:0",
        "--data",
        &c,
        "--port-file",
        &port,
    ]);
    wait_until(|| fs::read_to_string(&port).is_ok());
    let addr = format!("127.0.0.1:{}", fs::read_to_string(&port).unwrap().trim());
    let _daemon = processes.spawn(&[
        "daemon",
        "--coordinator",
        &addr,
        "--host-id",
        "host",
        "--socket",
        &sock,
        "--data",
        &d,
    ]);
    wait_until(|| std::path::Path::new(&sock).exists());
    let mut tracer = processes.spawn(&["session", "--socket", &sock, "--id", A, "--store", &s]);
    let mut line = String::new();
    loop {
        line.clear();
        assert!(tracer.read_line(&mut line).unwrap() > 0);
        if line.starts_with("REGISTERED ") {
            break;
        }
    }
    let send = |id: &str| {
        Command::new(BIN)
            .args([
                "send", "--socket", &sock, "--from", B, "--to", A, "--mail", id, "--body", "hi",
            ])
            .output()
            .unwrap()
    };
    assert!(send("live-1").status.success());
    wait_until(|| {
        fs::read_to_string(&c)
            .unwrap()
            .contains("\"ev\":\"receipt\"")
    });
    let before = (
        fs::read(&c).unwrap(),
        fs::read(&d).unwrap(),
        fs::read(&s).unwrap(),
    );
    let out = audit_ok(&["--coordinator-log", &c, "--daemon-log", &d, "--store", &s]);
    assert!(out.contains(&format!("REGISTER address={A} host=host at_ms=")));
    assert!(out
        .lines()
        .any(|l| l.starts_with("MAIL mail=live-1 ") && l.contains(" record=r-1 ")));
    assert_eq!(
        before,
        (
            fs::read(&c).unwrap(),
            fs::read(&d).unwrap(),
            fs::read(&s).unwrap()
        )
    );
    assert!(send("live-2").status.success());
    // End the processes while the Tracer's stdout is still open: live-2 can
    // reach it before this returns, and a print to a closed pipe is a panic.
    drop(processes);
}

/// The brief's fixture: a coordinator log of every line generation with a
/// torn tail, a host daemon log with the old field and a non-JSON line, and a
/// Store with and without `kind`/`headers`. Returns the three paths.
fn fixture(dir: &Dir) -> (String, String, String) {
    let coord = [
        format!(r#"{{"ev":"session","session":"{A}","host":"host-a"}}"#),
        format!(r#"{{"ev":"session","session":"{B}","host":"host-b","at_ms":1000}}"#),
        format!(r#"{{"ev":"label","session":"{A}","label":"[[0:FLEET:0]] lead"}}"#),
        format!(r#"{{"ev":"label","session":"{B}","label":"builder two","at_ms":1001}}"#),
        format!(
            r#"{{"ev":"declared","session":"{A}","declared":"{{\"scheme\":\"roger-declare/1\",\"n\":\"1\"}}","at_ms":1002}}"#
        ),
        format!(r#"{{"ev":"session","session":"{C}","host":"host-b","at_ms":1500}}"#),
        format!(
            r#"{{"ev":"mail","mail":"m1","from":"{A}","to":"{B}","body":"héllo ✓","headers":{HEADERS},"accepted_at_ms":2000}}"#
        ),
        format!(
            r#"{{"ev":"receipt","mail":"m1","session":"{B}","record":"r-1","receipted_at_ms":2075}}"#
        ),
        format!(r#"{{"ev":"mail","mail":"m0","from":"{B}","to":"{A}","body":"old"}}"#),
        format!(r#"{{"ev":"receipt","mail":"m0","session":"{A}","record":"r-1"}}"#),
        format!(
            r#"{{"ev":"mail","mail":"m2","from":"{B}","to":"{C}","body":"bc","headers":{{}},"accepted_at_ms":2500}}"#
        ),
        format!(
            r#"{{"ev":"receipt","mail":"m2","session":"{C}","record":"r-1","receipted_at_ms":2600}}"#
        ),
        format!(
            r#"{{"ev":"broadcast","mail":"b1","from":"{A}","body":"all hands","to":["{A}","{B}"],"headers":{{"x":"1"}},"accepted_at_ms":3000}}"#
        ),
        format!(
            r#"{{"ev":"receipt","mail":"b1/{A}","session":"{A}","record":"r-2","receipted_at_ms":3010}}"#
        ),
        format!(r#"{{"ev":"broadcast","mail":"b0","from":"{B}","body":"y","to":["{A}"]}}"#),
        format!(r#"{{"ev":"receipt","mail":"nope","session":"{B}","record":"r-7"}}"#),
        format!(r#"{{"ev":"claim","key":"[[0:build:0]]","owner":"{A}","expires_ms":9000}}"#),
        r#"{"ev":"release","key":"[[0:build:0]]"}"#.to_string(),
        format!(r#"{{"ev":"declared","session":"{A}","at_ms":4000}}"#),
        format!(r#"{{"ev":"unregister","session":"{A}","at_ms":5000}}"#),
    ]
    .join("\n")
        + "\n"
        + r#"{"ev":"mail""#;
    let daemon = [
        format!(
            r#"{{"ev":"deliver","mail":"m1","from":"{A}","to":"{B}","body":"héllo ✓","kind":"directed","headers":{HEADERS}}}"#
        ),
        format!(r#"{{"ev":"deliver","mail":"b1/{B}","from":"{A}","to":"{B}","body":"all hands","wake":false}}"#), // legacy-wire: read shape, a Broadcast copy
        "not a json line".to_string(),
        format!(r#"{{"ev":"receipt","mail":"m1","session":"{B}","record":"r-1"}}"#),
    ]
    .join("\n")
        + "\n";
    let store = [
        format!(
            r#"{{"record":"r-1","mail":"m1","from":"{A}","body":"héllo ✓","kind":"directed","headers":{HEADERS}}}"#
        ),
        format!(r#"{{"record":"r-2","mail":"b1/{B}","from":"{A}","body":"all hands"}}"#),
    ]
    .join("\n")
        + "\n";
    (
        dir.write("coord.log", &coord),
        dir.write("b.log", &daemon),
        dir.write("b.store", &store),
    )
}

#[test]
fn audit_checks_every_supplied_store_for_mismatches() {
    let dir = Dir::new("multiple-stores");
    let d = dir.write(
        "d",
        "{\"ev\":\"receipt\",\"mail\":\"m\",\"session\":\"b\",\"record\":\"r-1\"}\n",
    );
    let first = dir.write(
        "first",
        "{\"mail\":\"m\",\"record\":\"r-1\",\"from\":\"a\",\"body\":\"x\"}\n",
    );
    let second = dir.write(
        "second",
        "{\"mail\":\"m\",\"record\":\"r-3\",\"from\":\"a\",\"body\":\"x\"}\n",
    );
    let out = audit_ok(&["--daemon-log", &d, "--store", &first, "--store", &second]);
    assert!(
        out.contains("RECORD_MISMATCH mail=m coordinator=absent daemon=r-1 store=r-3\n"),
        "{out}"
    );
}

#[test]
fn audit_repeated_records_preserve_cross_source_disagreement() {
    let dir = Dir::new("repeated-records");
    for (daemon_records, store_records, daemon, store, mismatches) in [
        (vec!["r-1"], vec!["r-1", "r-3", "r-1"], "r-1", "r-3", 1),
        (vec!["r-1", "r-3", "r-1"], vec!["r-1"], "r-3", "r-1", 1),
        (vec!["r-1", "r-3"], vec!["r-3"], "r-1", "r-3", 1),
        (vec!["r-3"], vec!["r-1", "r-3"], "r-3", "r-1", 1),
        (vec!["r-1", "r-1"], vec!["r-1", "r-1"], "r-1", "r-1", 0),
        (vec![], vec!["r-1", "r-3"], "absent", "r-3", 0),
        (vec!["r-1", "r-3"], vec![], "r-3", "absent", 0),
    ] {
        let d = dir.write(
            "d",
            &daemon_records
                .iter()
                .map(|record| {
                    format!(r#"{{"ev":"receipt","mail":"m","session":"b","record":"{record}"}}"#)
                        + "\n"
                })
                .collect::<String>(),
        );
        let s = dir.write(
            "s",
            &store_records
                .iter()
                .map(|record| {
                    format!(
                        r#"{{"mail":"m","record":"{record}","from":"a","body":"x","headers":{{}}}}"#
                    ) + "\n"
                })
                .collect::<String>(),
        );
        let mut expected = vec![
            format!("SOURCE kind=daemon path={d} lines={}", daemon_records.len()),
            format!("SOURCE kind=store path={s} lines={}", store_records.len()),
        ];
        expected.extend(daemon_records.iter().enumerate().map(|(i, record)| {
            format!(
                "DAEMON_RECEIPT path={d} mail=m session=b record={record} line={}",
                i + 1
            )
        }));
        expected.extend(store_records.iter().enumerate().map(|(i, record)| {
            format!(
                "STORE path={s} record={record} mail=m from=a kind=absent line={}",
                i + 1
            )
        }));
        if mismatches > 0 {
            expected.push(format!(
                "RECORD_MISMATCH mail=m coordinator=absent daemon={daemon} store={store}"
            ));
        }
        expected.push(format!("AUDIT sources=2 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches={mismatches} torn=0 skipped=0 snapshots=0 culled=0"));
        assert_eq!(
            audit_ok(&["--daemon-log", &d, "--store", &s]),
            expected.join("\n") + "\n"
        );
        let out = audit_ok(&["--daemon-log", &d, "--store", &s, "--json"]);
        assert_eq!(
            out.lines()
                .map(|l| json::parse(l).unwrap())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|l| expected_json(l))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn audit_missing_headers_are_distinct_from_empty() {
    let dir = Dir::new("headers");
    let s = dir.write(
        "s",
        concat!(
            "{\"record\":\"r-1\",\"mail\":\"old\",\"from\":\"a\",\"body\":\"\"}\n",
            "{\"record\":\"r-2\",\"mail\":\"empty\",\"from\":\"a\",\"body\":\"\",\"headers\":{}}\n"
        ),
    );
    let out = audit_ok(&["--store", &s]);
    assert!(out.contains("HEADERS mail=old absent\n"), "{out}");
    assert!(!out.contains("HEADERS mail=empty"));
    let out = audit_ok(&["--store", &s, "--json"]);
    assert!(out.contains("\"headers\":null"));
}

#[test]
fn audit_sender_filter_keeps_receipt_only_evidence() {
    let dir = Dir::new("sender");
    let c = dir.write(
        "c",
        "{\"ev\":\"mail\",\"mail\":\"m\",\"from\":\"a\",\"to\":\"b\",\"body\":\"\"}\n",
    );
    let d = dir.write(
        "d",
        "{\"ev\":\"receipt\",\"mail\":\"m\",\"session\":\"b\",\"record\":\"r-1\"}\n",
    );
    let out = audit_ok(&[
        "--daemon-log",
        &d,
        "--coordinator-log",
        &c,
        "--address",
        "a",
    ]);
    assert!(
        out.contains(&format!(
            "DAEMON_RECEIPT path={d} mail=m session=b record=r-1 line=1\n"
        )),
        "{out}"
    );
}

/// The whole listing of the fixture, in the brief's formats and order.
fn expected_full(c: &str, d: &str, s: &str) -> Vec<String> {
    vec![
        format!("SOURCE kind=coordinator path={c} lines=20"),
        format!("TORN path={c} bytes=12"),
        format!("SOURCE kind=daemon path={d} lines=4"),
        format!("SKIPPED path={d} line=3"),
        format!("SOURCE kind=store path={s} lines=2"),
        format!("REGISTER address={A} host=host-a at_ms=absent line=1"),
        format!("REGISTER address={B} host=host-b at_ms=1000 line=2"),
        format!("LABEL address={A} at_ms=absent line=3 label=[[0:FLEET:0]] lead"),
        format!("LABEL address={B} at_ms=1001 line=4 label=builder two"),
        format!("DECLARED address={A} at_ms=1002 line=5 declared={DECLARED}"),
        format!("REGISTER address={C} host=host-b at_ms=1500 line=6"),
        format!("MAIL mail=m1 kind=directed from={A} to={B} accepted_at_ms=2000 receipted_at_ms=2075 latency_ms=75 record=r-1 receipt_line=8 body_bytes=10 line=7"),
        format!("HEADERS mail=m1 {HEADERS}"),
        format!("MAIL mail=m0 kind=directed from={B} to={A} accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=r-1 receipt_line=10 body_bytes=3 line=9"),
        "HEADERS mail=m0 absent".to_string(),
        format!("MAIL mail=m2 kind=directed from={B} to={C} accepted_at_ms=2500 receipted_at_ms=2600 latency_ms=100 record=r-1 receipt_line=12 body_bytes=2 line=11"),
        format!("MAIL mail=b1/{A} kind=broadcast from={A} to={A} accepted_at_ms=3000 receipted_at_ms=3010 latency_ms=10 record=r-2 receipt_line=14 body_bytes=9 line=13"),
        format!(r#"HEADERS mail=b1/{A} {{"x":"1"}}"#),
        format!("MAIL mail=b1/{B} kind=broadcast from={A} to={B} accepted_at_ms=3000 receipted_at_ms=absent latency_ms=absent record=pending receipt_line=absent body_bytes=9 line=13"),
        format!(r#"HEADERS mail=b1/{B} {{"x":"1"}}"#),
        format!("MAIL mail=b0/{A} kind=broadcast from={B} to={A} accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=pending receipt_line=absent body_bytes=1 line=15"),
        format!("HEADERS mail=b0/{A} absent"),
        format!("STRAY_RECEIPT mail=nope session={B} record=r-7 line=16"),
        format!("CLAIM key=[[0:build:0]] owner={A} expires_ms=9000 line=17"),
        "RELEASE key=[[0:build:0]] line=18".to_string(),
        format!("UNDECLARED address={A} at_ms=4000 line=19"),
        format!("UNREGISTER address={A} at_ms=5000 line=20"),
        format!("DAEMON_DELIVER path={d} mail=m1 kind=directed from={A} to={B} line=1"),
        format!("HEADERS mail=m1 {HEADERS}"),
        format!("DAEMON_DELIVER path={d} mail=b1/{B} kind=broadcast from={A} to={B} line=2"),
        format!("HEADERS mail=b1/{B} absent"),
        format!("DAEMON_RECEIPT path={d} mail=m1 session={B} record=r-1 line=4"),
        format!("STORE path={s} record=r-1 mail=m1 from={A} kind=directed line=1"),
        format!("HEADERS mail=m1 {HEADERS}"),
        format!("STORE path={s} record=r-2 mail=b1/{B} from={A} kind=absent line=2"),
        format!("HEADERS mail=b1/{B} absent"),
        format!("UNRECEIPTED mail=b1/{B} kind=broadcast from={A} to={B} accepted_at_ms=3000 line=13"),
        format!("UNRECEIPTED mail=b0/{A} kind=broadcast from={B} to={A} accepted_at_ms=absent line=15"),
        "AUDIT sources=3 registers=3 unregisters=1 mails=6 receipted=4 unreceipted=2 duplicates=0 mismatches=0 torn=1 skipped=1 snapshots=0 culled=0".to_string(),
    ]
}

#[test]
fn audit_stray_receipts_participate_in_record_findings() {
    let dir = Dir::new("stray");
    let c = dir.write(
        "c",
        "{\"ev\":\"receipt\",\"mail\":\"lost\",\"session\":\"a\",\"record\":\"r-1\"}\n",
    );
    let s = dir.write(
        "s",
        "{\"mail\":\"lost\",\"record\":\"r-3\",\"from\":\"b\",\"body\":\"x\"}\n",
    );
    let out = audit_ok(&["--coordinator-log", &c, "--store", &s]);
    assert!(
        out.contains("RECORD_MISMATCH mail=lost coordinator=r-1 daemon=absent store=r-3\n"),
        "{out}"
    );
}

#[test]
fn audit_lists_every_line_generation_in_log_order_byte_identical() {
    let dir = Dir::new("full");
    let (c, d, s) = fixture(&dir);
    let before = (
        fs::read(&c).unwrap(),
        fs::read(&d).unwrap(),
        fs::read(&s).unwrap(),
    );
    let out = audit_ok(&["--coordinator-log", &c, "--daemon-log", &d, "--store", &s]);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines, expected_full(&c, &d, &s), "{out}");
    assert_eq!(
        before,
        (
            fs::read(&c).unwrap(),
            fs::read(&d).unwrap(),
            fs::read(&s).unwrap()
        ),
        "audit changed an input"
    );
}

/// The text listing for `args` is `expected` exactly, and `--json` gives the
/// same records in their JSON shape.
fn assert_listing(args: &[&str], expected: &[String]) {
    let out = audit_ok(args);
    assert_eq!(out, expected.join("\n") + "\n", "{out}");
    let mut with_json = args.to_vec();
    with_json.push("--json");
    assert_eq!(
        audit_ok(&with_json)
            .lines()
            .map(|l| json::parse(l).unwrap())
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|l| expected_json(l))
            .collect::<Vec<_>>()
    );
}

fn lines_of(lines: &[&str]) -> String {
    lines.join("\n") + "\n"
}

/// A coordinator log rotated once: the archive `coord.log.1` as it was, and
/// the file at the path, whose snapshot restates the archive, culls m1 and
/// keeps m2 and bc/a settled. m3's receipt lands after the snapshot.
fn rotated_coordinator(dir: &Dir) -> (String, String) {
    let archive = dir.write(
        "coord.log.1",
        &lines_of(&[
            r#"{"ev":"session","session":"a","host":"h","at_ms":100}"#,
            r#"{"ev":"session","session":"b","host":"h","at_ms":110}"#,
            r#"{"ev":"label","session":"b","label":"two words","at_ms":120}"#,
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"hello","headers":{},"accepted_at_ms":200}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1","receipted_at_ms":250}"#,
            r#"{"ev":"mail","mail":"m2","from":"a","to":"b","body":"body ✓","headers":{},"accepted_at_ms":300}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2","receipted_at_ms":350}"#,
            r#"{"ev":"mail","mail":"m3","from":"a","to":"b","body":"later","headers":{},"accepted_at_ms":360}"#,
            r#"{"ev":"broadcast","mail":"bc","from":"a","body":"all","to":["a","b"],"headers":{},"accepted_at_ms":400}"#,
            r#"{"ev":"receipt","mail":"bc/a","session":"a","record":"r-1","receipted_at_ms":410}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":9000}"#,
        ]),
    );
    let live = dir.write(
        "coord.log",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"coord.log.1","at_ms":500}"#,
            r#"{"ev":"session","session":"a","host":"h"}"#,
            r#"{"ev":"session","session":"b","host":"h"}"#,
            r#"{"ev":"label","session":"b","label":"two words"}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":9000}"#,
            r#"{"ev":"broadcast","mail":"bc","from":"a","body":"","to":[],"settled":{"bytes":3,"mark":"0123456789abcdef"}}"#,
            r#"{"ev":"culled","mail":"m1"}"#,
            r#"{"ev":"mail","mail":"m2","from":"a","to":"b","body":"","headers":{},"accepted_at_ms":300,"settled":{"bytes":8,"mark":"fedcba9876543210"}}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2","receipted_at_ms":350}"#,
            r#"{"ev":"mail","mail":"bc/a","from":"a","to":"a","body":"","headers":{},"accepted_at_ms":400,"kind":"broadcast","settled":{"bytes":3,"mark":"0123456789abcdef"}}"#,
            r#"{"ev":"receipt","mail":"bc/a","session":"a","record":"r-1","receipted_at_ms":410}"#,
            r#"{"ev":"mail","mail":"m3","from":"a","to":"b","body":"later","headers":{},"accepted_at_ms":360}"#,
            r#"{"ev":"mail","mail":"bc/b","from":"a","to":"b","body":"all","headers":{},"accepted_at_ms":400,"kind":"broadcast"}"#,
            r#"{"snapshot_end":1}"#,
            r#"{"ev":"receipt","mail":"m3","session":"b","record":"r-3","receipted_at_ms":600}"#,
            r#"{"ev":"unregister","session":"a","at_ms":700}"#,
        ]),
    );
    (archive, live)
}

#[test]
fn audit_archive_and_live_coordinator_files_read_as_one_timeline() {
    let dir = Dir::new("rotated-coordinator");
    let (old, live) = rotated_coordinator(&dir);
    let expected: Vec<String> = vec![
        format!("SOURCE kind=coordinator path={old} lines=11"),
        format!("SOURCE kind=coordinator path={live} lines=16"),
        format!("REGISTER address=a host=h at_ms=100 path={old} line=1"),
        format!("REGISTER address=b host=h at_ms=110 path={old} line=2"),
        format!("LABEL address=b at_ms=120 path={old} line=3 label=two words"),
        format!("MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=200 receipted_at_ms=250 latency_ms=50 record=r-1 receipt_path={old} receipt_line=5 body_bytes=5 path={old} line=4"),
        format!("MAIL mail=m2 kind=directed from=a to=b accepted_at_ms=300 receipted_at_ms=350 latency_ms=50 record=r-2 receipt_path={old} receipt_line=7 body_bytes=8 path={old} line=6"),
        format!("MAIL mail=m3 kind=directed from=a to=b accepted_at_ms=360 receipted_at_ms=600 latency_ms=240 record=r-3 receipt_path={live} receipt_line=15 body_bytes=5 path={old} line=8"),
        format!("MAIL mail=bc/a kind=broadcast from=a to=a accepted_at_ms=400 receipted_at_ms=410 latency_ms=10 record=r-1 receipt_path={old} receipt_line=10 body_bytes=3 path={old} line=9"),
        format!("MAIL mail=bc/b kind=broadcast from=a to=b accepted_at_ms=400 receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=3 path={old} line=9"),
        format!("CLAIM key=k owner=a expires_ms=9000 path={old} line=11"),
        format!("SNAPSHOT path={live} seq=1 prior=coord.log.1 at_ms=500 next_record=absent lines=12 read=false"),
        format!("UNREGISTER address=a at_ms=700 path={live} line=16"),
        format!("UNRECEIPTED mail=bc/b kind=broadcast from=a to=b accepted_at_ms=400 path={old} line=9"),
        "AUDIT sources=2 registers=2 unregisters=1 mails=5 receipted=4 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=0".into(),
    ];
    assert_listing(
        &["--coordinator-log", &old, "--coordinator-log", &live],
        &expected,
    );
    // The archive is known by the file, whatever path names it.
    let other = format!("{}/./coord.log.1", dir.0.to_str().unwrap());
    let out = audit_ok(&["--coordinator-log", &other, "--coordinator-log", &live]);
    assert!(out.contains(" read=false\n"), "{out}");
}

#[test]
fn audit_live_coordinator_file_alone_reads_its_snapshot() {
    let dir = Dir::new("live-coordinator");
    let (_, live) = rotated_coordinator(&dir);
    let expected: Vec<String> = vec![
        format!("SOURCE kind=coordinator path={live} lines=16"),
        format!("SNAPSHOT path={live} seq=1 prior=coord.log.1 at_ms=500 next_record=absent lines=12 read=true"),
        "REGISTER address=a host=h at_ms=absent line=2".into(),
        "REGISTER address=b host=h at_ms=absent line=3".into(),
        "LABEL address=b at_ms=absent line=4 label=two words".into(),
        "CLAIM key=k owner=a expires_ms=9000 line=5".into(),
        "CULLED mail=m1 line=7".into(),
        "MAIL mail=m2 kind=directed from=a to=b accepted_at_ms=300 receipted_at_ms=350 latency_ms=50 record=r-2 receipt_line=9 body_bytes=8 line=8".into(),
        "MAIL mail=bc/a kind=broadcast from=a to=a accepted_at_ms=400 receipted_at_ms=410 latency_ms=10 record=r-1 receipt_line=11 body_bytes=3 line=10".into(),
        "MAIL mail=m3 kind=directed from=a to=b accepted_at_ms=360 receipted_at_ms=600 latency_ms=240 record=r-3 receipt_line=15 body_bytes=5 line=12".into(),
        "MAIL mail=bc/b kind=broadcast from=a to=b accepted_at_ms=400 receipted_at_ms=absent latency_ms=absent record=pending receipt_line=absent body_bytes=3 line=13".into(),
        "UNREGISTER address=a at_ms=700 line=16".into(),
        "UNRECEIPTED mail=bc/b kind=broadcast from=a to=b accepted_at_ms=400 line=13".into(),
        "AUDIT sources=1 registers=2 unregisters=1 mails=4 receipted=3 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=1".into(),
    ];
    assert_listing(&["--coordinator-log", &live], &expected);

    // A settled Mail has no body to print.
    let with_bodies = audit_ok(&["--coordinator-log", &live, "--bodies"]);
    let bodies: Vec<&str> = with_bodies
        .lines()
        .filter(|l| l.starts_with("BODY "))
        .collect();
    assert_eq!(
        bodies,
        ["BODY mail=m3 \"later\"", "BODY mail=bc/b \"all\""],
        "{with_bodies}"
    );

    // JSON types: integers, null for absent, a boolean for `read`.
    let json_out = audit_ok(&["--coordinator-log", &live, "--json"]);
    let lines: Vec<&str> = json_out.lines().collect();
    assert_eq!(
        lines[1],
        format!(
            r#"{{"type":"SNAPSHOT","path":"{live}","seq":1,"prior":"coord.log.1","at_ms":500,"next_record":null,"lines":12,"read":true}}"#
        )
    );
    assert_eq!(lines[6], r#"{"type":"CULLED","mail":"m1","line":7}"#);

    // A culled Mail names no Address: under `--address` it is listed only
    // when a supplied record joins it to that Address.
    let filtered = audit_ok(&["--coordinator-log", &live, "--address", "b"]);
    assert!(!filtered.contains("CULLED"), "{filtered}");
    assert!(filtered.contains(&format!("SNAPSHOT path={live} ")));
    assert!(filtered.ends_with(" snapshots=1 culled=0\n"), "{filtered}");
    let d = dir.write(
        "d.log",
        &lines_of(&[
            r#"{"ev":"deliver","mail":"m1","from":"a","to":"b","body":"hello","kind":"directed","headers":{}}"#,
        ]),
    );
    let joined = audit_ok(&[
        "--coordinator-log",
        &live,
        "--daemon-log",
        &d,
        "--address",
        "b",
    ]);
    assert!(joined.contains("\nCULLED mail=m1 line=7\n"), "{joined}");
    assert!(joined.ends_with(" snapshots=1 culled=1\n"), "{joined}");
}

#[test]
fn audit_culled_mail_is_settled_and_its_receipt_is_no_stray() {
    let dir = Dir::new("culled-settled");
    // The middle archive (c.log.2) is not supplied, so the block is read.
    let old = dir.write(
        "c.log.1",
        &lines_of(&[
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"x","headers":{}}"#,
            r#"{"ev":"mail","mail":"m5","from":"a","to":"b","body":"y","headers":{}}"#,
        ]),
    );
    let live = dir.write(
        "c.log",
        &lines_of(&[
            r#"{"snapshot":2,"prior":"c.log.2","at_ms":900}"#,
            r#"{"ev":"culled","mail":"m1"}"#,
            r#"{"ev":"culled","mail":"m9"}"#,
            r#"{"snapshot_end":2}"#,
            r#"{"ev":"receipt","mail":"m9","session":"b","record":"r-9"}"#,
            r#"{"ev":"receipt","mail":"nope","session":"b","record":"r-4"}"#,
        ]),
    );
    let expected: Vec<String> = vec![
        format!("SOURCE kind=coordinator path={old} lines=2"),
        format!("SOURCE kind=coordinator path={live} lines=6"),
        format!("MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=1 path={old} line=1"),
        format!("MAIL mail=m5 kind=directed from=a to=b accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=1 path={old} line=2"),
        format!("SNAPSHOT path={live} seq=2 prior=c.log.2 at_ms=900 next_record=absent lines=2 read=true"),
        format!("CULLED mail=m1 path={live} line=2"),
        format!("CULLED mail=m9 path={live} line=3"),
        format!("STRAY_RECEIPT mail=nope session=b record=r-4 path={live} line=6"),
        format!("UNRECEIPTED mail=m5 kind=directed from=a to=b accepted_at_ms=absent path={old} line=2"),
        "AUDIT sources=2 registers=0 unregisters=0 mails=2 receipted=0 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=2".into(),
    ];
    assert_listing(
        &["--coordinator-log", &old, "--coordinator-log", &live],
        &expected,
    );
}

#[test]
fn audit_daemon_log_snapshot_with_and_without_its_archive() {
    let dir = Dir::new("rotated-daemon");
    let old = dir.write(
        "d.log.1",
        &lines_of(&[
            r#"{"ev":"deliver","mail":"m1","from":"a","to":"b","body":"x","kind":"directed","headers":{}}"#,
            r#"{"ev":"deliver","mail":"m2","from":"a","to":"b","body":"yy","kind":"directed","headers":{}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
        ]),
    );
    let live = dir.write(
        "d.log",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"d.log.1"}"#,
            r#"{"ev":"deliver","mail":"m2","from":"a","to":"b","body":"yy","kind":"directed","headers":{}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
            r#"{"snapshot_end":1}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2"}"#,
        ]),
    );
    let summary = |sources: usize| {
        format!("AUDIT sources={sources} registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=0")
    };
    assert_listing(
        &["--daemon-log", &old, "--daemon-log", &live],
        &[
            format!("SOURCE kind=daemon path={old} lines=3"),
            format!("SOURCE kind=daemon path={live} lines=5"),
            format!("DAEMON_DELIVER path={old} mail=m1 kind=directed from=a to=b line=1"),
            format!("DAEMON_DELIVER path={old} mail=m2 kind=directed from=a to=b line=2"),
            format!("DAEMON_RECEIPT path={old} mail=m1 session=b record=r-1 line=3"),
            format!("SNAPSHOT path={live} seq=1 prior=d.log.1 at_ms=absent next_record=absent lines=2 read=false"),
            format!("DAEMON_RECEIPT path={live} mail=m2 session=b record=r-2 line=5"),
            summary(2),
        ],
    );
    let alone = [
        format!("SOURCE kind=daemon path={live} lines=5"),
        format!("SNAPSHOT path={live} seq=1 prior=d.log.1 at_ms=absent next_record=absent lines=2 read=true"),
        format!("DAEMON_DELIVER path={live} mail=m2 kind=directed from=a to=b line=2"),
        format!("DAEMON_RECEIPT path={live} mail=m1 session=b record=r-1 line=3"),
        format!("DAEMON_RECEIPT path={live} mail=m2 session=b record=r-2 line=5"),
        summary(1),
    ];
    assert_listing(&["--daemon-log", &live], &alone);
    // A snapshot describes a file and is listed for any Address.
    assert_listing(
        &["--daemon-log", &live, "--address", "zz"],
        &[alone[0].clone(), alone[1].clone(), summary(1)],
    );
}

#[test]
fn audit_store_snapshot_tombstone_and_stamp_with_and_without_its_archive() {
    let dir = Dir::new("rotated-store");
    let d = dir.write(
        "d.log",
        &lines_of(&[r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-7"}"#]),
    );
    let old = dir.write(
        "s.store.1",
        &lines_of(&[
            r#"{"record":"r-1","mail":"m1","from":"a","body":"x","kind":"directed","headers":{},"stored_at_ms":100}"#,
            r#"{"record":"r-2","mail":"m2","from":"a","body":"yy","kind":"directed","headers":{},"stored_at_ms":200}"#,
        ]),
    );
    let live = dir.write(
        "s.store",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"s.store.1","next_record":3}"#,
            r#"{"record":"r-1","mail":"m1","culled":true}"#,
            r#"{"record":"r-2","mail":"m2","from":"a","body":"yy","kind":"directed","headers":{},"stored_at_ms":200}"#,
            r#"{"snapshot_end":1}"#,
            r#"{"record":"r-3","mail":"m3","from":"a","body":"z","kind":"directed","headers":{},"stored_at_ms":300}"#,
        ]),
    );
    let receipt = format!("DAEMON_RECEIPT path={d} mail=m1 session=b record=r-7 line=1");
    let mismatch = "RECORD_MISMATCH mail=m1 coordinator=absent daemon=r-7 store=r-1".to_string();
    // `stored_at_ms` is not listed: STORE keeps its fields.
    assert_listing(
        &["--daemon-log", &d, "--store", &old, "--store", &live],
        &[
            format!("SOURCE kind=daemon path={d} lines=1"),
            format!("SOURCE kind=store path={old} lines=2"),
            format!("SOURCE kind=store path={live} lines=5"),
            receipt.clone(),
            format!("STORE path={old} record=r-1 mail=m1 from=a kind=directed line=1"),
            format!("STORE path={old} record=r-2 mail=m2 from=a kind=directed line=2"),
            format!("SNAPSHOT path={live} seq=1 prior=s.store.1 at_ms=absent next_record=3 lines=2 read=false"),
            format!("STORE path={live} record=r-3 mail=m3 from=a kind=directed line=5"),
            mismatch.clone(),
            "AUDIT sources=3 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=1 torn=0 skipped=0 snapshots=1 culled=0".into(),
        ],
    );
    // Alone, the tombstone is the Store's only evidence for m1.
    let alone = [
        format!("SOURCE kind=daemon path={d} lines=1"),
        format!("SOURCE kind=store path={live} lines=5"),
        receipt,
        format!("SNAPSHOT path={live} seq=1 prior=s.store.1 at_ms=absent next_record=3 lines=2 read=true"),
        format!("STORE_CULLED path={live} record=r-1 mail=m1 line=2"),
        format!("STORE path={live} record=r-2 mail=m2 from=a kind=directed line=3"),
        format!("STORE path={live} record=r-3 mail=m3 from=a kind=directed line=5"),
        mismatch,
        "AUDIT sources=2 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=1 torn=0 skipped=0 snapshots=1 culled=1".into(),
    ];
    assert_listing(&["--daemon-log", &d, "--store", &live], &alone);
    // A tombstone has no body to print.
    let with_bodies = audit_ok(&["--daemon-log", &d, "--store", &live, "--bodies"]);
    let mut expected: Vec<String> = alone.to_vec();
    expected.insert(6, "BODY mail=m2 \"yy\"".into());
    expected.insert(8, "BODY mail=m3 \"z\"".into());
    assert_eq!(with_bodies, expected.join("\n") + "\n");
    let json_out = audit_ok(&["--store", &live, "--json"]);
    assert!(json_out.contains(&format!(
        r#"{{"type":"STORE_CULLED","path":"{live}","record":"r-1","mail":"m1","line":2}}"#
    )));
}

#[test]
fn audit_malformed_snapshot_blocks_end_cleanly() {
    let dir = Dir::new("malformed-snapshots");
    // A stray end, a block that never ends, a non-JSON line inside it, and a
    // torn tail.
    let torn = r#"{"ev":"mail","mail":"m2""#;
    let c = dir.write(
        "c.log",
        &(lines_of(&[
            r#"{"ev":"session","session":"a","host":"h"}"#,
            r#"{"snapshot_end":4}"#,
            r#"{"snapshot":2,"prior":"gone.log.2"}"#,
            "not json",
            r#"{"ev":"culled","mail":"m0"}"#,
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"","headers":{},"settled":{"bytes":7,"mark":"0123456789abcdef"}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
        ]) + torn),
    );
    let expected: Vec<String> = vec![
        format!("SOURCE kind=coordinator path={c} lines=7"),
        format!("TORN path={c} bytes={}", torn.len()),
        format!("SKIPPED path={c} line=4"),
        "REGISTER address=a host=h at_ms=absent line=1".into(),
        format!("SNAPSHOT path={c} seq=2 prior=gone.log.2 at_ms=absent next_record=absent lines=4 read=true"),
        "CULLED mail=m0 line=5".into(),
        "MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent record=r-1 receipt_line=7 body_bytes=7 line=6".into(),
        "AUDIT sources=1 registers=1 unregisters=0 mails=1 receipted=1 unreceipted=0 duplicates=0 mismatches=0 torn=1 skipped=1 snapshots=1 culled=1".into(),
    ];
    assert_listing(&["--coordinator-log", &c], &expected);
    assert_eq!(
        audit_ok(&["--coordinator-log", &c, "--bodies"]),
        expected.join("\n") + "\n"
    );

    // A block that opens on the last line is empty; a `snapshot` inside
    // another's block ends it and opens its own; a tombstone and an entry
    // under one record id are a duplicate.
    let d = dir.write(
        "d.log",
        &lines_of(&[
            r#"{"snapshot_end":1}"#,
            r#"{"snapshot":5,"prior":"d.log.5"}"#,
        ]),
    );
    let s = dir.write(
        "s.store",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"x.1"}"#,
            r#"{"record":"r-1","mail":"m1","from":"a","body":"","headers":{}}"#,
            r#"{"snapshot":2,"prior":"x.2"}"#,
            r#"{"record":"r-4","mail":"m4","culled":true}"#,
            r#"{"snapshot_end":2}"#,
            r#"{"snapshot_end":2}"#,
            r#"{"record":"r-4","mail":"m5","from":"a","body":"","headers":{}}"#,
        ]),
    );
    let out = audit_ok(&["--daemon-log", &d, "--store", &s]);
    assert_eq!(
        out,
        [
            format!("SOURCE kind=daemon path={d} lines=2"),
            format!("SOURCE kind=store path={s} lines=7"),
            format!("SNAPSHOT path={d} seq=5 prior=d.log.5 at_ms=absent next_record=absent lines=0 read=true"),
            format!("SNAPSHOT path={s} seq=1 prior=x.1 at_ms=absent next_record=absent lines=1 read=true"),
            format!("STORE path={s} record=r-1 mail=m1 from=a kind=absent line=2"),
            format!("SNAPSHOT path={s} seq=2 prior=x.2 at_ms=absent next_record=absent lines=1 read=true"),
            format!("STORE_CULLED path={s} record=r-4 mail=m4 line=4"),
            format!("STORE path={s} record=r-4 mail=m5 from=a kind=absent line=7"),
            format!("DUPLICATE_RECORD path={s} record=r-4 mails=m4,m5"),
            "AUDIT sources=2 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=1 mismatches=0 torn=0 skipped=0 snapshots=3 culled=1".into(),
        ]
        .join("\n")
            + "\n"
    );
}

/// A coordinator log rotated twice, as the coordinator writes it, in
/// `dir/<sub>/`: `coord.log.1`, then `coord.log.2` whose snapshot restates it,
/// then `coord.log` whose snapshot restates `coord.log.2`. Every Mail but m5
/// is receipted; m2's Receipt is in `coord.log.2` and m4's in `coord.log`.
fn three_segments(dir: &Dir, sub: &str) -> [String; 3] {
    fs::create_dir_all(dir.0.join(sub)).unwrap();
    let first = dir.write(
        &format!("{sub}/coord.log.1"),
        &lines_of(&[
            r#"{"ev":"session","session":"a","host":"h","at_ms":100}"#,
            r#"{"ev":"session","session":"b","host":"h","at_ms":110}"#,
            r#"{"ev":"label","session":"b","label":"two words","at_ms":120}"#,
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"one","headers":{},"accepted_at_ms":200}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1","receipted_at_ms":210}"#,
            r#"{"ev":"mail","mail":"m2","from":"a","to":"b","body":"two","headers":{},"accepted_at_ms":300}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":9000}"#,
        ]),
    );
    let second = dir.write(
        &format!("{sub}/coord.log.2"),
        &lines_of(&[
            r#"{"snapshot":1,"prior":"coord.log.1","at_ms":400}"#,
            r#"{"ev":"session","session":"a","host":"h"}"#,
            r#"{"ev":"session","session":"b","host":"h"}"#,
            r#"{"ev":"label","session":"b","label":"two words"}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":9000}"#,
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"","headers":{},"accepted_at_ms":200,"settled":{"bytes":3,"mark":"0000000000000001"}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1","receipted_at_ms":210}"#,
            r#"{"ev":"mail","mail":"m2","from":"a","to":"b","body":"two","headers":{},"accepted_at_ms":300}"#,
            r#"{"snapshot_end":1}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2","receipted_at_ms":450}"#,
            r#"{"ev":"mail","mail":"m3","from":"a","to":"b","body":"three","headers":{},"accepted_at_ms":500}"#,
            r#"{"ev":"receipt","mail":"m3","session":"b","record":"r-3","receipted_at_ms":520}"#,
            r#"{"ev":"mail","mail":"m4","from":"a","to":"b","body":"four","headers":{},"accepted_at_ms":600}"#,
        ]),
    );
    let live = dir.write(
        &format!("{sub}/coord.log"),
        &lines_of(&[
            r#"{"snapshot":2,"prior":"coord.log.2","at_ms":700}"#,
            r#"{"ev":"session","session":"a","host":"h"}"#,
            r#"{"ev":"session","session":"b","host":"h"}"#,
            r#"{"ev":"label","session":"b","label":"two words"}"#,
            r#"{"ev":"claim","key":"k","owner":"a","expires_ms":9000}"#,
            r#"{"ev":"mail","mail":"m1","from":"a","to":"b","body":"","headers":{},"accepted_at_ms":200,"settled":{"bytes":3,"mark":"0000000000000001"}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1","receipted_at_ms":210}"#,
            r#"{"ev":"mail","mail":"m2","from":"a","to":"b","body":"","headers":{},"accepted_at_ms":300,"settled":{"bytes":3,"mark":"0000000000000002"}}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2","receipted_at_ms":450}"#,
            r#"{"ev":"mail","mail":"m3","from":"a","to":"b","body":"","headers":{},"accepted_at_ms":500,"settled":{"bytes":5,"mark":"0000000000000003"}}"#,
            r#"{"ev":"receipt","mail":"m3","session":"b","record":"r-3","receipted_at_ms":520}"#,
            r#"{"ev":"mail","mail":"m4","from":"a","to":"b","body":"four","headers":{},"accepted_at_ms":600}"#,
            r#"{"snapshot_end":2}"#,
            r#"{"ev":"receipt","mail":"m4","session":"b","record":"r-4","receipted_at_ms":800}"#,
            r#"{"ev":"mail","mail":"m5","from":"a","to":"b","body":"five","headers":{},"accepted_at_ms":900}"#,
        ]),
    );
    [first, second, live]
}

/// The whole history of `three_segments` given as `[first, second, live]`:
/// each Mail listed once, at its acceptance, with its one Receipt.
fn whole_history(paths: &[String; 3], read: bool) -> Vec<String> {
    let [p1, p2, live] = paths;
    vec![
        format!("SOURCE kind=coordinator path={p1} lines=7"),
        format!("SOURCE kind=coordinator path={p2} lines=13"),
        format!("SOURCE kind=coordinator path={live} lines=15"),
        format!("REGISTER address=a host=h at_ms=100 path={p1} line=1"),
        format!("REGISTER address=b host=h at_ms=110 path={p1} line=2"),
        format!("LABEL address=b at_ms=120 path={p1} line=3 label=two words"),
        format!("MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=200 receipted_at_ms=210 latency_ms=10 record=r-1 receipt_path={p1} receipt_line=5 body_bytes=3 path={p1} line=4"),
        format!("MAIL mail=m2 kind=directed from=a to=b accepted_at_ms=300 receipted_at_ms=450 latency_ms=150 record=r-2 receipt_path={p2} receipt_line=10 body_bytes=3 path={p1} line=6"),
        format!("CLAIM key=k owner=a expires_ms=9000 path={p1} line=7"),
        format!("SNAPSHOT path={p2} seq=1 prior=coord.log.1 at_ms=400 next_record=absent lines=7 read={read}"),
        format!("MAIL mail=m3 kind=directed from=a to=b accepted_at_ms=500 receipted_at_ms=520 latency_ms=20 record=r-3 receipt_path={p2} receipt_line=12 body_bytes=5 path={p2} line=11"),
        format!("MAIL mail=m4 kind=directed from=a to=b accepted_at_ms=600 receipted_at_ms=800 latency_ms=200 record=r-4 receipt_path={live} receipt_line=14 body_bytes=4 path={p2} line=13"),
        format!("SNAPSHOT path={live} seq=2 prior=coord.log.2 at_ms=700 next_record=absent lines=11 read={read}"),
        format!("MAIL mail=m5 kind=directed from=a to=b accepted_at_ms=900 receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=4 path={live} line=15"),
        format!("UNRECEIPTED mail=m5 kind=directed from=a to=b accepted_at_ms=900 path={live} line=15"),
        "AUDIT sources=3 registers=2 unregisters=0 mails=5 receipted=4 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=2 culled=0".into(),
    ]
}

#[test]
fn audit_archives_copied_elsewhere_still_read_as_one_history() {
    let dir = Dir::new("copied-archives");
    let [p1, p2, live] = three_segments(&dir, "live");
    let args = |paths: &[&str]| -> Vec<String> {
        paths
            .iter()
            .flat_map(|p| ["--coordinator-log".to_string(), p.to_string()])
            .collect()
    };
    let run = |paths: &[&str], expected: &[String]| {
        let a = args(paths);
        assert_listing(&a.iter().map(String::as_str).collect::<Vec<_>>(), expected);
    };
    // In place: each block's archive is the file beside it.
    run(
        &[&p1, &p2, &live],
        &whole_history(&[p1.clone(), p2.clone(), live.clone()], false),
    );

    // Copied to another directory under the same names: matched by name.
    fs::create_dir_all(dir.0.join("backup")).unwrap();
    let b1 = dir.write("backup/coord.log.1", &fs::read_to_string(&p1).unwrap());
    let b2 = dir.write("backup/coord.log.2", &fs::read_to_string(&p2).unwrap());
    run(
        &[&b1, &b2, &live],
        &whole_history(&[b1.clone(), b2.clone(), live.clone()], false),
    );

    // Copied under other names: nothing matches, every block is read, and
    // what each restates is already shown, so nothing is listed twice.
    let r1 = dir.write("backup/first.log", &fs::read_to_string(&p1).unwrap());
    let r2 = dir.write("backup/second.log", &fs::read_to_string(&p2).unwrap());
    run(
        &[&r1, &r2, &live],
        &whole_history(&[r1.clone(), r2.clone(), live.clone()], true),
    );
}

#[test]
fn audit_left_out_middle_archive_lists_each_mail_once() {
    let dir = Dir::new("middle-left-out");
    let [p1, _, live] = three_segments(&dir, "logs");
    // The block in `coord.log` restates `coord.log.2`, which is not given:
    // what `coord.log.1` already shows is not listed again, and what only
    // the left-out archive held is listed from the block.
    assert_listing(
        &["--coordinator-log", &p1, "--coordinator-log", &live],
        &[
            format!("SOURCE kind=coordinator path={p1} lines=7"),
            format!("SOURCE kind=coordinator path={live} lines=15"),
            format!("REGISTER address=a host=h at_ms=100 path={p1} line=1"),
            format!("REGISTER address=b host=h at_ms=110 path={p1} line=2"),
            format!("LABEL address=b at_ms=120 path={p1} line=3 label=two words"),
            format!("MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=200 receipted_at_ms=210 latency_ms=10 record=r-1 receipt_path={p1} receipt_line=5 body_bytes=3 path={p1} line=4"),
            format!("MAIL mail=m2 kind=directed from=a to=b accepted_at_ms=300 receipted_at_ms=450 latency_ms=150 record=r-2 receipt_path={live} receipt_line=9 body_bytes=3 path={p1} line=6"),
            format!("CLAIM key=k owner=a expires_ms=9000 path={p1} line=7"),
            format!("SNAPSHOT path={live} seq=2 prior=coord.log.2 at_ms=700 next_record=absent lines=11 read=true"),
            format!("MAIL mail=m3 kind=directed from=a to=b accepted_at_ms=500 receipted_at_ms=520 latency_ms=20 record=r-3 receipt_path={live} receipt_line=11 body_bytes=5 path={live} line=10"),
            format!("MAIL mail=m4 kind=directed from=a to=b accepted_at_ms=600 receipted_at_ms=800 latency_ms=200 record=r-4 receipt_path={live} receipt_line=14 body_bytes=4 path={live} line=12"),
            format!("MAIL mail=m5 kind=directed from=a to=b accepted_at_ms=900 receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=4 path={live} line=15"),
            format!("UNRECEIPTED mail=m5 kind=directed from=a to=b accepted_at_ms=900 path={live} line=15"),
            "AUDIT sources=2 registers=2 unregisters=0 mails=5 receipted=4 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=0".into(),
        ],
    );
}

#[test]
fn audit_one_file_given_twice_is_read_once() {
    let dir = Dir::new("given-twice");
    let [p1, p2, _] = three_segments(&dir, "logs");
    // What a rotation that stopped after linking its archive leaves: the
    // path and `coord.log.2` are one file.
    let live = dir.0.join("logs/coord.log");
    fs::remove_file(&live).unwrap();
    fs::hard_link(&p2, &live).unwrap();
    let live = live.to_str().unwrap().to_string();
    assert_listing(
        &[
            "--coordinator-log",
            &p1,
            "--coordinator-log",
            &p2,
            "--coordinator-log",
            &live,
        ],
        &[
            format!("SOURCE kind=coordinator path={p1} lines=7"),
            format!("SOURCE kind=coordinator path={p2} lines=13"),
            format!("SOURCE kind=coordinator path={live} lines=13"),
            format!("REGISTER address=a host=h at_ms=100 path={p1} line=1"),
            format!("REGISTER address=b host=h at_ms=110 path={p1} line=2"),
            format!("LABEL address=b at_ms=120 path={p1} line=3 label=two words"),
            format!("MAIL mail=m1 kind=directed from=a to=b accepted_at_ms=200 receipted_at_ms=210 latency_ms=10 record=r-1 receipt_path={p1} receipt_line=5 body_bytes=3 path={p1} line=4"),
            format!("MAIL mail=m2 kind=directed from=a to=b accepted_at_ms=300 receipted_at_ms=450 latency_ms=150 record=r-2 receipt_path={p2} receipt_line=10 body_bytes=3 path={p1} line=6"),
            format!("CLAIM key=k owner=a expires_ms=9000 path={p1} line=7"),
            format!("SNAPSHOT path={p2} seq=1 prior=coord.log.1 at_ms=400 next_record=absent lines=7 read=false"),
            format!("MAIL mail=m3 kind=directed from=a to=b accepted_at_ms=500 receipted_at_ms=520 latency_ms=20 record=r-3 receipt_path={p2} receipt_line=12 body_bytes=5 path={p2} line=11"),
            format!("MAIL mail=m4 kind=directed from=a to=b accepted_at_ms=600 receipted_at_ms=absent latency_ms=absent record=pending receipt_path=absent receipt_line=absent body_bytes=4 path={p2} line=13"),
            format!("UNRECEIPTED mail=m4 kind=directed from=a to=b accepted_at_ms=600 path={p2} line=13"),
            "AUDIT sources=3 registers=2 unregisters=0 mails=4 receipted=3 unreceipted=1 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=1 culled=0".into(),
        ],
    );

    // One path given twice, and a host daemon log and a Store given twice
    // (one of each through a hard link): each is listed as a source and read
    // once, its torn tail and skipped lines reported once.
    let d = dir.write(
        "d.log",
        &(lines_of(&[
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
            "not json",
        ]) + "{\"ev\""),
    );
    let s = dir.write(
        "s.store",
        &lines_of(&[r#"{"record":"r-1","mail":"m1","from":"a","body":"one","headers":{}}"#]),
    );
    let s2 = dir.0.join("s2.store");
    fs::hard_link(&s, &s2).unwrap();
    let s2 = s2.to_str().unwrap().to_string();
    assert_listing(
        &["--daemon-log", &d, "--daemon-log", &d, "--store", &s, "--store", &s2],
        &[
            format!("SOURCE kind=daemon path={d} lines=2"),
            format!("TORN path={d} bytes=5"),
            format!("SKIPPED path={d} line=2"),
            format!("SOURCE kind=daemon path={d} lines=2"),
            format!("SOURCE kind=store path={s} lines=1"),
            format!("SOURCE kind=store path={s2} lines=1"),
            format!("DAEMON_RECEIPT path={d} mail=m1 session=b record=r-1 line=1"),
            format!("STORE path={s} record=r-1 mail=m1 from=a kind=absent line=1"),
            "AUDIT sources=4 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=0 torn=1 skipped=1 snapshots=0 culled=0".into(),
        ],
    );
}

#[test]
fn audit_daemon_and_store_blocks_read_elsewhere_add_only_what_is_new() {
    let dir = Dir::new("moved-daemon-store");
    for sub in ["host-a", "host-b", "backup"] {
        fs::create_dir_all(dir.0.join(sub)).unwrap();
    }
    let archive = lines_of(&[
        r#"{"ev":"deliver","mail":"m1","from":"a","to":"b","body":"x","kind":"directed","headers":{}}"#,
        r#"{"ev":"deliver","mail":"m2","from":"a","to":"b","body":"y","kind":"directed","headers":{}}"#,
        r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
    ]);
    let old = dir.write("backup/daemon.log.1", &archive);
    let live = dir.write(
        "host-a/daemon.log",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"daemon.log.1"}"#,
            r#"{"ev":"deliver","mail":"m2","from":"a","to":"b","body":"y","kind":"directed","headers":{}}"#,
            r#"{"ev":"receipt","mail":"m1","session":"b","record":"r-1"}"#,
            r#"{"snapshot_end":1}"#,
            r#"{"ev":"receipt","mail":"m2","session":"b","record":"r-2"}"#,
        ]),
    );
    // Another host's live log names an archive of the same name, which is
    // not its own: its block is read and what it restates is listed.
    let other = dir.write(
        "host-b/daemon.log",
        &lines_of(&[
            r#"{"snapshot":4,"prior":"daemon.log.1"}"#,
            r#"{"ev":"deliver","mail":"m9","from":"a","to":"c","body":"z","kind":"directed","headers":{}}"#,
            r#"{"snapshot_end":4}"#,
        ]),
    );
    let s_old = dir.write(
        "backup/s.store.1",
        &lines_of(&[
            r#"{"record":"r-1","mail":"m1","from":"a","body":"x","kind":"directed","headers":{}}"#,
            r#"{"record":"r-2","mail":"m2","from":"a","body":"y","kind":"directed","headers":{}}"#,
        ]),
    );
    let s_live = dir.write(
        "host-a/s.store",
        &lines_of(&[
            r#"{"snapshot":1,"prior":"s.store.1","next_record":4}"#,
            r#"{"record":"r-1","mail":"m1","culled":true}"#,
            r#"{"record":"r-2","mail":"m2","from":"a","body":"y","kind":"directed","headers":{}}"#,
            r#"{"record":"r-3","mail":"m3","from":"a","body":"w","kind":"directed","headers":{}}"#,
            r#"{"snapshot_end":1}"#,
        ]),
    );
    assert_listing(
        &[
            "--daemon-log",
            &old,
            "--daemon-log",
            &live,
            "--daemon-log",
            &other,
            "--store",
            &s_old,
            "--store",
            &s_live,
        ],
        &[
            format!("SOURCE kind=daemon path={old} lines=3"),
            format!("SOURCE kind=daemon path={live} lines=5"),
            format!("SOURCE kind=daemon path={other} lines=3"),
            format!("SOURCE kind=store path={s_old} lines=2"),
            format!("SOURCE kind=store path={s_live} lines=5"),
            format!("DAEMON_DELIVER path={old} mail=m1 kind=directed from=a to=b line=1"),
            format!("DAEMON_DELIVER path={old} mail=m2 kind=directed from=a to=b line=2"),
            format!("DAEMON_RECEIPT path={old} mail=m1 session=b record=r-1 line=3"),
            format!("SNAPSHOT path={live} seq=1 prior=daemon.log.1 at_ms=absent next_record=absent lines=2 read=true"),
            format!("DAEMON_RECEIPT path={live} mail=m2 session=b record=r-2 line=5"),
            format!("SNAPSHOT path={other} seq=4 prior=daemon.log.1 at_ms=absent next_record=absent lines=1 read=true"),
            format!("DAEMON_DELIVER path={other} mail=m9 kind=directed from=a to=c line=2"),
            format!("STORE path={s_old} record=r-1 mail=m1 from=a kind=directed line=1"),
            format!("STORE path={s_old} record=r-2 mail=m2 from=a kind=directed line=2"),
            format!("SNAPSHOT path={s_live} seq=1 prior=s.store.1 at_ms=absent next_record=4 lines=3 read=true"),
            format!("STORE path={s_live} record=r-3 mail=m3 from=a kind=directed line=4"),
            "AUDIT sources=5 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0 snapshots=3 culled=0".into(),
        ],
    );
}

//! `roger audit` over fixture files of every line generation: exact text and
//! `--json` output, `--address`, findings, bodies, and a read of logs a live
//! coordinator, host daemon and Tracer hold locked. Expected values are the
//! literals chosen here, from the brief's line formats.

#[cfg(not(feature = "test-hooks"))]
compile_error!("tests/audit.rs needs the test hooks: cargo test --features test-hooks");

use roger_wire::json;

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
            ];
            let v = if raw == "absent" {
                Value::Null
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
    filtered.push("AUDIT sources=3 registers=1 unregisters=1 mails=5 receipted=3 unreceipted=2 duplicates=0 mismatches=0 torn=1 skipped=1".into());
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
    let summary = "AUDIT sources=1 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0\n";
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
/// A live coordinator, host daemon and Tracer hold their records open while
/// audits read them: one audit changes no byte, then 8 concurrent
/// `--bodies --json` audits run while a writer sends Mail, every one
/// complete and error-free. Works whether the records are logs or SQLite.
#[test]
fn audit_live_locked_logs_are_read_only() {
    let dir = Dir::new("live");
    let p = |name: &str| dir.0.join(name).to_str().unwrap().to_owned();
    let (c, d, s, port) = (p("c"), p("d"), p("s"), p("port"));
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
    let dport = p("dport");
    let _daemon = processes.spawn(&[
        "daemon",
        "--coordinator",
        &addr,
        "--host-id",
        "host",
        "--listen",
        "127.0.0.1:0",
        "--port-file",
        &dport,
        "--data",
        &d,
    ]);
    wait_until(|| fs::read_to_string(&dport).is_ok());
    let target = format!(
        "tcp://127.0.0.1:{}",
        fs::read_to_string(&dport).unwrap().trim()
    );
    let mut tracer = processes.spawn(&["session", "--socket", &target, "--id", A, "--store", &s]);
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
                "send", "--socket", &target, "--from", B, "--to", A, "--mail", id, "--body", "hi",
            ])
            .output()
            .unwrap()
    };
    let inputs = ["--coordinator-log", &c, "--daemon-log", &d, "--store", &s];
    assert!(send("live-1").status.success());
    wait_until(|| {
        let out = Command::new(BIN)
            .args(["receipt", "--socket", &target, "--mail", "live-1"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).starts_with("RECEIPT mail=live-1 ")
    });
    let receipted = |out: &str, id: &str| {
        out.lines()
            .any(|l| l.starts_with(&format!("MAIL mail={id} ")) && !l.contains(" record=pending "))
    };
    wait_until(|| receipted(&audit_ok(&inputs), "live-1"));
    // Logs are plain files; SQLite records live beside these paths.
    let bytes = || (fs::read(&c).ok(), fs::read(&d).ok(), fs::read(&s).unwrap());
    let before = bytes();
    let out = audit_ok(&inputs);
    assert!(out.contains(&format!("REGISTER address={A} host=host at_ms=")));
    assert!(out
        .lines()
        .any(|l| l.starts_with("MAIL mail=live-1 ") && l.contains(" record=r-1 ")));
    assert_eq!(before, bytes(), "audit changed a live input");

    let sent = 20;
    let writing = std::sync::atomic::AtomicBool::new(true);
    let failures = std::sync::Mutex::new(Vec::new());
    let runs = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let auditors: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| loop {
                    let last = !writing.load(std::sync::atomic::Ordering::SeqCst);
                    let out = audit(&[
                        "--coordinator",
                        &c,
                        "--daemon",
                        &d,
                        "--store",
                        &s,
                        "--bodies",
                        "--json",
                    ]);
                    runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let text = String::from_utf8_lossy(&out.stdout).into_owned();
                    let rows: Vec<_> = text.lines().map(json::parse).collect();
                    let complete = out.status.success()
                        && rows.iter().all(Result::is_ok)
                        && text.contains(r#""type":"MAIL","mail":"live-1""#)
                        && rows.last().and_then(|r| r.as_ref().ok()).and_then(|r| {
                            r.get("type").and_then(Value::as_str).map(str::to_string)
                        }) == Some("AUDIT".to_string());
                    if !complete {
                        failures
                            .lock()
                            .unwrap()
                            .push(format!("{}\n{text}", String::from_utf8_lossy(&out.stderr)));
                    }
                    if last {
                        break;
                    }
                })
            })
            .collect();
        for i in 0..sent {
            assert!(send(&format!("live-w-{i}")).status.success());
        }
        writing.store(false, std::sync::atomic::Ordering::SeqCst);
        for auditor in auditors {
            auditor.join().unwrap();
        }
    });
    let failures = failures.into_inner().unwrap();
    assert!(
        failures.is_empty(),
        "concurrent audits failed: {failures:?}"
    );
    assert!(runs.into_inner() >= 16);
    wait_until(|| {
        let out = audit_ok(&inputs);
        (0..sent).all(|i| receipted(&out, &format!("live-w-{i}")))
    });
    // End the processes while the Tracer's stdout is still open: Mail can
    // reach it before this returns, and a print to a closed pipe is a panic.
    drop(processes);
}

#[test]
fn audit_short_input_flags_are_aliases() {
    let dir = Dir::new("aliases");
    let (c, d, s) = fixture(&dir);
    assert_eq!(
        audit_ok(&["--coordinator", &c, "--daemon", &d, "--store", &s]),
        audit_ok(&["--coordinator-log", &c, "--daemon-log", &d, "--store", &s])
    );
    let twice = audit(&["--coordinator", &c, "--coordinator-log", &c]);
    assert!(!twice.status.success());
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
        expected.push(format!("AUDIT sources=2 registers=0 unregisters=0 mails=0 receipted=0 unreceipted=0 duplicates=0 mismatches={mismatches} torn=0 skipped=0"));
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
        "AUDIT sources=3 registers=3 unregisters=1 mails=6 receipted=4 unreceipted=2 duplicates=0 mismatches=0 torn=1 skipped=1".to_string(),
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

// Pruned group articles in a coordinator slice store.
// The DDL is the coordinator slice schema's tables audit reads, verbatim,
// with the prune floor column on `streams`.
const SLICE_DDL: &str = "
CREATE TABLE meta (key TEXT PRIMARY KEY, val TEXT NOT NULL);
CREATE TABLE live_sessions (address TEXT PRIMARY KEY, host TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '', declared TEXT, at_ms INTEGER);
CREATE TABLE live_claims (claim_key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_ms INTEGER NOT NULL);
CREATE TABLE timeline (seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, addr TEXT, host TEXT,
    old_host TEXT, label TEXT, declared TEXT, registration TEXT, claim_key TEXT,
    expires_ms INTEGER, mail_id TEXT, at_ms INTEGER, raw_line INTEGER);
CREATE TABLE streams (stream TEXT PRIMARY KEY, kind TEXT NOT NULL, next_article INTEGER NOT NULL,
    keep_last INTEGER, ttl_ms INTEGER, retired_at_ms INTEGER,
    pruned_through INTEGER NOT NULL DEFAULT 0);
CREATE TABLE seen_ids (mail_id TEXT PRIMARY KEY, kind TEXT NOT NULL, sender TEXT NOT NULL,
    target TEXT, body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL,
    article_num INTEGER NOT NULL DEFAULT 0, stream TEXT);
CREATE TABLE broadcasts (mail_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, sender TEXT NOT NULL,
    body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL, headers_json TEXT,
    recipients_json TEXT NOT NULL, accepted_at_ms INTEGER, raw_line INTEGER);
CREATE TABLE envelopes (seq INTEGER PRIMARY KEY, mail_id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL, article_num INTEGER NOT NULL, kind TEXT NOT NULL,
    sender TEXT NOT NULL, recipient TEXT NOT NULL, headers_json TEXT,
    body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL, accepted_at_ms INTEGER,
    day_bucket INTEGER NOT NULL, in_cold INTEGER NOT NULL DEFAULT 0, raw_line INTEGER,
    UNIQUE(stream, article_num));
CREATE TABLE hot_bodies (mail_id TEXT PRIMARY KEY, body TEXT NOT NULL);
CREATE TABLE positions (stream TEXT NOT NULL, reader TEXT NOT NULL,
    subscribed INTEGER NOT NULL DEFAULT 1, position INTEGER NOT NULL DEFAULT 0,
    record_id TEXT, receipted_at_ms INTEGER, updated_at_ms INTEGER,
    PRIMARY KEY(stream, reader));
CREATE TABLE receipts (mail_id TEXT NOT NULL, reader TEXT NOT NULL, stream TEXT NOT NULL,
    article_num INTEGER NOT NULL, record_id TEXT NOT NULL, receipted_at_ms INTEGER,
    receipt_seq INTEGER NOT NULL, raw_line INTEGER, PRIMARY KEY(mail_id, reader));
";

/// A slice store at `<dir>/coord.slices/slice-00/hot.sqlite` holding Group
/// `news` pruned through article 1: g-1 (pruned, no hot body) and g-2 (hot
/// body), with subscriber r3 holding no receipts. Returns the slice dir.
fn pruned_slice(dir: &Dir) -> PathBuf {
    let slice = dir.0.join("coord.slices").join("slice-00");
    fs::create_dir_all(&slice).unwrap();
    let conn = rusqlite::Connection::open(slice.join("hot.sqlite")).unwrap();
    conn.execute_batch(SLICE_DDL).unwrap();
    conn.execute_batch(
        "INSERT INTO streams(stream, kind, next_article, pruned_through) VALUES ('g:news', 'group', 3, 1);
         INSERT INTO envelopes VALUES (1, 'g-1', 'g:news', 1, 'group', 's', 'news', NULL, 3, 'x', 100, 0, 0, NULL);
         INSERT INTO envelopes VALUES (2, 'g-2', 'g:news', 2, 'group', 's', 'news', NULL, 4, 'x', 200, 0, 0, NULL);
         INSERT INTO hot_bodies VALUES ('g-2', 'kept');
         INSERT INTO positions(stream, reader, position) VALUES ('g:news', 'r3', 0);",
    )
    .unwrap();
    slice
}

#[test]
fn pruned_group_article_is_marked_absent_and_not_owed() {
    let dir = Dir::new("pruned");
    let slice = pruned_slice(&dir);
    let db = slice.join("hot.sqlite");
    let out = audit_ok(&["--coordinator", db.to_str().unwrap(), "--bodies"]);
    let lines: Vec<&str> = out.lines().collect();
    let mail = |id: &str| {
        lines
            .iter()
            .find(|l| l.starts_with(&format!("MAIL mail={id} ")))
            .unwrap_or_else(|| panic!("no MAIL line for {id}: {out}"))
            .to_string()
    };
    let g1 = mail("g-1");
    assert!(g1.ends_with(" pruned=true"), "{g1}");
    assert!(g1.contains(" line="), "{g1}");
    let g2 = mail("g-2");
    assert!(!g2.contains("pruned"), "{g2}");
    assert!(lines.contains(&"BODY mail=g-1 absent"), "{out}");
    assert!(lines.contains(&"BODY mail=g-2 \"kept\""), "{out}");
    assert!(
        lines.iter().any(|l| l.starts_with("UNRECEIPTED mail=g-2 ")),
        "{out}"
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("UNRECEIPTED mail=g-1 ")),
        "{out}"
    );
}

#[test]
fn audit_with_junk_cold_index_writes_nothing() {
    let dir = Dir::new("cold-junk");
    let slice = pruned_slice(&dir);
    let cold = slice.join("cold");
    fs::create_dir_all(cold.join("empty")).unwrap();
    fs::write(cold.join("day-0.idx"), b"junk \x00\xff not an index\n").unwrap();
    let snapshot = |p: &std::path::Path| {
        let mut entries: Vec<(PathBuf, Option<Vec<u8>>)> = fs::read_dir(p)
            .unwrap()
            .map(|e| {
                let path = e.unwrap().path();
                let bytes = path.is_file().then(|| fs::read(&path).unwrap());
                (path, bytes)
            })
            .collect();
        entries.sort();
        entries
    };
    let before = (snapshot(&cold), snapshot(&cold.join("empty")));
    let db = slice.join("hot.sqlite");
    let out = audit_ok(&["--coordinator", db.to_str().unwrap(), "--bodies"]);
    assert!(out.contains("MAIL mail=g-2 "), "{out}");
    assert_eq!((snapshot(&cold), snapshot(&cold.join("empty"))), before);
}

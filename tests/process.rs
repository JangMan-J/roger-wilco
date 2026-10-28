//! Process-level seam: real coordinator, daemon, and session processes on
//! loopback. Expected values are the literals chosen here, per proof 1.
//! The SIGKILL window needs the session's `--hold-before-ack` test hook.

#[cfg(not(feature = "test-hooks"))]
compile_error!("tests/process.rs needs the test hooks: cargo test --features test-hooks");

use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_roger");
const TOKEN: &str = "process-test-token";
const SENDER: &str = "11111111-1111-4111-8111-111111111111";
const RECIPIENT: &str = "22222222-2222-4222-8222-222222222222";

#[test]
fn cli_help_and_unknown_command() {
    for flag in ["--help", "-h", "help"] {
        let output = Command::new(BIN)
            .arg(flag)
            .env_remove("ROGER_TOKEN")
            .output()
            .unwrap();
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(help.starts_with("Usage: roger"));
        for command in [
            "coordinator",
            "daemon",
            "session",
            "adapter",
            "send",
            "claim",
            "renew",
            "release",
            "board",
            "label",
            "receipt",
        ] {
            assert!(help.contains(command));
        }
        assert!(output.stderr.is_empty());
    }
    let output = Command::new(BIN).arg("unknown-command").output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .starts_with("Usage: roger"));
}

struct Rig {
    dir: PathBuf,
    children: Vec<Child>,
    coord_port: String,
    coord_idx: usize,
    /// Extra coordinator flags, applied to the first start and every restart.
    coord_extra: Vec<String>,
}

impl Rig {
    fn new(tag: &str) -> Rig {
        // No PID in the name: Rig::new clears the directory, so each tag owns
        // one directory and a failing run's logs survive until the next run of
        // that tag instead of being deleted on drop. The checkout is part of
        // the name so sibling worktrees testing at once never share one.
        let tree = env!("CARGO_MANIFEST_DIR")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        let dir = std::env::temp_dir().join(format!("roger-process-{tree:016x}-{tag}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Rig {
            dir,
            children: Vec::new(),
            coord_port: String::new(),
            coord_idx: 0,
            coord_extra: Vec::new(),
        }
    }

    fn p(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn spawn(&mut self, args: &[&str], stdout_name: &str) -> usize {
        let out = fs::File::create(self.p(stdout_name)).unwrap();
        let child = Command::new(BIN)
            .args(args)
            .env("ROGER_TOKEN", TOKEN)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(
                fs::File::create(self.p(&format!("{stdout_name}.err"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        self.children.push(child);
        self.children.len() - 1
    }

    /// Start `roger adapter` on a host's daemon socket. Returns its stdin, the
    /// only way a shim speaks to it; its stdout is the file `out`.
    fn start_adapter(
        &mut self,
        sock: &str,
        id: &str,
        store: &str,
        out: &str,
        extra: &[&str],
    ) -> (usize, ChildStdin) {
        let child_out = fs::File::create(self.p(out)).unwrap();
        let mut child = Command::new(BIN)
            .args([
                "adapter",
                "--socket",
                self.p(sock).to_str().unwrap(),
                "--id",
                id,
                "--store",
                self.p(store).to_str().unwrap(),
            ])
            .args(extra)
            .stdin(Stdio::piped())
            .stdout(Stdio::from(child_out))
            .stderr(Stdio::from(
                fs::File::create(self.p(&format!("{out}.err"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        self.children.push(child);
        wait_for_text(&self.p(out), "\"type\":\"ready\"", Duration::from_secs(10));
        (self.children.len() - 1, stdin)
    }

    /// The coordinator argv, shared by the first start and every restart so a
    /// flag change cannot reach one and miss the other.
    fn coordinator_args(&self, bind: &str, port_file: &Path, data: &Path) -> Vec<String> {
        vec![
            "coordinator".into(),
            "--bind".into(),
            bind.into(),
            "--port-file".into(),
            port_file.to_str().unwrap().into(),
            "--data".into(),
            data.to_str().unwrap().into(),
        ]
        .into_iter()
        .chain(self.coord_extra.iter().cloned())
        .collect()
    }

    fn start_stack(&mut self) {
        let port_file = self.p("coord.port");
        let args = self.coordinator_args("127.0.0.1:0", &port_file, &self.p("coord.log"));
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.spawn(&arg_refs, "coord.out");
        wait_for_file(&port_file, Duration::from_secs(10));
        let port = fs::read_to_string(&port_file).unwrap().trim().to_string();
        self.coord_port = port.clone();
        let coord = format!("127.0.0.1:{port}");
        for (host, sock, data, out) in [
            ("host-a", "a.sock", "a.log", "daemon-a.out"),
            ("host-b", "b.sock", "b.log", "daemon-b.out"),
        ] {
            self.spawn(
                &[
                    "daemon",
                    "--coordinator",
                    &coord,
                    "--host-id",
                    host,
                    "--socket",
                    self.p(sock).to_str().unwrap(),
                    "--data",
                    self.p(data).to_str().unwrap(),
                ],
                out,
            );
            wait_for_file(&self.p(sock), Duration::from_secs(10));
        }
    }

    fn start_recipient(&mut self, out: &str, hold: bool) -> usize {
        self.start_recipient_with(out, hold, None)
    }

    /// Start the recipient session on host-b, announcing `label` verbatim
    /// at Register when one is given.
    fn start_recipient_with(&mut self, out: &str, hold: bool, label: Option<&str>) -> usize {
        let mut args: Vec<String> = vec![
            "session".to_string(),
            "--socket".to_string(),
            self.p("b.sock").to_str().unwrap().to_string(),
            "--id".to_string(),
            RECIPIENT.to_string(),
            "--store".to_string(),
            self.p("recipient.store").to_str().unwrap().to_string(),
        ];
        if hold {
            args.push("--hold-before-ack".to_string());
            args.push(self.p("held.marker").to_str().unwrap().to_string());
        }
        if let Some(label) = label {
            args.push("--label".to_string());
            args.push(label.to_string());
        }
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let idx = self.spawn(&arg_refs, out);
        wait_for_text(&self.p(out), "READY", Duration::from_secs(10));
        idx
    }

    /// Register `id` as a session on one host's daemon: a Tracer with its own
    /// Store, kept alive so the Address stays registered.
    fn register(&mut self, sock: &str, id: &str) -> usize {
        let tag = &sock[..1];
        let out = format!("session-{tag}-{}.out", &id[..4]);
        let store = self.p(&format!("session-{tag}-{}.store", &id[..4]));
        let idx = self.spawn(
            &[
                "session",
                "--socket",
                self.p(sock).to_str().unwrap(),
                "--id",
                id,
                "--store",
                store.to_str().unwrap(),
            ],
            &out,
        );
        wait_for_text(&self.p(&out), "READY", Duration::from_secs(10));
        idx
    }

    fn send(&self, mail: &str, body: &str) -> String {
        let output = Command::new(BIN)
            .args([
                "send",
                "--socket",
                self.p("a.sock").to_str().unwrap(),
                "--from",
                SENDER,
                "--to",
                RECIPIENT,
                "--mail",
                mail,
                "--body",
                body,
            ])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        assert!(
            output.status.success(),
            "send failed: {stdout} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        stdout
    }

    fn kill(&mut self, idx: usize) {
        self.children[idx].kill().unwrap(); // SIGKILL on Unix
        self.children[idx].wait().unwrap();
    }

    /// Restart the coordinator on the same port, replaying the same data
    /// file, so the host daemon's retry finds it again.
    fn restart_coordinator(&mut self) {
        self.restart_coordinator_on("coord.log", "coord2");
    }

    /// Restart the coordinator on the same port, replaying `data`. A different
    /// data file gives a coordinator that does not know the mail this host has
    /// already received.
    fn restart_coordinator_on(&mut self, data: &str, tag: &str) {
        self.kill(self.coord_idx);
        let bind = format!("127.0.0.1:{}", self.coord_port);
        let port_file = self.p(&format!("{tag}.port"));
        let start = Instant::now();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let _ = fs::remove_file(&port_file);
            let args = self.coordinator_args(&bind, &port_file, &self.p(data));
            let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            // A file per attempt: overwriting one name would hide the earlier
            // failures the panic message is supposed to report.
            let out = format!("{tag}-{attempt}.out");
            let idx = self.spawn(&arg_refs, &out);
            let deadline = Instant::now() + Duration::from_millis(1500);
            while Instant::now() < deadline {
                if port_file.exists() {
                    self.coord_idx = idx;
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            self.kill(idx);
            let err = read(&self.p(&format!("{tag}-{attempt}.out.err")));
            // Only a port still held by the killed process is worth retrying;
            // any other exit is a hard failure and must not be masked.
            if !err.contains("Address already in use") {
                panic!("coordinator failed to rebind {bind} on attempt {attempt}: {err}");
            }
            if start.elapsed() > Duration::from_secs(15) {
                panic!("coordinator never rebound {bind}; last attempt: {err}");
            }
        }
    }

    /// Restart the host daemon that serves the recipient, replaying the same
    /// data file, so receipts committed to that log are replayed from disk.
    fn restart_recipient_daemon(&mut self) {
        self.restart_recipient_daemon_as("host-b");
    }

    /// Restart that host daemon under Host id `host`, same socket and log.
    fn restart_recipient_daemon_as(&mut self, host: &str) {
        self.kill(2);
        let coord = format!("127.0.0.1:{}", self.coord_port);
        let args: Vec<String> = vec![
            "daemon".into(),
            "--coordinator".into(),
            coord,
            "--host-id".into(),
            host.into(),
            "--socket".into(),
            self.p("b.sock").to_str().unwrap().into(),
            "--data".into(),
            self.p("b.log").to_str().unwrap().into(),
        ];
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        let _ = fs::remove_file(self.p("b.sock"));
        self.spawn(&arg_refs, "daemon-b2.out");
        wait_for_file(&self.p("b.sock"), Duration::from_secs(10));
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        // A panicking test unwinds through Drop, so cleaning up
        // unconditionally would delete the very logs the panic message cannot
        // carry. Keep them on failure; Rig::new clears the directory before
        // the next run of this tag.
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

fn read(path: &Path) -> String {
    let mut s = String::new();
    if let Ok(mut f) = fs::File::open(path) {
        let _ = f.read_to_string(&mut s);
    }
    s
}

fn wait_for_file(path: &Path, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {}", path.display());
}

fn wait_for_text(path: &Path, needle: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if read(path).contains(needle) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "timed out waiting for {needle:?} in {}: got {:?}",
        path.display(),
        read(path)
    );
}

fn count_lines_with(text: &str, needle: &str) -> usize {
    text.lines().filter(|l| l.contains(needle)).count()
}

#[test]
fn receipt_survives_a_host_daemon_restart() {
    let mut rig = Rig::new("daemon-restart");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);

    rig.send("m-restart", "acked before the host daemon restarts");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));

    // Coordinator down, so the ack can only be committed to the host log.
    rig.kill(0);
    rig.kill(recipient);
    rig.start_recipient("recipient2.out", true);
    wait_for_text(
        &rig.p("b.log"),
        "\"ev\":\"receipt\",\"mail\":\"m-restart\"",
        Duration::from_secs(10),
    );

    // Nothing live may be able to produce the receipt from here on: the
    // session is gone and the daemon that committed it is replaced by one
    // whose receipts come only from replaying that log.
    rig.kill(4);
    rig.restart_recipient_daemon();
    rig.restart_coordinator();

    let start = Instant::now();
    loop {
        let coord = read(&rig.p("coord.log"));
        let rows = count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-restart\"");
        if rows == 1 {
            assert!(coord.contains("\"record\":\"r-1\""), "{coord}");
            return;
        }
        assert!(rows <= 1, "receipt duplicated: {coord}");
        if start.elapsed() > Duration::from_secs(20) {
            panic!("a receipt committed before the daemon restart never reached the coordinator: {coord}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn receipt_survives_an_upstream_that_never_accepted_the_mail() {
    let mut rig = Rig::new("orphan-upstream");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);

    rig.send("m-orphan", "acked before upstream loses the mail");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));

    // Coordinator down, so the receipt is committed locally only.
    rig.kill(0);
    rig.kill(recipient);
    rig.start_recipient("recipient2.out", true);
    wait_for_text(
        &rig.p("b.log"),
        "\"ev\":\"receipt\",\"mail\":\"m-orphan\"",
        Duration::from_secs(10),
    );

    // A coordinator with an empty store never accepted m-orphan, so it cannot
    // record a receipt for it. Its refusal must not be read as confirmation:
    // the receipt has to stay queued on this host.
    rig.restart_coordinator_on("coord-empty.log", "coord3");
    // Wait on the refusal itself, not on the line printed before the replay is
    // sent: waiting earlier lets the next restart win the race, and the test
    // passes whether or not the refusal was ever processed.
    wait_for_text(
        &rig.p("daemon-b.out.err"),
        "upstream refused the replayed receipt for m-orphan",
        Duration::from_secs(15),
    );
    let empty = read(&rig.p("coord-empty.log"));
    assert_eq!(
        count_lines_with(&empty, "\"ev\":\"receipt\""),
        0,
        "a coordinator that never accepted the mail cannot hold a receipt: {empty}"
    );

    // The coordinator that did accept it comes back, and the same receipt must
    // still be queued to reach it.
    rig.restart_coordinator_on("coord.log", "coord4");
    let start = Instant::now();
    loop {
        let coord = read(&rig.p("coord.log"));
        let rows = count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-orphan\"");
        if rows == 1 {
            assert!(coord.contains("\"record\":\"r-1\""), "{coord}");
            return;
        }
        assert!(rows <= 1, "receipt duplicated: {coord}");
        if start.elapsed() > Duration::from_secs(20) {
            panic!("a refusal from one coordinator dropped the receipt for the next: {coord}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn receipt_reaches_the_coordinator_after_it_returns() {
    let mut rig = Rig::new("receipt-late");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);

    rig.send("m-late", "acked while the coordinator is down");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));

    // Coordinator down: the ack can only be committed locally.
    rig.kill(0);
    rig.kill(recipient);
    rig.start_recipient("recipient2.out", true);

    // Pin the ordering before anything is restarted: the receipt must be
    // durable in the host daemon's own log, and the session must be answered,
    // while the coordinator is still down. Without these two waits the rest
    // of the test could pass through a live forward after the restart.
    wait_for_text(
        &rig.p("b.log"),
        "\"ev\":\"receipt\",\"mail\":\"m-late\"",
        Duration::from_secs(10),
    );
    wait_for_text(
        &rig.p("recipient2.out"),
        "DELIVERED mail=m-late record=r-1",
        Duration::from_secs(10),
    );

    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"receipt\""),
        0,
        "no receipt can exist while the coordinator is down: {coord}"
    );

    rig.restart_coordinator();
    let start = Instant::now();
    loop {
        let coord = read(&rig.p("coord.log"));
        if count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-late\"") == 1 {
            assert!(coord.contains("\"record\":\"r-1\""), "{coord}");
            // The restarted coordinator re-pushes pending mail on hello before
            // the replayed ack arrives; the body must still be in the Store
            // exactly once.
            let store = read(&rig.p("recipient.store"));
            assert_eq!(
                store.lines().count(),
                1,
                "re-push after the coordinator returned duplicated the body: {store}"
            );
            return;
        }
        assert!(
            count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-late\"") <= 1,
            "receipt duplicated: {coord}"
        );
        if start.elapsed() > Duration::from_secs(15) {
            panic!("locally committed receipt never reached the coordinator: {coord}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn mail_survives_recipient_kill_between_store_and_ack() {
    let mut rig = Rig::new("kill");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);

    let body = "survive the kill [[0:FLEET:0]]";
    rig.send("m-kill", body);

    // The marker appears only after the body is fsynced to the recipient's
    // Store and before the ack is sent. Kill in that window.
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));
    wait_for_text(
        &rig.p("recipient1.out"),
        "STORED mail=m-kill record=r-1",
        Duration::from_secs(10),
    );
    rig.kill(recipient);

    // No receipt may exist yet.
    let coord = read(&rig.p("coord.log"));
    assert_eq!(count_lines_with(&coord, "\"ev\":\"receipt\""), 0, "{coord}");

    rig.start_recipient("recipient2.out", true);
    wait_for_text(
        &rig.p("recipient2.out"),
        "DELIVERED mail=m-kill record=r-1",
        Duration::from_secs(10),
    );

    let store = read(&rig.p("recipient.store"));
    assert_eq!(
        store.lines().count(),
        1,
        "body exactly once in Store: {store}"
    );
    assert_eq!(count_lines_with(&store, body), 1, "{store}");

    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"mail\",\"mail\":\"m-kill\""),
        1,
        "no duplicate row: {coord}"
    );
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-kill\""),
        1,
        "{coord}"
    );
    assert!(
        coord.contains("\"record\":\"r-1\""),
        "receipt names the Store record: {coord}"
    );
}

#[test]
fn references_keep_both_integers_and_relabel_keeps_the_address() {
    let mut rig = Rig::new("references");
    rig.start_stack();
    rig.start_recipient_with("recipient.out", false, Some("[[153:Meteor Shower:7]]"));

    // Send, store, delivery: both integers come back byte-identical.
    let body = "start [[153:Meteor Shower:7]] then [[0:FLEET:0]]";
    rig.send("m-ref-1", body);
    wait_for_text(
        &rig.p("recipient.out"),
        "DELIVERED mail=m-ref-1",
        Duration::from_secs(10),
    );
    let store = read(&rig.p("recipient.store"));
    assert_eq!(store.lines().count(), 1, "{store}");
    assert_eq!(
        count_lines_with(&store, &format!("\"body\":\"{body}\"")),
        1,
        "body field verbatim: {store}"
    );

    // Relabel from the other host: the display changes, the Address does not.
    let new_label = "[[0:FLEET:0]] relabeled";
    let relabeled = Command::new(BIN)
        .args([
            "label",
            "--socket",
            rig.p("a.sock").to_str().unwrap(),
            "--from",
            RECIPIENT,
            "--label",
            new_label,
        ])
        .output()
        .unwrap();
    let relabeled_out = String::from_utf8_lossy(&relabeled.stdout).to_string();
    assert!(
        relabeled.status.success(),
        "label failed: {relabeled_out} {}",
        String::from_utf8_lossy(&relabeled.stderr)
    );
    assert!(
        relabeled_out.contains(&format!("\"label\":\"{new_label}\"")),
        "new label verbatim: {relabeled_out}"
    );

    // The same Address still owns the same mailbox.
    rig.send("m-ref-2", "after the relabel [[153:Meteor Shower:7]]");
    wait_for_text(
        &rig.p("recipient.out"),
        "DELIVERED mail=m-ref-2",
        Duration::from_secs(10),
    );
    let store = read(&rig.p("recipient.store"));
    assert_eq!(store.lines().count(), 2, "{store}");
    assert_eq!(
        count_lines_with(&store, "\"mail\":\"m-ref-2\""),
        1,
        "{store}"
    );

    // And after the coordinator replays its log, mail still lands there.
    rig.restart_coordinator();
    // The daemon reconnects on its own schedule; a send before that is
    // refused with "no upstream", so retry until one is accepted.
    let start = Instant::now();
    loop {
        let sent = Command::new(BIN)
            .args([
                "send",
                "--socket",
                rig.p("a.sock").to_str().unwrap(),
                "--from",
                SENDER,
                "--to",
                RECIPIENT,
                "--mail",
                "m-ref-3",
                "--body",
                "after the restart [[0:FLEET:0]]",
            ])
            .output()
            .unwrap();
        if sent.status.success() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "send never accepted after the coordinator restart: {}",
            String::from_utf8_lossy(&sent.stderr)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    wait_for_text(
        &rig.p("recipient.out"),
        "DELIVERED mail=m-ref-3",
        Duration::from_secs(10),
    );
    let store = read(&rig.p("recipient.store"));
    assert_eq!(store.lines().count(), 3, "{store}");
    assert_eq!(
        count_lines_with(&store, "\"mail\":\"m-ref-3\""),
        1,
        "{store}"
    );
}

#[test]
fn directed_mail_round_trips_once_in_order() {
    let mut rig = Rig::new("roundtrip");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);

    let accepted = rig.send("m-1", "first body");
    assert!(accepted.contains("ACCEPTED mail=m-1"), "{accepted}");
    rig.send("m-2", "second [[153:Meteor Shower:7]] body");

    wait_for_text(
        &rig.p("recipient.out"),
        "DELIVERED mail=m-2",
        Duration::from_secs(10),
    );

    let store = read(&rig.p("recipient.store"));
    assert_eq!(count_lines_with(&store, "first body"), 1, "{store}");
    assert_eq!(
        count_lines_with(&store, "second [[153:Meteor Shower:7]] body"),
        1,
        "{store}"
    );
    let first = store.find("first body").unwrap();
    let second = store.find("second").unwrap();
    assert!(first < second, "order preserved: {store}");

    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"mail\",\"mail\":\"m-1\""),
        1,
        "{coord}"
    );
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"receipt\",\"mail\":\"m-1\""),
        1,
        "{coord}"
    );
}

#[test]
fn adapter_session_and_tracer_exchange_mail_both_ways() {
    let mut rig = Rig::new("adapter");
    rig.start_stack();
    rig.start_recipient("tracer.out", false);
    let adapter_id = SENDER;
    let (_, mut shim_in) =
        rig.start_adapter("a.sock", adapter_id, "adapter.store", "adapter.out", &[]);

    // Shim -> Tracer: the shim forwards a send tool call as one line.
    let body_out = "from the shim [[153:Meteor Shower:7]]";
    writeln!(
        shim_in,
        "{{\"type\":\"send\",\"to\":\"{RECIPIENT}\",\"mail\":\"s-1\",\"body\":\"{body_out}\"}}"
    )
    .unwrap();
    wait_for_text(
        &rig.p("adapter.out"),
        "\"type\":\"accepted\",\"mail\":\"s-1\"",
        Duration::from_secs(10),
    );
    wait_for_text(
        &rig.p("tracer.out"),
        "DELIVERED mail=s-1",
        Duration::from_secs(10),
    );
    let tracer_store = read(&rig.p("recipient.store"));
    assert_eq!(
        count_lines_with(&tracer_store, body_out),
        1,
        "{tracer_store}"
    );

    // Tracer's host -> adapter: the shim is told the body, but only after
    // the adapter's own Store holds it, and the receipt names that record.
    let body_in = "to the shim [[0:FLEET:0]]";
    let sent = Command::new(BIN)
        .args([
            "send",
            "--socket",
            rig.p("b.sock").to_str().unwrap(),
            "--from",
            RECIPIENT,
            "--to",
            adapter_id,
            "--mail",
            "s-2",
            "--body",
            body_in,
        ])
        .output()
        .unwrap();
    assert!(sent.status.success());
    wait_for_text(
        &rig.p("adapter.out"),
        &format!("\"type\":\"mail\",\"mail\":\"s-2\",\"from\":\"{RECIPIENT}\",\"body\":\"{body_in}\",\"record\":\"r-1\""),
        Duration::from_secs(10),
    );
    let store = read(&rig.p("adapter.store"));
    assert_eq!(count_lines_with(&store, body_in), 1, "{store}");
    wait_for_text(
        &rig.p("coord.log"),
        "\"ev\":\"receipt\",\"mail\":\"s-2\"",
        Duration::from_secs(10),
    );
    let coord = read(&rig.p("coord.log"));
    assert!(coord.contains("\"record\":\"r-1\""), "{coord}");
}

/// An Address may be the whole composite `[[<integer>:<label>:<integer>]]<uuid>`
/// (the label may hold spaces). The core carries it as an opaque string:
/// registration, send, coordinator log, host inbox, Store, reply, Board.
/// A relabel changes the Board's display label and never the Address.
#[test]
fn a_composite_address_round_trips_send_store_deliver_reply_and_survives_relabel() {
    const AGENT: &str = "[[3:Meteor Shower:12]]aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const DIRECTOR: &str = "[[2:FLEET:5]]bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let mut rig = Rig::new("composite");
    rig.start_stack();
    let (_, mut agent_in) = rig.start_adapter("a.sock", AGENT, "agent.store", "agent.out", &[]);
    let out = "director.out";
    let store = rig.p("director.store");
    rig.spawn(
        &[
            "session",
            "--socket",
            rig.p("b.sock").to_str().unwrap(),
            "--id",
            DIRECTOR,
            "--store",
            store.to_str().unwrap(),
            "--label",
            "builder",
        ],
        out,
    );
    wait_for_text(&rig.p(out), "READY", Duration::from_secs(10));

    // Send: agent (through its shim line) -> director, to the whole composite.
    let body_out = "question for [[2:FLEET:5]]";
    let line = format!(
        "{{\"type\":\"send\",\"to\":\"{DIRECTOR}\",\"mail\":\"c-1\",\"body\":\"{body_out}\"}}"
    );
    writeln!(agent_in, "{line}").unwrap();
    wait_for_text(&rig.p(out), "DELIVERED mail=c-1", Duration::from_secs(10));
    let stored = read(&store);
    assert_eq!(
        count_lines_with(&stored, &format!("\"from\":\"{AGENT}\"")),
        1,
        "{stored}"
    );
    assert_eq!(count_lines_with(&stored, body_out), 1, "{stored}");
    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(
            &coord,
            &format!("\"from\":\"{AGENT}\",\"to\":\"{DIRECTOR}\"")
        ),
        1,
        "coordinator log keeps both Addresses whole: {coord}"
    );

    // Reply: director -> agent, to the whole composite; the agent's shim is
    // told the sender Address verbatim and only after its own Store holds it.
    let reply = |mail: &str, body: &str| {
        let sent = Command::new(BIN)
            .args([
                "send",
                "--socket",
                rig.p("b.sock").to_str().unwrap(),
                "--from",
                DIRECTOR,
                "--to",
                AGENT,
                "--mail",
                mail,
                "--body",
                body,
            ])
            .env("ROGER_TOKEN", TOKEN)
            .output()
            .unwrap();
        assert!(
            sent.status.success(),
            "{}",
            String::from_utf8_lossy(&sent.stderr)
        );
    };
    reply("c-2", "answer");
    wait_for_text(
        &rig.p("agent.out"),
        &format!("\"type\":\"mail\",\"mail\":\"c-2\",\"from\":\"{DIRECTOR}\",\"body\":\"answer\",\"record\":\"r-1\""),
        Duration::from_secs(10),
    );
    wait_for_text(
        &rig.p("coord.log"),
        "\"ev\":\"receipt\",\"mail\":\"c-2\"",
        Duration::from_secs(10),
    );
    assert_eq!(count_lines_with(&read(&rig.p("agent.store")), "answer"), 1);

    // Relabel: the display label changes; the Address, inner label included,
    // does not, and its mailbox is the same.
    let relabeled = Command::new(BIN)
        .args([
            "label",
            "--socket",
            rig.p("a.sock").to_str().unwrap(),
            "--from",
            DIRECTOR,
            "--label",
            "renamed",
        ])
        .env("ROGER_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(
        relabeled.status.success(),
        "{}",
        String::from_utf8_lossy(&relabeled.stderr)
    );
    let board = Command::new(BIN)
        .args(["board", "--socket", rig.p("a.sock").to_str().unwrap()])
        .env("ROGER_TOKEN", TOKEN)
        .output()
        .unwrap();
    let board = String::from_utf8_lossy(&board.stdout).to_string();
    assert!(board.contains(&format!("address={DIRECTOR} ")), "{board}");
    assert!(board.contains(&format!("address={AGENT} ")), "{board}");
    assert!(board.contains("label=renamed"), "{board}");
    writeln!(
        agent_in,
        "{{\"type\":\"send\",\"to\":\"{DIRECTOR}\",\"mail\":\"c-3\",\"body\":\"after relabel\"}}"
    )
    .unwrap();
    wait_for_text(&rig.p(out), "DELIVERED mail=c-3", Duration::from_secs(10));
}

#[test]
fn adapter_rejects_a_malformed_send_and_keeps_serving() {
    let mut rig = Rig::new("adapter-malformed");
    rig.start_stack();
    rig.start_recipient("tracer.out", false);
    let (_, mut shim_in) = rig.start_adapter("a.sock", SENDER, "adapter.store", "adapter.out", &[]);

    // Not JSON, then a mail id of the wrong type: each is an error reply and
    // neither reaches the core (a wrong-typed id must not become a fresh UUID).
    writeln!(shim_in, "not json").unwrap();
    writeln!(
        shim_in,
        "{{\"type\":\"send\",\"to\":\"{RECIPIENT}\",\"mail\":5,\"body\":\"x\"}}"
    )
    .unwrap();
    writeln!(
        shim_in,
        "{{\"type\":\"send\",\"to\":\"{RECIPIENT}\",\"mail\":\"ok-1\",\"body\":\"fine\"}}"
    )
    .unwrap();
    wait_for_text(
        &rig.p("adapter.out"),
        "\"type\":\"accepted\",\"mail\":\"ok-1\"",
        Duration::from_secs(10),
    );
    let out = read(&rig.p("adapter.out"));
    assert_eq!(count_lines_with(&out, "\"type\":\"error\""), 2, "{out}");
    let coord = read(&rig.p("coord.log"));
    assert_eq!(count_lines_with(&coord, "\"ev\":\"mail\""), 1, "{coord}");
}

#[test]
fn adapter_exits_on_stdin_eof_while_a_send_waits_on_a_silent_core() {
    let mut rig = Rig::new("adapter-eof");
    rig.start_stack();
    let (idx, mut shim_in) =
        rig.start_adapter("a.sock", SENDER, "adapter.store", "adapter.out", &[]);
    // Freeze the coordinator: its sockets stay open and it never answers, so
    // the daemon holds the send for its full upstream wait (10 s).
    let pid = rig.children[rig.coord_idx].id().to_string();
    assert!(Command::new("kill")
        .args(["-STOP", &pid])
        .status()
        .unwrap()
        .success());
    writeln!(
        shim_in,
        "{{\"type\":\"send\",\"to\":\"{RECIPIENT}\",\"body\":\"stuck\"}}"
    )
    .unwrap();
    drop(shim_in); // the harness is gone

    let start = Instant::now();
    while rig.children[idx].try_wait().unwrap().is_none() {
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "adapter outlived its shim's stdin while a send waited"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn adapter_redelivery_after_a_kill_reports_the_same_record() {
    let mut rig = Rig::new("adapter-redeliver");
    rig.start_stack();
    let hold = rig.p("held.marker");
    let hold = hold.to_str().unwrap();
    let (idx, _stdin) = rig.start_adapter(
        "b.sock",
        RECIPIENT,
        "adapter.store",
        "adapter1.out",
        &["--hold-before-ack", hold],
    );
    rig.send("m-1", "held body");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10)); // stored, not acked
    rig.kill(idx);

    let _second = rig.start_adapter(
        "b.sock",
        RECIPIENT,
        "adapter.store",
        "adapter2.out",
        &["--hold-before-ack", hold],
    );
    wait_for_text(
        &rig.p("adapter2.out"),
        "\"type\":\"mail\",\"mail\":\"m-1\"",
        Duration::from_secs(10),
    );
    for out in ["adapter1.out", "adapter2.out"] {
        let text = read(&rig.p(out));
        assert!(
            text.contains("\"body\":\"held body\",\"record\":\"r-1\""),
            "{out}: {text}"
        );
    }
    wait_for_text(
        &rig.p("coord.log"),
        "\"ev\":\"receipt\",\"mail\":\"m-1\"",
        Duration::from_secs(10),
    );
    let store = read(&rig.p("adapter.store"));
    assert_eq!(count_lines_with(&store, "held body"), 1, "{store}");
}

const KEY: &str = "[[153:Meteor Shower:7]]";

/// Run one claim-family client command (`claim`, `renew`, `release`) on a
/// host's daemon socket. Returns the exit code and stdout.
fn claim_cmd(rig: &Rig, sock: &str, verb: &str, owner: &str, ttl_ms: u64) -> (i32, String) {
    let output = Command::new(BIN)
        .args([
            verb,
            "--socket",
            rig.p(sock).to_str().unwrap(),
            "--owner",
            owner,
            "--key",
            KEY,
            "--ttl-ms",
            &ttl_ms.to_string(),
        ])
        .output()
        .unwrap();
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
    )
}

/// One `roger board` read on a host's daemon socket, however it ended.
fn board_output(rig: &Rig, sock: &str) -> std::process::Output {
    Command::new(BIN)
        .args(["board", "--socket", rig.p(sock).to_str().unwrap()])
        .output()
        .unwrap()
}

fn board(rig: &Rig, sock: &str) -> String {
    let output = board_output(rig, sock);
    assert!(output.status.success(), "board failed: {output:?}");
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Both hosts' Board shows exactly one claim on KEY, held by `owner`.
fn assert_one_holder(rig: &Rig, owner: &str) {
    for sock in ["a.sock", "b.sock"] {
        let b = board(rig, sock);
        let lines: Vec<&str> = b.lines().filter(|l| l.starts_with("CLAIM ")).collect();
        assert_eq!(lines.len(), 1, "{sock}: {b}");
        assert!(
            lines[0].starts_with(&format!("CLAIM holder={owner} ")),
            "{sock}: {b}"
        );
        assert!(lines[0].ends_with(&format!("key={KEY}")), "{sock}: {b}");
    }
}

/// A Claim belongs to a session: an owner that never registered is refused
/// before any decision, so it can neither hold a key nor renew one.
#[test]
fn claim_by_an_unregistered_owner_is_refused() {
    let mut rig = Rig::new("claims-unregistered");
    rig.start_stack();
    let stranger = "99999999-9999-4999-8999-999999999999";
    let (code, out) = claim_cmd(&rig, "a.sock", "claim", stranger, 60_000);
    assert_eq!(code, 3, "{out}");
    assert!(
        out.starts_with(&format!(
            "REFUSED op=acquire reason=\"owner {stranger} is not a registered session\" "
        )),
        "{out}"
    );
    assert!(board(&rig, "a.sock").contains("claims=0"));

    // The key is held by a session; the stranger cannot renew or release it.
    rig.register("a.sock", SENDER);
    assert_eq!(claim_cmd(&rig, "a.sock", "claim", SENDER, 60_000).0, 0);
    assert_eq!(claim_cmd(&rig, "b.sock", "renew", stranger, 60_000).0, 3);
    assert_eq!(claim_cmd(&rig, "b.sock", "release", stranger, 0).0, 3);
    assert_one_holder(&rig, SENDER);
}

#[test]
fn claim_is_exclusive_renewed_by_its_owner_and_freed_by_expiry() {
    let mut rig = Rig::new("claims");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    rig.register("b.sock", RECIPIENT);

    let (code, out) = claim_cmd(&rig, "a.sock", "claim", SENDER, 1500);
    // The first deadline is measured from the grant, not from before it.
    let granted = Instant::now();
    assert_eq!(code, 0, "{out}");
    assert!(
        out.starts_with(&format!("CLAIMED op=acquire holder={SENDER} ")),
        "{out}"
    );

    let (code, out) = claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 1500);
    assert_eq!(code, 3, "{out}");
    assert!(
        out.starts_with(&format!("REFUSED op=acquire holder={SENDER} ")),
        "{out}"
    );
    let (code, out) = claim_cmd(&rig, "b.sock", "renew", RECIPIENT, 60_000);
    assert_eq!(code, 3, "only the holder renews: {out}");
    assert_one_holder(&rig, SENDER);

    let (code, out) = claim_cmd(&rig, "a.sock", "renew", SENDER, 3000);
    assert_eq!(code, 0, "{out}");
    assert!(out.starts_with("RENEWED "), "{out}");

    // Past the first deadline, the renewal still holds.
    std::thread::sleep(Duration::from_millis(1700).saturating_sub(granted.elapsed()));
    assert!(granted.elapsed() >= Duration::from_millis(1500));
    assert_one_holder(&rig, SENDER);
    assert_eq!(claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 1500).0, 3);

    // Past the renewed deadline the key is free with no release and no sweep.
    let deadline = Instant::now() + Duration::from_secs(10);
    while board(&rig, "a.sock").contains("CLAIM ") {
        assert!(Instant::now() < deadline, "claim never expired");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(granted.elapsed() >= Duration::from_millis(3000));
    let (code, out) = claim_cmd(&rig, "a.sock", "renew", SENDER, 3000);
    assert_eq!(code, 3, "an expired claim cannot be renewed back: {out}");
    let (code, out) = claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 60_000);
    assert_eq!(code, 0, "{out}");
    assert_one_holder(&rig, RECIPIENT);

    let coord = read(&rig.p("coord.log"));
    assert_eq!(count_lines_with(&coord, "\"ev\":\"release\""), 0, "{coord}");
}

#[test]
fn release_is_the_holders_and_frees_the_key() {
    let mut rig = Rig::new("claims-release");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    rig.register("b.sock", RECIPIENT);
    assert_eq!(claim_cmd(&rig, "a.sock", "claim", SENDER, 60_000).0, 0);
    assert_eq!(claim_cmd(&rig, "b.sock", "release", RECIPIENT, 0).0, 3);
    assert_one_holder(&rig, SENDER);
    let (code, out) = claim_cmd(&rig, "a.sock", "release", SENDER, 0);
    assert_eq!(code, 0, "{out}");
    assert!(out.starts_with("RELEASED "), "{out}");
    assert!(board(&rig, "b.sock").contains("claims=0"));
    assert_eq!(claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 60_000).0, 0);
    assert_one_holder(&rig, RECIPIENT);
}

#[test]
fn claim_survives_a_coordinator_restart() {
    let mut rig = Rig::new("claims-restart");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    rig.register("b.sock", RECIPIENT);
    assert_eq!(claim_cmd(&rig, "a.sock", "claim", SENDER, 60_000).0, 0);
    rig.restart_coordinator();
    // Until the host daemon reconnects the request fails outright (exit 1);
    // the refusal must come from the replayed claim, not from an error.
    let deadline = Instant::now() + Duration::from_secs(10);
    let (code, out) = loop {
        let (code, out) = claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 60_000);
        if code != 1 || Instant::now() > deadline {
            break (code, out);
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, 3, "{out}");
    wait_until_board_reaches_both(&rig);
    assert_one_holder(&rig, SENDER);
}

fn wait_until_board_reaches_both(rig: &Rig) {
    let deadline = Instant::now() + Duration::from_secs(10);
    for sock in ["a.sock", "b.sock"] {
        while !board_output(rig, sock).status.success() {
            assert!(
                Instant::now() < deadline,
                "{sock} never reached the coordinator"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

const IDLE: &str = "44444444-4444-4444-8444-444444444444";

fn start_session(rig: &mut Rig, sock: &str, id: &str, store: &str, out: &str) -> usize {
    let args = [
        "session",
        "--socket",
        rig.p(sock).to_str().unwrap(),
        "--id",
        id,
        "--store",
        rig.p(store).to_str().unwrap(),
    ]
    .map(String::from);
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let idx = rig.spawn(&refs, out);
    wait_for_text(&rig.p(out), "READY", Duration::from_secs(10));
    idx
}

fn wait_for_count(path: &Path, needle: &str, n: usize) {
    let start = Instant::now();
    while count_lines_with(&read(path), needle) != n {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "expected {n} lines with {needle:?} in {}: {}",
            path.display(),
            read(path)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn broadcast_reaches_every_registered_session_once_without_a_wake() {
    let mut rig = Rig::new("broadcast");
    rig.start_stack();
    start_session(&mut rig, "a.sock", SENDER, "sender.store", "sender.out");
    start_session(&mut rig, "b.sock", IDLE, "idle.store", "idle.out");
    let recipient = rig.start_recipient("recipient1.out", true);
    wait_for_count(&rig.p("coord.log"), "\"ev\":\"session\"", 3);

    let body = "fleet notice [[0:FLEET:0]]";
    let output = Command::new(BIN)
        .args(["send", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args([
            "--from", SENDER, "--to", "*", "--mail", "b-1", "--body", body,
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");

    // Kill the held recipient between its Store write and its ack, then
    // restart the coordinator: the copy must come back from its log.
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));
    rig.kill(recipient);
    wait_for_count(&rig.p("coord.log"), "\"ev\":\"receipt\"", 2);
    rig.restart_coordinator();
    rig.start_recipient("recipient2.out", true);
    wait_for_text(
        &rig.p("recipient2.out"),
        &format!("DELIVERED mail=b-1/{RECIPIENT} record=r-1"),
        Duration::from_secs(10),
    );
    wait_for_count(&rig.p("coord.log"), "\"ev\":\"receipt\"", 3);

    // A resent broadcast id commits and delivers nothing new.
    let again = Command::new(BIN)
        .args(["send", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args([
            "--from", SENDER, "--to", "*", "--mail", "b-1", "--body", body,
        ])
        .output()
        .unwrap();
    assert!(again.status.success(), "{again:?}");
    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"broadcast\""),
        1,
        "{coord}"
    );

    for (store, out) in [
        ("sender.store", "sender.out"),
        ("idle.store", "idle.out"),
        ("recipient.store", "recipient1.out"),
    ] {
        let text = read(&rig.p(store));
        assert_eq!(text.lines().count(), 1, "{store}: {text}");
        assert_eq!(count_lines_with(&text, body), 1, "{store}: {text}");
        let out = read(&rig.p(out));
        assert!(out.contains("record=r-1 wake=false"), "{out}");
    }
}

#[test]
fn directed_mail_requests_a_wake() {
    let mut rig = Rig::new("directed-wake");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);
    rig.send("m-1", "directed");
    wait_for_text(
        &rig.p("recipient.out"),
        "STORED mail=m-1 record=r-1 wake=true",
        Duration::from_secs(10),
    );
}

#[test]
fn a_directed_mail_id_can_never_equal_a_broadcast_copy_id() {
    let mut rig = Rig::new("copy-id");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);
    wait_for_count(&rig.p("coord.log"), "\"ev\":\"session\"", 1);

    // The id a broadcast `b-1` gives the recipient's copy, sent as directed
    // mail first: refused, so the broadcast's copy is still delivered.
    let copy = format!("b-1/{RECIPIENT}");
    let output = Command::new(BIN)
        .args(["send", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args(["--from", SENDER, "--to", RECIPIENT, "--mail", &copy])
        .args(["--body", "impostor"])
        .output()
        .unwrap();
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("must not contain '/'"),
        "{output:?}"
    );

    let output = Command::new(BIN)
        .args(["send", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args([
            "--from", SENDER, "--to", "*", "--mail", "b-1", "--body", "real",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    wait_for_text(
        &rig.p("recipient.out"),
        &format!("DELIVERED mail={copy} record=r-1"),
        Duration::from_secs(10),
    );
    let store = read(&rig.p("recipient.store"));
    assert_eq!(store.lines().count(), 1, "{store}");
    assert_eq!(count_lines_with(&store, "\"body\":\"real\""), 1, "{store}");
    let coord = read(&rig.p("coord.log"));
    assert!(!coord.contains("impostor"), "{coord}");
}

/// A session's declared text is opaque: the Board returns it byte-identical
/// from both hosts, and a restarted coordinator replays it from its log.
#[test]
fn declared_text_reaches_the_board_verbatim_and_survives_a_coordinator_restart() {
    let mut rig = Rig::new("declared");
    rig.start_stack();
    let declared = "wake-latency-ms=1500 [[153:Meteor Shower:7]]";
    let sock = rig.p("b.sock").to_str().unwrap().to_string();
    let store = rig.p("recipient.store").to_str().unwrap().to_string();
    rig.spawn(
        &[
            "session",
            "--socket",
            &sock,
            "--id",
            RECIPIENT,
            "--store",
            &store,
            "--declare",
            declared,
        ],
        "recipient.out",
    );
    wait_for_text(&rig.p("recipient.out"), "READY", Duration::from_secs(10));
    let want = format!("DECLARED address={RECIPIENT} declared={declared}\n");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !board(&rig, "a.sock").contains(&want) {
        assert!(Instant::now() < deadline, "{}", board(&rig, "a.sock"));
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(board(&rig, "b.sock").contains(&want));

    rig.restart_coordinator();
    wait_until_board_reaches_both(&rig);
    for s in ["a.sock", "b.sock"] {
        let b = board(&rig, s);
        assert!(b.contains(&want), "{s}: {b}");
    }
    let coord = read(&rig.p("coord.log"));
    // The daemon's re-Register repeats the same text, which logs nothing new.
    assert_eq!(
        count_lines_with(&coord, "\"ev\":\"declared\""),
        1,
        "{coord}"
    );
}

/// RECIPIENT's Board row as (connected, last_seen_ms), read through `sock`
/// by `reader` (if any).
fn presence(rig: &Rig, sock: &str, reader: Option<&str>) -> (bool, i64) {
    let sock = rig.p(sock);
    let mut args = vec!["board", "--socket", sock.to_str().unwrap()];
    if let Some(r) = reader {
        args.extend(["--from", r]);
    }
    let output = Command::new(BIN).args(&args).output().unwrap();
    assert!(output.status.success(), "board failed: {output:?}");
    let out = String::from_utf8_lossy(&output.stdout).to_string();
    let line = out
        .lines()
        .find(|l| l.starts_with(&format!("SESSION address={RECIPIENT} ")))
        .unwrap_or_else(|| panic!("no row for {RECIPIENT}: {out}"));
    let field = |name: &str| {
        line.split(' ')
            .find_map(|w| w.strip_prefix(name))
            .unwrap_or_else(|| panic!("no {name}: {line}"))
            .to_string()
    };
    (
        field("connected=") == "true",
        field("last_seen_ms=").parse().unwrap(),
    )
}

/// Wait until RECIPIENT's `connected` reads `want` through `sock`.
fn wait_connected(rig: &Rig, sock: &str, want: bool) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // A board read fails outright while a host daemon is reconnecting.
        let ok = Command::new(BIN)
            .args(["board", "--socket", rig.p(sock).to_str().unwrap()])
            .output()
            .unwrap()
            .status
            .success();
        if ok {
            let (connected, seen) = presence(rig, sock, None);
            if connected == want {
                return seen;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{RECIPIENT} never became connected={want}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn send_from_recipient(rig: &Rig, mail: &str) {
    let sent = Command::new(BIN)
        .args([
            "send",
            "--socket",
            rig.p("a.sock").to_str().unwrap(),
            "--from",
            RECIPIENT,
            "--to",
            SENDER,
            "--mail",
            mail,
            "--body",
            "out",
        ])
        .output()
        .unwrap();
    assert!(sent.status.success(), "{sent:?}");
}

/// An idle session is present for as long as its connection is open; once it
/// closes, last_seen_ms is the disconnect moment and only the session's own
/// activity moves it.
#[test]
fn presence_is_the_open_session_connection_then_own_activity() {
    let mut rig = Rig::new("presence");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient.out", false);

    // Connected and idle: last seen is the moment of each read.
    let (connected, first) = presence(&rig, "a.sock", None);
    assert!(connected);
    std::thread::sleep(Duration::from_millis(50));
    let (connected, later) = presence(&rig, "a.sock", None);
    assert!(connected);
    assert!(later >= first + 50, "{later} >= {first} + 50");

    // The connection closes: disconnected at that moment, and nothing
    // refreshes it on a clock.
    rig.kill(recipient);
    let gone = wait_connected(&rig, "a.sock", false);
    assert!(gone >= later, "{gone} >= {later}");
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(presence(&rig, "a.sock", None), (false, gone));

    // Mail sent to it is not its act: no Register, no ack, no stamp.
    rig.send("m-presence-0", "while it is gone");
    assert_eq!(presence(&rig, "a.sock", None), (false, gone));

    // Its own Board read and send are its activity.
    std::thread::sleep(Duration::from_millis(5));
    let (_, read_at) = presence(&rig, "a.sock", Some(RECIPIENT));
    assert!(read_at > gone, "{read_at} > {gone}");
    std::thread::sleep(Duration::from_millis(5));
    send_from_recipient(&rig, "m-presence-1");
    let (connected, after_send) = presence(&rig, "a.sock", None);
    assert!(!connected);
    assert!(after_send > read_at, "{after_send} > {read_at}");

    // Registering again makes it connected again.
    rig.start_recipient("recipient2.out", false);
    wait_connected(&rig, "a.sock", true);
}

/// A host daemon's link dropping disconnects its sessions; a coordinator
/// that comes back learns who is connected from the re-registration alone.
#[test]
fn host_link_drop_disconnects_its_sessions_and_reregistration_restores_them() {
    let mut rig = Rig::new("presence-link");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);
    wait_connected(&rig, "a.sock", true);

    // A fresh coordinator holds no presence; host-b's re-registration of its
    // live session restores it.
    rig.restart_coordinator();
    wait_connected(&rig, "a.sock", true);

    // host-b's daemon dies, so it can report nothing: the dropped link alone
    // marks its session disconnected.
    let before = presence(&rig, "a.sock", None).1;
    rig.kill(2);
    let gone = wait_connected(&rig, "a.sock", false);
    assert!(gone >= before, "{gone} >= {before}");
}

/// A receipt the host daemon replays after its link returns was acked while
/// the link was down; it is not the session's activity.
#[test]
fn a_replayed_ack_is_not_activity() {
    let mut rig = Rig::new("presence-replay");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);
    rig.send("m-replay", "acked while the coordinator is down");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));

    rig.kill(0);
    rig.kill(recipient);
    let recipient = rig.start_recipient("recipient2.out", true);
    wait_for_text(
        &rig.p("recipient2.out"),
        "DELIVERED mail=m-replay record=r-1",
        Duration::from_secs(10),
    );
    rig.kill(recipient);

    // The new coordinator hears only the replayed receipt for RECIPIENT: no
    // Register, no live ack.
    rig.restart_coordinator();
    let deadline = Instant::now() + Duration::from_secs(15);
    while count_lines_with(
        &read(&rig.p("coord.log")),
        "\"ev\":\"receipt\",\"mail\":\"m-replay\"",
    ) == 0
    {
        assert!(Instant::now() < deadline, "receipt never replayed");
        std::thread::sleep(Duration::from_millis(50));
    }
    wait_until_board_reaches_both(&rig);
    assert_eq!(presence(&rig, "a.sock", None), (false, 0));
}

/// One raw request line on a host daemon socket, answered by one line.
fn raw_request(rig: &Rig, sock: &str, line: &str) -> String {
    let mut conn = std::os::unix::net::UnixStream::connect(rig.p(sock)).unwrap();
    writeln!(conn, "{line}").unwrap();
    let mut answer = String::new();
    let mut byte = [0u8; 1];
    while conn.read(&mut byte).unwrap() == 1 && byte[0] != b'\n' {
        answer.push(byte[0] as char);
    }
    answer
}

/// A claim with an unknown op is refused with a reason, and the host
/// daemon's link to the coordinator stays up.
#[test]
fn an_unknown_claim_op_is_refused_without_dropping_the_host_link() {
    let mut rig = Rig::new("claim-bad-op");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);
    wait_connected(&rig, "a.sock", true);
    let answer = raw_request(
        &rig,
        "b.sock",
        &format!(
            r#"{{"type":"claim","req":7,"op":"steal","key":"k","owner":"{RECIPIENT}","ttl_ms":1000}}"#
        ),
    );
    assert!(answer.contains(r#""type":"error""#), "{answer}");
    assert!(
        answer.contains("expected op acquire, renew, or release; saw steal"),
        "{answer}"
    );
    let coord_err = read(&rig.p("coord.out.err"));
    assert!(!coord_err.contains("connection ended"), "{coord_err}");
    let daemon_err = read(&rig.p("daemon-b.out.err"));
    assert!(!daemon_err.contains("upstream"), "{daemon_err}");
}

/// Relabel names a registered session; an Address nobody registered gets no
/// label and no log row.
#[test]
fn relabel_of_an_unregistered_address_is_refused() {
    let mut rig = Rig::new("relabel-stranger");
    rig.start_stack();
    rig.start_recipient("recipient.out", false);
    let label = |who: &str| {
        Command::new(BIN)
            .args([
                "label",
                "--socket",
                rig.p("a.sock").to_str().unwrap(),
                "--from",
                who,
                "--label",
                "parked",
            ])
            .output()
            .unwrap()
    };
    let refused = label(SENDER);
    assert!(!refused.status.success(), "{refused:?}");
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains("expected a registered session to relabel"),
        "{refused:?}"
    );
    assert!(label(RECIPIENT).status.success());
    let coord = read(&rig.p("coord.log"));
    assert_eq!(count_lines_with(&coord, "\"ev\":\"label\""), 1, "{coord}");
}

/// Wait until the Board read through `sock` does (`want`) or does not list
/// `id`, and return that read.
fn wait_board_lists(rig: &Rig, sock: &str, id: &str, want: bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // A board read fails outright while a host daemon is reconnecting.
        let output = board_output(rig, sock);
        let out = String::from_utf8_lossy(&output.stdout).to_string();
        if output.status.success() && out.contains(&format!("SESSION address={id} ")) == want {
            return out;
        }
        assert!(
            Instant::now() < deadline,
            "listed={want} never held for {id}: {out}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn broadcast_from_sender(rig: &Rig, mail: &str) -> String {
    let output = Command::new(BIN)
        .args(["send", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args([
            "--from", SENDER, "--to", "*", "--mail", mail, "--body", "all",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    read(&rig.p("coord.log"))
        .lines()
        .find(|l| l.contains(&format!("\"ev\":\"broadcast\",\"mail\":\"{mail}\"")))
        .unwrap_or_else(|| panic!("broadcast {mail} not committed"))
        .to_string()
}

/// A shim closing the adapter's stdin ends the session on purpose: it leaves
/// the Board and every later Broadcast at once, and Mail sent to its Address
/// afterwards is still kept for it.
#[test]
fn adapter_stdin_eof_unregisters_at_once_and_keeps_later_mail() {
    let mut rig = Rig::new("unregister");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    let (idx, stdin) = rig.start_adapter("b.sock", RECIPIENT, "adapter.store", "adapter1.out", &[]);
    wait_board_lists(&rig, "a.sock", RECIPIENT, true);

    drop(stdin);
    rig.children[idx].wait().unwrap();
    wait_board_lists(&rig, "a.sock", RECIPIENT, false);
    let coord = read(&rig.p("coord.log"));
    assert_eq!(
        count_lines_with(
            &coord,
            &format!("\"ev\":\"unregister\",\"session\":\"{RECIPIENT}\"")
        ),
        1,
        "{coord}"
    );
    let b = broadcast_from_sender(&rig, "b-after");
    assert!(!b.contains(RECIPIENT), "{b}");
    assert!(b.contains(SENDER), "{b}");

    rig.send("m-after", "kept for a later Register");
    rig.start_adapter("b.sock", RECIPIENT, "adapter.store", "adapter2.out", &[]);
    wait_for_text(
        &rig.p("adapter2.out"),
        "\"mail\":\"m-after\"",
        Duration::from_secs(10),
    );
}

/// A session that dies without unregistering stays registered while it is
/// disconnected for less than the coordinator's `--unregister-after-ms`; the
/// first registered-set read after that unregisters it. A connected session
/// never expires, including across a coordinator restart, and no Mail is lost.
#[test]
fn a_dead_session_is_unregistered_after_its_grace_and_keeps_its_mail() {
    let mut rig = Rig::new("unregister-grace");
    rig.coord_extra = vec!["--unregister-after-ms".into(), "600".into()];
    rig.start_stack();
    rig.register("a.sock", SENDER);
    let recipient = rig.start_recipient("recipient1.out", false);
    rig.kill(recipient);
    wait_connected(&rig, "a.sock", false);
    rig.send("m-grace", "sent while it is dead");

    std::thread::sleep(Duration::from_millis(700));
    let out = wait_board_lists(&rig, "a.sock", RECIPIENT, false);
    assert!(out.contains(&format!("SESSION address={SENDER} ")), "{out}");
    let b = broadcast_from_sender(&rig, "b-grace");
    assert!(!b.contains(RECIPIENT) && b.contains(SENDER), "{b}");

    // A restarted coordinator counts replayed sessions from its start; the
    // live one re-registers through its host daemon and stays.
    rig.restart_coordinator();
    std::thread::sleep(Duration::from_millis(700));
    wait_board_lists(&rig, "a.sock", SENDER, true);

    rig.start_recipient("recipient2.out", false);
    wait_for_text(
        &rig.p("recipient2.out"),
        "DELIVERED mail=m-grace",
        Duration::from_secs(10),
    );
}

/// Two processes on one Store would number records from the same replay; the
/// second is refused at start and the first keeps serving.
#[test]
fn a_second_session_on_one_store_is_refused() {
    let mut rig = Rig::new("store-lock");
    rig.start_stack();
    rig.start_recipient("recipient1.out", false);
    let mut second = Command::new(BIN)
        .args(["session", "--socket", rig.p("b.sock").to_str().unwrap()])
        .args([
            "--id",
            IDLE,
            "--store",
            rig.p("recipient.store").to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = second.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = second.kill();
            panic!("a second session on one Store kept running");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut err = String::new();
    second
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(!status.success(), "{status:?}");
    assert!(err.contains("held by another process"), "{err}");
    rig.send("m-lock", "still served");
    wait_for_text(
        &rig.p("recipient1.out"),
        "DELIVERED mail=m-lock record=r-1",
        Duration::from_secs(10),
    );
}

/// Run `roger daemon` on `sock` with its own log and Host id; wait up to 10 s
/// for it to exit and return its status and stderr, or None if it kept
/// running (it is then killed).
fn second_daemon(rig: &Rig, sock: &Path, data: &str) -> Option<(std::process::ExitStatus, String)> {
    let mut child = Command::new(BIN)
        .args([
            "daemon",
            "--coordinator",
            &format!("127.0.0.1:{}", rig.coord_port),
            "--host-id",
            "host-c",
            "--socket",
            sock.to_str().unwrap(),
            "--data",
            rig.p(data).to_str().unwrap(),
        ])
        .env("ROGER_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    Some((status, err))
}

#[test]
fn a_second_daemon_on_a_live_socket_is_refused_and_the_first_keeps_serving() {
    let mut rig = Rig::new("socket-live");
    rig.start_stack();
    rig.start_recipient("recipient1.out", false);
    let sock = rig.p("b.sock");
    let Some((status, err)) = second_daemon(&rig, &sock, "c.log") else {
        panic!("a second daemon on a live socket kept running");
    };
    assert!(!status.success(), "{status:?}");
    assert!(err.contains(sock.to_str().unwrap()), "{err}");
    assert!(err.contains("refusing to take over"), "{err}");
    // A session that connects now is served by the first daemon, host-b.
    start_session(&mut rig, "b.sock", IDLE, "idle.store", "idle.out");
    let b = wait_board_lists(&rig, "a.sock", IDLE, true);
    assert!(b.contains(&format!("address={IDLE} host=host-b")), "{b}");
    rig.send("m-sock", "still served");
    wait_for_text(
        &rig.p("recipient1.out"),
        "DELIVERED mail=m-sock record=r-1",
        Duration::from_secs(10),
    );
}

#[test]
fn a_daemon_replaces_a_stale_socket_left_by_a_dead_one() {
    let mut rig = Rig::new("socket-stale");
    rig.start_stack();
    let sock = rig.p("stale.sock");
    // Bound and dropped: the file stays, nothing listens behind it.
    drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
    assert!(sock.exists());
    let args = [
        "daemon",
        "--coordinator",
        &format!("127.0.0.1:{}", rig.coord_port),
        "--host-id",
        "host-c",
        "--socket",
        sock.to_str().unwrap(),
        "--data",
        rig.p("c.log").to_str().unwrap(),
    ]
    .map(String::from);
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    rig.spawn(&refs, "daemon-c.out");
    wait_for_text(
        &rig.p("daemon-c.out.err"),
        "host daemon listening",
        Duration::from_secs(10),
    );
    start_session(&mut rig, "stale.sock", IDLE, "idle.store", "idle.out");
}

#[test]
fn a_daemon_refuses_to_remove_a_non_socket_at_its_path() {
    let mut rig = Rig::new("socket-file");
    rig.start_stack();
    let path = rig.p("plain.sock");
    fs::write(&path, "keep me").unwrap();
    let Some((status, err)) = second_daemon(&rig, &path, "c.log") else {
        panic!("a daemon on a regular file kept running");
    };
    assert!(!status.success(), "{status:?}");
    assert!(err.contains("refusing to remove it"), "{err}");
    assert_eq!(fs::read_to_string(&path).unwrap(), "keep me");
}

#[test]
fn a_second_coordinator_on_a_bound_address_is_refused() {
    let mut rig = Rig::new("coord-bind");
    rig.start_stack();
    let out = Command::new(BIN)
        .args([
            "coordinator",
            "--bind",
            &format!("127.0.0.1:{}", rig.coord_port),
            "--data",
            rig.p("coord-2.log").to_str().unwrap(),
        ])
        .env("ROGER_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(!out.status.success(), "{:?}", out.status);
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("Address already in use"), "{err}");
    rig.start_recipient("recipient1.out", false);
    rig.send("m-coord", "still served");
    wait_for_text(
        &rig.p("recipient1.out"),
        "DELIVERED mail=m-coord record=r-1",
        Duration::from_secs(10),
    );
}

/// The core falls back to no path: without its flags a daemon or coordinator
/// refuses to start, so a test that forgets one can never reach a deployed
/// host's socket or logs.
#[test]
fn the_core_has_no_default_socket_or_data_path() {
    for (args, missing) in [
        (
            vec![
                "daemon",
                "--coordinator",
                "127.0.0.1:1",
                "--host-id",
                "h",
                "--data",
                "/nonexistent/d.log",
            ],
            "missing --socket",
        ),
        (
            vec![
                "daemon",
                "--coordinator",
                "127.0.0.1:1",
                "--host-id",
                "h",
                "--socket",
                "/nonexistent/s.sock",
            ],
            "missing --data",
        ),
        (
            vec!["coordinator", "--bind", "127.0.0.1:0"],
            "missing --data",
        ),
        (vec!["board"], "missing --socket"),
    ] {
        let out = Command::new(BIN)
            .args(&args)
            .env("ROGER_TOKEN", TOKEN)
            .output()
            .unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success() && err.contains(missing),
            "{args:?}: {err}"
        );
    }
}

fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// `roger receipt` for one Mail id, read through host-a's daemon.
fn receipt_line(rig: &Rig, mail: &str) -> String {
    let output = Command::new(BIN)
        .args(["receipt", "--socket", rig.p("a.sock").to_str().unwrap()])
        .args(["--mail", mail])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// One `key=<integer>` from a `RECEIPT` or `PENDING` line.
fn stamp_in(line: &str, key: &str) -> i64 {
    line.split(' ')
        .find_map(|w| w.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("no {key} in {line}"))
        .parse()
        .unwrap_or_else(|_| panic!("{key} is not an integer in {line}"))
}

/// The coordinator stamps a Mail when it answers Accepted and when it records
/// the Receipt, on its own clock, so their difference is end-to-end latency.
/// Directed mail and Broadcast copies are stamped alike, the stamps survive a
/// coordinator restart.
#[test]
fn receipt_carries_accepted_and_receipted_stamps_on_the_coordinator_clock() {
    let mut rig = Rig::new("ack-time");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    rig.start_recipient("recipient1.out", false);

    let before = epoch_ms();
    rig.send("m-time", "stamp me");
    wait_for_text(
        &rig.p("recipient1.out"),
        "DELIVERED mail=m-time record=r-1",
        Duration::from_secs(10),
    );
    broadcast_from_sender(&rig, "b-time");
    let copy = format!("b-time/{RECIPIENT}");
    wait_for_text(
        &rig.p("recipient1.out"),
        &format!("DELIVERED mail={copy} record=r-2"),
        Duration::from_secs(10),
    );
    let after = epoch_ms();

    let mut lines = Vec::new();
    for (mail, record) in [("m-time", "r-1"), (copy.as_str(), "r-2")] {
        let line = receipt_line(&rig, mail);
        assert!(
            line.starts_with(&format!(
                "RECEIPT mail={mail} to={RECIPIENT} record={record} "
            )),
            "{line}"
        );
        let (a, r) = (
            stamp_in(&line, "accepted_at_ms"),
            stamp_in(&line, "receipted_at_ms"),
        );
        assert!(
            before <= a && a <= r && r <= after,
            "{before} {line} {after}"
        );
        assert_eq!(stamp_in(&line, "latency_ms"), r - a, "{line}");
        lines.push(line);
    }
    // Replay keeps every stamp as committed.
    rig.restart_coordinator();
    let start = Instant::now();
    for (line, mail) in lines.iter().zip(["m-time", copy.as_str()]) {
        loop {
            let output = Command::new(BIN)
                .args(["receipt", "--socket", rig.p("a.sock").to_str().unwrap()])
                .args(["--mail", mail])
                .output()
                .unwrap();
            let got = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if output.status.success() {
                assert_eq!(&got, line, "stamps changed across a coordinator restart");
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(15), "{output:?}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// A Store entry written while entries still carried `stored_at_ms` and
/// `clock` replays: its Mail keeps its record, and a new entry takes the next
/// record and carries neither field.
#[test]
fn a_store_entry_with_retired_fields_replays() {
    let mut rig = Rig::new("store-retired-fields");
    fs::write(
        rig.p("recipient.store"),
        format!(
            "{{\"record\":\"r-1\",\"mail\":\"m-old\",\"from\":\"{SENDER}\",\"body\":\"old\",\"stored_at_ms\":1761000000000,\"clock\":\"old-host\"}}\n"
        ),
    )
    .unwrap();
    rig.start_stack();
    rig.start_recipient("recipient1.out", false);
    rig.send("m-old", "old");
    rig.send("m-new", "new");
    for line in [
        "DELIVERED mail=m-old record=r-1",
        "DELIVERED mail=m-new record=r-2",
    ] {
        wait_for_text(&rig.p("recipient1.out"), line, Duration::from_secs(10));
    }
    let store = read(&rig.p("recipient.store"));
    assert_eq!(count_lines_with(&store, "\"mail\":\"m-old\""), 1, "{store}");
    let new = store.lines().nth(1).unwrap_or_else(|| panic!("{store}"));
    assert!(
        new.contains("\"record\":\"r-2\",\"mail\":\"m-new\""),
        "{store}"
    );
    assert!(
        !new.contains("stored_at_ms") && !new.contains("\"clock\""),
        "{new}"
    );
}

/// Before its Ack a Mail is pending, with its Accepted stamp only.
#[test]
fn a_mail_not_yet_acked_reads_pending_with_its_accepted_stamp() {
    let mut rig = Rig::new("ack-time-pending");
    rig.start_stack();
    rig.start_recipient("recipient1.out", true);
    let before = epoch_ms();
    rig.send("m-pend", "held before its ack");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));
    let line = receipt_line(&rig, "m-pend");
    assert!(
        line.starts_with(&format!(
            "PENDING mail=m-pend to={RECIPIENT} accepted_at_ms="
        )),
        "{line}"
    );
    assert!(stamp_in(&line, "accepted_at_ms") >= before, "{line}");
    assert!(!line.contains("record="), "{line}");
}

/// A log written before the stamps existed replays as is: its Mail and
/// Receipts read with every stamp absent, never zero.
#[test]
fn unstamped_log_rows_replay_with_their_stamps_absent() {
    let mut rig = Rig::new("ack-time-old");
    let to = RECIPIENT;
    fs::write(
        rig.p("coord.log"),
        format!(
            "{{\"ev\":\"session\",\"session\":\"{to}\",\"host\":\"host-b\"}}\n\
             {{\"ev\":\"mail\",\"mail\":\"old-1\",\"from\":\"{SENDER}\",\"to\":\"{to}\",\"body\":\"x\"}}\n\
             {{\"ev\":\"receipt\",\"mail\":\"old-1\",\"session\":\"{to}\",\"record\":\"r-1\"}}\n\
             {{\"ev\":\"broadcast\",\"mail\":\"old-b\",\"from\":\"{SENDER}\",\"body\":\"y\",\"to\":[\"{to}\"]}}\n"
        ),
    )
    .unwrap();
    rig.start_stack();
    assert_eq!(
        receipt_line(&rig, "old-1"),
        format!(
            "RECEIPT mail=old-1 to={to} record=r-1 accepted_at_ms=absent receipted_at_ms=absent latency_ms=absent"
        )
    );
    assert_eq!(
        receipt_line(&rig, &format!("old-b/{to}")),
        format!("PENDING mail=old-b/{to} to={to} accepted_at_ms=absent")
    );
}

/// The host daemon's wait for the coordinator to answer a forwarded ack runs
/// out while the link stays up (the coordinator is stopped, not gone). The
/// forward is already on the link, so the coordinator records the Receipt as
/// soon as it runs again, with no reconnect and no replay: its
/// `receipted_at_ms` follows the stall, not the next reconnect.
#[test]
fn a_receipt_whose_answer_stalled_past_the_wait_lands_when_the_coordinator_resumes() {
    let mut rig = Rig::new("ack-time-stall");
    rig.start_stack();
    let recipient = rig.start_recipient("recipient1.out", true);
    rig.send("m-stall", "acked into a stalled coordinator");
    wait_for_file(&rig.p("held.marker"), Duration::from_secs(10));

    let coord = rig.children[rig.coord_idx].id().to_string();
    let signal = |sig: &str| {
        assert!(Command::new("kill")
            .args([sig, &coord])
            .status()
            .unwrap()
            .success());
    };
    signal("-STOP");
    rig.kill(recipient);
    rig.start_recipient("recipient2.out", true);
    // The daemon's 10 s wait ran out with the link still up.
    wait_for_text(
        &rig.p("daemon-b.out.err"),
        "receipt m-stall committed, upstream forward deferred: upstream did not answer",
        Duration::from_secs(15),
    );
    assert_eq!(
        count_lines_with(&read(&rig.p("coord.log")), "\"ev\":\"receipt\""),
        0
    );
    let resumed = epoch_ms();
    signal("-CONT");
    wait_for_text(
        &rig.p("coord.log"),
        "\"ev\":\"receipt\",\"mail\":\"m-stall\"",
        Duration::from_secs(2),
    );
    let line = receipt_line(&rig, "m-stall");
    let r = stamp_in(&line, "receipted_at_ms");
    assert!(
        resumed <= r && r <= resumed + 2000,
        "receipt recorded {}ms after the coordinator resumed: {line}",
        r - resumed
    );
    let daemon = read(&rig.p("daemon-b.out.err"));
    assert!(
        !daemon.contains("upstream connection closed") && !daemon.contains("replaying"),
        "the receipt needed a reconnect: {daemon}"
    );
}

/// A host daemon restarted under a new Host id keeps everything: its
/// session re-registers there, Mail accepted while it was down and Mail held
/// in its local inbox are each delivered once, and the session's claim stays
/// held, because Mail and claims are keyed by Address and the Host id only
/// routes.
#[test]
fn a_host_id_change_keeps_sessions_pending_mail_and_claims() {
    let mut rig = Rig::new("host-id-change");
    rig.start_stack();
    rig.register("a.sock", SENDER);
    let recipient = rig.start_recipient("recipient1.out", false);
    assert_eq!(claim_cmd(&rig, "b.sock", "claim", RECIPIENT, 60_000).0, 0);
    rig.send("m-before", "before the rename");
    wait_for_text(
        &rig.p("recipient1.out"),
        "DELIVERED mail=m-before record=r-1",
        Duration::from_secs(10),
    );

    // Session gone, daemon up: this Mail waits in host-b's local inbox.
    rig.kill(recipient);
    wait_connected(&rig, "a.sock", false);
    rig.send("m-inbox", "held by the daemon");
    wait_for_text(&rig.p("b.log"), "m-inbox", Duration::from_secs(10));
    // Daemon gone too: this Mail waits only at the coordinator.
    rig.kill(2);
    rig.send("m-coord", "held by the coordinator");

    rig.restart_recipient_daemon_as("host-b-renamed");
    rig.start_recipient("recipient2.out", false);
    for mail in ["m-inbox", "m-coord"] {
        wait_for_text(
            &rig.p("recipient2.out"),
            &format!("DELIVERED mail={mail} record="),
            Duration::from_secs(10),
        );
    }

    let b = board(&rig, "a.sock");
    assert!(
        b.contains(&format!(
            "SESSION address={RECIPIENT} host=host-b-renamed connected=true "
        )),
        "{b}"
    );
    assert_eq!(count_lines_with(&b, "SESSION "), 2, "{b}");
    assert_one_holder(&rig, RECIPIENT);
    assert_eq!(claim_cmd(&rig, "b.sock", "renew", RECIPIENT, 60_000).0, 0);

    let store = read(&rig.p("recipient.store"));
    assert_eq!(store.lines().count(), 3, "{store}");
    let coord = read(&rig.p("coord.log"));
    for mail in ["m-before", "m-inbox", "m-coord"] {
        assert_eq!(
            count_lines_with(&store, &format!("\"{mail}\"")),
            1,
            "{store}"
        );
        let row = format!("\"ev\":\"receipt\",\"mail\":\"{mail}\"");
        assert_eq!(count_lines_with(&coord, &row), 1, "{coord}");
    }
}

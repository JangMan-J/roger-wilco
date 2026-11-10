//! Process-level seam for a session's Store and its rotation: a real
//! coordinator, host daemon and `roger session` (the Tracer) or
//! `roger adapter` on loopback, each Store in this test's own directory. The
//! expected values are the literals chosen here.

#[cfg(not(feature = "test-hooks"))]
compile_error!("tests/store.rs needs the test hooks: cargo test --features test-hooks");

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_roger");
const TOKEN: &str = "store-test-token";
const SENDER: &str = "11111111-1111-4111-8111-111111111111";
const RECIPIENT: &str = "22222222-2222-4222-8222-222222222222";

// ---- the rig: coordinator, one host daemon, and the session under test ----

struct Rig {
    dir: PathBuf,
    children: Vec<Child>,
    /// Open stdins of adapters: closing one ends that adapter.
    stdins: Vec<ChildStdin>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stdins.clear();
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        // A failing test keeps its files for the next look; Rig::new clears
        // the directory before the next run of the same tag.
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

fn until<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if let Some(v) = f() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out after {secs}s waiting for {what}");
}

fn read(path: &std::path::Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

fn count(text: &str, needle: &str) -> usize {
    text.lines().filter(|l| l.contains(needle)).count()
}

/// A Store entry in the shape a session has always written.
fn entry(record: &str, mail: &str, body: &str) -> String {
    format!(
        "{{\"record\":\"{record}\",\"mail\":\"{mail}\",\"from\":\"{SENDER}\",\"body\":\"{body}\",\"kind\":\"directed\",\"headers\":{{}}}}"
    )
}

fn tombstone(record: &str, mail: &str) -> String {
    format!("{{\"record\":\"{record}\",\"mail\":\"{mail}\",\"culled\":true}}")
}

/// The string value of `"key":"..."` in one line.
fn field(line: &str, key: &str) -> Option<String> {
    let rest = &line[line.find(&format!("\"{key}\":\""))? + key.len() + 4..];
    Some(rest[..rest.find('"')?].to_string())
}

/// The Record ids of a Store's entries and tombstones, in file order.
fn records(text: &str) -> Vec<String> {
    text.lines()
        .filter(|l| l.starts_with("{\"record\":\""))
        .filter_map(|l| field(l, "record"))
        .collect()
}

/// The Mail ids of a Store's tombstones (`culled`) or entries, in file order.
fn mails(text: &str, culled: bool) -> Vec<String> {
    text.lines()
        .filter(|l| l.starts_with("{\"record\":\"") && l.ends_with("\"culled\":true}") == culled)
        .filter_map(|l| field(l, "mail"))
        .collect()
}

/// What a rotation of the file `text` restates, by the rule as written here:
/// snapshot lines dropped, tombstones kept, and with `cull` an entry inside
/// the snapshot block made a tombstone; every other entry kept as it is.
fn restated(text: &str, cull: bool) -> String {
    let mut inside = false;
    let mut out = String::new();
    for l in text.lines() {
        if l.starts_with("{\"snapshot\":") {
            inside = true;
        } else if l.starts_with("{\"snapshot_end\":") {
            inside = false;
        } else if l.starts_with("{\"record\":\"") {
            if cull && inside && !l.ends_with("\"culled\":true}") {
                out += &tombstone(&field(l, "record").unwrap(), &field(l, "mail").unwrap());
            } else {
                out += l;
            }
            out.push('\n');
        }
    }
    out
}

/// The fresh file of rotation `seq` of `t.store`: its snapshot block holding
/// `lines`, nothing after it yet.
fn block(seq: u64, next_record: u64, lines: &str) -> String {
    format!(
        "{{\"snapshot\":{seq},\"prior\":\"t.store.{seq}\",\"next_record\":{next_record}}}\n{lines}{{\"snapshot_end\":{seq}}}\n"
    )
}

impl Rig {
    fn new(tag: &str) -> Rig {
        // The checkout is part of the name so sibling worktrees testing at
        // once never share a directory.
        let tree = env!("CARGO_MANIFEST_DIR")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        let dir = std::env::temp_dir().join(format!("roger-store-{tree:016x}-{tag}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut rig = Rig {
            dir,
            children: Vec::new(),
            stdins: Vec::new(),
        };
        let port_file = rig.p("coord.port");
        let coord_log = rig.p("coord.log");
        rig.spawn(
            &[
                "coordinator",
                "--bind",
                "127.0.0.1:0",
                "--port-file",
                port_file.to_str().unwrap(),
                "--data",
                coord_log.to_str().unwrap(),
            ],
            "coord.out",
        );
        let port = until("the coordinator port", 10, || {
            Some(read(&port_file).trim().to_string()).filter(|p| p.parse::<u16>().is_ok())
        });
        let (sock, daemon_log) = (rig.sock(), rig.p("daemon.log"));
        rig.spawn(
            &[
                "daemon",
                "--coordinator",
                &format!("127.0.0.1:{port}"),
                "--host-id",
                "h1",
                "--socket",
                sock.to_str().unwrap(),
                "--data",
                daemon_log.to_str().unwrap(),
            ],
            "daemon.out",
        );
        // The socket file exists before the daemon listens: wait for an accept.
        until("the daemon socket to accept", 10, || {
            std::os::unix::net::UnixStream::connect(&sock)
                .ok()
                .map(drop)
        });
        rig
    }

    fn p(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn sock(&self) -> PathBuf {
        self.p("d.sock")
    }

    /// The Store under test, as it is now.
    fn store(&self) -> String {
        read(&self.p("t.store"))
    }

    fn archive(&self, seq: u64) -> PathBuf {
        self.p(&format!("t.store.{seq}"))
    }

    /// Start `roger <args>` with stdout in the file `out` and stderr beside
    /// it. It runs with core dumps off, since the rotation crash hook aborts.
    fn spawn(&mut self, args: &[&str], out: &str) -> usize {
        let child = Command::new("sh")
            .arg("-c")
            .arg("ulimit -c 0; exec \"$0\" \"$@\"")
            .arg(BIN)
            .args(args)
            .env("ROGER_TOKEN", TOKEN)
            .stdin(Stdio::null())
            .stdout(Stdio::from(fs::File::create(self.p(out)).unwrap()))
            .stderr(Stdio::from(
                fs::File::create(self.p(&format!("{out}.err"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        self.children.push(child);
        self.children.len() - 1
    }

    /// The Tracer for RECIPIENT on the Store `t.store` with `extra` flags,
    /// once the coordinator holds its Register.
    fn tracer(&mut self, out: &str, extra: &[&str]) -> usize {
        let (sock, store) = (self.sock(), self.p("t.store"));
        let mut args = vec![
            "session",
            "--socket",
            sock.to_str().unwrap(),
            "--id",
            RECIPIENT,
            "--store",
            store.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        let idx = self.spawn(&args, out);
        self.wait_for(out, "REGISTERED");
        idx
    }

    fn wait_for(&self, out: &str, needle: &str) {
        let path = self.p(out);
        until(&format!("{needle:?} in {out}"), 15, || {
            read(&path).contains(needle).then_some(())
        });
    }

    fn send(&self, mail: &str, body: &str, headers: &[&str]) {
        let mut cmd = Command::new(BIN);
        cmd.args([
            "send",
            "--socket",
            self.sock().to_str().unwrap(),
            "--from",
            SENDER,
            "--to",
            RECIPIENT,
            "--mail",
            mail,
            "--body",
            body,
        ]);
        for h in headers {
            cmd.args(["--header", h]);
        }
        let out = cmd.output().unwrap();
        assert!(
            out.status.success(),
            "send {mail}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Send `mail` and wait for the Tracer's `DELIVERED` line naming `record`.
    fn deliver(&self, out: &str, mail: &str, body: &str, record: &str) {
        self.send(mail, body, &[]);
        self.wait_for(out, &format!("DELIVERED mail={mail} record={record}\n"));
    }

    /// Deliver `u<n>` as Record `r-<n>`, from `*n` on, until the Store has
    /// rotated for the `seq`th time; a rotation is done before its Ack. Checks
    /// that the archive is the file as it was plus the entry that made it
    /// due, and returns the archive.
    fn deliver_until_rotation(&self, out: &str, n: &mut u64, seq: u64) -> String {
        for _ in 0..40 {
            let before = self.store();
            let mail = format!("u{n:02}");
            self.deliver(out, &mail, &format!("body {n}"), &format!("r-{n}"));
            *n += 1;
            if self.archive(seq).exists() {
                let archive = read(&self.archive(seq));
                let added = archive
                    .strip_prefix(&before)
                    .unwrap_or_else(|| panic!("archive {seq} is the file as it was: {archive}"));
                assert_eq!(added.lines().count(), 1, "{added}");
                assert!(added.contains(&format!("\"mail\":\"{mail}\"")), "{added}");
                return archive;
            }
        }
        panic!("no rotation {seq} after 40 deliveries: {}", self.store());
    }

    fn kill(&mut self, idx: usize) {
        self.children[idx].kill().unwrap();
        self.children[idx].wait().unwrap();
    }
}

// ---- rotation ----

#[test]
fn a_rotated_store_keeps_every_entry_and_never_reuses_a_record_id() {
    let mut rig = Rig::new("rotate");
    let flags = ["--rotate-bytes", "400"];
    let t = rig.tracer("t1.out", &flags);
    let mut n = 1;
    for seq in 1..=2 {
        let archive = rig.deliver_until_rotation("t1.out", &mut n, seq);
        assert_eq!(
            rig.store(),
            block(seq, n, &restated(&archive, false)),
            "without --cull the fresh file restates every entry as it was"
        );
    }
    assert!(!rig.p("t.store.next").exists());

    // A restart replays the fresh file and numbers on from its snapshot.
    rig.kill(t);
    rig.tracer("t2.out", &flags);
    let last = n;
    rig.deliver("t2.out", "last", "after the restart", &format!("r-{last}"));
    let text = rig.store();
    assert_eq!(
        records(&text),
        (1..=last).map(|n| format!("r-{n}")).collect::<Vec<_>>(),
        "{text}"
    );
    for n in 1..last {
        assert_eq!(count(&text, &format!("\"mail\":\"u{n:02}\"")), 1, "{text}");
    }
    assert!(
        !text.contains("\"culled\""),
        "nothing is culled without --cull"
    );
}

#[test]
fn with_cull_a_rotation_culls_what_the_one_before_restated_and_keeps_what_came_after() {
    let mut rig = Rig::new("cull");
    // s-1 and s-2 were stored before; no coordinator has seen them.
    let seeded = format!(
        "{}\n{}\n",
        entry("r-1", "s-1", "one"),
        entry("r-2", "s-2", "two")
    );
    fs::write(rig.p("t.store"), &seeded).unwrap();
    let flags = ["--rotate-bytes", "200", "--cull"];
    let t = rig.tracer("t1.out", &flags);

    // The replay found the Store due. The first rotation of a file with no
    // snapshot block culls nothing.
    assert_eq!(
        read(&rig.archive(1)),
        seeded,
        "the archive is the file as it was"
    );
    assert_eq!(rig.store(), block(1, 3, &seeded));

    // The second culls exactly what the first restated and keeps what came after.
    let mut n = 3;
    let archive = rig.deliver_until_rotation("t1.out", &mut n, 2);
    let second = rig.store();
    assert_eq!(second, block(2, n, &restated(&archive, true)));
    assert_eq!(mails(&second, true), ["s-1", "s-2"], "{second}");
    let kept: Vec<String> = (3..n).map(|i| format!("u{i:02}")).collect();
    assert_eq!(mails(&second, false), kept, "{second}");

    // A culled Mail delivered again is acked with its own Record id, and
    // nothing is stored.
    rig.deliver("t1.out", "s-1", "one", "r-1");
    assert!(!read(&rig.p("t1.out")).contains("STORED mail=s-1 "));
    assert_eq!(rig.store(), second);

    // The third culls what the second kept, keeps its tombstones as they
    // were, and keeps what came after it.
    let from = n;
    let archive = rig.deliver_until_rotation("t1.out", &mut n, 3);
    let third = rig.store();
    assert_eq!(third, block(3, n, &restated(&archive, true)));
    let mut culled = vec!["s-1".to_string(), "s-2".to_string()];
    culled.extend(kept);
    assert_eq!(mails(&third, true), culled, "{third}");
    assert_eq!(
        mails(&third, false),
        (from..n).map(|i| format!("u{i:02}")).collect::<Vec<_>>(),
        "{third}"
    );
    assert!(third.contains(&format!("{}\n", tombstone("r-1", "s-1"))));

    // After a restart a tombstone still names its Mail: a redelivery of s-2
    // is acked with r-2 and stores nothing, and new Mail numbers on.
    rig.kill(t);
    rig.tracer("t2.out", &flags);
    rig.deliver("t2.out", "s-2", "two", "r-2");
    rig.deliver("t2.out", "next", "new", &format!("r-{n}"));
    let text = rig.store();
    let after = text
        .strip_prefix(&third)
        .unwrap_or_else(|| panic!("{text}"));
    assert_eq!(records(after), [format!("r-{n}")], "{text}");
    assert_eq!(count(&text, "\"mail\":\"s-2\""), 1, "{text}");
    assert!(!read(&rig.p("t2.out")).contains("STORED mail=s-2 "));
}

#[test]
fn the_next_record_id_follows_the_snapshot_mark_not_the_lines_it_holds() {
    let mut rig = Rig::new("high-water");
    // A snapshot block that restated seven Record ids now holds two, as if
    // tombstones had been removed by hand; r-8 was appended after it.
    let seeded = format!(
        "{{\"snapshot\":3,\"prior\":\"t.store.3\",\"next_record\":8}}\n{}\n{}\n{{\"snapshot_end\":3}}\n{}\n",
        tombstone("r-2", "h-2"),
        entry("r-5", "h-5", "five"),
        entry("r-8", "h-8", "eight")
    );
    fs::write(rig.p("t.store"), &seeded).unwrap();
    rig.tracer("t.out", &[]);
    rig.deliver("t.out", "h-new", "fresh", "r-9");
    // The tombstone's Mail is acked with its own Record id and stored nothing.
    rig.deliver("t.out", "h-2", "two", "r-2");
    let text = rig.store();
    let added = text
        .strip_prefix(&seeded)
        .unwrap_or_else(|| panic!("{text}"));
    assert_eq!(added, format!("{}\n", entry("r-9", "h-new", "fresh")));
}

#[test]
fn a_store_never_rotated_is_in_the_shape_it_always_had() {
    let mut rig = Rig::new("plain");
    rig.tracer("t.out", &[]);
    rig.send("p-1", "one", &["k=v"]);
    rig.wait_for("t.out", "DELIVERED mail=p-1 record=r-1\n");
    rig.deliver("t.out", "p-2", "two", "r-2");
    let text = rig.store();
    assert_eq!(
        text,
        format!(
            "{{\"record\":\"r-1\",\"mail\":\"p-1\",\"from\":\"{SENDER}\",\"body\":\"one\",\"kind\":\"directed\",\"headers\":{{\"k\":\"v\"}}}}\n{}\n",
            entry("r-2", "p-2", "two")
        )
    );
    assert!(!text.contains("stored_at_ms"), "{text}");
    let mut beside: Vec<String> = fs::read_dir(&rig.dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("t.store"))
        .collect();
    beside.sort();
    assert_eq!(beside, ["t.store"], "no archive and no rotation file");
    let shown = read(&rig.p("t.out"));
    assert!(
        shown
            .contains("STORED mail=p-1 record=r-1 kind=directed\nHEADERS mail=p-1 {\"k\":\"v\"}\n"),
        "{shown}"
    );
    assert!(
        shown.contains("STORED mail=p-2 record=r-2 kind=directed\n"),
        "{shown}"
    );
}

#[test]
fn the_adapter_rotates_its_store_and_tells_its_client_what_it_always_did() {
    let mut rig = Rig::new("adapter");
    let (sock, store) = (rig.sock(), rig.p("t.store"));
    let child = Command::new(BIN)
        .args([
            "adapter",
            "--socket",
            sock.to_str().unwrap(),
            "--id",
            RECIPIENT,
            "--store",
            store.to_str().unwrap(),
            "--rotate-bytes",
            "1",
            "--cull",
            "--registered",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::from(fs::File::create(rig.p("a.out")).unwrap()))
        .stderr(Stdio::from(fs::File::create(rig.p("a.err")).unwrap()))
        .spawn()
        .unwrap();
    rig.children.push(child);
    let stdin = rig.children.last_mut().unwrap().stdin.take().unwrap();
    rig.stdins.push(stdin);
    rig.wait_for("a.out", "\"type\":\"registered\"");
    rig.send("a-1", "one", &[]);
    let line = format!(
        "{{\"type\":\"mail\",\"mail\":\"a-1\",\"from\":\"{SENDER}\",\"body\":\"one\",\"record\":\"r-1\",\"kind\":\"directed\",\"headers\":{{}}}}\n"
    );
    rig.wait_for("a.out", &line);
    // The first append made the Store due, and its first rotation kept the
    // entry whole; the client's line followed the rotation.
    let archive = read(&rig.archive(1));
    assert_eq!(archive, format!("{}\n", entry("r-1", "a-1", "one")));
    assert_eq!(rig.store(), block(1, 2, &archive));
}

#[test]
fn rotation_flags_are_checked_on_every_face() {
    // No daemon: a face that read past its flags would fail on the socket
    // instead, and say something else.
    let dir = std::env::temp_dir().join(format!("roger-store-flags-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let (sock, store) = (dir.join("none.sock"), dir.join("never.store"));
    for face in ["session", "adapter", "mcp"] {
        for (flags, says) in [
            (
                &["--cull"][..],
                "expected --rotate-bytes above 0 with --cull (culling happens when a Store rotates), saw none",
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
            let out = Command::new(BIN)
                .args([
                    face,
                    "--socket",
                    sock.to_str().unwrap(),
                    "--store",
                    store.to_str().unwrap(),
                ])
                .args(flags)
                .stdin(Stdio::null())
                .output()
                .unwrap();
            assert!(!out.status.success(), "{face} {flags:?}");
            let err = String::from_utf8_lossy(&out.stderr);
            assert!(err.contains(says), "{face} {flags:?}: {err}");
            assert!(out.stdout.is_empty(), "{face} {flags:?}");
        }
    }
    assert!(!store.exists(), "a refused start opens no Store");
    let _ = fs::remove_dir_all(&dir);
}

// ---- a crash at each step of a rotation ----

/// Deliver until a rotation stops the Tracer at `step`, then restart it
/// without the hook: no entry is lost, no Record id is taken twice, and the
/// Mail in flight is stored once and acked with the Record id it was stored
/// under.
fn a_crash_at(step: &str) {
    let mut rig = Rig::new(&format!("crash-{step}"));
    // Each entry is some 320 bytes: the third makes the Store due.
    let rotate = ["--rotate-bytes", "850", "--cull"];
    let body = "k".repeat(200);
    let t = rig.tracer(
        "t1.out",
        &[&rotate[..], &["--crash-at-rotate", step][..]].concat(),
    );
    let mut stopped = 0;
    for n in 1..=6 {
        let mail = format!("k-{n}");
        rig.send(&mail, &body, &[]);
        let delivered = format!("DELIVERED mail={mail} record=r-{n}\n");
        let out = rig.p("t1.out");
        let ended = until(&format!("{mail} acked or the Tracer stopped"), 15, || {
            if rig.children[t].try_wait().unwrap().is_some() {
                Some(true)
            } else {
                read(&out).contains(&delivered).then_some(false)
            }
        });
        if ended {
            stopped = n;
            break;
        }
    }
    assert_eq!(stopped, 3, "the third delivery's rotation stops the Tracer");

    let (store, archive, next) = (rig.p("t.store"), rig.archive(1), rig.p("t.store.next"));
    let old = match step {
        "written" => {
            assert!(next.exists() && !archive.exists());
            rig.store()
        }
        "linked" => {
            assert!(next.exists());
            let (a, b) = (
                fs::metadata(&store).unwrap(),
                fs::metadata(&archive).unwrap(),
            );
            assert_eq!(
                (a.ino(), a.dev()),
                (b.ino(), b.dev()),
                "one file, two names"
            );
            rig.store()
        }
        _ => {
            assert!(!next.exists());
            assert!(
                rig.store().starts_with("{\"snapshot\":1,"),
                "{}",
                rig.store()
            );
            read(&archive)
        }
    };
    assert_eq!(records(&old), ["r-1", "r-2", "r-3"], "{old}");
    for n in 1..=3 {
        assert_eq!(count(&old, &format!("\"mail\":\"k-{n}\"")), 1, "{old}");
    }

    // The restart acks the Mail in flight with its Record id, storing nothing.
    rig.tracer("t2.out", &rotate);
    rig.wait_for("t2.out", "DELIVERED mail=k-3 record=r-3\n");
    rig.deliver("t2.out", "k-4", &body, "r-4");
    assert!(!read(&rig.p("t2.out")).contains("STORED mail=k-3 "));

    assert_eq!(read(&archive), old, "one archive: the file as it was");
    assert!(!rig.archive(2).exists() && !next.exists());
    let text = rig.store();
    let after = text
        .strip_prefix(&block(1, 4, &old))
        .unwrap_or_else(|| panic!("expected the snapshot block of {old}, saw {text}"));
    assert_eq!(records(after), ["r-4"], "{text}");
    assert!(after.contains("\"mail\":\"k-4\""), "{text}");
}

#[test]
fn a_crash_once_the_new_file_is_written_loses_nothing() {
    a_crash_at("written");
}

#[test]
fn a_crash_once_the_archive_is_linked_loses_nothing() {
    a_crash_at("linked");
}

#[test]
fn a_crash_once_the_new_file_is_in_place_loses_nothing() {
    a_crash_at("renamed");
}

//! Process-level seam for `roger mcp`: a real coordinator, host daemon and
//! MCP server on loopback, driven over stdio by the small JSON-RPC client
//! below. The expected values are the literals chosen here.

#[cfg(not(feature = "test-hooks"))]
compile_error!("tests/mcp.rs needs the test hooks: cargo test --features test-hooks");

#[allow(dead_code)]
#[path = "../src/json.rs"]
mod json;

use json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_roger");
const TOKEN: &str = "mcp-test-token";
const A1: &str = "11111111-1111-4111-8111-111111111111";
const A2: &str = "22222222-2222-4222-8222-222222222222";
const COMPOSITE: &str = "[[1a:FLEET:zz]]22222222-2222-4222-8222-222222222222";

// ---- the rig: coordinator and one host daemon ----

struct Rig {
    dir: PathBuf,
    daemon: usize,
    children: Vec<Child>,
    port: String,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn until<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if let Some(v) = f() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out after {secs}s waiting for {what}");
}

impl Rig {
    fn new(tag: &str) -> Rig {
        let tree = env!("CARGO_MANIFEST_DIR")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        let dir = std::env::temp_dir().join(format!("roger-mcp-{tree:016x}-{tag}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut rig = Rig {
            dir,
            daemon: 0,
            children: Vec::new(),
            port: String::new(),
        };
        let port_file = rig.p("coord.port");
        rig.spawn(&[
            "coordinator",
            "--bind",
            "127.0.0.1:0",
            "--port-file",
            port_file.to_str().unwrap(),
            "--data",
            rig.p("coord.log").to_str().unwrap(),
        ]);
        until("the coordinator port", 10, || {
            fs::read_to_string(&port_file)
                .ok()
                .filter(|t| !t.is_empty())
        });
        rig.port = fs::read_to_string(&port_file).unwrap().trim().to_string();
        let coord = format!("127.0.0.1:{}", rig.port);
        rig.daemon = rig.spawn(&[
            "daemon",
            "--coordinator",
            &coord,
            "--host-id",
            "h1",
            "--socket",
            rig.sock().to_str().unwrap(),
            "--data",
            rig.p("daemon.log").to_str().unwrap(),
        ]);
        // The socket file exists before the daemon listens: wait for an accept.
        until("the daemon socket to accept", 10, || {
            std::os::unix::net::UnixStream::connect(rig.sock())
                .ok()
                .map(drop)
        });
        rig
    }

    fn p(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Kill the coordinator (the first child) and, with `restart`, start it
    /// again on the same port and log.
    fn coordinator_down(&mut self) {
        self.children[0].kill().unwrap();
        self.children[0].wait().unwrap();
    }

    fn coordinator_up(&mut self) {
        let bind = format!("127.0.0.1:{}", self.port);
        let port_file = self.p("coord2.port");
        let log = self.p("coord.log");
        let args = [
            "coordinator",
            "--bind",
            &bind,
            "--port-file",
            port_file.to_str().unwrap(),
            "--data",
            log.to_str().unwrap(),
        ];
        let idx = self.spawn(&args);
        until("the restarted coordinator port", 15, || {
            port_file.exists().then_some(())
        });
        self.children.swap(0, idx);
    }

    fn sock(&self) -> PathBuf {
        self.p("d.sock")
    }

    fn spawn(&mut self, args: &[&str]) -> usize {
        let n = self.children.len();
        let child = Command::new(BIN)
            .args(args)
            .env("ROGER_TOKEN", TOKEN)
            .stdout(Stdio::from(
                fs::File::create(self.p(&format!("p{n}.out"))).unwrap(),
            ))
            .stderr(Stdio::from(
                fs::File::create(self.p(&format!("p{n}.err"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        self.children.push(child);
        n
    }

    /// `roger mcp` on this rig's daemon with `extra` flags and `envs` set.
    fn mcp(&self, name: &str, extra: &[&str], envs: &[(&str, &str)]) -> Mcp {
        let mut cmd = Command::new(BIN);
        cmd.arg("mcp")
            .args(["--socket", self.sock().to_str().unwrap()])
            .args(extra)
            .env_remove("ROGER_ADDRESS")
            .env_remove("ROGER_SOCKET")
            .env_remove("ROGER_STORE")
            .env_remove("XDG_RUNTIME_DIR")
            .envs(envs.iter().copied());
        Mcp::start(cmd, &self.p(&format!("{name}.err")))
    }

    /// A session with its own explicit Store, for most tests.
    fn session(&self, name: &str, address: &str, extra: &[&str]) -> Mcp {
        let store = self.p(&format!("{name}.store"));
        let mut args = vec!["--id", address, "--store", store.to_str().unwrap()];
        args.extend_from_slice(extra);
        let mut m = self.unregistered_session(name, &args);
        // The coordinator must hold the Register before a first send.
        let r = m.ok("roger_registered", r#"{"wait_seconds":20}"#);
        assert_eq!(r.get("registered"), Some(&Value::Bool(true)), "{r:?}");
        m
    }

    /// A session started and initialized, not waited for: it may be registered
    /// at the coordinator or not yet (or never, during an outage).
    fn unregistered_session(&self, name: &str, args: &[&str]) -> Mcp {
        let mut m = self.mcp(name, args, &[]);
        m.initialize();
        m
    }

    fn on_board(&self, address: &str) -> bool {
        let out = Command::new(BIN)
            .args(["board", "--socket", self.sock().to_str().unwrap()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).contains(&format!("SESSION address={address} "))
    }

    fn receipt_line(&self, mail: &str) -> String {
        let out = Command::new(BIN)
            .args([
                "receipt",
                "--socket",
                self.sock().to_str().unwrap(),
                "--mail",
                mail,
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).to_string()
    }
}

// ---- a JSON-RPC client over stdio ----

struct Mcp {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    seen: Vec<Value>,
    next_id: i64,
    err: PathBuf,
}

impl Mcp {
    fn start(mut cmd: Command, err: &Path) -> Mcp {
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(fs::File::create(err).unwrap()))
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines().map_while(Result::ok) {
                if tx.send(l).is_err() {
                    break;
                }
            }
        });
        Mcp {
            stdin: child.stdin.take(),
            child,
            lines,
            seen: Vec::new(),
            next_id: 0,
            err: err.to_path_buf(),
        }
    }

    fn write(&mut self, line: &str) {
        let w = self.stdin.as_mut().expect("stdin is open");
        writeln!(w, "{line}").unwrap();
        w.flush().unwrap();
    }

    fn send_request(&mut self, method: &str, params: &str) -> i64 {
        self.next_id += 1;
        let id = self.next_id;
        self.write(&format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params}}}"#
        ));
        id
    }

    /// The next line within `secs`, parsed; every output line must be one JSON object.
    fn next_line(&mut self, secs: u64) -> Option<Value> {
        let line = self.lines.recv_timeout(Duration::from_secs(secs)).ok()?;
        let v =
            json::parse(&line).unwrap_or_else(|e| panic!("stdout line is not JSON ({e}): {line}"));
        assert_eq!(
            v.get("jsonrpc").and_then(|j| j.as_str()),
            Some("2.0"),
            "{line}"
        );
        Some(v)
    }

    fn response(&mut self, id: i64, secs: u64) -> Value {
        if let Some(i) = self
            .seen
            .iter()
            .position(|v| v.get("id") == Some(&Value::Int(id)))
        {
            return self.seen.remove(i);
        }
        let end = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < end {
            let left = end
                .saturating_duration_since(Instant::now())
                .as_secs()
                .max(1);
            match self.next_line(left) {
                Some(v) if v.get("id") == Some(&Value::Int(id)) => return v,
                Some(v) => self.seen.push(v),
                None => break,
            }
        }
        panic!(
            "no response to request {id}; stderr: {}",
            fs::read_to_string(&self.err).unwrap_or_default()
        );
    }

    fn rpc(&mut self, method: &str, params: &str) -> Value {
        let id = self.send_request(method, params);
        self.response(id, 20)
    }

    fn initialize(&mut self) -> Value {
        let r = self.rpc(
            "initialize",
            r#"{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}"#,
        );
        self.write(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#);
        r
    }

    fn request_tool(&mut self, tool: &str, args: &str) -> i64 {
        self.send_request(
            "tools/call",
            &format!(r#"{{"name":"{tool}","arguments":{args}}}"#),
        )
    }

    /// (isError, text) of a tool result.
    fn result_of(resp: &Value) -> (bool, String) {
        let result = resp
            .get("result")
            .unwrap_or_else(|| panic!("no result: {resp:?}"));
        let Some(Value::Arr(content)) = result.get("content") else {
            panic!("no content: {resp:?}");
        };
        assert_eq!(content.len(), 1, "one content item: {resp:?}");
        assert_eq!(
            content[0].get("type").and_then(|t| t.as_str()),
            Some("text")
        );
        let text = content[0]
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap()
            .to_string();
        (result.get("isError") == Some(&Value::Bool(true)), text)
    }

    fn call(&mut self, tool: &str, args: &str) -> (bool, String) {
        let id = self.request_tool(tool, args);
        Mcp::result_of(&self.response(id, 20))
    }

    /// A successful tool result, parsed.
    fn ok(&mut self, tool: &str, args: &str) -> Value {
        let (err, text) = self.call(tool, args);
        assert!(!err, "{tool} failed: {text}");
        json::parse(&text).unwrap_or_else(|e| panic!("{tool} text is not JSON ({e}): {text}"))
    }

    /// A failed tool result's text.
    fn fails(&mut self, tool: &str, args: &str) -> String {
        let (err, text) = self.call(tool, args);
        assert!(err, "{tool} should be an isError result, saw {text}");
        text
    }

    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    fn wait_exit(&mut self) -> std::process::ExitStatus {
        until("the process to exit", 15, || self.child.try_wait().unwrap())
    }

    fn kill9(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.err).unwrap_or_default()
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn s(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or_else(|| panic!("no string {key} in {v:?}"))
        .to_string()
}

fn arr<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    match v.get(key) {
        Some(Value::Arr(a)) => a,
        other => panic!("no array {key}: {other:?}"),
    }
}

fn mails(inbox: &Value) -> Vec<String> {
    arr(inbox, "mail").iter().map(|m| s(m, "mail")).collect()
}

fn store_lines(path: &Path, needle: &str) -> usize {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| l.contains(needle))
        .count()
}

// ---- protocol ----

#[test]
fn handshake_versions_ping_unknown_method_and_parse_error() {
    let rig = Rig::new("protocol");
    let mut a = rig.mcp(
        "a",
        &["--id", A1, "--store", rig.p("a.store").to_str().unwrap()],
        &[],
    );
    let r = a.initialize();
    let result = r.get("result").unwrap();
    assert_eq!(s(result, "protocolVersion"), "2025-06-18");
    assert_eq!(
        result.get("capabilities"),
        json::parse(r#"{"tools":{}}"#).ok().as_ref()
    );
    let info = result.get("serverInfo").unwrap();
    assert_eq!(s(info, "name"), "roger");
    assert_eq!(s(info, "version"), env!("CARGO_PKG_VERSION"));

    for (asked, answered) in [
        ("2025-03-26", "2025-03-26"),
        ("2024-11-05", "2024-11-05"),
        ("2099-01-01", "2025-06-18"),
    ] {
        let r = a.rpc("initialize", &format!(r#"{{"protocolVersion":"{asked}"}}"#));
        assert_eq!(s(r.get("result").unwrap(), "protocolVersion"), answered);
    }

    assert_eq!(a.rpc("ping", "{}").get("result"), Some(&Value::Obj(vec![])));
    let r = a.rpc("no/such/method", "{}");
    assert_eq!(
        r.get("error").and_then(|e| e.get("code")),
        Some(&Value::Int(-32601))
    );

    let r = a.rpc("tools/list", "{}");
    let mut names: Vec<String> = arr(r.get("result").unwrap(), "tools")
        .iter()
        .map(|t| s(t, "name"))
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "roger_board",
            "roger_claim",
            "roger_inbox",
            "roger_label",
            "roger_receipt",
            "roger_registered",
            "roger_release",
            "roger_renew",
            "roger_send"
        ]
    );

    // A notification gets no reply, and unparseable input is -32700 with a null id.
    a.write(r#"{"jsonrpc":"2.0","method":"notifications/anything","params":{}}"#);
    a.write("{ this is not json");
    let v = a.next_line(10).expect("the parse error answer");
    assert_eq!(v.get("id"), Some(&Value::Null));
    assert_eq!(
        v.get("error").and_then(|e| e.get("code")),
        Some(&Value::Int(-32700))
    );
    assert_eq!(a.rpc("ping", "{}").get("result"), Some(&Value::Obj(vec![])));
}

#[test]
fn a_waiting_inbox_blocks_no_other_call_and_the_next_delivery_ends_the_wait() {
    let rig = Rig::new("concurrent");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", A2, &[]);
    let waiting = b.request_tool("roger_inbox", r#"{"wait_seconds":30}"#);
    assert_eq!(b.rpc("ping", "{}").get("result"), Some(&Value::Obj(vec![])));
    b.ok("roger_board", "{}");
    let started = Instant::now();
    a.ok(
        "roger_send",
        &format!(r#"{{"to":"{A2}","mail":"w1","body":"late"}}"#),
    );
    let (err, text) = Mcp::result_of(&b.response(waiting, 10));
    assert!(!err, "{text}");
    assert_eq!(mails(&json::parse(&text).unwrap()), ["w1"]);
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[test]
fn a_wait_with_nothing_to_read_ends_at_its_bound_with_no_entries_and_a_null_cursor() {
    let rig = Rig::new("bound");
    let mut a = rig.session("a", A1, &[]);
    let started = Instant::now();
    let r = a.ok("roger_inbox", r#"{"wait_seconds":1}"#);
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert_eq!(arr(&r, "mail").len(), 0);
    assert_eq!(r.get("cursor"), Some(&Value::Null));
    let started = Instant::now();
    a.ok("roger_inbox", r#"{"wait_seconds":0}"#);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn argument_errors_are_results_that_say_what_was_expected_and_seen() {
    let rig = Rig::new("args");
    let mut a = rig.session("a", A1, &[]);
    let t = a.fails("roger_send", r#"{"to":5,"body":"x"}"#);
    assert!(
        t.contains("expected") && t.contains("`to`") && t.contains("saw an integer"),
        "{t}"
    );
    let t = a.fails("roger_send", r#"{"to":"x"}"#);
    assert!(t.contains("`body`") && t.contains("saw nothing"), "{t}");
    let t = a.fails("roger_inbox", r#"{"limit":"ten"}"#);
    assert!(t.contains("an integer") && t.contains("a string"), "{t}");
    let t = a.fails("roger_inbox", r#"{"limit":0}"#);
    assert!(t.contains("from 1 to 200") && t.contains("saw 0"), "{t}");
    let t = a.fails("roger_inbox", r#"{"peek":1}"#);
    assert!(t.contains("a boolean"), "{t}");
    let t = a.fails("roger_renew", r#"{"key":"k"}"#);
    assert!(t.contains("ttl_seconds"), "{t}");
    let t = a.fails("roger_claim", r#"{"key":"k","ttl_seconds":"soon"}"#);
    assert!(t.contains("ttl_seconds") && t.contains("a string"), "{t}");
    let t = a.fails("roger_nothing", "{}");
    assert!(
        t.contains("roger_nothing") && t.contains("roger_send"),
        "{t}"
    );
    let t = a.fails("roger_label", r#"{"label":7}"#);
    assert!(t.contains("`label`"), "{t}");
    let (err, text) = a.call("roger_board", "[]");
    assert!(err && text.contains("an object"), "{text}");
    // The server is still serving.
    assert_eq!(a.rpc("ping", "{}").get("result"), Some(&Value::Obj(vec![])));
}

#[test]
fn a_refused_header_set_is_the_cores_reason_in_an_error_result() {
    let rig = Rig::new("headers");
    let mut a = rig.session("a", A1, &[]);
    let t = a.fails(
        "roger_send",
        &format!(r#"{{"to":"{A2}","body":"x","headers":{{"a":1}}}}"#),
    );
    assert!(
        t.contains("headers: expected an object of string values, saw a number"),
        "{t}"
    );
    let t = a.fails(
        "roger_send",
        &format!(r#"{{"to":"{A2}","mail":"h-nope","body":"x","headers":"flat"}}"#),
    );
    assert!(
        t.contains("expected an object") && t.contains("a string"),
        "{t}"
    );
    assert!(!rig.receipt_line("h-nope").starts_with("RECEIPT"));
    assert!(
        rig.receipt_line("h-nope").is_empty(),
        "a refused send commits nothing"
    );
}

// ---- mail, the Store and the inbox ----

#[test]
fn two_sessions_exchange_mail_with_headers_and_each_body_is_stored_once() {
    let rig = Rig::new("exchange");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", COMPOSITE, &[]);
    // The composite Address is registered exactly as given.
    assert_eq!(s(&b.ok("roger_board", "{}"), "me"), COMPOSITE);
    until("b on the Board", 10, || {
        rig.on_board(COMPOSITE).then_some(())
    });

    let sent = a.ok(
        "roger_send",
        &format!(
            r#"{{"to":"{COMPOSITE}","mail":"m1","body":"hello ✓","headers":{{"x-ticket":"4417","note":"a \"b\"\n"}}}}"#
        ),
    );
    assert_eq!(s(&sent, "mail"), "m1");
    let inbox = b.ok("roger_inbox", r#"{"wait_seconds":10}"#);
    let m = &arr(&inbox, "mail")[0];
    assert_eq!(arr(&inbox, "mail").len(), 1);
    assert_eq!(s(m, "mail"), "m1");
    assert_eq!(s(m, "from"), A1);
    assert_eq!(s(m, "body"), "hello ✓");
    assert_eq!(s(m, "kind"), "directed");
    assert_eq!(
        m.get("headers"),
        json::parse(r#"{"x-ticket":"4417","note":"a \"b\"\n"}"#)
            .ok()
            .as_ref()
    );
    assert_eq!(s(&inbox, "cursor"), s(m, "record"));
    assert_eq!(s(m, "record"), "r-1");

    let again = b.ok("roger_inbox", r#"{"wait_seconds":0}"#);
    assert_eq!(arr(&again, "mail").len(), 0);
    assert_eq!(store_lines(&rig.p("b.store"), r#""mail":"m1""#), 1);

    // The receipt names the record the Store gave it.
    let receipt = until("the receipt", 10, || {
        let r = a.ok("roger_receipt", r#"{"mail":"m1"}"#);
        r.get("record").is_some().then_some(r)
    });
    assert_eq!(s(&receipt, "record"), "r-1");
    assert_eq!(s(&receipt, "to"), COMPOSITE);
    assert_eq!(s(&receipt, "kind"), "directed");
    assert!(receipt
        .get("accepted_at_ms")
        .and_then(|t| t.as_i64())
        .is_some());
    assert!(receipt
        .get("receipted_at_ms")
        .and_then(|t| t.as_i64())
        .is_some());
    assert_eq!(
        receipt.get("headers").and_then(|h| h.get("x-ticket")),
        Some(&json::s("4417"))
    );
    let t = a.fails("roger_receipt", r#"{"mail":"no-such-mail"}"#);
    assert!(t.contains("no-such-mail"), "{t}");

    // A Broadcast reaches b with kind broadcast.
    a.ok("roger_send", r#"{"to":"*","mail":"bc1","body":"all"}"#);
    let bc = b.ok("roger_inbox", r#"{"wait_seconds":10}"#);
    assert_eq!(s(&arr(&bc, "mail")[0], "kind"), "broadcast");
}

#[test]
fn a_generated_mail_id_is_returned_and_a_store_entry_without_kind_or_headers_shows_neither() {
    let rig = Rig::new("old-entry");
    fs::write(
        rig.p("a.store"),
        format!(
            "{{\"record\":\"r-1\",\"mail\":\"old\",\"from\":\"{A2}\",\"body\":\"before kinds\"}}\n"
        ),
    )
    .unwrap();
    let mut a = rig.session("a", A1, &[]);
    let inbox = a.ok("roger_inbox", r#"{"wait_seconds":0}"#);
    let m = &arr(&inbox, "mail")[0];
    assert_eq!(s(m, "mail"), "old");
    assert!(
        m.get("kind").is_none() && m.get("headers").is_none(),
        "{m:?}"
    );
    let sent = a.ok("roger_send", &format!(r#"{{"to":"{A2}","body":"x"}}"#));
    assert!(!s(&sent, "mail").is_empty());
}

#[test]
fn limit_and_peek_and_the_durable_cursor_across_a_restart() {
    let rig = Rig::new("position");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", A2, &[]);
    for m in ["p1", "p2", "p3"] {
        a.ok(
            "roger_send",
            &format!(r#"{{"to":"{A2}","mail":"{m}","body":"{m}"}}"#),
        );
    }
    until("p3 in the Store", 10, || {
        (store_lines(&rig.p("b.store"), r#""mail":"p3""#) == 1).then_some(())
    });
    // peek returns without moving the position, however often it is asked
    for _ in 0..2 {
        let r = b.ok("roger_inbox", r#"{"peek":true,"limit":2}"#);
        assert_eq!(mails(&r), ["p1", "p2"]);
    }
    assert!(
        !rig.p("b.store.cursor").exists(),
        "peek must not save a position"
    );
    let r = b.ok("roger_inbox", r#"{"limit":2}"#);
    assert_eq!(mails(&r), ["p1", "p2"]);
    assert_eq!(s(&r, "cursor"), "r-2");
    assert_eq!(
        fs::read_to_string(rig.p("b.store.cursor")).unwrap().trim(),
        "r-2"
    );

    // The position survives a restart: p3 only, once.
    b.close_stdin();
    assert_eq!(b.wait_exit().code(), Some(0));
    let mut b = rig.session("b", A2, &[]);
    let r = b.ok("roger_inbox", r#"{"wait_seconds":0}"#);
    assert_eq!(mails(&r), ["p3"]);
    assert_eq!(
        mails(&b.ok("roger_inbox", r#"{"wait_seconds":0}"#)).len(),
        0
    );

    // And a limit above the maximum is held to it, not refused.
    b.ok("roger_inbox", r#"{"limit":100000,"wait_seconds":0}"#);
}

#[test]
fn sigkill_between_the_store_write_and_the_ack_loses_nothing_and_stores_once() {
    let rig = Rig::new("sigkill");
    let mut a = rig.session("a", A1, &[]);
    let marker = rig.p("held");
    let mut b = rig.session("b", A2, &["--hold-before-ack", marker.to_str().unwrap()]);
    a.ok(
        "roger_send",
        &format!(r#"{{"to":"{A2}","mail":"c0","body":"consumed"}}"#),
    );
    // The hold hook parks before the ack and leaves the marker: c0 is in the Store.
    until("the held marker", 15, || marker.exists().then_some(()));
    b.kill9();
    assert!(
        !rig.receipt_line("c0").starts_with("RECEIPT"),
        "no receipt before the Ack"
    );
    assert_eq!(store_lines(&rig.p("b.store"), r#""mail":"c0""#), 1);

    // Restart (the marker exists, so it acks): the receipt appears and the body is stored once.
    let mut b = rig.session("b", A2, &[]);
    until("the receipt after restart", 15, || {
        rig.receipt_line("c0").starts_with("RECEIPT").then_some(())
    });
    let line = rig.receipt_line("c0");
    assert!(line.contains("record=r-1"), "{line}");
    assert_eq!(store_lines(&rig.p("b.store"), r#""mail":"c0""#), 1);
    let r = b.ok("roger_inbox", r#"{"wait_seconds":5}"#);
    assert_eq!(
        mails(&r),
        ["c0"],
        "an entry stored before the kill is returned once"
    );
    assert_eq!(
        mails(&b.ok("roger_inbox", r#"{"wait_seconds":0}"#)).len(),
        0
    );

    // An entry consumed before a SIGKILL is not returned again.
    a.ok(
        "roger_send",
        &format!(r#"{{"to":"{A2}","mail":"c1","body":"x"}}"#),
    );
    assert_eq!(
        mails(&b.ok("roger_inbox", r#"{"wait_seconds":10}"#)),
        ["c1"]
    );
    b.kill9();
    let mut b = rig.session("b", A2, &[]);
    assert_eq!(
        mails(&b.ok("roger_inbox", r#"{"wait_seconds":0}"#)).len(),
        0
    );
}

// ---- board, label, claims ----

#[test]
fn board_label_and_claims_follow_the_core() {
    let rig = Rig::new("board");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", A2, &[]);
    until("both on the Board", 10, || {
        (rig.on_board(A1) && rig.on_board(A2)).then_some(())
    });
    let board = a.ok("roger_board", "{}");
    assert_eq!(s(&board, "me"), A1);
    assert!(board.get("now_ms").and_then(|n| n.as_i64()).unwrap() > 0);
    let who: Vec<String> = arr(&board, "sessions")
        .iter()
        .map(|r| s(r, "session"))
        .collect();
    assert!(
        who.contains(&A1.to_string()) && who.contains(&A2.to_string()),
        "{who:?}"
    );

    let l = a.ok("roger_label", r#"{"label":"builder [[0:FLEET:0]]"}"#);
    assert_eq!(s(&l, "session"), A1);
    assert_eq!(s(&l, "label"), "builder [[0:FLEET:0]]");
    let board = b.ok("roger_board", "{}");
    let row = arr(&board, "sessions")
        .iter()
        .find(|r| s(r, "session") == A1)
        .unwrap();
    assert_eq!(s(row, "label"), "builder [[0:FLEET:0]]");

    let c = a.ok("roger_claim", r#"{"key":"job-1","ttl_seconds":60}"#);
    assert_eq!(c.get("ok"), Some(&Value::Bool(true)));
    assert_eq!(s(&c, "op"), "acquire");
    assert_eq!(s(&c, "key"), "job-1");
    // A second session's claim is a refusal result naming the holder, not an error.
    let c = b.ok("roger_claim", r#"{"key":"job-1"}"#);
    assert_eq!(c.get("ok"), Some(&Value::Bool(false)));
    assert_eq!(s(&c, "holder"), A1);
    let c = b.ok("roger_release", r#"{"key":"job-1"}"#);
    assert_eq!(
        c.get("ok"),
        Some(&Value::Bool(false)),
        "only the holder releases"
    );
    let c = a.ok("roger_renew", r#"{"key":"job-1","ttl_seconds":120}"#);
    assert_eq!(c.get("ok"), Some(&Value::Bool(true)));
    assert!(c.get("remaining_ms").and_then(|r| r.as_i64()).unwrap() > 60_000);
    assert_eq!(arr(&a.ok("roger_board", "{}"), "claims").len(), 1);
    let c = a.ok("roger_release", r#"{"key":"job-1"}"#);
    assert_eq!(c.get("ok"), Some(&Value::Bool(true)));
    assert_eq!(arr(&a.ok("roger_board", "{}"), "claims").len(), 0);
    assert_eq!(
        b.ok("roger_claim", r#"{"key":"job-1"}"#).get("ok"),
        Some(&Value::Bool(true))
    );
}

#[test]
fn the_board_lists_only_present_sessions_unless_all() {
    let rig = Rig::new("present");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", A2, &[]);
    until("both on the Board", 10, || {
        (rig.on_board(A1) && rig.on_board(A2)).then_some(())
    });
    // A session that dropped without unregistering stays registered but is not connected.
    b.kill9();
    until("b to be disconnected", 10, || {
        let board = a.ok("roger_board", r#"{"all":true}"#);
        let row = arr(&board, "sessions")
            .iter()
            .find(|r| s(r, "session") == A2)
            .cloned()?;
        (row.get("connected") == Some(&Value::Bool(false))).then_some(())
    });
    let seen_recently = |m: &mut Mcp, args: &str| {
        let board = m.ok("roger_board", args);
        arr(&board, "sessions")
            .iter()
            .any(|r| s(r, "session") == A2)
    };
    assert!(
        seen_recently(&mut a, "{}"),
        "seen within the default window"
    );
    assert!(
        !seen_recently(&mut a, r#"{"window_seconds":0}"#),
        "not within a zero window"
    );
    assert!(
        seen_recently(&mut a, r#"{"all":true,"window_seconds":0}"#),
        "all lists every session"
    );
    let t = a.fails("roger_board", r#"{"window_seconds":-1}"#);
    assert!(t.contains("window_seconds") && t.contains("saw -1"), "{t}");
}

// ---- the process ----

#[test]
fn address_precedence_flag_then_named_variable_then_default_variable() {
    let rig = Rig::new("address");
    let store = |n: &str| rig.p(n).to_str().unwrap().to_string();
    let me = |m: &mut Mcp| {
        // A Board read needs the coordinator to hold the Register.
        let r = m.ok("roger_registered", r#"{"wait_seconds":20}"#);
        assert_eq!(r.get("registered"), Some(&Value::Bool(true)), "{r:?}");
        s(&m.ok("roger_board", "{}"), "me")
    };

    let (s1, s2, s3, s4) = (
        store("1.store"),
        store("2.store"),
        store("3.store"),
        store("4.store"),
    );
    let mut m = rig.mcp(
        "m1",
        &["--id", A1, "--id-env", "NAMED", "--store", &s1],
        &[("NAMED", "from-named"), ("ROGER_ADDRESS", "from-default")],
    );
    m.initialize();
    assert_eq!(me(&mut m), A1);
    let mut m2 = rig.mcp(
        "m2",
        &["--id-env", "NAMED", "--store", &s2],
        &[("NAMED", COMPOSITE), ("ROGER_ADDRESS", "from-default")],
    );
    m2.initialize();
    assert_eq!(me(&mut m2), COMPOSITE);
    let mut m3 = rig.mcp(
        "m3",
        &["--id-env", "UNSET_NAME", "--store", &s3],
        &[("ROGER_ADDRESS", "from-default")],
    );
    m3.initialize();
    assert_eq!(me(&mut m3), "from-default");
    let mut m4 = rig.mcp(
        "m4",
        &["--id-env", "EMPTY", "--store", &s4],
        &[("EMPTY", "")],
    );
    m4.initialize();
    let fresh = me(&mut m4);
    assert_eq!(fresh.len(), 36, "a fresh UUID: {fresh}");
}

#[test]
fn an_empty_default_store_is_removed_at_exit_and_an_explicit_one_never() {
    let rig = Rig::new("default-store");
    let state = rig.p("state");
    let state_s = state.to_str().unwrap();
    let default = state.join("roger/mcp/health.store");

    // A health check: starts, answers, closes. It leaves nothing.
    let mut h = rig.mcp("h", &["--id", "health"], &[("XDG_STATE_HOME", state_s)]);
    h.initialize();
    assert!(
        default.exists(),
        "the default Store is XDG_STATE_HOME/roger/mcp/<address>.store"
    );
    h.close_stdin();
    assert_eq!(h.wait_exit().code(), Some(0));
    assert!(!default.exists());
    assert!(!state.join("roger/mcp/health.store.cursor").exists());
    assert!(h.stderr().is_empty(), "{}", h.stderr());

    // One that received Mail keeps its Store and position.
    let mut a = rig.session("a", A1, &[]);
    let mut g = rig.mcp("g", &["--id", "keeper"], &[("XDG_STATE_HOME", state_s)]);
    g.initialize();
    until("keeper on the Board", 10, || {
        rig.on_board("keeper").then_some(())
    });
    a.ok("roger_send", r#"{"to":"keeper","body":"x"}"#);
    assert_eq!(
        arr(&g.ok("roger_inbox", r#"{"wait_seconds":10}"#), "mail").len(),
        1
    );
    g.close_stdin();
    assert_eq!(g.wait_exit().code(), Some(0));
    assert!(state.join("roger/mcp/keeper.store").exists());
    assert!(state.join("roger/mcp/keeper.store.cursor").exists());

    // An explicit Store is never removed, even when empty.
    let explicit = rig.p("explicit.store");
    let mut e = rig.mcp(
        "e",
        &["--id", "explicit", "--store", explicit.to_str().unwrap()],
        &[],
    );
    e.initialize();
    e.close_stdin();
    assert_eq!(e.wait_exit().code(), Some(0));
    assert!(explicit.exists());
}

#[test]
fn closing_stdin_unregisters_and_losing_the_daemon_exits_nonzero_with_a_reason() {
    let mut rig = Rig::new("exit");
    let mut a = rig.session("a", A1, &[]);
    let mut b = rig.session("b", A2, &[]);
    until("both on the Board", 10, || {
        (rig.on_board(A1) && rig.on_board(A2)).then_some(())
    });
    b.close_stdin();
    assert_eq!(b.wait_exit().code(), Some(0));
    until("b to leave the Board", 10, || {
        (!rig.on_board(A2)).then_some(())
    });

    let daemon = rig.daemon;
    rig.children[daemon].kill().unwrap();
    rig.children[daemon].wait().unwrap();
    let status = a.wait_exit();
    assert_eq!(status.code(), Some(1));
    assert!(a.stderr().contains("host daemon"), "{}", a.stderr());
    // stdout held protocol messages only: the reader panics on anything else.
    while a.next_line(1).is_some() {}
}

#[test]
fn a_missing_socket_is_an_error_naming_what_was_expected_and_seen_and_stdout_stays_clean() {
    let dir = std::env::temp_dir().join(format!("roger-mcp-nosock-{}", std::process::id()));
    let _ = fs::create_dir_all(&dir);
    let gone = dir.join("none.sock");
    let out = Command::new(BIN)
        .args([
            "mcp",
            "--socket",
            gone.to_str().unwrap(),
            "--store",
            dir.join("x.store").to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("expected") && err.contains("saw") && err.contains("none.sock"),
        "{err}"
    );

    let out = Command::new(BIN)
        .arg("mcp")
        .env_remove("ROGER_SOCKET")
        .env_remove("XDG_RUNTIME_DIR")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("XDG_RUNTIME_DIR") && err.contains("saw"),
        "{err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn a_second_server_on_one_store_is_refused_not_started() {
    let rig = Rig::new("twice");
    let _a = rig.session("a", A1, &[]);
    let store = rig.p("a.store");
    let mut second = rig.mcp("a2", &["--id", A2, "--store", store.to_str().unwrap()], &[]);
    let status = second.wait_exit();
    assert!(!status.success());
    assert!(
        second.stderr().contains("held by another process"),
        "{}",
        second.stderr()
    );
}

#[test]
fn roger_registered_waits_for_the_coordinator_to_hold_the_register() {
    let rig = Rig::new("registered");
    let mut a = rig.session("a", A1, &[]);
    let r = a.ok("roger_registered", r#"{"wait_seconds":10}"#);
    assert_eq!(r.get("registered"), Some(&Value::Bool(true)));
    assert!(rig.on_board(A1));
    let r = a.ok("roger_registered", "{}");
    assert_eq!(r.get("registered"), Some(&Value::Bool(true)));
}

#[test]
fn roger_registered_is_false_during_an_outage_and_true_after_the_restart() {
    let mut rig = Rig::new("registered-outage");
    rig.coordinator_down();
    let store = rig.p("a.store");
    let mut a = rig.unregistered_session("a", &["--id", A1, "--store", store.to_str().unwrap()]);
    let r = a.ok("roger_registered", r#"{"wait_seconds":0}"#);
    assert_eq!(r.get("registered"), Some(&Value::Bool(false)));
    rig.coordinator_up();
    let r = a.ok("roger_registered", r#"{"wait_seconds":20}"#);
    assert_eq!(r.get("registered"), Some(&Value::Bool(true)));
    assert!(rig.on_board(A1));
    let e = a.fails("roger_registered", r#"{"wait_seconds":-1}"#);
    assert!(e.contains("wait_seconds"), "{e}");
}

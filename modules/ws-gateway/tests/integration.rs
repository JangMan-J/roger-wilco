//! End-to-end suite for `roger-ws-gateway` against real `roger coordinator`,
//! `roger daemon`, `roger send`, `roger board`, `roger receipt`, and `roger audit` processes.

#[cfg(not(feature = "test-hooks"))]
compile_error!("integration tests require `--features test-hooks`");

use futures_util::{SinkExt, StreamExt};
use roger_ws_gateway::durlog::store_path;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const GATEWAY_BIN: &str = env!("CARGO_BIN_EXE_roger-ws-gateway");
const FLEET_TOKEN: &str = "fleet-integration-token";
const GATEWAY_BEARER: &str = "ws-gateway-secret-bearer";

const ADDR_A: &str = "11111111-1111-4111-8111-111111111111";
const ADDR_B: &str = "22222222-2222-4222-8222-222222222222";
const ADDR_COMPOSITE: &str = "[[153:Meteor Shower:7]]33333333-3333-4333-8333-333333333333";
const ADDR_COMPOSITE_Q: &str =
    "%5B%5B153%3AMeteor%20Shower%3A7%5D%5D33333333-3333-4333-8333-333333333333";

/// Build `roger` (with `--features test-hooks`) once per test binary into the root `target/` dir.
fn roger_bin() -> &'static Path {
    static ROGER: OnceLock<PathBuf> = OnceLock::new();
    ROGER.get_or_init(|| {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("modules/ws-gateway lives two levels under repo root");
        let root_manifest = repo_root.join("Cargo.toml");
        let target_dir = repo_root.join("target").join("roger-for-ws-gateway-tests");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args([
                "build",
                "--offline",
                "--features",
                "test-hooks",
                "--manifest-path",
                root_manifest.to_str().unwrap(),
                "--target-dir",
                target_dir.to_str().unwrap(),
            ])
            .status()
            .expect("failed to invoke cargo build for roger");
        assert!(status.success(), "building roger binary failed");
        let bin = target_dir
            .join("debug")
            .join(format!("roger{}", std::env::consts::EXE_SUFFIX));
        assert!(bin.exists(), "expected built roger at {}", bin.display());
        bin
    })
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(mut cmd: Command, out_path: &Path, err_path: &Path) -> Self {
        let out = fs::File::create(out_path).unwrap();
        let err = fs::File::create(err_path).unwrap();
        let child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .expect("failed to spawn child process");
        Self { child }
    }

    fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

async fn wait_until<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {secs}s waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct TestRig {
    dir: PathBuf,
    coord_port: u16,
    daemon_tcp_port: u16,
    processes: Vec<ChildGuard>,
    seq: usize,
}

impl TestRig {
    async fn start(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("rws-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let mut rig = Self {
            dir,
            coord_port: 0,
            daemon_tcp_port: 0,
            processes: Vec::new(),
            seq: 0,
        };

        let coord_port_file = rig.path("coord.port");
        let coord_log = rig.coord_log();
        rig.spawn_roger(&[
            "coordinator",
            "--bind",
            "127.0.0.1:0",
            "--port-file",
            coord_port_file.to_str().unwrap(),
            "--data",
            coord_log.to_str().unwrap(),
        ]);

        let port_str = wait_until("coordinator port file", 10, || {
            fs::read_to_string(&coord_port_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .await;
        rig.coord_port = port_str.parse().unwrap();

        let coord_addr = format!("127.0.0.1:{}", rig.coord_port);
        let dlog = rig.daemon_log();
        let dport_file = rig.path("daemon.port");
        #[cfg(unix)]
        let sock_path = rig.path("daemon.sock");
        #[cfg(unix)]
        rig.spawn_roger(&[
            "daemon",
            "--coordinator",
            &coord_addr,
            "--host-id",
            "host-a",
            "--socket",
            sock_path.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            dport_file.to_str().unwrap(),
            "--data",
            dlog.to_str().unwrap(),
        ]);
        #[cfg(not(unix))]
        rig.spawn_roger(&[
            "daemon",
            "--coordinator",
            &coord_addr,
            "--host-id",
            "host-a",
            "--listen",
            "127.0.0.1:0",
            "--port-file",
            dport_file.to_str().unwrap(),
            "--data",
            dlog.to_str().unwrap(),
        ]);

        let dport_str = wait_until("daemon port file", 10, || {
            fs::read_to_string(&dport_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .await;
        rig.daemon_tcp_port = dport_str.parse().unwrap();

        #[cfg(unix)]
        wait_until("daemon unix socket ready", 10, || {
            std::os::unix::net::UnixStream::connect(&sock_path)
                .ok()
                .map(drop)
        })
        .await;

        rig
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn coord_log(&self) -> PathBuf {
        self.path("coord.log")
    }

    fn daemon_log(&self) -> PathBuf {
        self.path("daemon.log")
    }

    fn daemon_sock(&self) -> PathBuf {
        #[cfg(unix)]
        {
            self.path("daemon.sock")
        }
        #[cfg(not(unix))]
        {
            PathBuf::from(format!("tcp://127.0.0.1:{}", self.daemon_tcp_port))
        }
    }

    fn store_dir(&self) -> PathBuf {
        self.path("stores")
    }

    fn next_logs(&mut self, label: &str) -> (PathBuf, PathBuf) {
        self.seq += 1;
        (
            self.path(&format!("{}-{label}.out", self.seq)),
            self.path(&format!("{}-{label}.err", self.seq)),
        )
    }

    fn spawn_roger(&mut self, args: &[&str]) -> usize {
        let (out, err) = self.next_logs("roger");
        let mut cmd = Command::new(roger_bin());
        cmd.args(args).env("ROGER_TOKEN", FLEET_TOKEN);
        let idx = self.processes.len();
        self.processes.push(ChildGuard::spawn(cmd, &out, &err));
        idx
    }

    async fn spawn_gateway(&mut self, extra_args: &[&str], envs: &[(&str, &str)]) -> (usize, u16) {
        self.spawn_gateway_with_socket(
            self.daemon_sock().to_str().unwrap(),
            &self.store_dir(),
            extra_args,
            envs,
        )
        .await
    }

    async fn spawn_gateway_with_socket(
        &mut self,
        socket_arg: &str,
        store_dir: &Path,
        extra_args: &[&str],
        envs: &[(&str, &str)],
    ) -> (usize, u16) {
        let port_file = self.path(&format!("gw-{}.port", self.seq + 1));
        let _ = fs::remove_file(&port_file);
        let (out, err) = self.next_logs("gateway");
        let mut cmd = Command::new(GATEWAY_BIN);
        cmd.args([
            "--bind",
            "127.0.0.1:0",
            "--socket",
            socket_arg,
            "--store-dir",
            store_dir.to_str().unwrap(),
            "--port-file",
            port_file.to_str().unwrap(),
        ])
        .args(extra_args);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let idx = self.processes.len();
        self.processes.push(ChildGuard::spawn(cmd, &out, &err));

        let port_text = wait_until("gateway port file", 10, || {
            fs::read_to_string(&port_file)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .await;
        (idx, port_text.parse().unwrap())
    }
}

impl Drop for TestRig {
    fn drop(&mut self) {
        for proc in &mut self.processes {
            proc.kill_and_wait();
        }
        let _ = fs::remove_dir_all(&self.dir);
    }
}

// ---- WebSocket test client ----

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One WS session seen from the client: unsolicited lines (`ready`, `registered`,
/// `superseded`, `mail`) are queued apart from replies, which pair by order.
struct Client {
    ws: Ws,
    events: VecDeque<(Value, String)>,
}

fn unsolicited(v: &Value) -> bool {
    matches!(
        v["type"].as_str(),
        Some("ready" | "registered" | "superseded" | "mail")
    )
}

async fn ws_open(port: u16, query: &str, bearer: Option<&str>) -> Result<Client, WsError> {
    let mut req = format!("ws://127.0.0.1:{port}/?{query}")
        .into_client_request()
        .unwrap();
    if let Some(b) = bearer {
        req.headers_mut()
            .insert("authorization", format!("Bearer {b}").parse().unwrap());
    }
    let (ws, _) = connect_async(req).await?;
    Ok(Client {
        ws,
        events: VecDeque::new(),
    })
}

fn refused_status(r: Result<Client, WsError>) -> u16 {
    match r {
        Err(WsError::Http(resp)) => resp.status().as_u16(),
        Err(e) => panic!("expected an HTTP refusal, saw {e}"),
        Ok(_) => panic!("expected an HTTP refusal, saw an upgrade"),
    }
}

impl Client {
    async fn read_frame(&mut self) -> Option<(Value, String)> {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(10), self.ws.next())
                .await
                .expect("timed out waiting for a frame");
            match msg {
                Some(Ok(Message::Text(t))) => {
                    let raw = t.to_string();
                    let v: Value = serde_json::from_str(&raw).expect("frame holds one JSON line");
                    return Some((v, raw));
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => {}
            }
        }
    }

    async fn event(&mut self) -> (Value, String) {
        if let Some(e) = self.events.pop_front() {
            return e;
        }
        let (v, raw) = self
            .read_frame()
            .await
            .expect("stream ended before an event");
        assert!(
            unsolicited(&v),
            "expected an unsolicited line, saw reply {raw}"
        );
        (v, raw)
    }

    async fn reply(&mut self) -> Value {
        loop {
            let (v, raw) = self
                .read_frame()
                .await
                .expect("stream ended before a reply");
            if unsolicited(&v) {
                self.events.push_back((v, raw));
            } else {
                return v;
            }
        }
    }

    async fn send_text(&mut self, text: &str) {
        self.ws.send(Message::text(text.to_string())).await.unwrap();
    }

    async fn call(&mut self, v: Value) -> Value {
        self.send_text(&v.to_string()).await;
        self.reply().await
    }

    async fn expect_ready(&mut self, address: &str, registered: bool) {
        let (v, _) = self.event().await;
        assert_eq!(v, json!({"type":"ready","session":address}));
        if registered {
            let (v, _) = self.event().await;
            assert_eq!(v, json!({"type":"registered","session":address}));
        }
    }

    async fn close(mut self) {
        let _ = self.ws.close(None).await;
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_secs(5), self.ws.next()).await
        {}
    }
}

fn roger(args: &[&str]) -> String {
    let out = Command::new(roger_bin()).args(args).output().unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

async fn wait_receipt(rig: &TestRig, mail: &str) -> String {
    let sock = rig.daemon_sock();
    wait_until("receipt", 10, || {
        let s = roger(&[
            "receipt",
            "--socket",
            sock.to_str().unwrap(),
            "--mail",
            mail,
        ]);
        s.starts_with("RECEIPT ").then_some(s)
    })
    .await
}

// ---- Test 1: send/receive across two sessions, board/claim/relabel parity, close unregisters ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_exchange_mail_with_board_claim_relabel_parity_and_close_unregisters() {
    let mut rig = TestRig::start("exchange").await;
    let (_, port) = rig.spawn_gateway(&[], &[]).await;
    let sock = rig.daemon_sock();
    let sock = sock.to_str().unwrap();

    let mut a = ws_open(port, &format!("id={ADDR_A}&label=alpha&registered=1"), None)
        .await
        .unwrap();
    a.expect_ready(ADDR_A, true).await;
    let mut b = ws_open(
        port,
        &format!("session={ADDR_COMPOSITE_Q}&registered=1&declare=desk%202"),
        None,
    )
    .await
    .unwrap();
    b.expect_ready(ADDR_COMPOSITE, true).await;

    // Directed send with ordered headers: accepted, then one mail frame on B.
    let accepted = a
        .call(
            json!({"type":"send","to":ADDR_COMPOSITE,"body":"hello b","mail":"m-1",
                     "headers":{"z-k":"9","a-k":"1"}}),
        )
        .await;
    assert_eq!(accepted, json!({"type":"accepted","mail":"m-1"}));
    let (_, raw) = b.event().await;
    assert_eq!(
        raw,
        format!(
            r#"{{"type":"mail","mail":"m-1","from":"{ADDR_A}","body":"hello b","record":"r-1","kind":"directed","headers":{{"z-k":"9","a-k":"1"}}}}"#
        )
    );
    assert!(wait_receipt(&rig, "m-1").await.contains("record=r-1 "));

    // Reverse direction with a generated mail id.
    let accepted = b
        .call(json!({"type":"send","to":ADDR_A,"body":"hello a"}))
        .await;
    assert_eq!(accepted["type"], "accepted");
    let generated = accepted["mail"].as_str().unwrap().to_string();
    let (v, _) = a.event().await;
    assert_eq!(v["mail"], generated.as_str());
    assert_eq!(v["from"], ADDR_COMPOSITE);
    assert_eq!(v["record"], "r-1");
    assert_eq!(v["headers"], json!({}));

    // Broadcast reaches B as kind broadcast.
    let accepted = a
        .call(json!({"type":"send","to":"*","body":"all hands","mail":"m-bc"}))
        .await;
    assert_eq!(accepted["type"], "accepted");
    let (v, _) = b.event().await;
    assert_eq!(v["kind"], "broadcast");
    assert_eq!(v["body"], "all hands");
    assert_eq!(v["record"], "r-2");

    // Store holds each body once, in roger's Store format.
    let b_store = fs::read_to_string(store_path(&rig.store_dir(), ADDR_COMPOSITE)).unwrap();
    assert_eq!(b_store.lines().count(), 2, "{b_store}");
    assert!(b_store
        .lines()
        .next()
        .unwrap()
        .contains(r#""headers":{"z-k":"9","a-k":"1"}"#));

    // Board parity with `roger board`.
    let board = a.call(json!({"type":"board"})).await;
    assert_eq!(board["type"], "board");
    let sessions = board["sessions"].as_array().unwrap();
    let find = |addr: &str| {
        sessions
            .iter()
            .find(|s| s["session"] == addr)
            .cloned()
            .unwrap_or_else(|| panic!("board lacks {addr}: {board}"))
    };
    assert_eq!(find(ADDR_A)["label"], "alpha");
    assert_eq!(find(ADDR_COMPOSITE)["declared"], "desk 2");
    let cli_board = roger(&["board", "--socket", sock]);
    assert!(
        cli_board.contains(&format!("SESSION address={ADDR_A} ")),
        "{cli_board}"
    );
    assert!(cli_board.contains(&format!(
        "DECLARED address={ADDR_COMPOSITE} declared=desk 2"
    )));
    assert!(
        cli_board.contains(&format!("sessions={} ", sessions.len())),
        "{cli_board}"
    );

    // Claims: owner is the session; a second session is refused; the CLI agrees.
    let got = a
        .call(json!({"type":"claim","op":"acquire","key":"desk-7","ttl_ms":60000}))
        .await;
    assert_eq!(got["type"], "claim_result");
    assert_eq!(got["ok"], true);
    let refused = b
        .call(json!({"type":"claim","op":"acquire","key":"desk-7","ttl_ms":60000}))
        .await;
    assert_eq!(refused["ok"], false);
    assert_eq!(refused["holder"], ADDR_A);
    let cli_board = roger(&["board", "--socket", sock]);
    assert!(cli_board.contains(&format!("holder={ADDR_A} ")) && cli_board.contains("key=desk-7"));
    let cli_claim = roger(&[
        "claim", "--socket", sock, "--owner", ADDR_B, "--key", "desk-7", "--ttl-ms", "1000",
    ]);
    assert!(cli_claim.starts_with("REFUSED "), "{cli_claim}");
    let released = a
        .call(json!({"type":"claim","op":"release","key":"desk-7"}))
        .await;
    assert_eq!(released["ok"], true);

    // Relabel: own session by default.
    let relabeled = a.call(json!({"type":"relabel","label":"beta"})).await;
    assert_eq!(relabeled["type"], "relabeled");
    let cli_board = roger(&["board", "--socket", sock]);
    assert!(
        cli_board.contains(&format!("SESSION address={ADDR_A} "))
            && cli_board.contains("label=beta"),
        "{cli_board}"
    );

    // Pipelined requests are answered in order; malformed ones get error replies.
    a.send_text(r#"{"type":"board"}"#).await;
    a.send_text("not json").await;
    a.send_text(r#"{"type":"nope"}"#).await;
    a.send_text(r#"{"type":"claim","key":"k"}"#).await;
    a.ws.send(Message::binary(vec![1u8, 2])).await.unwrap();
    a.send_text(r#"{"type":"send","to":"x","body":"b","mail":7}"#)
        .await;
    assert_eq!(a.reply().await["type"], "board");
    let e = a.reply().await;
    assert_eq!(e["type"], "error");
    assert!(e["error"].is_string());
    let e = a.reply().await;
    assert_eq!(
        e,
        json!({"type":"error","error":"expected request send, board, relabel, or claim; saw nope"})
    );
    assert_eq!(
        a.reply().await,
        json!({"type":"error","error":"missing op"})
    );
    let e = a.reply().await;
    assert!(e["error"].as_str().unwrap().contains("binary frame"), "{e}");
    assert_eq!(
        a.reply().await,
        json!({"type":"error","error":"mail must be a string"})
    );

    // Closing B unregisters it at once.
    b.close().await;
    wait_until("B leaves the Board", 10, || {
        let s = roger(&["board", "--socket", sock]);
        (!s.contains("Meteor Shower")).then_some(())
    })
    .await;
    a.close().await;
}

// ---- Test 2: SIGKILL between Store write and Ack, restart, body exactly once ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_between_store_and_ack_redelivers_with_same_record_exactly_once() {
    let mut rig = TestRig::start("crash-before-ack").await;
    let marker = rig.path("hold.marker");
    let (gw_idx, port) = rig
        .spawn_gateway(&["--hold-before-ack", marker.to_str().unwrap()], &[])
        .await;
    let sock = rig.daemon_sock();
    let sock = sock.to_str().unwrap();

    let mut b = ws_open(port, &format!("id={ADDR_COMPOSITE_Q}&registered=1"), None)
        .await
        .unwrap();
    b.expect_ready(ADDR_COMPOSITE, true).await;

    let out = roger(&[
        "send",
        "--socket",
        sock,
        "--from",
        ADDR_A,
        "--to",
        ADDR_COMPOSITE,
        "--mail",
        "mail-crash",
        "--body",
        "once-across-sigkill",
        "--header",
        "z=9",
        "--header",
        "a=1",
    ]);
    assert!(out.starts_with("ACCEPTED "), "{out}");

    // The client is told after the Store write and before the Ack.
    let (v, _) = b.event().await;
    assert_eq!(v["record"], "r-1");
    assert_eq!(v["body"], "once-across-sigkill");
    wait_until("hold marker", 10, || {
        fs::read_to_string(&marker).ok().filter(|s| s == "held\n")
    })
    .await;
    let pending = roger(&["receipt", "--socket", sock, "--mail", "mail-crash"]);
    assert!(
        pending.starts_with("PENDING "),
        "expected PENDING, saw {pending}"
    );

    rig.processes[gw_idx].kill_and_wait();
    drop(b);

    let (_, port2) = rig
        .spawn_gateway(&["--hold-before-ack", marker.to_str().unwrap()], &[])
        .await;
    let mut b2 = ws_open(port2, &format!("id={ADDR_COMPOSITE_Q}"), None)
        .await
        .unwrap();
    b2.expect_ready(ADDR_COMPOSITE, false).await;
    let (v, raw) = b2.event().await;
    assert_eq!(v["mail"], "mail-crash");
    assert_eq!(v["record"], "r-1", "{raw}");
    assert!(raw.contains(r#""headers":{"z":"9","a":"1"}"#), "{raw}");

    let receipt = wait_receipt(&rig, "mail-crash").await;
    assert!(receipt.contains("record=r-1 "), "{receipt}");

    let s_file = store_path(&rig.store_dir(), ADDR_COMPOSITE);
    let raw_store = fs::read_to_string(&s_file).unwrap();
    assert_eq!(
        raw_store.matches("once-across-sigkill").count(),
        1,
        "{raw_store}"
    );
    assert_eq!(raw_store.lines().count(), 1);

    let audit = Command::new(roger_bin())
        .args([
            "audit",
            "--coordinator-log",
            rig.coord_log().to_str().unwrap(),
            "--daemon-log",
            rig.daemon_log().to_str().unwrap(),
            "--store",
            s_file.to_str().unwrap(),
            "--bodies",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&audit.stdout);
    assert!(
        audit.status.success(),
        "{}",
        String::from_utf8_lossy(&audit.stderr)
    );
    assert!(
        text.contains("unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0"),
        "{text}"
    );
    b2.close().await;
}

// ---- Test 3: Bearer before upgrade, path/query/Store refusals ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bearer_and_upgrade_refusals_happen_before_any_session() {
    let mut rig = TestRig::start("auth").await;
    let (_, port) = rig
        .spawn_gateway(
            &["--token-env", "WS_GW_TOKEN"],
            &[("WS_GW_TOKEN", GATEWAY_BEARER)],
        )
        .await;
    let q = format!("id={ADDR_A}");
    assert_eq!(refused_status(ws_open(port, &q, None).await), 401);
    assert_eq!(refused_status(ws_open(port, &q, Some("wrong")).await), 401);
    // A refused upgrade opened nothing: no Store file exists.
    assert!(!store_path(&rig.store_dir(), ADDR_A).exists());

    let bad_key = format!("{q}&colour=1");
    assert_eq!(
        refused_status(ws_open(port, &bad_key, Some(GATEWAY_BEARER)).await),
        400
    );
    assert_eq!(
        refused_status(ws_open(port, &format!("{q}&after=r-1"), Some(GATEWAY_BEARER)).await),
        400
    );

    let mut a = ws_open(port, &q, Some(GATEWAY_BEARER)).await.unwrap();
    a.expect_ready(ADDR_A, false).await;
    // The Address's Store is held by the live connection.
    assert_eq!(
        refused_status(ws_open(port, &q, Some(GATEWAY_BEARER)).await),
        409
    );
    let board = a.call(json!({"type":"board"})).await;
    assert_eq!(board["type"], "board");

    // Wrong path is 404 even with a good token.
    let mut req = format!("ws://127.0.0.1:{port}/other?{q}")
        .into_client_request()
        .unwrap();
    req.headers_mut().insert(
        "authorization",
        format!("Bearer {GATEWAY_BEARER}").parse().unwrap(),
    );
    match connect_async(req).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status().as_u16(), 404),
        other => panic!("expected 404, saw {:?}", other.map(|_| ())),
    }
    a.close().await;
}

// ---- Test 4: resume with after=<record> over a tcp:// daemon connection ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_record_replays_store_in_line_order_then_goes_live() {
    let mut rig = TestRig::start("resume").await;
    let tcp = format!("tcp://127.0.0.1:{}", rig.daemon_tcp_port);
    let store_dir = rig.store_dir();
    let (_, port) = rig
        .spawn_gateway_with_socket(&tcp, &store_dir, &[], &[])
        .await;
    let sock = rig.daemon_sock();
    let sock = sock.to_str().unwrap();
    let send = |mail: &str, body: &str| {
        let out = roger(&[
            "send", "--socket", sock, "--from", ADDR_A, "--to", ADDR_B, "--mail", mail, "--body",
            body,
        ]);
        assert!(out.starts_with("ACCEPTED "), "{out}");
    };

    let mut b = ws_open(port, &format!("id={ADDR_B}&registered=1"), None)
        .await
        .unwrap();
    b.expect_ready(ADDR_B, true).await;
    for (m, body) in [("m-1", "one"), ("m-2", "two"), ("m-3", "three")] {
        send(m, body);
        let (v, _) = b.event().await;
        assert_eq!(v["mail"], m);
    }
    wait_receipt(&rig, "m-3").await;
    b.close().await;
    wait_until("B leaves the Board", 10, || {
        (!roger(&["board", "--socket", sock]).contains(ADDR_B)).then_some(())
    })
    .await;

    // Accepted while B is away: pending at the coordinator, delivered on Register.
    send("m-4", "four");

    let mut b = ws_open(port, &format!("id={ADDR_B}&after=r-1"), None)
        .await
        .unwrap();
    b.expect_ready(ADDR_B, false).await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        let (v, _) = b.event().await;
        assert_eq!(v["type"], "mail");
        seen.push((
            v["record"].as_str().unwrap().to_string(),
            v["body"].as_str().unwrap().to_string(),
        ));
    }
    assert_eq!(
        seen,
        [
            ("r-2".to_string(), "two".to_string()),
            ("r-3".to_string(), "three".to_string()),
            ("r-4".to_string(), "four".to_string()),
        ]
    );
    assert!(wait_receipt(&rig, "m-4").await.contains("record=r-4 "));

    // Live after replay.
    send("m-5", "five");
    let (v, _) = b.event().await;
    assert_eq!(
        (v["record"].as_str(), v["body"].as_str()),
        (Some("r-5"), Some("five"))
    );
    b.close().await;

    // after= (empty) replays the whole Store.
    let mut b = ws_open(port, &format!("id={ADDR_B}&after="), None)
        .await
        .unwrap();
    b.expect_ready(ADDR_B, false).await;
    for want in ["r-1", "r-2", "r-3", "r-4", "r-5"] {
        assert_eq!(b.event().await.0["record"], want);
    }
    let raw_store = fs::read_to_string(store_path(&store_dir, ADDR_B)).unwrap();
    assert_eq!(raw_store.lines().count(), 5, "{raw_store}");
    b.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_token_reaches_a_guarded_daemon_over_tcp() {
    let mut rig = TestRig::start("client-token").await;
    let coord_addr = format!("127.0.0.1:{}", rig.coord_port);
    let gport_file = rig.path("guarded.port");
    let glog = rig.path("guarded.log");
    // The rig sets ROGER_TOKEN on roger processes; the guarded daemon reuses it as its client token.
    rig.spawn_roger(&[
        "daemon",
        "--coordinator",
        &coord_addr,
        "--host-id",
        "host-guarded",
        "--listen",
        "127.0.0.1:0",
        "--port-file",
        gport_file.to_str().unwrap(),
        "--client-token-env",
        "ROGER_TOKEN",
        "--data",
        glog.to_str().unwrap(),
    ]);
    let gport = wait_until("guarded daemon port file", 10, || {
        fs::read_to_string(&gport_file)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
    .await;
    let target = format!("tcp://127.0.0.1:{gport}");

    let ok_dir = rig.path("stores-ok");
    let (_, port_ok) = rig
        .spawn_gateway_with_socket(
            &target,
            &ok_dir,
            &[],
            &[("ROGER_CLIENT_TOKEN", FLEET_TOKEN)],
        )
        .await;
    let mut a = ws_open(port_ok, &format!("id={ADDR_A}&registered=1"), None)
        .await
        .unwrap();
    a.expect_ready(ADDR_A, true).await;

    let bad_dir = rig.path("stores-bad");
    let (_, port_bad) = rig
        .spawn_gateway_with_socket(&target, &bad_dir, &[], &[("ROGER_CLIENT_TOKEN", "wrong")])
        .await;
    if let Ok(mut b) = ws_open(port_bad, &format!("id={ADDR_B}&registered=1"), None).await {
        // The daemon closes the link after the refused auth line: no Register follows.
        while let Some((v, raw)) = b.read_frame().await {
            assert_ne!(v["type"], "registered", "{raw}");
        }
    }
    let sock = rig.daemon_sock();
    let board = roger(&["board", "--socket", sock.to_str().unwrap()]);
    assert!(
        board.contains(&format!("SESSION address={ADDR_A} ")),
        "{board}"
    );
    assert!(
        !board.contains(&format!("SESSION address={ADDR_B} ")),
        "{board}"
    );
}

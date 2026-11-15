//! End-to-end integration suite for `roger-mcp-gateway` against real `roger coordinator`,
//! `roger daemon`, `roger session`, and `roger audit` processes.

#[cfg(not(feature = "test-hooks"))]
compile_error!("integration tests require `--features test-hooks`");

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use roger_mcp_gateway::durlog::{cursor_path, store_path};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

const GATEWAY_BIN: &str = env!("CARGO_BIN_EXE_roger-mcp-gateway");
const FLEET_TOKEN: &str = "fleet-integration-token";
const GATEWAY_BEARER: &str = "gateway-secret-bearer-token";

const ADDR_1: &str = "11111111-1111-4111-8111-111111111111";
const ADDR_2: &str = "22222222-2222-4222-8222-222222222222";
const ADDR_COMPOSITE: &str = "[[153:Meteor Shower:7]]33333333-3333-4333-8333-333333333333";
const ADDR_TRACER: &str = "44444444-4444-4444-8444-444444444444";

/// Build `roger` (with `--features test-hooks`) once per test binary into the root `target/` dir.
fn roger_bin() -> &'static Path {
    static ROGER: OnceLock<PathBuf> = OnceLock::new();
    ROGER.get_or_init(|| {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .and_then(Path::parent)
            .expect("modules/mcp-gateway lives two levels under repo root");
        let root_manifest = repo_root.join("Cargo.toml");
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| repo_root.join("target"))
            .join("roger-for-gateway-tests");
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
        let dir =
            std::env::temp_dir().join(format!("roger-mcp-gw-test-{}-{tag}", std::process::id()));
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

// ---- HTTP/1.1 & HTTP/2 (h2c) + SSE test clients ----

#[derive(Debug, Clone)]
struct SseEvent {
    id: u64,
    event: String,
    data: Value,
    raw_data: String,
}

struct SseReader {
    body: Incoming,
    buf: String,
}

impl SseReader {
    fn new(body: Incoming) -> Self {
        Self {
            body,
            buf: String::new(),
        }
    }

    async fn next_event(&mut self) -> Option<SseEvent> {
        tokio::time::timeout(Duration::from_secs(10), self.next_event_inner())
            .await
            .expect("timed out waiting for SSE frame")
    }

    async fn next_event_inner(&mut self) -> Option<SseEvent> {
        loop {
            if let Some(pos) = self.buf.find("\n\n") {
                let block = self.buf[..pos].to_string();
                self.buf.drain(..pos + 2);
                if block.trim().is_empty() || block.starts_with(':') {
                    continue;
                }
                let mut id: Option<u64> = None;
                let mut ev = String::new();
                let mut data_lines = Vec::new();
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id: ") {
                        id = Some(v.trim().parse().expect("SSE id must be u64"));
                    } else if let Some(v) = line.strip_prefix("event: ") {
                        ev = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        data_lines.push(v);
                    }
                }
                let raw_data = data_lines.join("\n");
                let data = serde_json::from_str::<Value>(&raw_data)
                    .expect("SSE data must be compact JSON");
                return Some(SseEvent {
                    id: id.expect("SSE frame missing id"),
                    event: ev,
                    data,
                    raw_data,
                });
            }

            match self.body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(chunk) = frame.into_data() {
                        self.buf
                            .push_str(std::str::from_utf8(&chunk).expect("SSE chunk UTF-8"));
                    }
                }
                Some(Err(e)) => panic!("SSE stream read error: {e}"),
                None => return None,
            }
        }
    }
}

async fn h1_request(
    port: u16,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: impl Into<Bytes>,
) -> (StatusCode, HeaderMap, Bytes) {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(http::header::HOST, format!("127.0.0.1:{port}"));
    for (k, v) in headers {
        builder = builder.header(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    let req = builder.body(Full::new(body.into())).unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let data = resp.into_body().collect().await.unwrap().to_bytes();
    (status, resp_headers, data)
}

async fn h1_open_sse(
    port: u16,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, SseReader) {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(http::header::HOST, format!("127.0.0.1:{port}"))
        .header(http::header::ACCEPT, "text/event-stream");
    for (k, v) in headers {
        builder = builder.header(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    let req = builder.body(Full::new(Bytes::new())).unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let resp_headers = resp.headers().clone();
    (status, resp_headers, SseReader::new(resp.into_body()))
}

#[derive(Clone)]
struct H2MultiplexClient {
    port: u16,
    sender: http2::SendRequest<Full<Bytes>>,
}

impl H2MultiplexClient {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (sender, conn) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Self { port, sender }
    }

    async fn request(
        &self,
        method: Method,
        path_and_query: &str,
        headers: &[(&str, &str)],
        body: impl Into<Bytes>,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut sender = self.sender.clone();
        let uri = format!("http://127.0.0.1:{}{path_and_query}", self.port);
        let mut builder = Request::builder().method(method).uri(uri);
        for (k, v) in headers {
            builder = builder.header(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_bytes(v.as_bytes()).unwrap(),
            );
        }
        let req = builder.body(Full::new(body.into())).unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        let data = resp.into_body().collect().await.unwrap().to_bytes();
        (status, resp_headers, data)
    }

    async fn open_sse(
        &self,
        path_and_query: &str,
        headers: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, SseReader) {
        let mut sender = self.sender.clone();
        let uri = format!("http://127.0.0.1:{}{path_and_query}", self.port);
        let mut builder = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(http::header::ACCEPT, "text/event-stream");
        for (k, v) in headers {
            builder = builder.header(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_bytes(v.as_bytes()).unwrap(),
            );
        }
        let req = builder.body(Full::new(Bytes::new())).unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        (status, resp_headers, SseReader::new(resp.into_body()))
    }
}

fn parse_tool_payload(rpc_body: &[u8]) -> Value {
    let rpc: Value = serde_json::from_slice(rpc_body).unwrap();
    assert!(
        rpc.get("error").is_none(),
        "unexpected JSON-RPC error: {rpc}"
    );
    let res = &rpc["result"];
    assert_ne!(
        res.get("isError"),
        Some(&Value::Bool(true)),
        "unexpected tool error: {rpc}"
    );
    let text = res["content"][0]["text"].as_str().unwrap();
    serde_json::from_str::<Value>(text).unwrap()
}

// ---- Test 1: Multi-client concurrent push over HTTP/1.1 AND HTTP/2 (h2c) + TCP daemon bridge ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_client_concurrent_push_over_h1_and_h2c_preserves_headers_and_broadcasts() {
    let mut rig = TestRig::start("multi-push").await;

    // Connect both `roger-mcp-gateway` and a `roger session` Tracer directly to native `roger daemon --listen` over TCP!
    let tcp_target = format!("tcp://127.0.0.1:{}", rig.daemon_tcp_port);
    let store_dir = rig.store_dir();
    let (_, gw_port) = rig
        .spawn_gateway_with_socket(&tcp_target, &store_dir, &[], &[])
        .await;

    // Healthz check
    let (st, _, hbody) = h1_request(gw_port, Method::GET, "/healthz", &[], "").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&hbody[..], br#"{"ok":true}"#);

    // Start a real `roger session` Tracer over `tcp://` alongside the 3 MCP gateway sessions!
    let tracer_store = rig.path("tracer.store");
    rig.spawn_roger(&[
        "session",
        "--socket",
        &tcp_target,
        "--store",
        tracer_store.to_str().unwrap(),
        "--id",
        ADDR_TRACER,
        "--label",
        "tracer-peer",
    ]);

    // Client 1: HTTP/2 prior-knowledge (`h2c`) with composite Address via URL query `?id=...&label=...`
    let h2 = H2MultiplexClient::connect(gw_port).await;
    let query_uri = format!(
        "/mcp?id={}&label=Meteor%20Worker&declare=role%3Dobserver",
        "[[153:Meteor%20Shower:7]]33333333-3333-4333-8333-333333333333"
    );
    let (st, hdrs1, body1) = h2
        .request(
            Method::POST,
            &query_uri,
            &[],
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2025-06-18" }
            }))
            .unwrap(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let sid_comp = hdrs1
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        hdrs1.get("x-roger-address").unwrap().to_str().unwrap(),
        ADDR_COMPOSITE
    );
    let init_json: Value = serde_json::from_slice(&body1).unwrap();
    assert_eq!(
        init_json["result"]["serverInfo"]["name"],
        "roger-mcp-gateway"
    );

    // Client 2: HTTP/1.1 with `X-Roger-Address` header
    let (st, hdrs2, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_1), ("x-roger-label", "alpha")],
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "init-2",
            "method": "initialize",
            "params": { "protocolVersion": "2025-03-26" }
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid_1 = hdrs2
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Client 3: HTTP/2 (`h2c`) with `params._meta.roger_address`
    let (st, hdrs3, _) = h2
        .request(
            Method::POST,
            "/mcp",
            &[],
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "initialize",
                "params": {
                    "_meta": {
                        "roger_address": ADDR_2,
                        "roger_label": "beta"
                    }
                }
            }))
            .unwrap(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let sid_2 = hdrs3
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Wait for coordinator registration confirmation on all 3 MCP sessions
    for sid in [&sid_comp, &sid_1, &sid_2] {
        let (st, _, b) = h2
            .request(
                Method::POST,
                "/mcp",
                &[("mcp-session-id", sid)],
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": { "name": "roger_registered", "arguments": { "wait_seconds": 15 } }
                }))
                .unwrap(),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
        let reg = parse_tool_payload(&b);
        assert_eq!(reg["registered"], Value::Bool(true));
    }

    // Also wait until Tracer session appears on Board
    wait_until("tracer on board", 10, || {
        let out = Command::new(roger_bin())
            .args(["board", "--socket", rig.daemon_sock().to_str().unwrap()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .contains(&format!("SESSION address={ADDR_TRACER} "))
            .then_some(())
    })
    .await;

    // Open simultaneous SSE streams: Client 1 & Client 3 multiplexed over ONE HTTP/2 connection,
    // Client 2 over HTTP/1.1!
    let (st, _, mut sse_comp) = h2.open_sse("/mcp", &[("mcp-session-id", &sid_comp)]).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, mut sse_1) = h1_open_sse(gw_port, "/mcp", &[("mcp-session-id", &sid_1)]).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, mut sse_2) = h2.open_sse("/mcp", &[("mcp-session-id", &sid_2)]).await;
    assert_eq!(st, StatusCode::OK);

    // Helper to read the next `notifications/roger/mail` event from an SSE stream
    async fn next_mail_sse(sse: &mut SseReader) -> SseEvent {
        loop {
            let ev = sse.next_event().await.expect("SSE stream ended early");
            assert_eq!(ev.event, "message");
            if ev.data["method"] == "notifications/roger/mail" {
                return ev;
            }
        }
    }

    // While `roger_inbox` blocks waiting on ADDR_2 over HTTP/2, concurrently send directed Mail
    // from ADDR_1 to ADDR_COMPOSITE with ordered non-ASCII headers!
    let h2_wait = h2.clone();
    let sid_2_clone = sid_2.clone();
    let waiting_inbox = tokio::spawn(async move {
        h2_wait
            .request(
                Method::POST,
                "/mcp",
                &[("mcp-session-id", &sid_2_clone)],
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": 99,
                    "method": "tools/call",
                    "params": {
                        "name": "roger_inbox",
                        "arguments": { "wait_seconds": 15 }
                    }
                }))
                .unwrap(),
            )
            .await
    });

    // Ordered headers raw JSON: z-last, a-first, café-unicode!
    let send_raw = format!(
        r#"{{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{{"name":"roger_send","arguments":{{"to":"{ADDR_COMPOSITE}","mail":"m-ordered-1","body":"hello meteor","headers":{{"z-omega":"last","a-alpha":"first","café":"☕-snow-☃"}}}}}}}}"#
    );
    let (st, _, send_resp) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", &sid_1)],
        send_raw,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(parse_tool_payload(&send_resp)["mail"], "m-ordered-1");

    // Verify real-time SSE push on ADDR_COMPOSITE with exact header key order preserved!
    let pushed = next_mail_sse(&mut sse_comp).await;
    assert_eq!(pushed.data["params"]["mail"], "m-ordered-1");
    assert_eq!(pushed.data["params"]["record"], "r-1");
    assert_eq!(pushed.data["params"]["from"], ADDR_1);
    assert_eq!(pushed.data["params"]["body"], "hello meteor");
    assert_eq!(pushed.data["params"]["kind"], "directed");
    let hdr_keys: Vec<&str> = pushed.data["params"]["headers"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(hdr_keys, ["z-omega", "a-alpha", "café"]);
    assert!(
        pushed
            .raw_data
            .contains(r#""headers":{"z-omega":"last","a-alpha":"first","café":"☕-snow-☃"}"#),
        "raw SSE JSON must preserve exact key order: {}",
        pushed.raw_data
    );

    // Now send Broadcast `*` from ADDR_COMPOSITE -> arrives in real time on ADDR_1, ADDR_2,
    // ADDR_COMPOSITE, AND the `roger session` Tracer, and unblocks `waiting_inbox` on ADDR_2!
    let (_, _, bcast_resp) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("mcp-session-id", &sid_comp)],
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 21,
                "method": "tools/call",
                "params": {
                    "name": "roger_send",
                    "arguments": {
                        "to": "*",
                        "mail": "m-bcast-1",
                        "body": "fleet-wide-announcement"
                    }
                }
            }))
            .unwrap(),
        )
        .await;
    assert_eq!(parse_tool_payload(&bcast_resp)["mail"], "m-bcast-1");

    let (ev_1, ev_2, ev_comp) = tokio::join!(
        next_mail_sse(&mut sse_1),
        next_mail_sse(&mut sse_2),
        next_mail_sse(&mut sse_comp),
    );
    assert_eq!(ev_1.data["params"]["body"], "fleet-wide-announcement");
    assert_eq!(ev_1.data["params"]["kind"], "broadcast");
    assert_eq!(ev_2.data["params"]["body"], "fleet-wide-announcement");
    assert_eq!(ev_2.data["params"]["kind"], "broadcast");
    assert_eq!(ev_comp.data["params"]["body"], "fleet-wide-announcement");

    let (st, _, inbox_bytes) = waiting_inbox.await.unwrap();
    assert_eq!(st, StatusCode::OK);
    let inbox_payload = parse_tool_payload(&inbox_bytes);
    assert_eq!(inbox_payload["mail"].as_array().unwrap().len(), 1);
    assert_eq!(inbox_payload["mail"][0]["body"], "fleet-wide-announcement");
    assert_eq!(inbox_payload["cursor"], "r-1");

    // Verify `roger session` Tracer ALSO stored the broadcast!
    wait_until("tracer stored broadcast", 10, || {
        fs::read_to_string(&tracer_store)
            .ok()
            .filter(|s| s.contains("fleet-wide-announcement"))
    })
    .await;
}

// ---- Test 2: Startup race & Last-Event-ID SSE resumption ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_race_and_last_event_id_resumption_deliver_exact_events_without_duplicates() {
    let mut rig = TestRig::start("sse-resume").await;
    let (_, gw_port) = rig.spawn_gateway(&[], &[]).await;

    // Send Mail M1 to ADDR_1 *before* ADDR_1 ever initializes!
    let send_out = Command::new(roger_bin())
        .args([
            "send",
            "--socket",
            rig.daemon_sock().to_str().unwrap(),
            "--from",
            "pre-sender",
            "--to",
            ADDR_1,
            "--mail",
            "mail-before-init",
            "--body",
            "queued-before-init",
        ])
        .output()
        .unwrap();
    assert!(send_out.status.success());

    // Now POST initialize for ADDR_1
    let (st, hdrs, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_1)],
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid = hdrs
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Wait until coordinator has recorded the Receipt (Ack) for `mail-before-init`
    // *before* opening `GET /mcp`!
    wait_until("receipt for mail-before-init", 10, || {
        let out = Command::new(roger_bin())
            .args([
                "receipt",
                "--socket",
                rig.daemon_sock().to_str().unwrap(),
                "--mail",
                "mail-before-init",
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .starts_with("RECEIPT ")
            .then_some(())
    })
    .await;

    // Also wait for `registered` confirmation so both startup events (mail + registered) are settled
    let (_, _, reg_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", &sid)],
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": { "name": "roger_registered", "arguments": { "wait_seconds": 10 } }
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(parse_tool_payload(&reg_b)["registered"], Value::Bool(true));

    // Open `GET /mcp` WITHOUT `Last-Event-ID`: the Mail pushed during `initialize` must arrive immediately!
    let (st, _, mut sse1) = h1_open_sse(gw_port, "/mcp", &[("mcp-session-id", &sid)]).await;
    assert_eq!(st, StatusCode::OK);

    let e1 = sse1.next_event().await.unwrap();
    let e2 = sse1.next_event().await.unwrap();
    assert_eq!(e1.id, 1);
    assert_eq!(e2.id, 2);
    let methods = [
        e1.data["method"].as_str().unwrap(),
        e2.data["method"].as_str().unwrap(),
    ];
    assert!(
        methods.contains(&"notifications/roger/mail")
            && methods.contains(&"notifications/roger/registered"),
        "expected mail + registered startup events, saw {methods:?}"
    );
    let mail_ev = if e1.data["method"] == "notifications/roger/mail" {
        &e1
    } else {
        &e2
    };
    assert_eq!(mail_ev.data["params"]["mail"], "mail-before-init");
    assert_eq!(mail_ev.data["params"]["body"], "queued-before-init");

    // Disconnect SSE stream 1!
    drop(sse1);

    // Send two more Mails while SSE is disconnected and wait for their Receipts!
    for (mid, body) in [("mail-missed-2", "second"), ("mail-missed-3", "third")] {
        let out = Command::new(roger_bin())
            .args([
                "send",
                "--socket",
                rig.daemon_sock().to_str().unwrap(),
                "--from",
                "pre-sender",
                "--to",
                ADDR_1,
                "--mail",
                mid,
                "--body",
                body,
            ])
            .output()
            .unwrap();
        assert!(out.status.success());
        wait_until(mid, 10, || {
            let r = Command::new(roger_bin())
                .args([
                    "receipt",
                    "--socket",
                    rig.daemon_sock().to_str().unwrap(),
                    "--mail",
                    mid,
                ])
                .output()
                .unwrap();
            String::from_utf8_lossy(&r.stdout)
                .starts_with("RECEIPT ")
                .then_some(())
        })
        .await;
    }

    // Reconnect `GET /mcp` with `Last-Event-ID: 1`: must replay event 2 + events 3 and 4 in exact order,
    // without duplicating event 1!
    let (st, _, mut sse2) = h1_open_sse(
        gw_port,
        "/mcp",
        &[("mcp-session-id", &sid), ("last-event-id", "1")],
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let r2 = sse2.next_event().await.unwrap();
    let r3 = sse2.next_event().await.unwrap();
    let r4 = sse2.next_event().await.unwrap();
    assert_eq!((r2.id, r3.id, r4.id), (2, 3, 4));
    assert_eq!(r2.raw_data, e2.raw_data);
    assert_eq!(r3.data["params"]["mail"], "mail-missed-2");
    assert_eq!(r3.data["params"]["record"], "r-2");
    assert_eq!(r4.data["params"]["mail"], "mail-missed-3");
    assert_eq!(r4.data["params"]["record"], "r-3");
}

// ---- Test 3: All 9 MCP tools + Bearer auth + container restart + takeover + DELETE ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_nine_tools_bearer_auth_container_restart_store_lock_and_delete_unregister() {
    let mut rig = TestRig::start("tools-auth-restart").await;
    let (_, gw_port) = rig
        .spawn_gateway(
            &["--token-env", "TEST_GW_BEARER"],
            &[("TEST_GW_BEARER", GATEWAY_BEARER)],
        )
        .await;

    // 1. Missing or wrong Bearer token -> 401 Unauthorized with WWW-Authenticate: Bearer
    let (st, hdrs, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(hdrs.get(http::header::WWW_AUTHENTICATE).unwrap(), "Bearer");

    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("authorization", "Bearer wrong-secret")],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let auth_hdr = format!("Bearer {GATEWAY_BEARER}");

    // 2. Valid Bearer token initializes ADDR_1
    let h2 = H2MultiplexClient::connect(gw_port).await;
    let (st, hdrs, _) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("x-roger-address", ADDR_1)],
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2025-06-18" }
            }))
            .unwrap(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let sid_old = hdrs
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Notifications return 202 Accepted empty body; ping returns {}; tools/list returns all 9 tools
    let (st, _, empty_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    assert!(empty_b.is_empty());

    let (st, _, ping_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&ping_b).unwrap()["result"],
        json!({})
    );

    let (_, _, list_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#,
        )
        .await;
    let tools_json: Value = serde_json::from_slice(&list_b).unwrap();
    assert_eq!(tools_json["result"]["tools"].as_array().unwrap().len(), 9);

    // 406 Not Acceptable when Accept excludes text/event-stream and */*
    let (st, _, _) = h2
        .request(
            Method::GET,
            "/mcp",
            &[
                ("authorization", &auth_hdr),
                ("mcp-session-id", &sid_old),
                ("accept", "application/json"),
            ],
            "",
        )
        .await;
    assert_eq!(st, StatusCode::NOT_ACCEPTABLE);

    // Unknown JSON-RPC method -> -32601; invalid JSON -> -32700
    let (_, _, unk_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            r#"{"jsonrpc":"2.0","id":4,"method":"unknown/method"}"#,
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&unk_b).unwrap()["error"]["code"],
        -32601
    );
    let (_, _, bad_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            "{invalid json",
        )
        .await;
    assert_eq!(
        serde_json::from_slice::<Value>(&bad_b).unwrap()["error"]["code"],
        -32700
    );

    // 3. Second gateway process on the SAME `--store-dir` must be refused by `File::try_lock()`!
    let (gw2_idx, gw2_port) = rig.spawn_gateway(&[], &[]).await;
    let (st_conflict, _, conflict_b) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_1)],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st_conflict, StatusCode::CONFLICT);
    assert!(
        String::from_utf8_lossy(&conflict_b).contains("held by another process"),
        "{}",
        String::from_utf8_lossy(&conflict_b)
    );
    rig.processes[gw2_idx].kill_and_wait();

    // 4. Container restart on the SAME gateway: open SSE on `sid_old`, then re-initialize ADDR_1!
    let (_, _, mut old_sse) = h2
        .open_sse(
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
        )
        .await;

    let (st, hdrs_new, _) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("x-roger-address", ADDR_1)],
            r#"{"jsonrpc":"2.0","id":5,"method":"initialize"}"#,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let sid_new = hdrs_new
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_ne!(sid_old, sid_new);

    // Old SSE stream ends cleanly (drain any buffered startup event then hit None),
    // and old Mcp-Session-Id returns 404!
    while old_sse.next_event().await.is_some() {}
    let (st_old, _, _) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_old)],
            r#"{"jsonrpc":"2.0","id":6,"method":"ping"}"#,
        )
        .await;
    assert_eq!(st_old, StatusCode::NOT_FOUND);

    // 5. Exercise all 9 MCP tools on `sid_new`!
    let call = |id: i64, name: &str, arguments: Value| {
        let h2 = h2.clone();
        let auth = auth_hdr.clone();
        let sid = sid_new.clone();
        let name = name.to_string();
        async move {
            let (st, _, b) = h2
                .request(
                    Method::POST,
                    "/mcp",
                    &[("authorization", &auth), ("mcp-session-id", &sid)],
                    serde_json::to_vec(&json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": "tools/call",
                        "params": { "name": name, "arguments": arguments }
                    }))
                    .unwrap(),
                )
                .await;
            assert_eq!(st, StatusCode::OK);
            parse_tool_payload(&b)
        }
    };

    // (1) roger_registered
    let reg = call(11, "roger_registered", json!({ "wait_seconds": 10 })).await;
    assert_eq!(reg["registered"], Value::Bool(true));
    assert_eq!(reg["superseded"], Value::Null);

    // (2) roger_label
    let lbl = call(12, "roger_label", json!({ "label": "gateway-worker-1" })).await;
    assert_eq!(lbl["session"], ADDR_1);
    assert_eq!(lbl["label"], "gateway-worker-1");

    // (3) roger_claim
    let clm = call(
        13,
        "roger_claim",
        json!({ "key": "task/42", "ttl_seconds": 120 }),
    )
    .await;
    assert_eq!(clm["ok"], Value::Bool(true));
    assert_eq!(clm["holder"], ADDR_1);

    // (4) roger_renew
    let ren = call(
        14,
        "roger_renew",
        json!({ "key": "task/42", "ttl_seconds": 300 }),
    )
    .await;
    assert_eq!(ren["ok"], Value::Bool(true));

    // (5) roger_board
    let brd = call(15, "roger_board", json!({})).await;
    assert_eq!(brd["me"], ADDR_1);
    assert!(brd["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["session"] == ADDR_1 && r["label"] == "gateway-worker-1"));
    assert!(brd["claims"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["key"] == "task/42" && c["holder"] == ADDR_1));

    // (6) roger_release
    let rel = call(16, "roger_release", json!({ "key": "task/42" })).await;
    assert_eq!(rel["ok"], Value::Bool(true));

    // (7) roger_send (self-delivery)
    let sent = call(
        17,
        "roger_send",
        json!({ "to": ADDR_1, "mail": "m-self-9", "body": "tool-parity-check" }),
    )
    .await;
    assert_eq!(sent["mail"], "m-self-9");

    // (8) roger_inbox (peek first, then consume!)
    let peeked = call(
        18,
        "roger_inbox",
        json!({ "limit": 10, "wait_seconds": 10, "peek": true }),
    )
    .await;
    assert_eq!(peeked["mail"][0]["mail"], "m-self-9");
    assert_eq!(peeked["cursor"], "r-1");

    let consumed = call(
        19,
        "roger_inbox",
        json!({ "limit": 10, "wait_seconds": 5, "peek": false }),
    )
    .await;
    assert_eq!(consumed["mail"][0]["mail"], "m-self-9");
    assert_eq!(consumed["cursor"], "r-1");

    let after_consume = call(20, "roger_inbox", json!({ "limit": 10, "wait_seconds": 0 })).await;
    assert!(after_consume["mail"].as_array().unwrap().is_empty());
    assert_eq!(after_consume["cursor"], Value::Null);

    // (9) roger_receipt
    let rec = call(21, "roger_receipt", json!({ "mail": "m-self-9" })).await;
    assert_eq!(rec["mail"], "m-self-9");
    assert_eq!(rec["record"], "r-1");

    // 6. Initialize an empty session ADDR_2 and `DELETE /mcp`:
    //    must unregister from Board AND delete the empty `.store` file!
    let (_, hdrs_a2, _) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("x-roger-address", ADDR_2)],
            r#"{"jsonrpc":"2.0","id":30,"method":"initialize"}"#,
        )
        .await;
    let sid_a2 = hdrs_a2
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let (_, _, reg_a2) = h2
        .request(
            Method::POST,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_a2)],
            r#"{"jsonrpc":"2.0","id":31,"method":"tools/call","params":{"name":"roger_registered","arguments":{"wait_seconds":10}}}"#,
        )
        .await;
    assert_eq!(parse_tool_payload(&reg_a2)["registered"], Value::Bool(true));

    let a2_store = store_path(&rig.store_dir(), ADDR_2);
    assert!(a2_store.exists(), "Store exists while session is open");

    let (del_st, _, del_b) = h2
        .request(
            Method::DELETE,
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_a2)],
            "",
        )
        .await;
    assert_eq!(del_st, StatusCode::OK);
    assert_eq!(&del_b[..], br#"{"ok":true}"#);
    assert!(
        !a2_store.exists(),
        "empty Store file must be cleaned up on DELETE"
    );

    // Verify ADDR_2 left the Board!
    let brd_after = call(32, "roger_board", json!({ "all": true })).await;
    assert!(!brd_after["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["session"] == ADDR_2));

    // 7. Cross-host takeover -> `notifications/roger/superseded` SSE push + sticky `roger_registered`!
    let (_, _, mut sse_takeover) = h2
        .open_sse(
            "/mcp",
            &[("authorization", &auth_hdr), ("mcp-session-id", &sid_new)],
        )
        .await;

    let d2_port_file = rig.path("daemon2.port");
    let d2_log = rig.path("daemon2.log");
    let coord_addr = format!("127.0.0.1:{}", rig.coord_port);
    rig.spawn_roger(&[
        "daemon",
        "--coordinator",
        &coord_addr,
        "--host-id",
        "host-b",
        "--listen",
        "127.0.0.1:0",
        "--port-file",
        d2_port_file.to_str().unwrap(),
        "--data",
        d2_log.to_str().unwrap(),
    ]);
    let d2_port = wait_until("second daemon port file", 10, || {
        fs::read_to_string(&d2_port_file)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    })
    .await;
    let d2_target = format!("tcp://127.0.0.1:{d2_port}");

    let takeover_store = rig.path("takeover.store");
    rig.spawn_roger(&[
        "session",
        "--socket",
        &d2_target,
        "--store",
        takeover_store.to_str().unwrap(),
        "--id",
        ADDR_1,
    ]);

    let sup_ev = loop {
        let ev = sse_takeover.next_event().await.unwrap();
        if ev.data["method"] == "notifications/roger/superseded" {
            break ev;
        }
    };
    assert_eq!(sup_ev.data["params"]["session"], ADDR_1);
    assert_eq!(sup_ev.data["params"]["host"], "host-b");

    let reg_after = call(40, "roger_registered", json!({ "wait_seconds": 0 })).await;
    assert_eq!(reg_after["registered"], Value::Bool(false));
    assert_eq!(reg_after["superseded"]["host"], "host-b");
}

// ---- Test 4: SIGKILL between Store write and Ack (--hold-before-ack) + roger audit ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_between_store_fsync_and_ack_recovers_without_duplicate_and_passes_roger_audit() {
    let mut rig = TestRig::start("crash-before-ack").await;
    let marker = rig.path("hold.marker");

    let (gw_idx, gw_port) = rig
        .spawn_gateway(&["--hold-before-ack", marker.to_str().unwrap()], &[])
        .await;

    // Initialize ADDR_COMPOSITE on gateway 1
    let (st, hdrs, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_COMPOSITE)],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid1 = hdrs
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let (_, _, reg_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", &sid1)],
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"roger_registered","arguments":{"wait_seconds":10}}}"#,
    )
    .await;
    assert_eq!(parse_tool_payload(&reg_b)["registered"], Value::Bool(true));

    // Send Mail with ordered headers; gateway writes `r-1` to Store, writes `"held\n"` to marker,
    // and parks BEFORE sending `ack`!
    let send_out = Command::new(roger_bin())
        .args([
            "send",
            "--socket",
            rig.daemon_sock().to_str().unwrap(),
            "--from",
            ADDR_1,
            "--to",
            ADDR_COMPOSITE,
            "--mail",
            "mail-crash-window",
            "--body",
            "committed-once-across-sigkill",
            "--header",
            "z-order=9",
            "--header",
            "a-order=1",
        ])
        .output()
        .unwrap();
    assert!(send_out.status.success());

    wait_until("hold-before-ack marker", 10, || {
        fs::read_to_string(&marker).ok().filter(|s| s == "held\n")
    })
    .await;

    // Receipt must STILL be PENDING before SIGKILL!
    let pending_out = Command::new(roger_bin())
        .args([
            "receipt",
            "--socket",
            rig.daemon_sock().to_str().unwrap(),
            "--mail",
            "mail-crash-window",
        ])
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&pending_out.stdout).starts_with("PENDING "),
        "expected PENDING before ack, saw {}",
        String::from_utf8_lossy(&pending_out.stdout)
    );

    // SIGKILL the gateway right in the Store-written / unacked window!
    rig.processes[gw_idx].kill_and_wait();

    // Restart `roger-mcp-gateway` on the same `--store-dir` and `--socket`
    let (_, gw2_port) = rig
        .spawn_gateway(&["--hold-before-ack", marker.to_str().unwrap()], &[])
        .await;

    // Re-initialize ADDR_COMPOSITE
    let (st, hdrs2, _) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_COMPOSITE)],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid2 = hdrs2
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Open SSE stream: the unconsumed `r-1` seeded from Store must arrive immediately on SSE!
    let (st, _, mut sse) = h1_open_sse(gw2_port, "/mcp", &[("mcp-session-id", &sid2)]).await;
    assert_eq!(st, StatusCode::OK);
    let seeded_ev = loop {
        let ev = sse.next_event().await.unwrap();
        if ev.data["method"] == "notifications/roger/mail" {
            break ev;
        }
    };
    assert_eq!(seeded_ev.data["params"]["record"], "r-1");
    assert_eq!(seeded_ev.data["params"]["mail"], "mail-crash-window");
    assert_eq!(
        seeded_ev.data["params"]["body"],
        "committed-once-across-sigkill"
    );

    // Wait for Receipt at coordinator: must settle with `record=r-1`!
    let receipt_line = wait_until("receipt after restart", 10, || {
        let out = Command::new(roger_bin())
            .args([
                "receipt",
                "--socket",
                rig.daemon_sock().to_str().unwrap(),
                "--mail",
                "mail-crash-window",
            ])
            .output()
            .unwrap();
        let s = String::from_utf8_lossy(&out.stdout).to_string();
        s.starts_with("RECEIPT ").then_some(s)
    })
    .await;
    assert!(
        receipt_line.contains("record=r-1 "),
        "expected record=r-1 in Receipt, saw {receipt_line}"
    );

    // Store file must hold `r-1` ONCE (exact 1 line)!
    let s_file = store_path(&rig.store_dir(), ADDR_COMPOSITE);
    let raw_store = fs::read_to_string(&s_file).unwrap();
    let store_lines: Vec<&str> = raw_store.lines().collect();
    assert_eq!(
        store_lines.len(),
        1,
        "Store must hold exactly one record: {raw_store}"
    );
    assert!(
        store_lines[0].contains(r#""headers":{"z-order":"9","a-order":"1"}"#),
        "Store must preserve header key order: {}",
        store_lines[0]
    );

    // `roger_inbox` returns `r-1` once and persists `.cursor` across re-init!
    let (_, _, inbox1) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", &sid2)],
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"roger_inbox","arguments":{"wait_seconds":5}}}"#,
    )
    .await;
    let p1 = parse_tool_payload(&inbox1);
    assert_eq!(p1["mail"].as_array().unwrap().len(), 1);
    assert_eq!(p1["mail"][0]["record"], "r-1");
    assert_eq!(p1["cursor"], "r-1");
    assert_eq!(
        fs::read_to_string(cursor_path(&s_file)).unwrap().trim(),
        "r-1"
    );

    // Re-initialize once more and verify `.cursor` prevents re-reading `r-1`!
    let (_, hdrs3, _) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &[("x-roger-address", ADDR_COMPOSITE)],
        r#"{"jsonrpc":"2.0","id":4,"method":"initialize"}"#,
    )
    .await;
    let sid3 = hdrs3
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let (_, _, inbox2) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", &sid3)],
        r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"roger_inbox","arguments":{"wait_seconds":0}}}"#,
    )
    .await;
    let p2 = parse_tool_payload(&inbox2);
    assert!(p2["mail"].as_array().unwrap().is_empty());

    // Run `roger audit --coordinator-log ... --daemon-log ... --store ... --bodies`:
    // must report 0 duplicates, 0 mismatches, 0 unreceipted, 0 torn, 0 skipped!
    let audit_out = Command::new(roger_bin())
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
    let audit_text = String::from_utf8_lossy(&audit_out.stdout);
    assert!(
        audit_out.status.success(),
        "roger audit failed: {}",
        String::from_utf8_lossy(&audit_out.stderr)
    );
    assert!(
        audit_text.contains("STORE ") && audit_text.contains("record=r-1 mail=mail-crash-window"),
        "audit missing STORE line:\n{audit_text}"
    );
    assert!(
        audit_text.contains("unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0"),
        "expected clean audit summary:\n{audit_text}"
    );
}

// ---- Test 5: CLI error/help paths, custom endpoint, 413 limit, tool errors, fresh UUID, daemon EOF ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_flags_custom_endpoint_limits_tool_errors_and_daemon_eof_behave_cleanly() {
    // 1. --help / -h exit 0; bad/missing flags exit nonzero with expected/saw or missing
    for flag in ["--help", "-h"] {
        let out = Command::new(GATEWAY_BIN).arg(flag).output().unwrap();
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage: roger-mcp-gateway"));
    }

    let missing = Command::new(GATEWAY_BIN)
        .args(["--socket", "/tmp/s", "--store-dir", "/tmp/d"])
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing --bind"));

    let bad_flag = Command::new(GATEWAY_BIN).arg("--bogus").output().unwrap();
    assert!(!bad_flag.status.success());
    let stderr = String::from_utf8_lossy(&bad_flag.stderr);
    assert!(
        stderr.contains("expected") && stderr.contains("saw --bogus"),
        "{stderr}"
    );

    let unset_tok = Command::new(GATEWAY_BIN)
        .args([
            "--bind",
            "127.0.0.1:0",
            "--socket",
            "/tmp/s",
            "--store-dir",
            "/tmp/d",
            "--token-env",
            "DEFINITELY_UNSET_ROGER_GW_VAR",
        ])
        .env_remove("DEFINITELY_UNSET_ROGER_GW_VAR")
        .output()
        .unwrap();
    assert!(!unset_tok.status.success());
    assert!(String::from_utf8_lossy(&unset_tok.stderr).contains("DEFINITELY_UNSET_ROGER_GW_VAR"));

    // 2. Custom `--endpoint /v1/mcp`, fresh UUIDv4 Address generation, HTTP status edge cases
    let mut rig = TestRig::start("edge-cases").await;
    let (_, gw_port) = rig.spawn_gateway(&["--endpoint", "/v1/mcp"], &[]).await;

    // Default `/mcp` returns 404 when `--endpoint /v1/mcp` is configured
    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Non-initialize without Mcp-Session-Id -> 400; unknown Mcp-Session-Id -> 404; PUT -> 405
    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/v1/mcp",
        &[],
        r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, _, _) = h1_request(
        gw_port,
        Method::DELETE,
        "/v1/mcp",
        &[("mcp-session-id", "non-existent")],
        "",
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    let (st, hdrs_405, _) = h1_request(gw_port, Method::PUT, "/v1/mcp", &[], "").await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        hdrs_405.get(http::header::ALLOW).unwrap(),
        "GET, POST, DELETE"
    );

    // Body > 16 MiB -> 413 Payload Too Large (checked both via Content-Length and streaming body)
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut raw_tcp = TcpStream::connect(("127.0.0.1", gw_port)).await.unwrap();
        let req_head = format!(
            "POST /v1/mcp HTTP/1.1\r\nHost: 127.0.0.1:{gw_port}\r\nContent-Length: {}\r\n\r\n",
            16 * 1024 * 1024 + 64
        );
        raw_tcp.write_all(req_head.as_bytes()).await.unwrap();
        let mut resp_buf = vec![0u8; 512];
        let n = raw_tcp.read(&mut resp_buf).await.unwrap();
        let resp_head = String::from_utf8_lossy(&resp_buf[..n]);
        assert!(
            resp_head.starts_with("HTTP/1.1 413 "),
            "expected 413 Payload Too Large, saw {resp_head}"
        );
    }
    {
        // Also test HTTP/2 streaming > 16 MiB without Content-Length header!
        let (body_tx, body_rx) = tokio::sync::mpsc::channel::<
            Result<hyper::body::Frame<Bytes>, std::convert::Infallible>,
        >(4);
        struct ChanBody(
            tokio::sync::mpsc::Receiver<
                Result<hyper::body::Frame<Bytes>, std::convert::Infallible>,
            >,
        );
        impl hyper::body::Body for ChanBody {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>>
            {
                self.0.poll_recv(cx)
            }
        }
        let stream = TcpStream::connect(("127.0.0.1", gw_port)).await.unwrap();
        let (mut h2_stream_sender, conn) =
            http2::handshake::<_, _, ChanBody>(TokioExecutor::new(), TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("http://127.0.0.1:{gw_port}/v1/mcp"))
            .body(ChanBody(body_rx))
            .unwrap();
        let resp_fut = tokio::spawn(async move { h2_stream_sender.send_request(req).await });
        let chunk = Bytes::from(vec![b'a'; 1024 * 1024]);
        for _ in 0..17 {
            if body_tx
                .send(Ok(hyper::body::Frame::data(chunk.clone())))
                .await
                .is_err()
            {
                break;
            }
        }
        drop(body_tx);
        let resp = resp_fut.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    // Initialize with no Address supplied -> generates fresh UUIDv4 and returns X-Roger-Address!
    let (st, hdrs, _) = h1_request(
        gw_port,
        Method::POST,
        "/v1/mcp",
        &[],
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid = hdrs
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let gen_addr = hdrs
        .get("x-roger-address")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        gen_addr.len(),
        36,
        "expected UUIDv4 Address, saw {gen_addr}"
    );

    // Tool validation error returns `isError: true` with exact `expected ..., saw ...` text
    let (st, _, err_b) = h1_request(
        gw_port,
        Method::POST,
        "/v1/mcp",
        &[("mcp-session-id", &sid)],
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"roger_send","arguments":{"to":"x"}}}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let err_rpc: Value = serde_json::from_slice(&err_b).unwrap();
    assert_eq!(err_rpc["result"]["isError"], Value::Bool(true));
    assert_eq!(
        err_rpc["result"]["content"][0]["text"],
        "roger_send: expected `body` to be a string, saw nothing"
    );

    // Open SSE stream, then terminate `roger daemon` (process index 1): SSE stream ends cleanly!
    let (st, _, mut sse) = h1_open_sse(gw_port, "/v1/mcp", &[("mcp-session-id", &sid)]).await;
    assert_eq!(st, StatusCode::OK);
    rig.processes[1].kill_and_wait();
    while sse.next_event().await.is_some() {}

    // Dead session now returns 404
    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/v1/mcp",
        &[("mcp-session-id", &sid)],
        r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_sends_the_client_token_to_a_guarded_daemon_over_tcp() {
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

    let init = |addr: &'static str| {
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": { "protocolVersion": "2025-06-18", "_meta": { "roger_address": addr } }
        }))
        .unwrap()
    };
    let registered = |sid: String, port: u16| async move {
        let (st, _, b) = h1_request(
            port,
            Method::POST,
            "/mcp",
            &[("mcp-session-id", &sid)],
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": { "name": "roger_registered", "arguments": { "wait_seconds": 3 } }
            }))
            .unwrap(),
        )
        .await;
        (st, b)
    };

    // With the token the gateway's session registers through the guarded daemon.
    let store_ok = rig.path("stores-ok");
    let (_, port_ok) = rig
        .spawn_gateway_with_socket(
            &target,
            &store_ok,
            &[],
            &[("ROGER_CLIENT_TOKEN", FLEET_TOKEN)],
        )
        .await;
    let (st, hdrs, _) = h1_request(port_ok, Method::POST, "/mcp", &[], init(ADDR_1)).await;
    assert_eq!(st, StatusCode::OK);
    let sid = hdrs
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let (st, b) = registered(sid, port_ok).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(parse_tool_payload(&b)["registered"], Value::Bool(true));

    // With a wrong token the session never registers.
    let store_bad = rig.path("stores-bad");
    let (_, port_bad) = rig
        .spawn_gateway_with_socket(&target, &store_bad, &[], &[("ROGER_CLIENT_TOKEN", "wrong")])
        .await;
    // With a wrong token the session never registers: the daemon closes the link.
    let (st, hdrs, _) = h1_request(port_bad, Method::POST, "/mcp", &[], init(ADDR_2)).await;
    if st == StatusCode::OK {
        let sid = hdrs
            .get("mcp-session-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let _ = registered(sid, port_bad).await;
    }
    let board = Command::new(roger_bin())
        .args(["board", "--socket", rig.daemon_sock().to_str().unwrap()])
        .output()
        .unwrap();
    let board = String::from_utf8_lossy(&board.stdout);
    assert!(
        board.contains(&format!("SESSION address={ADDR_1} ")),
        "{board}"
    );
    assert!(
        !board.contains(&format!("SESSION address={ADDR_2} ")),
        "{board}"
    );
}

// ---- Test 6: Board watch opt-in forwards committed Board changes over SSE ----

async fn init_session(port: u16, headers: &[(&str, &str)], params: Value) -> String {
    let (st, hdrs, body) = h1_request(
        port,
        Method::POST,
        "/mcp",
        headers,
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": params
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    hdrs.get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

async fn call(port: u16, sid: &str, name: &str, args: Value) -> Value {
    let (_, _, body) = h1_request(
        port,
        Method::POST,
        "/mcp",
        &[("mcp-session-id", sid)],
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 9, "method": "tools/call",
            "params": { "name": name, "arguments": args }
        }))
        .unwrap(),
    )
    .await;
    parse_tool_payload(&body)
}

/// The next `notifications/roger/board` frame whose `event` is `event`,
/// skipping every other frame.
async fn next_board(sse: &mut SseReader, event: &str) -> Value {
    next_board_of(sse, event, None).await
}

/// As `next_board`, for one session's change only when `session` is given.
async fn next_board_of(sse: &mut SseReader, event: &str, session: Option<&str>) -> Value {
    loop {
        let e = sse.next_event().await.expect("SSE stream ended");
        let p = &e.data["params"];
        if e.data["method"] == "notifications/roger/board"
            && p["event"] == event
            && session.is_none_or(|s| p["session"] == s)
        {
            assert!(
                e.raw_data.contains(r#""params":{"event":"#),
                "{}",
                e.raw_data
            );
            return p.clone();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn board_watch_opt_in_forwards_committed_changes_and_others_see_none() {
    let mut rig = TestRig::start("board-watch").await;
    let (_, gw_port) = rig.spawn_gateway(&[], &[]).await;

    // Opted in by header, by _meta, and not at all.
    let watcher = init_session(
        gw_port,
        &[("x-roger-address", ADDR_1), ("x-roger-watch", "1")],
        json!({}),
    )
    .await;
    let meta_watcher = init_session(
        gw_port,
        &[("x-roger-address", ADDR_COMPOSITE)],
        json!({"_meta": {"roger_watch": true}}),
    )
    .await;
    let (_, _, mut sse) = h1_open_sse(gw_port, "/mcp", &[("mcp-session-id", &watcher)]).await;
    let (_, _, mut meta_sse) =
        h1_open_sse(gw_port, "/mcp", &[("mcp-session-id", &meta_watcher)]).await;
    let plain = init_session(
        gw_port,
        &[("x-roger-address", ADDR_2), ("x-roger-label", "desk 4")],
        json!({}),
    )
    .await;
    let (_, _, mut plain_sse) = h1_open_sse(gw_port, "/mcp", &[("mcp-session-id", &plain)]).await;

    for s in [&mut sse, &mut meta_sse] {
        let reg = next_board_of(s, "register", Some(ADDR_2)).await;
        assert_eq!(reg["session"], ADDR_2, "{reg}");
        assert!(reg["at_ms"].is_i64(), "{reg}");
        assert!(reg.get("type").is_none(), "{reg}");
        let label = next_board(s, "relabel").await;
        assert_eq!(
            (&label["session"], &label["label"]),
            (&json!(ADDR_2), &json!("desk 4"))
        );
    }

    let claimed = call(
        gw_port,
        &plain,
        "roger_claim",
        json!({"key": "desk-4", "ttl_seconds": 60}),
    )
    .await;
    assert_eq!(claimed["ok"], true, "{claimed}");
    let acquired = next_board(&mut sse, "acquire").await;
    assert_eq!(
        (&acquired["key"], &acquired["owner"]),
        (&json!("desk-4"), &json!(ADDR_2))
    );
    assert!(acquired["expires_ms"].is_i64(), "{acquired}");
    call(
        gw_port,
        &plain,
        "roger_renew",
        json!({"key": "desk-4", "ttl_seconds": 120}),
    )
    .await;
    let renewed = next_board(&mut sse, "renew").await;
    assert!(renewed["expires_ms"].as_i64() > acquired["expires_ms"].as_i64());
    call(gw_port, &plain, "roger_release", json!({"key": "desk-4"})).await;
    let released = next_board(&mut sse, "release").await;
    assert_eq!(
        (&released["key"], &released["owner"]),
        (&json!("desk-4"), &json!(ADDR_2))
    );

    // The session that did not ask sees no Board change: a Mail sent after
    // all of them arrives with none ahead of it.
    let out = Command::new(roger_bin())
        .args([
            "send",
            "--socket",
            rig.daemon_sock().to_str().unwrap(),
            "--from",
            ADDR_1,
            "--to",
            ADDR_2,
            "--mail",
            "after-board",
            "--body",
            "b",
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    loop {
        let e = plain_sse.next_event().await.expect("SSE stream ended");
        assert_ne!(
            e.data["method"], "notifications/roger/board",
            "{}",
            e.raw_data
        );
        if e.data["method"] == "notifications/roger/mail" {
            assert_eq!(e.data["params"]["mail"], "after-board");
            break;
        }
    }

    // Unregister on DELETE shows too.
    let (st, _, _) = h1_request(
        gw_port,
        Method::DELETE,
        "/mcp",
        &[("mcp-session-id", &plain)],
        Bytes::new(),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let gone = next_board(&mut sse, "unregister").await;
    assert_eq!(gone["session"], ADDR_2, "{gone}");

    // A bad opt-in value is refused, naming what was expected.
    let (st, _, body) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[("x-roger-watch", "yes")],
        serde_json::to_vec(
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        )
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("expected X-Roger-Watch"));
}

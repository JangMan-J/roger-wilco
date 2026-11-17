//! End-to-end integration suite for `roger-mcp-gateway` against real `roger coordinator`,
//! `roger daemon`, `roger session`, and `roger audit` processes using MCP `2026-07-28`
//! (sessionless Streamable HTTP, `_meta`, `server/discover`, `subscriptions/listen`,
//! `roger_register`/`roger_unregister`, and SEP-3415 `events/{list,poll,stream}`).

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

// ---- HTTP/1.1 & HTTP/2 (h2c) + MCP 2026-07-28 + SSE helpers ----

#[derive(Debug, Clone)]
struct SseFrame {
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

    async fn next_frame(&mut self) -> Option<SseFrame> {
        tokio::time::timeout(Duration::from_secs(10), self.next_frame_inner())
            .await
            .expect("timed out waiting for SSE frame")
    }

    async fn next_frame_inner(&mut self) -> Option<SseFrame> {
        loop {
            if let Some(pos) = self.buf.find("\n\n") {
                let block = self.buf[..pos].to_string();
                self.buf.drain(..pos + 2);
                if block.trim().is_empty() || block.starts_with(':') {
                    continue;
                }
                let mut data_lines = Vec::new();
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("data: ") {
                        data_lines.push(v);
                    }
                }
                if data_lines.is_empty() {
                    continue;
                }
                let raw_data = data_lines.join("\n");
                let data = serde_json::from_str::<Value>(&raw_data)
                    .expect("SSE data must be compact JSON");
                return Some(SseFrame { data, raw_data });
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

fn meta_2026() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": { "io.modelcontextprotocol/events": {} }
        },
        "io.modelcontextprotocol/clientInfo": { "name": "gw-test", "version": "1.0" }
    })
}

fn rpc_body(id: Value, method: &str, mut params: Value) -> Vec<u8> {
    if let Some(obj) = params.as_object_mut() {
        obj.entry("_meta").or_insert_with(meta_2026);
    }
    serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    }))
    .unwrap()
}

fn rpc_headers<'a>(
    method: &'a str,
    name: Option<&'a str>,
    extra: &[(&'a str, &'a str)],
) -> Vec<(&'a str, &'a str)> {
    let mut h = vec![
        ("accept", "application/json, text/event-stream"),
        ("content-type", "application/json"),
        ("mcp-protocol-version", "2026-07-28"),
        ("mcp-method", method),
    ];
    if let Some(n) = name {
        h.push(("mcp-name", n));
    }
    h.extend_from_slice(extra);
    h
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

async fn h1_open_post_sse(
    port: u16,
    uri: &str,
    method: &str,
    name: Option<&str>,
    extra: &[(&str, &str)],
    params: Value,
) -> (StatusCode, HeaderMap, SseReader) {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (mut sender, conn) = http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(http::header::HOST, format!("127.0.0.1:{port}"));
    for (k, v) in rpc_headers(method, name, extra) {
        builder = builder.header(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    let body = rpc_body(json!(101), method, params);
    let req = builder.body(Full::new(Bytes::from(body))).unwrap();
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

    async fn open_post_sse(
        &self,
        path_and_query: &str,
        method: &str,
        name: Option<&str>,
        extra: &[(&str, &str)],
        params: Value,
    ) -> (StatusCode, HeaderMap, SseReader) {
        let mut sender = self.sender.clone();
        let uri = format!("http://127.0.0.1:{}{path_and_query}", self.port);
        let mut builder = Request::builder().method(Method::POST).uri(uri);
        for (k, v) in rpc_headers(method, name, extra) {
            builder = builder.header(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_bytes(v.as_bytes()).unwrap(),
            );
        }
        let body = rpc_body(json!(202), method, params);
        let req = builder.body(Full::new(Bytes::from(body))).unwrap();
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
    assert_eq!(res["resultType"], "complete");
    assert_ne!(
        res.get("isError"),
        Some(&Value::Bool(true)),
        "unexpected tool error: {rpc}"
    );
    let text = res["content"][0]["text"].as_str().unwrap();
    serde_json::from_str::<Value>(text).unwrap()
}

async fn h1_call(port: u16, uri: &str, extra: &[(&str, &str)], tool: &str, args: Value) -> Value {
    let (st, _, b) = h1_request(
        port,
        Method::POST,
        uri,
        &rpc_headers("tools/call", Some(tool), extra),
        rpc_body(
            json!(1),
            "tools/call",
            json!({ "name": tool, "arguments": args }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&b));
    parse_tool_payload(&b)
}

async fn h2_call(h2: &H2MultiplexClient, extra: &[(&str, &str)], tool: &str, args: Value) -> Value {
    let (st, _, b) = h2
        .request(
            Method::POST,
            "/mcp",
            &rpc_headers("tools/call", Some(tool), extra),
            rpc_body(
                json!(1),
                "tools/call",
                json!({ "name": tool, "arguments": args }),
            ),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{}", String::from_utf8_lossy(&b));
    parse_tool_payload(&b)
}

async fn h1_register(port: u16, extra: &[(&str, &str)], args: Value) -> (String, String) {
    let v = h1_call(port, "/mcp", extra, "roger_register", args).await;
    (
        v["session"].as_str().unwrap().to_string(),
        v["address"].as_str().unwrap().to_string(),
    )
}

async fn h2_register(
    h2: &H2MultiplexClient,
    extra: &[(&str, &str)],
    args: Value,
) -> (String, String) {
    let v = h2_call(h2, extra, "roger_register", args).await;
    (
        v["session"].as_str().unwrap().to_string(),
        v["address"].as_str().unwrap().to_string(),
    )
}

// ---- Test 1: Multi-client concurrent push over HTTP/1.1 AND HTTP/2 (h2c) + TCP daemon bridge ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_client_concurrent_push_over_h1_and_h2c_preserves_headers_and_broadcasts() {
    let mut rig = TestRig::start("multi-push").await;
    let tcp_target = format!("tcp://127.0.0.1:{}", rig.daemon_tcp_port);
    let store_dir = rig.store_dir();
    let (_, gw_port) = rig
        .spawn_gateway_with_socket(&tcp_target, &store_dir, &[], &[])
        .await;

    let (st, _, hbody) = h1_request(gw_port, Method::GET, "/healthz", &[], "").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(&hbody[..], br#"{"ok":true}"#);

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

    let h2 = H2MultiplexClient::connect(gw_port).await;
    let (sid_comp, addr_comp) = h2_register(
        &h2,
        &[],
        json!({ "id": ADDR_COMPOSITE, "label": "Meteor Worker", "declare": "role=observer" }),
    )
    .await;
    assert_eq!(addr_comp, ADDR_COMPOSITE);

    let (sid_1, _) = h1_register(gw_port, &[], json!({ "id": ADDR_1, "label": "alpha" })).await;
    let (sid_2, _) = h2_register(&h2, &[], json!({ "id": ADDR_2, "label": "beta" })).await;

    for sid in [&sid_comp, &sid_1, &sid_2] {
        let reg = h2_call(
            &h2,
            &[],
            "roger_registered",
            json!({ "session": sid, "wait_seconds": 15 }),
        )
        .await;
        assert_eq!(reg["registered"], Value::Bool(true));
    }

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

    let (st, hdrs_sse, mut sse_comp) = h2
        .open_post_sse(
            "/mcp",
            "events/stream",
            Some("roger.mail"),
            &[],
            json!({ "name": "roger.mail", "arguments": { "session": sid_comp }, "cursor": "r-0" }),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs_sse.get("x-accel-buffering").unwrap(), "no");

    let (st, _, mut sse_1) = h1_open_post_sse(
        gw_port,
        "/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid_1 }, "cursor": "r-0" }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, mut sse_2) = h2
        .open_post_sse(
            "/mcp",
            "events/stream",
            Some("roger.mail"),
            &[],
            json!({ "name": "roger.mail", "arguments": { "session": sid_2 }, "cursor": "r-0" }),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    assert_eq!(
        sse_comp.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    assert_eq!(
        sse_1.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    assert_eq!(
        sse_2.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );

    async fn next_mail_sse(sse: &mut SseReader) -> SseFrame {
        loop {
            let ev = sse.next_frame().await.expect("SSE stream ended early");
            if ev.data["method"] == "notifications/events/event" {
                return ev;
            }
        }
    }

    let h2_wait = h2.clone();
    let sid_2_clone = sid_2.clone();
    let waiting_inbox = tokio::spawn(async move {
        h2_call(
            &h2_wait,
            &[],
            "roger_inbox",
            json!({ "session": sid_2_clone, "wait_seconds": 15 }),
        )
        .await
    });

    // Ordered headers raw JSON: z-omega, a-alpha, café-unicode!
    let send_raw = format!(
        r#"{{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{{"_meta":{{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{{}}}},"name":"roger_send","arguments":{{"session":"{sid_1}","to":"{ADDR_COMPOSITE}","mail":"m-ordered-1","body":"hello meteor","headers":{{"z-omega":"last","a-alpha":"first","café":"☕-snow-☃"}}}}}}}}"#
    );
    let (st, _, send_resp) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("tools/call", Some("roger_send"), &[]),
        send_raw,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(parse_tool_payload(&send_resp)["mail"], "m-ordered-1");

    let pushed = next_mail_sse(&mut sse_comp).await;
    let data = &pushed.data["params"]["data"];
    assert_eq!(pushed.data["params"]["eventId"], "m-ordered-1");
    assert_eq!(pushed.data["params"]["cursor"], "r-1");
    assert_eq!(data["mail"], "m-ordered-1");
    assert_eq!(data["record"], "r-1");
    assert_eq!(data["from"], ADDR_1);
    assert_eq!(data["body"], "hello meteor");
    assert_eq!(data["kind"], "directed");
    let hdr_keys: Vec<&str> = data["headers"]
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

    let bcast = h2_call(
        &h2,
        &[],
        "roger_send",
        json!({ "session": sid_comp, "to": "*", "mail": "m-bcast-1", "body": "fleet-wide-announcement" }),
    )
    .await;
    assert_eq!(bcast["mail"], "m-bcast-1");

    let (ev_1, ev_2, ev_comp) = tokio::join!(
        next_mail_sse(&mut sse_1),
        next_mail_sse(&mut sse_2),
        next_mail_sse(&mut sse_comp),
    );
    assert_eq!(
        ev_1.data["params"]["data"]["body"],
        "fleet-wide-announcement"
    );
    assert_eq!(ev_1.data["params"]["data"]["kind"], "broadcast");
    assert_eq!(
        ev_2.data["params"]["data"]["body"],
        "fleet-wide-announcement"
    );
    assert_eq!(ev_2.data["params"]["data"]["kind"], "broadcast");
    assert_eq!(
        ev_comp.data["params"]["data"]["body"],
        "fleet-wide-announcement"
    );

    let inbox_payload = waiting_inbox.await.unwrap();
    assert_eq!(inbox_payload["mail"].as_array().unwrap().len(), 1);
    assert_eq!(inbox_payload["mail"][0]["body"], "fleet-wide-announcement");
    assert_eq!(inbox_payload["cursor"], "r-1");

    wait_until("tracer stored broadcast", 10, || {
        fs::read_to_string(&tracer_store)
            .ok()
            .filter(|s| s.contains("fleet-wide-announcement"))
    })
    .await;
}

// ---- Test 2: Startup race & DurLog cursor resumption without duplicates ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_race_and_cursor_resumption_deliver_exact_events_without_duplicates() {
    let mut rig = TestRig::start("sse-resume").await;
    let (_, gw_port) = rig.spawn_gateway(&[], &[]).await;

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

    let (sid, _) = h1_register(gw_port, &[], json!({ "id": ADDR_1 })).await;
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
    let reg = h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_registered",
        json!({ "session": sid, "wait_seconds": 10 }),
    )
    .await;
    assert_eq!(reg["registered"], Value::Bool(true));

    // Open `roger.mail` stream with cursor="r-0": replays r-1 immediately!
    let (st, _, mut sse1) = h1_open_post_sse(
        gw_port,
        "/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid }, "cursor": "r-0" }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let act1 = sse1.next_frame().await.unwrap();
    assert_eq!(act1.data["method"], "notifications/events/active");
    let e1 = sse1.next_frame().await.unwrap();
    assert_eq!(e1.data["method"], "notifications/events/event");
    assert_eq!(e1.data["params"]["eventId"], "mail-before-init");
    assert_eq!(e1.data["params"]["cursor"], "r-1");
    drop(sse1);

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

    // Resume `events/stream` with cursor="r-1": replays r-2 and r-3 without duplicating r-1!
    let (st, _, mut sse2) = h1_open_post_sse(
        gw_port,
        "/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid }, "cursor": "r-1" }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        sse2.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    let r2 = sse2.next_frame().await.unwrap();
    let r3 = sse2.next_frame().await.unwrap();
    assert_eq!(r2.data["params"]["eventId"], "mail-missed-2");
    assert_eq!(r2.data["params"]["cursor"], "r-2");
    assert_eq!(r3.data["params"]["eventId"], "mail-missed-3");
    assert_eq!(r3.data["params"]["cursor"], "r-3");
}

// ---- Test 3: 2026-07-28 headers/errors + Bearer auth + all tools + restart + unregister + takeover ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_tools_2026_07_28_validation_bearer_auth_restart_store_lock_and_unregister() {
    let mut rig = TestRig::start("tools-auth-restart").await;
    let (_, gw_port) = rig
        .spawn_gateway(
            &["--token-env", "TEST_GW_BEARER"],
            &[("TEST_GW_BEARER", GATEWAY_BEARER)],
        )
        .await;

    // 1. Auth checks
    let (st, hdrs, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("server/discover", None, &[]),
        rpc_body(json!(1), "server/discover", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(hdrs.get(http::header::WWW_AUTHENTICATE).unwrap(), "Bearer");

    let auth_hdr = format!("Bearer {GATEWAY_BEARER}");
    let auth_extra = [("authorization", auth_hdr.as_str())];

    // 2. Origin, Method, Accept, HeaderMismatch, UnsupportedProtocolVersion, 404 on initialize/ping
    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers(
            "server/discover",
            None,
            &[
                ("authorization", &auth_hdr),
                ("origin", "https://evil.example"),
            ],
        ),
        rpc_body(json!(1), "server/discover", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, hdrs_405, _) = h1_request(gw_port, Method::GET, "/mcp", &auth_extra, "").await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(hdrs_405.get(http::header::ALLOW).unwrap(), "POST");
    let (st, _, _) = h1_request(gw_port, Method::DELETE, "/mcp", &auth_extra, "").await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);

    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[
            ("authorization", &auth_hdr),
            ("accept", "text/plain"),
            ("mcp-protocol-version", "2026-07-28"),
            ("mcp-method", "server/discover"),
        ],
        rpc_body(json!(1), "server/discover", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::NOT_ACCEPTABLE);

    // HeaderMismatch (-32020) when Mcp-Method / Mcp-Name / MCP-Protocol-Version disagree or missing
    let (st, _, mm_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("tools/list", None, &auth_extra),
        rpc_body(json!(1), "server/discover", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&mm_b).unwrap()["error"]["code"],
        -32020
    );

    let (st, _, mn_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("tools/call", Some("wrong_name"), &auth_extra),
        rpc_body(
            json!(1),
            "tools/call",
            json!({ "name": "roger_register", "arguments": {} }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&mn_b).unwrap()["error"]["code"],
        -32020
    );

    // Mcp-Name base64 sentinel decoding: "roger_register" -> cm9nZXJfcmVnaXN0ZXI=
    let (st, _, b64_ok) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers(
            "tools/call",
            Some("=?base64?cm9nZXJfcmVnaXN0ZXI=?="),
            &auth_extra,
        ),
        rpc_body(
            json!(1),
            "tools/call",
            json!({ "name": "roger_register", "arguments": { "id": ADDR_1 } }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sid_old = parse_tool_payload(&b64_ok)["session"]
        .as_str()
        .unwrap()
        .to_string();

    // UnsupportedProtocolVersionError (-32022)
    let (st, _, ver_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &[
            ("authorization", &auth_hdr),
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", "2025-11-25"),
            ("mcp-method", "server/discover"),
        ],
        serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2025-11-25",
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let ver_v: Value = serde_json::from_slice(&ver_b).unwrap();
    assert_eq!(ver_v["error"]["code"], -32022);
    assert_eq!(ver_v["error"]["data"]["supported"], json!(["2026-07-28"]));

    // Missing _meta -> -32602; unknown method (`initialize` / `ping`) -> 404 + -32601
    let (st, _, meta_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("server/discover", None, &auth_extra),
        r#"{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}"#,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&meta_b).unwrap()["error"]["code"],
        -32602
    );

    for dead_method in ["initialize", "ping", "unknown/method"] {
        let (st, _, ub) = h1_request(
            gw_port,
            Method::POST,
            "/mcp",
            &rpc_headers(dead_method, None, &auth_extra),
            rpc_body(json!(4), dead_method, json!({})),
        )
        .await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{dead_method}");
        assert_eq!(
            serde_json::from_slice::<Value>(&ub).unwrap()["error"]["code"],
            -32601
        );
    }

    // 3. server/discover + tools/list + notifications -> 202 + subscriptions/listen
    let h2 = H2MultiplexClient::connect(gw_port).await;
    let (st, _, disc_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &rpc_headers("server/discover", None, &auth_extra),
            rpc_body(json!(2), "server/discover", json!({})),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let disc: Value = serde_json::from_slice(&disc_b).unwrap();
    assert_eq!(disc["result"]["supportedVersions"], json!(["2026-07-28"]));
    assert_eq!(
        disc["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "roger-mcp-gateway"
    );
    assert!(
        disc["result"]["capabilities"]["extensions"]["io.modelcontextprotocol/events"].is_object()
    );

    let (st, _, empty_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &auth_extra,
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        )
        .await;
    assert_eq!(st, StatusCode::ACCEPTED);
    assert!(empty_b.is_empty());

    let (_, _, list_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &rpc_headers("tools/list", None, &auth_extra),
            rpc_body(json!(3), "tools/list", json!({})),
        )
        .await;
    let tools_json: Value = serde_json::from_slice(&list_b).unwrap();
    assert_eq!(tools_json["result"]["tools"].as_array().unwrap().len(), 11);
    assert_eq!(tools_json["result"]["ttlMs"], 300_000);

    let (st, _, mut listen_sse) = h2
        .open_post_sse(
            "/mcp",
            "subscriptions/listen",
            None,
            &auth_extra,
            json!({ "notifications": { "toolsListChanged": true } }),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let ack = listen_sse.next_frame().await.unwrap();
    assert_eq!(
        ack.data["method"],
        "notifications/subscriptions/acknowledged"
    );
    drop(listen_sse);

    // 4. Second gateway process on SAME --store-dir -> File::try_lock() refusal (isError: true)
    let (gw2_idx, gw2_port) = rig.spawn_gateway(&[], &[]).await;
    let (st_conflict, _, conflict_b) = h1_request(
        gw2_port,
        Method::POST,
        "/mcp",
        &rpc_headers("tools/call", Some("roger_register"), &[]),
        rpc_body(
            json!(1),
            "tools/call",
            json!({ "name": "roger_register", "arguments": { "id": ADDR_1 } }),
        ),
    )
    .await;
    assert_eq!(st_conflict, StatusCode::OK);
    let c_rpc: Value = serde_json::from_slice(&conflict_b).unwrap();
    assert_eq!(c_rpc["result"]["isError"], Value::Bool(true));
    assert!(
        c_rpc["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("held by another process"),
        "{c_rpc}"
    );
    rig.processes[gw2_idx].kill_and_wait();

    // 5. Container restart on SAME gateway: open events/stream on sid_old, re-register ADDR_1!
    let (_, _, mut old_sse) = h2
        .open_post_sse(
            "/mcp",
            "events/stream",
            Some("roger.mail"),
            &auth_extra,
            json!({ "name": "roger.mail", "arguments": { "session": sid_old } }),
        )
        .await;
    assert_eq!(
        old_sse.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );

    let (sid_new, _) = h2_register(&h2, &auth_extra, json!({ "id": ADDR_1 })).await;
    assert_ne!(sid_old, sid_new);

    // Old stream receives notifications/events/terminated, and old handle returns isError!
    let term = old_sse.next_frame().await.unwrap();
    assert_eq!(term.data["method"], "notifications/events/terminated");
    assert!(old_sse.next_frame().await.is_none());

    let (st, _, old_err_b) = h2
        .request(
            Method::POST,
            "/mcp",
            &rpc_headers("tools/call", Some("roger_board"), &auth_extra),
            rpc_body(
                json!(6),
                "tools/call",
                json!({ "name": "roger_board", "arguments": { "session": sid_old } }),
            ),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let old_err: Value = serde_json::from_slice(&old_err_b).unwrap();
    assert_eq!(old_err["result"]["isError"], Value::Bool(true));

    // 6. Exercise all 9 tools on `sid_new`!
    let reg = h2_call(
        &h2,
        &auth_extra,
        "roger_registered",
        json!({ "session": sid_new, "wait_seconds": 10 }),
    )
    .await;
    assert_eq!(reg["registered"], Value::Bool(true));
    let lbl = h2_call(
        &h2,
        &auth_extra,
        "roger_label",
        json!({ "session": sid_new, "label": "gateway-worker-1" }),
    )
    .await;
    assert_eq!(lbl["label"], "gateway-worker-1");
    let clm = h2_call(
        &h2,
        &auth_extra,
        "roger_claim",
        json!({ "session": sid_new, "key": "task/42", "ttl_seconds": 120 }),
    )
    .await;
    assert_eq!(clm["ok"], Value::Bool(true));
    let ren = h2_call(
        &h2,
        &auth_extra,
        "roger_renew",
        json!({ "session": sid_new, "key": "task/42", "ttl_seconds": 300 }),
    )
    .await;
    assert_eq!(ren["ok"], Value::Bool(true));
    let brd = h2_call(
        &h2,
        &auth_extra,
        "roger_board",
        json!({ "session": sid_new }),
    )
    .await;
    assert!(brd["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["session"] == ADDR_1 && r["label"] == "gateway-worker-1"));
    let rel = h2_call(
        &h2,
        &auth_extra,
        "roger_release",
        json!({ "session": sid_new, "key": "task/42" }),
    )
    .await;
    assert_eq!(rel["ok"], Value::Bool(true));

    let sent = h2_call(&h2, &auth_extra, "roger_send", json!({ "session": sid_new, "to": ADDR_1, "mail": "m-self-9", "body": "tool-parity-check" })).await;
    assert_eq!(sent["mail"], "m-self-9");

    let peeked = h2_call(
        &h2,
        &auth_extra,
        "roger_inbox",
        json!({ "session": sid_new, "limit": 10, "wait_seconds": 10, "peek": true }),
    )
    .await;
    assert_eq!(peeked["mail"][0]["mail"], "m-self-9");
    let consumed = h2_call(
        &h2,
        &auth_extra,
        "roger_inbox",
        json!({ "session": sid_new, "limit": 10, "wait_seconds": 5, "peek": false }),
    )
    .await;
    assert_eq!(consumed["cursor"], "r-1");
    let after_consume = h2_call(
        &h2,
        &auth_extra,
        "roger_inbox",
        json!({ "session": sid_new, "limit": 10, "wait_seconds": 0 }),
    )
    .await;
    assert!(after_consume["mail"].as_array().unwrap().is_empty());

    let rec = h2_call(
        &h2,
        &auth_extra,
        "roger_receipt",
        json!({ "session": sid_new, "mail": "m-self-9" }),
    )
    .await;
    assert_eq!(rec["record"], "r-1");

    // 7. Register ADDR_2, verify Store exists, then roger_unregister removes empty Store & leaves Board!
    let (sid_a2, _) = h2_register(&h2, &auth_extra, json!({ "id": ADDR_2 })).await;
    let reg_a2 = h2_call(
        &h2,
        &auth_extra,
        "roger_registered",
        json!({ "session": sid_a2, "wait_seconds": 10 }),
    )
    .await;
    assert_eq!(reg_a2["registered"], Value::Bool(true));
    let a2_store = store_path(&rig.store_dir(), ADDR_2);
    assert!(a2_store.exists());

    let unreg = h2_call(
        &h2,
        &auth_extra,
        "roger_unregister",
        json!({ "session": sid_a2 }),
    )
    .await;
    assert_eq!(unreg["ok"], Value::Bool(true));
    assert_eq!(unreg["address"], ADDR_2);
    assert!(!a2_store.exists());

    let brd_after = h2_call(
        &h2,
        &auth_extra,
        "roger_board",
        json!({ "session": sid_new, "all": true }),
    )
    .await;
    assert!(!brd_after["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["session"] == ADDR_2));

    // 8. Cross-host takeover -> `roger.registration` emits `superseded`!
    let (_, _, mut sse_reg) = h2
        .open_post_sse(
            "/mcp",
            "events/stream",
            Some("roger.registration"),
            &auth_extra,
            json!({ "name": "roger.registration", "arguments": { "session": sid_new } }),
        )
        .await;
    assert_eq!(
        sse_reg.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    let initial_reg = sse_reg.next_frame().await.unwrap();
    assert_eq!(initial_reg.data["params"]["data"]["event"], "registered");

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
    rig.spawn_roger(&[
        "session",
        "--socket",
        &format!("tcp://127.0.0.1:{d2_port}"),
        "--store",
        rig.path("takeover.store").to_str().unwrap(),
        "--id",
        ADDR_1,
    ]);

    let sup_ev = sse_reg.next_frame().await.unwrap();
    assert_eq!(sup_ev.data["params"]["data"]["event"], "superseded");
    assert_eq!(sup_ev.data["params"]["data"]["host"], "host-b");

    let reg_after = h2_call(
        &h2,
        &auth_extra,
        "roger_registered",
        json!({ "session": sid_new, "wait_seconds": 0 }),
    )
    .await;
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

    let (sid1, _) = h1_register(gw_port, &[], json!({ "id": ADDR_COMPOSITE })).await;
    let reg = h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_registered",
        json!({ "session": sid1, "wait_seconds": 10 }),
    )
    .await;
    assert_eq!(reg["registered"], Value::Bool(true));

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

    rig.processes[gw_idx].kill_and_wait();

    let (_, gw2_port) = rig
        .spawn_gateway(&["--hold-before-ack", marker.to_str().unwrap()], &[])
        .await;

    let (sid2, _) = h1_register(gw2_port, &[], json!({ "id": ADDR_COMPOSITE })).await;

    let (st, _, mut sse) = h1_open_post_sse(
        gw2_port,
        "/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid2 }, "cursor": "r-0" }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        sse.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    let seeded_ev = sse.next_frame().await.unwrap();
    assert_eq!(seeded_ev.data["params"]["cursor"], "r-1");
    assert_eq!(seeded_ev.data["params"]["eventId"], "mail-crash-window");
    assert_eq!(
        seeded_ev.data["params"]["data"]["body"],
        "committed-once-across-sigkill"
    );

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
    assert!(receipt_line.contains("record=r-1 "), "{receipt_line}");

    let s_file = store_path(&rig.store_dir(), ADDR_COMPOSITE);
    let raw_store = fs::read_to_string(&s_file).unwrap();
    assert_eq!(raw_store.lines().count(), 1, "{raw_store}");
    assert!(raw_store.contains(r#""headers":{"z-order":"9","a-order":"1"}"#));

    let p1 = h1_call(
        gw2_port,
        "/mcp",
        &[],
        "roger_inbox",
        json!({ "session": sid2, "wait_seconds": 5 }),
    )
    .await;
    assert_eq!(p1["mail"].as_array().unwrap().len(), 1);
    assert_eq!(p1["cursor"], "r-1");
    assert_eq!(
        fs::read_to_string(cursor_path(&s_file)).unwrap().trim(),
        "r-1"
    );

    let (sid3, _) = h1_register(gw2_port, &[], json!({ "id": ADDR_COMPOSITE })).await;
    let p2 = h1_call(
        gw2_port,
        "/mcp",
        &[],
        "roger_inbox",
        json!({ "session": sid3, "wait_seconds": 0 }),
    )
    .await;
    assert!(p2["mail"].as_array().unwrap().is_empty());

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
    assert!(audit_out.status.success());
    assert!(audit_text.contains("record=r-1 mail=mail-crash-window"));
    assert!(audit_text.contains("unreceipted=0 duplicates=0 mismatches=0 torn=0 skipped=0"));
}

// ---- Test 5: CLI flags, custom endpoint, 413 limit, tool errors, fresh UUID, daemon EOF ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_flags_custom_endpoint_limits_tool_errors_and_daemon_eof_behave_cleanly() {
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

    let mut rig = TestRig::start("edge-cases").await;
    let (_, gw_port) = rig.spawn_gateway(&["--endpoint", "/v1/mcp"], &[]).await;

    let (st, _, _) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("server/discover", None, &[]),
        rpc_body(json!(1), "server/discover", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    let (st, hdrs_405, _) = h1_request(gw_port, Method::PUT, "/v1/mcp", &[], "").await;
    assert_eq!(st, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(hdrs_405.get(http::header::ALLOW).unwrap(), "POST");

    // > 16 MiB Content-Length -> 413
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
        assert!(resp_head.starts_with("HTTP/1.1 413 "), "{resp_head}");
    }

    let reg = h1_call(gw_port, "/v1/mcp", &[], "roger_register", json!({})).await;
    let sid = reg["session"].as_str().unwrap().to_string();
    let gen_addr = reg["address"].as_str().unwrap().to_string();
    assert_eq!(gen_addr.len(), 36);

    let (st, _, err_b) = h1_request(
        gw_port,
        Method::POST,
        "/v1/mcp",
        &rpc_headers("tools/call", Some("roger_send"), &[]),
        rpc_body(
            json!(2),
            "tools/call",
            json!({ "name": "roger_send", "arguments": { "session": sid, "to": "x" } }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let err_rpc: Value = serde_json::from_slice(&err_b).unwrap();
    assert_eq!(err_rpc["result"]["isError"], Value::Bool(true));
    assert_eq!(
        err_rpc["result"]["content"][0]["text"],
        "roger_send: expected `body` to be a string, saw nothing"
    );

    let (st, _, mut sse) = h1_open_post_sse(
        gw_port,
        "/v1/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid } }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        sse.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );
    rig.processes[1].kill_and_wait();
    let term = sse.next_frame().await.unwrap();
    assert_eq!(term.data["method"], "notifications/events/terminated");
    assert!(sse.next_frame().await.is_none());
}

// ---- Test 6: Guarded daemon ROGER_CLIENT_TOKEN ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_sends_the_client_token_to_a_guarded_daemon_over_tcp() {
    let mut rig = TestRig::start("client-token").await;
    let coord_addr = format!("127.0.0.1:{}", rig.coord_port);
    let gport_file = rig.path("guarded.port");
    let glog = rig.path("guarded.log");
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

    let store_ok = rig.path("stores-ok");
    let (_, port_ok) = rig
        .spawn_gateway_with_socket(
            &target,
            &store_ok,
            &[],
            &[("ROGER_CLIENT_TOKEN", FLEET_TOKEN)],
        )
        .await;
    let (sid_ok, _) = h1_register(port_ok, &[], json!({ "id": ADDR_1 })).await;
    let reg_ok = h1_call(
        port_ok,
        "/mcp",
        &[],
        "roger_registered",
        json!({ "session": sid_ok, "wait_seconds": 3 }),
    )
    .await;
    assert_eq!(reg_ok["registered"], Value::Bool(true));

    let store_bad = rig.path("stores-bad");
    let (_, port_bad) = rig
        .spawn_gateway_with_socket(&target, &store_bad, &[], &[("ROGER_CLIENT_TOKEN", "wrong")])
        .await;
    let _ = h1_request(
        port_bad,
        Method::POST,
        "/mcp",
        &rpc_headers("tools/call", Some("roger_register"), &[]),
        rpc_body(
            json!(1),
            "tools/call",
            json!({ "name": "roger_register", "arguments": { "id": ADDR_2 } }),
        ),
    )
    .await;

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

// ---- Test 7: SEP-3415 events/{list,poll,stream}, roger.board watch, heartbeats, abort cancel ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sep3415_events_poll_stream_heartbeat_abort_and_board_watch() {
    let mut rig = TestRig::start("sep3415-gw").await;
    let (_, gw_port) = rig.spawn_gateway(&["--groups"], &[]).await;

    let (sid1, _) = h1_register(gw_port, &[], json!({ "id": ADDR_1, "watch": true })).await;
    let (sid2, _) = h1_register(gw_port, &[], json!({ "id": ADDR_2 })).await;

    // events/list
    let (st, _, list_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("events/list", None, &[]),
        rpc_body(json!(1), "events/list", json!({})),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let ev_list: Value = serde_json::from_slice(&list_b).unwrap();
    let names: Vec<&str> = ev_list["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["roger.mail", "roger.board", "roger.registration"]);

    // Unsupported poll on roger.board -> -32026
    let (st, _, unsup_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("events/poll", Some("roger.board"), &[]),
        rpc_body(
            json!(2),
            "events/poll",
            json!({ "name": "roger.board", "arguments": { "session": sid1 } }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&unsup_b).unwrap()["error"]["code"],
        -32026
    );

    // Open roger.board stream on sid2 (triggers lazy ensure_watch) & roger.mail stream with heartbeatMs=1000
    let (st, _, mut board_sse) = h1_open_post_sse(
        gw_port,
        "/mcp",
        "events/stream",
        Some("roger.board"),
        &[],
        json!({ "name": "roger.board", "arguments": { "session": sid2 }, "heartbeatMs": 5000 }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        board_sse.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );

    let (st, _, mut mail_sse) = h1_open_post_sse(
        gw_port,
        "/mcp",
        "events/stream",
        Some("roger.mail"),
        &[],
        json!({ "name": "roger.mail", "arguments": { "session": sid2 }, "heartbeatMs": 1000 }),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        mail_sse.next_frame().await.unwrap().data["method"],
        "notifications/events/active"
    );

    // Wait for heartbeat on quiet mail_sse (~1s)
    let hb = mail_sse.next_frame().await.unwrap();
    assert_eq!(hb.data["method"], "notifications/events/heartbeat");
    assert_eq!(hb.data["params"]["cursor"], "r-0");

    // Trigger board event via roger_label on sid1 -> observed on board_sse
    h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_label",
        json!({ "session": sid1, "label": "desk-9" }),
    )
    .await;
    let bev = loop {
        let f = board_sse.next_frame().await.unwrap();
        if f.data["method"] == "notifications/events/event"
            && f.data["params"]["data"]["event"] == "relabel"
        {
            break f;
        }
    };
    assert_eq!(bev.data["params"]["data"]["label"], "desk-9");

    // Send 2 mails to sid2 -> poll with maxEvents=1 + hasMore, and live arrival on mail_sse
    h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_send",
        json!({ "session": sid1, "to": ADDR_2, "mail": "p-1", "body": "one" }),
    )
    .await;
    h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_send",
        json!({ "session": sid1, "to": ADDR_2, "mail": "p-2", "body": "two" }),
    )
    .await;

    let m1 = loop {
        let f = mail_sse.next_frame().await.unwrap();
        if f.data["method"] == "notifications/events/event" {
            break f;
        }
    };
    assert_eq!(m1.data["params"]["eventId"], "p-1");
    let m2 = loop {
        let f = mail_sse.next_frame().await.unwrap();
        if f.data["method"] == "notifications/events/event" {
            break f;
        }
    };
    assert_eq!(m2.data["params"]["eventId"], "p-2");

    // Poll page 1 (maxEvents=1) -> hasMore: true, then page 2
    let (st, _, poll1_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("events/poll", Some("roger.mail"), &[]),
        rpc_body(
            json!(3),
            "events/poll",
            json!({ "name": "roger.mail", "arguments": { "session": sid2 }, "cursor": "r-0", "maxEvents": 1 }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let poll1: Value = serde_json::from_slice(&poll1_b).unwrap();
    assert_eq!(poll1["result"]["events"].as_array().unwrap().len(), 1);
    assert_eq!(poll1["result"]["hasMore"], true);
    let cur1 = poll1["result"]["cursor"].as_str().unwrap();

    let (st, _, poll2_b) = h1_request(
        gw_port,
        Method::POST,
        "/mcp",
        &rpc_headers("events/poll", Some("roger.mail"), &[]),
        rpc_body(
            json!(4),
            "events/poll",
            json!({ "name": "roger.mail", "arguments": { "session": sid2 }, "cursor": cur1 }),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let poll2: Value = serde_json::from_slice(&poll2_b).unwrap();
    assert_eq!(poll2["result"]["events"][0]["eventId"], "p-2");
    assert_eq!(poll2["result"]["hasMore"], false);

    // Abort streams cleanly by dropping SseReader, and unregister sessions
    drop(mail_sse);
    drop(board_sse);
    h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_unregister",
        json!({ "session": sid1 }),
    )
    .await;
    h1_call(
        gw_port,
        "/mcp",
        &[],
        "roger_unregister",
        json!({ "session": sid2 }),
    )
    .await;
}

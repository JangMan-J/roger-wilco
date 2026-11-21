//! Standalone Named Groups coverage using a mock host daemon over TCP.

#[cfg(not(feature = "test-hooks"))]
compile_error!("integration tests require `--features test-hooks`");

use futures_util::{SinkExt, StreamExt};
use roger_ws_gateway::durlog::store_path;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const GATEWAY_BIN: &str = env!("CARGO_BIN_EXE_roger-ws-gateway");
const ADDRESS: &str = "44444444-4444-4444-8444-444444444444";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new(tag: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("roger-ws-groups-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn spawn_gateway(dir: &Path, daemon: &str) -> (ChildGuard, u16, PathBuf) {
    let port_file = dir.join("gateway.port");
    let store_dir = dir.join("stores");
    let out = fs::File::create(dir.join("gateway.out")).unwrap();
    let err = fs::File::create(dir.join("gateway.err")).unwrap();
    let child = Command::new(GATEWAY_BIN)
        .args([
            "--bind",
            "127.0.0.1:0",
            "--socket",
            &format!("tcp://{daemon}"),
            "--store-dir",
            store_dir.to_str().unwrap(),
            "--port-file",
            port_file.to_str().unwrap(),
        ])
        .env_remove("ROGER_CLIENT_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    let port = wait_file(&port_file).await.parse().unwrap();
    (ChildGuard(child), port, store_dir)
}

async fn wait_file(path: &Path) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = fs::read_to_string(path) {
            if !text.trim().is_empty() {
                return text.trim().to_string();
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "missing {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

enum MockEvent {
    Session(Value, OwnedWriteHalf),
    /// A request and the id of the daemon connection that carried it.
    Request(u64, Value),
    Line(Value),
}

struct MockDaemon {
    address: String,
    events: mpsc::UnboundedReceiver<MockEvent>,
    accept: JoinHandle<()>,
    /// Last `req` seen per daemon connection: request sockets may persist, so
    /// `req` must rise strictly on each one.
    last_req: HashMap<u64, i64>,
}

impl MockDaemon {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        let accept = tokio::spawn(async move {
            let mut conn = 0u64;
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                conn += 1;
                let tx = tx.clone();
                tokio::spawn(async move {
                    serve_connection(conn, stream, tx).await;
                });
            }
        });
        Self {
            address,
            events: rx,
            accept,
            last_req: HashMap::new(),
        }
    }

    async fn event(&mut self) -> MockEvent {
        tokio::time::timeout(Duration::from_secs(10), self.events.recv())
            .await
            .expect("mock daemon event arrived")
            .expect("mock daemon event channel stays open")
    }

    async fn session(&mut self) -> (Value, OwnedWriteHalf) {
        loop {
            match self.event().await {
                MockEvent::Session(request, writer) => return (request, writer),
                MockEvent::Line(_) => {}
                MockEvent::Request(_, v) => panic!("expected Register, saw request {v}"),
            }
        }
    }

    /// The next request with its `req` checked (an integer rising strictly on
    /// its daemon connection) and removed.
    async fn request(&mut self) -> Value {
        match self.event().await {
            MockEvent::Request(conn, mut v) => {
                let req = v["req"]
                    .as_i64()
                    .unwrap_or_else(|| panic!("expected integer req, saw {v}"));
                let last = self.last_req.insert(conn, req).unwrap_or(0);
                assert!(
                    req > last,
                    "expected req > {last} on connection {conn}, saw {v}"
                );
                v.as_object_mut().unwrap().remove("req");
                v
            }
            MockEvent::Session(v, _) => panic!("expected a request, saw Register {v}"),
            MockEvent::Line(v) => panic!("expected a request, saw session line {v}"),
        }
    }

    async fn line(&mut self) -> Value {
        match self.event().await {
            MockEvent::Line(v) => v,
            MockEvent::Request(_, v) => panic!("expected a session line, saw request {v}"),
            MockEvent::Session(v, _) => panic!("expected a session line, saw Register {v}"),
        }
    }
}

impl Drop for MockDaemon {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn serve_connection(conn: u64, stream: TcpStream, events: mpsc::UnboundedSender<MockEvent>) {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let Some(line) = lines.next_line().await.unwrap() else {
        return;
    };
    let request: Value = serde_json::from_str(&line).unwrap();
    if request["type"] == "register" {
        events
            .send(MockEvent::Session(request, write))
            .expect("Register event receiver is live");
        while let Ok(Some(line)) = lines.next_line().await {
            let value = serde_json::from_str(&line).unwrap();
            if events.send(MockEvent::Line(value)).is_err() {
                return;
            }
        }
        return;
    }
    // A request connection may carry any number of requests, one reply each.
    let mut request = request;
    loop {
        if events
            .send(MockEvent::Request(conn, request.clone()))
            .is_err()
        {
            return;
        }
        let reply = mock_reply(&request);
        if write
            .write_all(format!("{reply}\n").as_bytes())
            .await
            .is_err()
        {
            return;
        }
        let _ = write.flush().await;
        match lines.next_line().await {
            Ok(Some(line)) => request = serde_json::from_str(&line).unwrap(),
            _ => return,
        }
    }
}

fn mock_reply(request: &Value) -> Value {
    let req = request["req"].clone();
    match request["type"].as_str().unwrap() {
        // The mock Group's high water is 7: any other expected_article loses.
        "send" if request.get("expected_article").is_some_and(|e| e != 7) => json!({
            "type":"error", "req":req,
            "reason":format!("expected_article {} but the Group is at 7", request["expected_article"])
        }),
        "send" if request.get("group").is_some() => json!({
            "type":"accepted", "req":req, "mail":request["mail"],
            "group":request["group"], "article":7
        }),
        "send" => json!({"type":"accepted", "req":req, "mail":request["mail"]}),
        "subscribe" => {
            let position = if request["latest"] == true {
                12
            } else {
                request["after"].as_i64().unwrap_or(0)
            };
            json!({
                "type":"subscribed", "req":req, "session":request["session"],
                "group":request["group"], "position":position, "high_water":12
            })
        }
        "unsubscribe" => json!({
            "type":"unsubscribed", "req":req, "session":request["session"],
            "group":request["group"], "position":3
        }),
        "groups" if request.get("prefix").is_some() => json!({
            "type":"groups", "req":req, "groups":[{
                "group":"st-old", "high_water":2, "subscribers":0,
                "subscribed":false, "retired":true
            }]
        }),
        "articles" => json!({
            "type":"articles", "req":req, "group":request["group"], "high_water":12,
            "articles":[{"article":3, "mail":"m-3", "from":"author-1", "body":"three", "headers":{}}]
        }),
        "group_policy" => json!({
            "type":"group_policy", "req":req, "group":request["group"],
            "keep_last":request["keep_last"], "ttl_ms":request["ttl_ms"],
            "retired":request["retired"]
        }),
        "receipt" => json!({
            "type":"receipt", "req":req, "mail":request["mail"], "to":"reader-2",
            "kind":"directed", "accepted_at_ms":100, "record":"r-9", "acked_at_ms":150
        }),
        "groups" => json!({
            "type":"groups", "req":req, "groups":[{
                "group":"staff", "high_water":12, "subscribers":1,
                "subscribed":true, "position":3, "record":"r-3"
            }]
        }),
        other => json!({"type":"error", "reason":format!("unexpected {other}")}),
    }
}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Client {
    ws: Ws,
}

impl Client {
    async fn open(port: u16, query: &str) -> Self {
        let request = format!("ws://127.0.0.1:{port}/?{query}")
            .into_client_request()
            .unwrap();
        let (ws, _) = connect_async(request).await.unwrap();
        Self { ws }
    }

    async fn frame(&mut self) -> (Value, String) {
        let message = tokio::time::timeout(Duration::from_secs(10), self.ws.next())
            .await
            .expect("WebSocket frame arrived")
            .expect("WebSocket stream remains open")
            .unwrap();
        let Message::Text(text) = message else {
            panic!("expected a text frame, saw {message}");
        };
        let raw = text.to_string();
        (serde_json::from_str(&raw).unwrap(), raw)
    }

    async fn ready(&mut self) {
        let (value, _) = self.frame().await;
        assert_eq!(value, json!({"type":"ready", "session":ADDRESS}));
    }

    async fn event(&mut self) -> (Value, String) {
        let (value, raw) = self.frame().await;
        assert_eq!(value["type"], "mail", "expected Mail, saw {raw}");
        (value, raw)
    }

    async fn call(&mut self, request: Value) -> Value {
        self.ws
            .send(Message::text(request.to_string()))
            .await
            .unwrap();
        let (value, raw) = self.frame().await;
        assert!(
            !matches!(value["type"].as_str(), Some("ready" | "mail")),
            "expected a reply, saw {raw}"
        );
        value
    }

    async fn close(mut self) {
        let _ = self.ws.close(None).await;
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_secs(5), self.ws.next()).await
        {}
    }
}

async fn deliver(writer: &mut OwnedWriteHalf, value: Value) {
    writer
        .write_all(format!("{value}\n").as_bytes())
        .await
        .unwrap();
    writer.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_stay_disabled_without_upgrade_opt_in() {
    let dir = TestDir::new("default");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    let mut client = Client::open(port, &format!("id={ADDRESS}")).await;
    client.ready().await;
    let (register, _writer) = daemon.session().await;
    assert_eq!(register["session"], ADDRESS);

    let reply = client
        .call(json!({"type":"send", "group":"staff", "body":"hello"}))
        .await;
    assert_eq!(reply, json!({"type":"error", "error":"missing to"}));
    for request_type in [
        "subscribe",
        "unsubscribe",
        "groups",
        "articles",
        "group_policy",
    ] {
        let reply = client
            .call(json!({"type":request_type, "group":"staff"}))
            .await;
        assert_eq!(reply["type"], "error", "{reply}");
        let error = reply["error"].as_str().unwrap();
        assert!(
            error.starts_with("expected request send, board")
                && error.ends_with(&format!("; saw {request_type}")),
            "{error}"
        );
    }

    let accepted = client
        .call(json!({"type":"send", "to":"reader-2", "mail":"m-direct", "body":"hello"}))
        .await;
    assert_eq!(accepted, json!({"type":"accepted", "mail":"m-direct"}));
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"send", "mail":"m-direct", "from":ADDRESS,
            "to":"reader-2", "body":"hello"
        })
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_opt_in_relays_positions_lists_and_either_send_target() {
    let dir = TestDir::new("requests");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    let mut client = Client::open(port, &format!("id={ADDRESS}&groups=1")).await;
    client.ready().await;
    let (register, _writer) = daemon.session().await;
    assert_eq!(register["session"], ADDRESS);

    let reply = client
        .call(json!({"type":"subscribe", "group":"staff", "after":2}))
        .await;
    assert_eq!(
        reply,
        json!({
            "type":"subscribed", "session":ADDRESS, "group":"staff",
            "position":2, "high_water":12
        })
    );
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"subscribe", "session":ADDRESS,
            "group":"staff", "after":2
        })
    );

    let reply = client
        .call(json!({"type":"subscribe", "group":"staff", "latest":true}))
        .await;
    assert_eq!(reply["position"], 12);
    assert_eq!(reply["high_water"], 12);
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"subscribe", "session":ADDRESS,
            "group":"staff", "latest":true
        })
    );

    let reply = client
        .call(json!({"type":"unsubscribe", "group":"staff"}))
        .await;
    assert_eq!(
        reply,
        json!({
            "type":"unsubscribed", "session":ADDRESS, "group":"staff", "position":3
        })
    );
    assert_eq!(
        daemon.request().await,
        json!({"type":"unsubscribe", "session":ADDRESS, "group":"staff"})
    );

    let reply = client.call(json!({"type":"groups"})).await;
    assert_eq!(
        reply,
        json!({
            "type":"groups", "groups":[{
                "group":"staff", "high_water":12, "subscribers":1,
                "subscribed":true, "position":3, "record":"r-3"
            }]
        })
    );
    assert_eq!(
        daemon.request().await,
        json!({"type":"groups", "session":ADDRESS})
    );

    let reply = client
        .call(json!({
            "type":"send", "group":"staff", "mail":"m-group",
            "body":"article", "headers":{"x-note":"1"}
        }))
        .await;
    assert_eq!(
        reply,
        json!({"type":"accepted", "mail":"m-group", "group":"staff", "article":7})
    );
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"send", "mail":"m-group", "from":ADDRESS,
            "group":"staff", "body":"article", "headers":{"x-note":"1"}
        })
    );

    let reply = client
        .call(json!({"type":"send", "to":"reader-2", "mail":"m-direct", "body":"direct"}))
        .await;
    assert_eq!(reply, json!({"type":"accepted", "mail":"m-direct"}));
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"send", "mail":"m-direct", "from":ADDRESS,
            "to":"reader-2", "body":"direct"
        })
    );

    let both = client
        .call(json!({"type":"send", "to":"reader-2", "group":"staff", "body":"no"}))
        .await;
    assert_eq!(both["error"], "send accepts either to or group, not both");
    let neither = client.call(json!({"type":"send", "body":"no"})).await;
    assert_eq!(neither["error"], "send requires either to or group");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_delivery_store_ack_and_resume_keep_positions_and_other_kinds_unchanged() {
    let dir = TestDir::new("delivery");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, store_dir) = spawn_gateway(&dir.0, &daemon.address).await;
    let store = store_path(&store_dir, ADDRESS);
    let mut client = Client::open(port, &format!("id={ADDRESS}&groups=1")).await;
    client.ready().await;
    let (_, mut writer) = daemon.session().await;

    deliver(
        &mut writer,
        json!({
            "type":"deliver", "mail":"m-g1", "from":"author-1", "to":ADDRESS,
            "group":"staff", "article":4, "body":"first", "kind":"group",
            "headers":{"x-note":"a"}
        }),
    )
    .await;
    let (_, raw) = client.event().await;
    assert_eq!(
        raw,
        r#"{"type":"mail","mail":"m-g1","from":"author-1","group":"staff","article":4,"body":"first","record":"r-1","kind":"group","headers":{"x-note":"a"}}"#
    );
    let entries = fs::read_to_string(&store).unwrap();
    assert_eq!(
        entries.lines().next().unwrap(),
        r#"{"record":"r-1","mail":"m-g1","from":"author-1","group":"staff","article":4,"body":"first","kind":"group","headers":{"x-note":"a"}}"#
    );
    assert_eq!(
        daemon.line().await,
        json!({"type":"ack", "mail":"m-g1", "record":"r-1"})
    );

    for (mail, kind, body, record, req) in [
        ("m-d1", "directed", "direct", "r-2", 2),
        ("m-b1", "broadcast", "broadcast", "r-3", 3),
    ] {
        deliver(
            &mut writer,
            json!({
                "type":"deliver", "mail":mail, "from":"author-1", "to":ADDRESS,
                "body":body, "kind":kind, "headers":{}
            }),
        )
        .await;
        let (_, raw) = client.event().await;
        assert_eq!(
            raw,
            format!(
                r#"{{"type":"mail","mail":"{mail}","from":"author-1","body":"{body}","record":"{record}","kind":"{kind}","headers":{{}}}}"#
            )
        );
        let ack = daemon.line().await;
        assert_eq!(ack["record"], record);
        assert_eq!(ack["req"], req);
    }

    deliver(
        &mut writer,
        json!({
            "type":"deliver", "mail":"m-g2", "from":"author-2", "to":ADDRESS,
            "group":"staff", "article":5, "body":"second", "kind":"group", "headers":{}
        }),
    )
    .await;
    let (mail, _) = client.event().await;
    assert_eq!(mail["record"], "r-4");
    assert_eq!(mail["group"], "staff");
    assert_eq!(mail["article"], 5);
    assert_eq!(daemon.line().await["record"], "r-4");
    client.close().await;

    let mut client = Client::open(port, &format!("id={ADDRESS}&groups=1&after=r-2")).await;
    client.ready().await;
    let (_, mut writer) = daemon.session().await;
    let (broadcast, _) = client.event().await;
    assert_eq!(broadcast["record"], "r-3");
    assert!(broadcast.get("group").is_none());
    assert!(broadcast.get("article").is_none());
    let (_, raw) = client.event().await;
    assert_eq!(
        raw,
        r#"{"type":"mail","mail":"m-g2","from":"author-2","group":"staff","article":5,"body":"second","record":"r-4","kind":"group","headers":{}}"#
    );

    // A redelivery of replayed Mail is acked with its old record and not sent twice.
    deliver(
        &mut writer,
        json!({
            "type":"deliver", "mail":"m-g2", "from":"author-2", "to":ADDRESS,
            "group":"staff", "article":5, "body":"second", "kind":"group", "headers":{}
        }),
    )
    .await;
    deliver(
        &mut writer,
        json!({
            "type":"deliver", "mail":"m-g3", "from":"author-3", "to":ADDRESS,
            "group":"staff", "article":6, "body":"third", "kind":"group", "headers":{}
        }),
    )
    .await;
    let (live, _) = client.event().await;
    assert_eq!(live["mail"], "m-g3");
    assert_eq!(live["record"], "r-5");
    assert_eq!(live["group"], "staff");
    assert_eq!(live["article"], 6);
    assert_eq!(daemon.line().await["record"], "r-4");
    assert_eq!(daemon.line().await["record"], "r-5");

    let entries = fs::read_to_string(&store).unwrap();
    assert_eq!(entries.lines().count(), 5, "{entries}");
    assert_eq!(entries.matches(r#""mail":"m-g2""#).count(), 1);
    assert!(entries.lines().all(|line| {
        let entry: Value = serde_json::from_str(line).unwrap();
        if entry["kind"] == "group" {
            entry.get("group").is_some() && entry.get("article").is_some()
        } else {
            entry.get("group").is_none() && entry.get("article").is_none()
        }
    }));
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_send_relays_expected_article_and_surfaces_a_lost_race() {
    let dir = TestDir::new("expected");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    let mut client = Client::open(port, &format!("id={ADDRESS}&groups=1")).await;
    client.ready().await;
    let _session = daemon.session().await;

    let reply = client
        .call(json!({
            "type":"send", "group":"staff", "mail":"m-cas", "body":"next",
            "expected_article":7
        }))
        .await;
    assert_eq!(
        reply,
        json!({"type":"accepted", "mail":"m-cas", "group":"staff", "article":7})
    );
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"send", "mail":"m-cas", "from":ADDRESS, "group":"staff",
            "body":"next", "expected_article":7
        })
    );

    let reply = client
        .call(json!({
            "type":"send", "group":"staff", "mail":"m-late", "body":"late",
            "expected_article":3
        }))
        .await;
    assert_eq!(reply["type"], "error", "{reply}");
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("expected_article 3 but the Group is at 7"),
        "{reply}"
    );
    assert_eq!(daemon.request().await["expected_article"], 3);

    // Refused before reaching the host daemon.
    for bad in [json!(-1), json!("7"), json!(1.5)] {
        let reply = client
            .call(json!({"type":"send", "group":"staff", "body":"x", "expected_article":bad}))
            .await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert!(
            reply["error"]
                .as_str()
                .unwrap()
                .contains("expected_article"),
            "{reply}"
        );
    }

    // A later request proves no refused send reached the daemon.
    client.call(json!({"type":"groups"})).await;
    assert_eq!(daemon.request().await["type"], "groups");
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn groups_articles_and_group_policy_relay_their_fields() {
    let dir = TestDir::new("listing");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    let mut client = Client::open(port, &format!("id={ADDRESS}&groups=1")).await;
    client.ready().await;
    let _session = daemon.session().await;

    let reply = client
        .call(json!({"type":"groups", "prefix":"st", "include_retired":true}))
        .await;
    assert_eq!(
        reply,
        json!({
            "type":"groups", "groups":[{
                "group":"st-old", "high_water":2, "subscribers":0,
                "subscribed":false, "retired":true
            }]
        })
    );
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"groups", "session":ADDRESS, "prefix":"st", "include_retired":true
        })
    );

    let reply = client
        .call(json!({"type":"articles", "group":"staff", "after":2, "limit":5}))
        .await;
    assert_eq!(
        reply,
        json!({
            "type":"articles", "group":"staff", "high_water":12,
            "articles":[{"article":3, "mail":"m-3", "from":"author-1", "body":"three", "headers":{}}]
        })
    );
    let request = daemon.request().await;
    assert_eq!(request["type"], "articles");
    assert_eq!(request["group"], "staff");
    assert_eq!(request["after"], 2);
    assert_eq!(request["limit"], 5);
    assert_ne!(request["follow"], true, "a request socket never follows");

    let reply = client
        .call(json!({
            "type":"group_policy", "group":"staff", "keep_last":10,
            "ttl_ms":null, "retired":true
        }))
        .await;
    assert_eq!(
        reply,
        json!({
            "type":"group_policy", "group":"staff", "keep_last":10,
            "ttl_ms":null, "retired":true
        })
    );
    assert_eq!(
        daemon.request().await,
        json!({
            "type":"group_policy", "group":"staff", "keep_last":10,
            "ttl_ms":null, "retired":true
        })
    );
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_is_available_with_and_without_groups() {
    let dir = TestDir::new("receipt");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    for query in [format!("id={ADDRESS}"), format!("id={ADDRESS}&groups=1")] {
        let mut client = Client::open(port, &query).await;
        client.ready().await;
        let _session = daemon.session().await;
        // Two requests in a row: req rises on a persistent request socket.
        for _ in 0..2 {
            let reply = client
                .call(json!({"type":"receipt", "mail":"m-direct"}))
                .await;
            assert_eq!(
                reply,
                json!({
                    "type":"receipt", "mail":"m-direct", "to":"reader-2", "kind":"directed",
                    "accepted_at_ms":100, "record":"r-9", "acked_at_ms":150
                })
            );
            assert_eq!(
                daemon.request().await,
                json!({"type":"receipt", "mail":"m-direct"})
            );
        }
        let reply = client.call(json!({"type":"receipt"})).await;
        assert_eq!(reply["type"], "error", "{reply}");
        client.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_registers_for_and_relays_committed_board_events() {
    let dir = TestDir::new("watch");
    let mut daemon = MockDaemon::start().await;
    let (_gateway, port, _) = spawn_gateway(&dir.0, &daemon.address).await;
    let event = json!({
        "type":"board_event", "event":"register", "session":"peer-1",
        "host":"host-a", "at_ms":42
    });

    let mut client = Client::open(port, &format!("id={ADDRESS}&watch=1")).await;
    client.ready().await;
    let (register, mut writer) = daemon.session().await;
    assert_eq!(register["watch"], true, "{register}");
    deliver(&mut writer, event.clone()).await;
    let (frame, _) = client.frame().await;
    assert_eq!(frame, event);
    client.close().await;

    // Without watch=1 the Register asks for nothing and no board_event reaches the client.
    let mut client = Client::open(port, &format!("id={ADDRESS}")).await;
    client.ready().await;
    let (register, mut writer) = daemon.session().await;
    assert!(register.get("watch").is_none(), "{register}");
    deliver(&mut writer, event).await;
    deliver(
        &mut writer,
        json!({
            "type":"deliver", "mail":"m-after", "from":"author-1", "to":ADDRESS,
            "body":"after", "kind":"directed", "headers":{}
        }),
    )
    .await;
    let (mail, _) = client.event().await;
    assert_eq!(mail["mail"], "m-after");
    client.close().await;
}

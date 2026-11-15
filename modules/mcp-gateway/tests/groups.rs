use roger_mcp_gateway::{
    cli::DaemonSocket,
    durlog::read_store_entries,
    mcp,
    session::{InitParams, SessionManager},
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
#[test]
fn group_tool_isolation() {
    let base = mcp::tools_list();
    assert_eq!(base["tools"].as_array().unwrap().len(), 9);
    let opt = mcp::tools_list_groups(true);
    assert_eq!(opt["tools"].as_array().unwrap().len(), 12);
    assert_eq!(opt["tools"][9]["name"], "roger_subscribe");
    assert_eq!(
        base["tools"][0]["inputSchema"]["required"],
        json!(["to", "body"])
    );
    assert!(opt["tools"][0]["inputSchema"]["properties"]["group"].is_object());
}
#[tokio::test]
async fn group_delivery_store_ack_and_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dir = std::env::temp_dir().join(format!("gateway-groups-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let manager = SessionManager::new(
        DaemonSocket::Tcp(listener.local_addr().unwrap().to_string()),
        dir.clone(),
        None,
    );
    let store = roger_mcp_gateway::durlog::store_path(&dir, "reader");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut lines = BufReader::new(r).lines();
        let _: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        w.write_all(b"{\"type\":\"deliver\",\"mail\":\"m-group\",\"from\":\"sender\",\"to\":\"reader\",\"group\":\"news\",\"article\":7,\"body\":\"hello\",\"kind\":\"group\"}\n").await.unwrap();
        let ack: Value = serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(ack["type"], "ack");
        assert_eq!(read_store_entries(&store).unwrap()[0]["article"], 7);
        ack
    });
    let session = manager
        .initialize(InitParams {
            address: Some("reader".into()),
            label: None,
            declared: None,
            watch: false,
            groups: true,
        })
        .await
        .unwrap();
    let ack = server.await.unwrap();
    let entries = read_store_entries(&session.store_path).unwrap();
    assert_eq!(entries[0]["record"], ack["record"]);
    assert_eq!(entries[0]["group"], "news");
    assert_eq!(entries[0]["article"], 7);
    let (items, _) = session
        .inbox
        .read(10, std::time::Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(items[0]["article"], 7);
    let seq = {
        let log = session.event_log.lock().unwrap();
        let payload: Value = serde_json::from_str(&log.events[0].1).unwrap();
        assert_eq!(payload["method"], "notifications/roger/mail");
        assert_eq!(payload["params"]["group"], "news");
        log.events[0].0
    };
    assert_eq!(session.initial_sse_cursor(Some(seq)), seq);
    use http_body_util::BodyExt;
    let mut live = roger_mcp_gateway::http::SseBody::new(session.clone(), 0);
    let bytes = live.frame().await.unwrap().unwrap().into_data().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("notifications/roger/mail") && text.contains(r#""article":7"#));
    let mut recovered =
        roger_mcp_gateway::http::SseBody::new(session.clone(), session.initial_sse_cursor(Some(0)));
    assert_eq!(
        recovered
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap(),
        bytes
    );
    let mut caught_up = roger_mcp_gateway::http::SseBody::new(
        session.clone(),
        session.initial_sse_cursor(Some(seq)),
    );
    assert!(caught_up.frame().await.is_none());
}

#[tokio::test]
async fn group_request_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dir = std::env::temp_dir().join(format!("gateway-frames-{}", std::process::id()));
    let manager = SessionManager::new(
        DaemonSocket::Tcp(listener.local_addr().unwrap().to_string()),
        dir,
        None,
    );
    let server = tokio::spawn(async move {
        let (registered, _) = listener.accept().await.unwrap();
        let mut registration = BufReader::new(registered).lines();
        registration.next_line().await.unwrap();
        for (expected, response) in [
            (
                json!({"type":"subscribe","req":1,"session":"reader","group":"news","after":0,"latest":false}),
                json!({"type":"subscribed","session":"reader","group":"news","position":0,"high_water":7}),
            ),
            (
                json!({"type":"unsubscribe","req":1,"session":"reader","group":"news"}),
                json!({"type":"unsubscribed","session":"reader","group":"news","position":0}),
            ),
            (
                json!({"type":"groups","req":1,"session":"reader"}),
                json!({"type":"groups","groups":[]}),
            ),
            (
                json!({"type":"send","req":1,"from":"reader","group":"news","mail":"posted","body":"text","headers":{"x":"y"}}),
                json!({"type":"accepted","mail":"posted","group":"news","article":8}),
            ),
        ] {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, mut w) = stream.into_split();
            let line = BufReader::new(r)
                .lines()
                .next_line()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), expected);
            w.write_all(format!("{}\n", response).as_bytes())
                .await
                .unwrap();
        }
    });
    let session = manager
        .initialize(InitParams {
            address: Some("reader".into()),
            label: None,
            declared: None,
            watch: false,
            groups: true,
        })
        .await
        .unwrap();
    for args in [json!({}), json!({"to":"a","group":"news"})] {
        let error = mcp::call_tool(&session, &json!({"name":"roger_send","arguments":args}))
            .await
            .unwrap_err();
        assert!(error.contains("expected") && error.contains("saw"));
    }
    for args in [
        json!({"group":""}),
        json!({"group":"news","after":-1}),
        json!({"group":"news","latest":1}),
    ] {
        assert!(mcp::call_tool(
            &session,
            &json!({"name":"roger_subscribe","arguments":args})
        )
        .await
        .is_err());
    }
    let sub = mcp::call_tool(
        &session,
        &json!({"name":"roger_subscribe","arguments":{"group":"news","after":0,"latest":false}}),
    )
    .await
    .unwrap();
    assert_eq!(
        sub,
        json!({"session":"reader","group":"news","position":0,"high_water":7})
    );
    let unsub = mcp::call_tool(
        &session,
        &json!({"name":"roger_unsubscribe","arguments":{"group":"news"}}),
    )
    .await
    .unwrap();
    assert_eq!(unsub["position"], 0);
    assert_eq!(
        mcp::call_tool(&session, &json!({"name":"roger_groups"}))
            .await
            .unwrap(),
        json!({"groups":[]})
    );
    assert_eq!(mcp::call_tool(&session,&json!({"name":"roger_send","arguments":{"group":"news","mail":"posted","body":"text","headers":{"x":"y"}}})).await.unwrap(),json!({"mail":"posted","group":"news","article":8}));
    server.await.unwrap();
}

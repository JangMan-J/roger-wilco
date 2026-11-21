use roger_mcp_gateway::{
    cli::DaemonSocket,
    durlog::read_store_entries,
    mcp,
    session::{RegisterParams, SessionManager},
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

#[test]
fn group_tool_isolation() {
    let base = mcp::tools_list(false);
    let base_tools = base["tools"].as_array().unwrap();
    assert_eq!(base_tools.len(), 11);
    let opt = mcp::tools_list(true);
    let opt_tools = opt["tools"].as_array().unwrap();
    assert_eq!(opt_tools.len(), 16);
    let schema = |name: &str| {
        assert!(!base_tools.iter().any(|tool| tool["name"] == name));
        &opt_tools.iter().find(|tool| tool["name"] == name).unwrap()["inputSchema"]
    };
    let articles = schema("roger_articles");
    assert_eq!(articles["required"], json!(["session", "group"]));
    assert_eq!(articles["properties"]["after"]["minimum"], 0);
    assert_eq!(articles["properties"]["limit"]["minimum"], 1);
    assert_eq!(articles["properties"]["limit"]["maximum"], 500);
    let policy = schema("roger_group_policy");
    assert_eq!(policy["required"], json!(["session", "group"]));
    for field in ["keep_last", "ttl_ms"] {
        assert!(policy["properties"][field].is_object());
    }
    assert_eq!(policy["properties"]["retired"]["type"], "boolean");
    assert_eq!(opt_tools[11]["name"], "roger_subscribe");
    // tools[2] is roger_send
    assert_eq!(
        base_tools[2]["inputSchema"]["required"],
        json!(["session", "to", "body"])
    );
    assert!(opt_tools[2]["inputSchema"]["properties"]["group"].is_object());
}

#[tokio::test]
async fn group_delivery_store_ack_and_events_poll_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dir = std::env::temp_dir().join(format!("gateway-groups-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let manager = SessionManager::new(
        DaemonSocket::Tcp(listener.local_addr().unwrap().to_string()),
        dir.clone(),
        true,
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
        let entries = read_store_entries(&store).unwrap();
        assert_eq!(entries[0]["article"], 7);
        assert!(entries[0]["stored_at_ms"].is_i64());
        ack
    });
    let session = manager
        .register(RegisterParams {
            address: Some("reader".into()),
            label: None,
            declared: None,
            watch: false,
        })
        .await
        .unwrap();
    let ack = server.await.unwrap();
    let entries = read_store_entries(&session.store_path).unwrap();
    assert_eq!(entries[0]["record"], ack["record"]);
    assert_eq!(entries[0]["group"], "news");
    assert_eq!(entries[0]["article"], 7);

    // Poll SEP-3415 roger.mail without touching roger_inbox cursor.
    let polled = mcp::events_poll(
        &manager,
        &json!({
            "name": "roger.mail",
            "arguments": { "session": session.session_id, "group": "news" },
            "cursor": "r-0"
        }),
    )
    .await
    .unwrap();
    let evs = polled["events"].as_array().unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0]["eventId"], "m-group");
    assert_eq!(evs[0]["data"]["group"], "news");
    assert_eq!(evs[0]["data"]["article"], 7);
    assert_eq!(polled["cursor"], "r-1");

    // roger_inbox still sees the entry independently.
    let (items, _) = session
        .inbox
        .read(10, std::time::Duration::ZERO, true)
        .await
        .unwrap();
    assert_eq!(items[0]["article"], 7);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn group_request_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dir = std::env::temp_dir().join(format!("gateway-frames-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let manager = SessionManager::new(
        DaemonSocket::Tcp(listener.local_addr().unwrap().to_string()),
        dir.clone(),
        true,
        None,
    );
    let server = tokio::spawn(async move {
        let (registered, _) = listener.accept().await.unwrap();
        let mut registration = BufReader::new(registered).lines();
        registration.next_line().await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let (r, mut w) = stream.into_split();
        let mut requests = BufReader::new(r).lines();
        for (index, (mut expected, mut response)) in [
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
            (
                json!({"type":"send","req":1,"from":"reader","group":"news","mail":"stale-news","body":"no","expected_article":0}),
                json!({"type":"error","reason":"expected group news at article 0, saw 8"}),
            ),
            (
                json!({"type":"send","req":1,"from":"reader","group":"news","mail":"cas-news","body":"yes","expected_article":8}),
                json!({"type":"accepted","mail":"cas-news","group":"news","article":9}),
            ),
            (
                json!({"type":"groups","req":1,"session":"reader","prefix":"ne","include_retired":true}),
                json!({"type":"groups","groups":[]}),
            ),
            (
                json!({"type":"articles","group":"news","after":7,"limit":1}),
                json!({"type":"articles","group":"news","high_water":9,"retired":false,"articles":[{"article":8,"mail":"posted","from":"reader","body":"text","headers":{"x":"y"}}]}),
            ),
            (
                json!({"type":"group_policy","group":"news","keep_last":3,"retired":false}),
                json!({"type":"group_policy","group":"news","high_water":9,"subscribers":0,"retired":false,"keep_last":3}),
            ),
            (
                json!({"type":"group_policy","group":"news","keep_last":null,"ttl_ms":0}),
                json!({"type":"group_policy","group":"news","high_water":9,"subscribers":0,"retired":false,"ttl_ms":0}),
            ),
        ].into_iter().enumerate() {
            expected["req"] = json!(index + 1);
            response["req"] = expected["req"].clone();
            let line = requests.next_line().await.unwrap().unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), expected);
            w.write_all(format!("{}\n", response).as_bytes())
                .await
                .unwrap();
        }
    });
    let session = manager
        .register(RegisterParams {
            address: Some("reader".into()),
            label: None,
            declared: None,
            watch: false,
        })
        .await
        .unwrap();
    let sid = session.session_id.clone();

    for args in [
        json!({"session": sid}),
        json!({"session": sid, "to":"a","group":"news"}),
    ] {
        let error = mcp::call_tool(&manager, &json!({"name":"roger_send","arguments":args}))
            .await
            .unwrap_err();
        assert!(error.contains("expected") && error.contains("saw"));
    }
    for args in [
        json!({"session": sid, "group":""}),
        json!({"session": sid, "group":"news","after":-1}),
        json!({"session": sid, "group":"news","latest":1}),
    ] {
        assert!(mcp::call_tool(
            &manager,
            &json!({"name":"roger_subscribe","arguments":args})
        )
        .await
        .is_err());
    }
    let sub = mcp::call_tool(
        &manager,
        &json!({"name":"roger_subscribe","arguments":{"session": sid, "group":"news","after":0,"latest":false}}),
    )
    .await
    .unwrap();
    assert_eq!(
        sub,
        json!({"session":"reader","group":"news","position":0,"high_water":7})
    );
    let unsub = mcp::call_tool(
        &manager,
        &json!({"name":"roger_unsubscribe","arguments":{"session": sid, "group":"news"}}),
    )
    .await
    .unwrap();
    assert_eq!(unsub["position"], 0);
    assert_eq!(
        mcp::call_tool(
            &manager,
            &json!({"name":"roger_groups","arguments":{"session": sid}})
        )
        .await
        .unwrap(),
        json!({"groups":[]})
    );
    assert_eq!(
        mcp::call_tool(
            &manager,
            &json!({"name":"roger_send","arguments":{"session": sid, "group":"news","mail":"posted","body":"text","headers":{"x":"y"}}})
        )
        .await
        .unwrap(),
        json!({"mail":"posted","group":"news","article":8})
    );
    // expected_article CAS + prefix/include_retired + groupPrefix filter
    assert!(mcp::call_tool(
        &manager,
        &json!({"name":"roger_send","arguments":{"session": sid, "group":"news","mail":"stale-news","body":"no","expected_article": 0}})
    )
    .await
    .is_err());
    assert_eq!(
        mcp::call_tool(
            &manager,
            &json!({"name":"roger_send","arguments":{"session": sid, "group":"news","mail":"cas-news","body":"yes","expected_article": 8}})
        )
        .await
        .unwrap(),
        json!({"mail":"cas-news","group":"news","article":9})
    );
    let filtered_groups = mcp::call_tool(
        &manager,
        &json!({"name":"roger_groups","arguments":{"session": sid, "prefix":"ne", "include_retired": true}}),
    )
    .await
    .unwrap();
    assert_eq!(filtered_groups, json!({"groups":[]}));
    for (tool, args, result) in [
        (
            "roger_articles",
            json!({"session": sid,"group":"news","after":7,"limit":1}),
            json!({"group":"news","high_water":9,"retired":false,"articles":[{"article":8,"mail":"posted","from":"reader","body":"text","headers":{"x":"y"}}]}),
        ),
        (
            "roger_group_policy",
            json!({"session": sid,"group":"news","keep_last":3,"retired":false}),
            json!({"group":"news","high_water":9,"subscribers":0,"retired":false,"keep_last":3}),
        ),
        (
            "roger_group_policy",
            json!({"session": sid,"group":"news","keep_last":null,"ttl_ms":0}),
            json!({"group":"news","high_water":9,"subscribers":0,"retired":false,"ttl_ms":0}),
        ),
    ] {
        assert_eq!(
            mcp::call_tool(&manager, &json!({"name":tool,"arguments":args}))
                .await
                .unwrap(),
            result
        );
    }
    let prefix_req = mcp::validate_event_req(
        &manager,
        &json!({"name":"roger.mail","cursor":"r-0","arguments":{"session": sid, "groupPrefix":"ne"}}),
        true,
    )
    .unwrap();
    assert!(prefix_req
        .filter
        .matches(&json!({"group":"news","kind":"group"})));
    assert!(!prefix_req
        .filter
        .matches(&json!({"group":"ops","kind":"group"})));
    server.await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

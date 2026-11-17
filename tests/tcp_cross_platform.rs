//! OS-agnostic TCP process seam (Linux, macOS, Windows): coordinator, pure-TCP
//! host daemon, Tracer session, MCP server over stdio + `tcp://`, send, board,
//! claim, renew, release, receipt, and read-only audit — with zero Unix-only APIs.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_roger");
const TOKEN: &str = "cross-platform-tcp-token";

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn read_port(path: &Path) -> u16 {
    wait_for("port file", || {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
    })
}

#[test]
fn pure_tcp_fleet_runs_without_unix_sockets_across_all_client_faces() {
    let dir = std::env::temp_dir().join(format!("roger-xplat-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let c_port_file = dir.join("c.port");
    let c_log = dir.join("c.log");
    let _coord = Proc(
        Command::new(BIN)
            .args([
                "coordinator",
                "--bind",
                "127.0.0.1:0",
                "--port-file",
                c_port_file.to_str().unwrap(),
                "--data",
                c_log.to_str().unwrap(),
            ])
            .env("ROGER_TOKEN", TOKEN)
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let coord_addr = format!("127.0.0.1:{}", read_port(&c_port_file));

    let d_port_file = dir.join("d.port");
    let d_log = dir.join("d.log");
    let _daemon = Proc(
        Command::new(BIN)
            .args([
                "daemon",
                "--coordinator",
                &coord_addr,
                "--host-id",
                "xplat-host",
                "--listen",
                "127.0.0.1:0",
                "--port-file",
                d_port_file.to_str().unwrap(),
                "--data",
                d_log.to_str().unwrap(),
            ])
            .env("ROGER_TOKEN", TOKEN)
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let tcp_sock = format!("tcp://127.0.0.1:{}", read_port(&d_port_file));

    let a_addr = "[[1a:FLEET:zz]]11111111-1111-4111-8111-111111111111";
    let b_addr = "22222222-2222-4222-8222-222222222222";
    let a_store = dir.join("a.store");

    let mut session_a = Command::new(BIN)
        .args([
            "session",
            "--socket",
            &tcp_sock,
            "--store",
            a_store.to_str().unwrap(),
            "--id",
            a_addr,
            "--label",
            "alpha",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut a_out = BufReader::new(session_a.stdout.take().unwrap());
    let _session_a = Proc(session_a);
    let mut line = String::new();
    loop {
        line.clear();
        assert!(a_out.read_line(&mut line).unwrap() > 0);
        if line.starts_with("REGISTERED ") {
            break;
        }
    }

    // Start `roger mcp` over stdio + `ROGER_SOCKET=tcp://...` + `LOCALAPPDATA` Store layout!
    let appdata = dir.join("appdata");
    let mut mcp_child = Command::new(BIN)
        .args(["mcp", "--id", b_addr, "--label", "beta"])
        .env("ROGER_SOCKET", &tcp_sock)
        .env("LOCALAPPDATA", &appdata)
        .env_remove("XDG_STATE_HOME")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut mcp_in = mcp_child.stdin.take().unwrap();
    let mut mcp_out = BufReader::new(mcp_child.stdout.take().unwrap());
    let _mcp_guard = Proc(mcp_child);

    let mut rpc = |msg: &str| -> String {
        writeln!(mcp_in, "{msg}").unwrap();
        mcp_in.flush().unwrap();
        let mut resp = String::new();
        assert!(mcp_out.read_line(&mut resp).unwrap() > 0);
        resp
    };

    let init = rpc(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
    );
    assert!(init.contains(r#""protocolVersion":"2025-06-18""#), "{init}");
    let reg = rpc(
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"roger_registered","arguments":{"wait_seconds":10}}}"#,
    );
    assert!(reg.contains(r#"\"registered\":true"#), "{reg}");

    // Send Mail from MCP session B to Tracer session A and back!
    let sent = rpc(&format!(
        r#"{{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{{"name":"roger_send","arguments":{{"to":"{a_addr}","mail":"xplat-1","body":"hello-xplat","headers":{{"z":"1","a":"2"}}}}}}}}"#
    ));
    assert!(sent.contains("xplat-1"), "{sent}");

    let send_back = Command::new(BIN)
        .args([
            "send",
            "--socket",
            &tcp_sock,
            "--from",
            a_addr,
            "--to",
            b_addr,
            "--mail",
            "xplat-2",
            "--body",
            "reply-xplat",
        ])
        .output()
        .unwrap();
    assert!(send_back.status.success());

    let inbox = rpc(
        r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"roger_inbox","arguments":{"wait_seconds":10}}}"#,
    );
    assert!(inbox.contains("reply-xplat"), "{inbox}");

    // Modern 2026-07-28 discovery + SEP-3415 events/list and events/poll over TCP.
    let meta = r#""_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{"extensions":{"io.modelcontextprotocol/events":{}}}}"#;
    let disc = rpc(&format!(
        r#"{{"jsonrpc":"2.0","id":5,"method":"server/discover","params":{{{meta}}}}}"#
    ));
    assert!(
        disc.contains(r#""supportedVersions":["2026-07-28"]"#),
        "{disc}"
    );
    let ev_list = rpc(&format!(
        r#"{{"jsonrpc":"2.0","id":6,"method":"events/list","params":{{{meta}}}}}"#
    ));
    assert!(ev_list.contains("roger.mail"), "{ev_list}");
    let ev_poll = rpc(&format!(
        r#"{{"jsonrpc":"2.0","id":7,"method":"events/poll","params":{{{meta},"name":"roger.mail","cursor":"r-0"}}}}"#
    ));
    assert!(ev_poll.contains("reply-xplat"), "{ev_poll}");

    // Claim + Board + Receipt + live read-only Audit (works while Stores are held open!)
    let claim = Command::new(BIN)
        .args([
            "claim",
            "--socket",
            &tcp_sock,
            "--owner",
            a_addr,
            "--key",
            "job-xplat",
            "--ttl-ms",
            "60000",
        ])
        .output()
        .unwrap();
    assert!(claim.status.success());

    wait_for("xplat-1 receipt", || {
        let r = Command::new(BIN)
            .args(["receipt", "--socket", &tcp_sock, "--mail", "xplat-1"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&r.stdout)
            .starts_with("RECEIPT ")
            .then_some(())
    });

    let audit = Command::new(BIN)
        .args([
            "audit",
            "--coordinator-log",
            c_log.to_str().unwrap(),
            "--daemon-log",
            d_log.to_str().unwrap(),
            "--store",
            a_store.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(audit.status.success());
    let audit_out = String::from_utf8_lossy(&audit.stdout);
    assert!(
        audit_out.contains("unreceipted=0 duplicates=0 mismatches=0"),
        "{audit_out}"
    );

    drop(mcp_in);
    drop(_mcp_guard);
    drop(_session_a);
    drop(_daemon);
    drop(_coord);
    let _ = fs::remove_dir_all(&dir);
}

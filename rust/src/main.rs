//! Roger-Wilco Rust client CLI. Written from `docs/contract.md`.
//!
//! Environment: RW_COORDINATOR_URL, RW_CREDENTIAL, RW_INBOX_SOCK,
//! RW_NAMESPACE, RW_SESSION_ID. The sender is always the bound session.

use roger_wilco_rs::{Coordinator, Ipc};
use serde_json::{json, Value};
use std::env;
use std::process::ExitCode;

fn env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} is required"))
}

fn session() -> Result<Value, String> {
    Ok(json!({ "namespace": env("RW_NAMESPACE")?, "sessionId": env("RW_SESSION_ID")? }))
}

fn ipc() -> Result<Ipc, String> {
    Ipc::connect(&env("RW_INBOX_SOCK")?)
}

fn coordinator() -> Result<Coordinator, String> {
    Ok(Coordinator::new(&env("RW_COORDINATOR_URL")?, &env("RW_CREDENTIAL")?))
}

fn print(value: &Value) {
    println!("{}", serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()));
}

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let command = args.first().map(String::as_str).ok_or(
        "usage: roger-wilco-rs <register|send|board|inbox|claim|release|bind|pending|ack> [args]")?;
    let me = session()?;
    match command {
        // Registration goes through the daemon socket, like every adapter.
        "register" => {
            if args.len() > 1 {
                return Err("register takes no name; a call sign is an entity, for example [[0:FLEET]] in a message".to_string());
            }
            let mut client = ipc()?;
            print(&client.invoke("awaitRegistration", json!({ "registration": { "session": me } }))?);
        }
        // Tool calls go to the coordinator, as the bound session.
        "send" => {
            let recipient = args.get(1).ok_or("usage: send <recipient|*> <body>")?;
            let body = args[2..].join(" ");
            if body.is_empty() { return Err("usage: send <recipient|*> <body>".to_string()); }
            print(&coordinator()?.call("POST", "/v1/messages", Some(&json!({
                "sender": me, "message": { "recipient": recipient, "body": body }
            })))?);
        }
        "board" => {
            let http = coordinator()?;
            let sessions = http.call("GET", "/v1/sessions", None)?;
            let claims = http.call("GET", "/v1/claims", None)?;
            print(&json!({ "sessions": sessions, "claims": claims }));
        }
        "inbox" => {
            let namespace = env("RW_NAMESPACE")?;
            let session_id = env("RW_SESSION_ID")?;
            print(&coordinator()?.call("GET",
                &format!("/v1/inbox?namespace={namespace}&sessionId={session_id}&since=0&limit=100"), None)?);
        }
        "claim" => {
            let key = args.get(1).ok_or("usage: claim <key>")?;
            print(&coordinator()?.call("POST", "/v1/claims", Some(&json!({ "owner": me, "key": key })))?);
        }
        "release" => {
            let key = args.get(1).ok_or("usage: release <key>")?;
            print(&coordinator()?.call("POST", "/v1/claims/release", Some(&json!({ "owner": me, "key": key })))?);
        }
        // Mailbox operations stay on the socket.
        "bind" => {
            let runtime = args.get(1).ok_or("usage: bind <runtimeId>")?;
            let mut client = ipc()?;
            print(&client.invoke("bind", json!({ "session": me, "runtimeId": runtime }))?);
        }
        "pending" => {
            let token = args.get(1).ok_or("usage: pending <token>")?;
            let mut client = ipc()?;
            print(&client.invoke("pending", json!({ "token": token }))?);
        }
        _ => return Err(format!("unknown command: {command}")),
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

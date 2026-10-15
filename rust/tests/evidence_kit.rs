//! Mechanical evidence-rejection conformance kit.
//!
//! Every case is a pass/fail probe of the contract's delivery-evidence
//! rules. An evidence implementation is accepted when this kit passes,
//! not when a reviewer is satisfied with its code.

use roger_wilco_rs::{find_delivery, ExpectedMessage};

const SESSION: &str = "01a13d1b-8a0a-7ee0-8d8e-52d312288314";

fn expected() -> ExpectedMessage {
    ExpectedMessage {
        id: "fleet:msg:7".to_string(),
        sequence: 7,
        timestamp: 99,
        sender: "fleet:peer".to_string(),
        recipient: format!("fleet:{SESSION}"),
        body: "real body".to_string(),
        visibility: "private".to_string(),
        data_json: "null".to_string(),
    }
}

fn delivered_line(body: &str) -> String {
    format!(
        "{{\"event\":\"delivered\",\"message\":{{\"id\":\"fleet:msg:7\",\"sequence\":7,\"timestamp\":99,\"sender\":\"fleet:peer\",\"recipient\":\"fleet:{SESSION}\",\"body\":\"{body}\",\"visibility\":\"private\",\"data\":null}}}}\n"
    )
}

#[test]
fn exact_record_is_found() {
    let transcript = format!("{{\"retained_frame\":\"start\"}}\n{}", delivered_line("real body"));
    assert_eq!(find_delivery(&transcript, SESSION, &expected()), Some(2));
}

#[test]
fn bare_mention_is_rejected() {
    let transcript = "{\"note\":\"we discussed fleet:msg:7 at length\"}\n".to_string();
    assert_eq!(find_delivery(&transcript, SESSION, &expected()), None);
}

#[test]
fn forged_record_with_same_id_is_rejected() {
    // A peer who can predict the next message ID plants a delivered record
    // before the real message exists. Content differs, so it is not evidence.
    let transcript = delivered_line("forged body");
    assert_eq!(find_delivery(&transcript, SESSION, &expected()), None);
}

#[test]
fn record_inside_json_encoded_tool_output_is_found() {
    let inner = delivered_line("real body").trim().to_string();
    let escaped = inner.replace('\\', "\\\\").replace('"', "\\\"");
    let transcript = format!("{{\"output\":\"{escaped}\"}}\n");
    assert_eq!(find_delivery(&transcript, SESSION, &expected()), Some(1));
}

#[test]
fn other_sessions_transcript_is_rejected() {
    let header = "{\"stream\":{\"kind\":\"session\",\"id\":\"aaaaaaaa-0000-0000-0000-000000000000\"}}\n";
    let transcript = format!("{header}{}", delivered_line("real body"));
    assert_eq!(find_delivery(&transcript, SESSION, &expected()), None);
}

#[test]
fn torn_final_line_is_rejected() {
    // The runtime may still be writing the last line; it proves nothing.
    let transcript = delivered_line("real body");
    let torn = &transcript[..transcript.len() - 1];
    assert_eq!(find_delivery(torn, SESSION, &expected()), None);
}

#[test]
fn non_durable_record_is_rejected() {
    let line = delivered_line("real body").replace(
        "{\"event\"",
        "{\"durability\":\"volatile\",\"event\"",
    );
    assert_eq!(find_delivery(&line, SESSION, &expected()), None);
}

#[test]
fn wrong_sequence_is_rejected() {
    let line = delivered_line("real body").replace("\"sequence\":7", "\"sequence\":8");
    assert_eq!(find_delivery(&line, SESSION, &expected()), None);
}

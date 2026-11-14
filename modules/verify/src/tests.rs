//! Each failure class, over hand-built audit JSON lines.

use crate::canon;
use crate::roster::{self, Roster};
use crate::verify::*;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{json, Value};

const A: &str = "[[1:LEAD:0]]aaaa-1";
const B: &str = "[[3:BUILDER:1]]bbbb-2";
const C: &str = "cccc-3";

fn sk(n: u8) -> SigningKey {
    SigningKey::from_bytes(&[n; 32])
}

fn pk(n: u8) -> String {
    STANDARD.encode(sk(n).verifying_key().as_bytes())
}

/// Authority `auth` = key 1; A signs with `ka` = key 2; B with `kb` = key 3.
fn roster() -> Roster {
    roster::parse(
        &json!({
            "scheme": "roger-roster/1",
            "authorities": {"auth": pk(1)},
            "signers": {A: {"ka": pk(2)}, B: {"kb": pk(3)}},
        })
        .to_string(),
    )
    .unwrap()
}

fn declare_with(address: &str, nonce: &str, key: u8, kid: &str) -> String {
    let mut v = json!({
        "scheme": "roger-declare/1",
        "address": address,
        "facts": {"role": "builder", "parent": A},
        "nonce": nonce,
        "issued_at_ms": 1793545146038u64,
        "kid": kid,
    });
    let form = canon::declare_form(&v.to_string()).unwrap();
    let sig = STANDARD.encode(sk(key).sign(&form).to_bytes());
    v["sig"] = Value::from(sig);
    v.to_string()
}

fn declare(address: &str, nonce: &str) -> String {
    declare_with(address, nonce, 1, "auth")
}

fn decl_line(address: &str, text: &str, line: u64) -> Value {
    json!({"type":"DECLARED","address":address,"at_ms":1,"line":line,"declared":text})
}

#[allow(clippy::too_many_arguments)]
fn headers(key: u8, kid: &str, from: &str, to: &str, mail: &str, seq: u64, body: &str) -> Value {
    let bh = canon::sha256_hex(body.as_bytes());
    let form = canon::sig_form(from, to, mail, &seq.to_string(), "1700", &bh);
    json!({
        "sent_at_ms": "1700",
        "seq": seq.to_string(),
        "body_sha256": bh,
        "kid": kid,
        "sig": STANDARD.encode(sk(key).sign(&form).to_bytes()),
    })
}

fn mail_lines(
    id: &str,
    kind: &str,
    from: &str,
    to: &str,
    h: Option<Value>,
    body: &str,
) -> Vec<Value> {
    let mut out = vec![json!({
        "type":"MAIL","mail":id,"kind":kind,"from":from,"to":to,
        "accepted_at_ms":1,"receipted_at_ms":2,"latency_ms":1,"record":"r",
        "receipt_line":9,"body_bytes":body.len(),"line":5
    })];
    if let Some(h) = h {
        out.push(json!({"type":"HEADERS","mail":id,"headers":h}));
    }
    out.push(json!({"type":"BODY","mail":id,"body":body}));
    out
}

/// A directed Mail from A to B, signed by A's key.
fn signed(id: &str, seq: u64, body: &str) -> Vec<Value> {
    mail_lines(
        id,
        "directed",
        A,
        B,
        Some(headers(2, "ka", A, B, id, seq, body)),
        body,
    )
}

fn run(lines: &[Value]) -> (Vec<Report>, u8) {
    run_opts(lines, Options::default())
}

fn run_opts(lines: &[Value], opts: Options) -> (Vec<Report>, u8) {
    let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
    verify_text(&text, &roster(), opts).unwrap()
}

fn mails(r: &[Report]) -> Vec<&Report> {
    r.iter().filter(|x| x.word == "MAIL").collect()
}

fn decls(r: &[Report]) -> Vec<&Report> {
    r.iter().filter(|x| x.word == "DECLARATION").collect()
}

fn base() -> Vec<Value> {
    vec![
        decl_line(A, &declare(A, "n-a"), 1),
        decl_line(B, &declare(B, "n-b"), 2),
    ]
}

#[test]
fn signed_mail_and_declarations_verify() {
    let mut l = base();
    l.extend(signed("m1", 1, "hello"));
    l.extend(signed("m2", 2, "again"));
    let (r, status) = run(&l);
    assert_eq!(status, 0, "{r:?}");
    assert!(decls(&r)
        .iter()
        .all(|d| d.get("status") == Some(&json!("verified"))));
    assert_eq!(mails(&r).len(), 2);
    assert!(mails(&r).iter().all(|m| m.failures().is_empty()));
    assert_eq!(r.last().unwrap().word, "VERIFY");
}

#[test]
fn missing_signature_when_headers_absent_empty_or_without_sig() {
    let mut l = base();
    l.extend(mail_lines("m1", "directed", A, B, Some(Value::Null), "x"));
    l.extend(mail_lines("m2", "directed", A, B, None, "x"));
    l.extend(mail_lines(
        "m3",
        "directed",
        A,
        B,
        Some(json!({"seq":"1"})),
        "x",
    ));
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    for m in mails(&r) {
        assert_eq!(m.failures(), vec![MISSING_SIGNATURE]);
    }
}

#[test]
fn invalid_signature_when_any_signed_part_differs() {
    let mut l = base();
    // Signed for m1 but committed under m9: a replay under a new id.
    let h = headers(2, "ka", A, B, "m1", 1, "x");
    l.extend(mail_lines("m9", "directed", A, B, Some(h), "x"));
    // Signed by B's key under A's kid.
    let h = headers(3, "ka", A, B, "m2", 2, "x");
    l.extend(mail_lines("m2", "directed", A, B, Some(h), "x"));
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    for m in mails(&r) {
        assert_eq!(m.failures(), vec![INVALID_SIGNATURE]);
    }
}

#[test]
fn unknown_signer_for_unlisted_from_or_kid() {
    let mut l = base();
    l.push(decl_line(C, &declare(C, "n-c"), 3));
    let h = headers(2, "ka", C, B, "m1", 1, "x");
    l.extend(mail_lines("m1", "directed", C, B, Some(h), "x"));
    let h = headers(2, "other", A, B, "m2", 1, "x");
    l.extend(mail_lines("m2", "directed", A, B, Some(h), "x"));
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    for m in mails(&r) {
        assert_eq!(m.failures(), vec![UNKNOWN_SIGNER]);
    }
}

#[test]
fn body_hash_mismatch_and_body_unavailable() {
    let mut l = base();
    let h = headers(2, "ka", A, B, "m1", 1, "original");
    l.extend(mail_lines("m1", "directed", A, B, Some(h), "tampered"));
    let mut no_body = signed("m2", 2, "x");
    no_body.pop();
    l.extend(no_body);
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    let m = mails(&r);
    // The signature over the claimed hash still holds; only the body differs.
    assert_eq!(m[0].failures(), vec![BODY_HASH_MISMATCH]);
    assert_eq!(m[1].failures(), vec![BODY_UNAVAILABLE]);
}

#[test]
fn malformed_headers_when_a_required_header_is_missing_or_bad() {
    let mut l = base();
    let mut h = headers(2, "ka", A, B, "m1", 1, "x");
    h.as_object_mut().unwrap().remove("kid");
    l.extend(mail_lines("m1", "directed", A, B, Some(h), "x"));
    let mut h = headers(2, "ka", A, B, "m2", 2, "x");
    h["seq"] = json!("two");
    l.extend(mail_lines("m2", "directed", A, B, Some(h), "x"));
    let mut h = headers(2, "ka", A, B, "m3", 3, "x");
    h["sig"] = json!("not base64!");
    l.extend(mail_lines("m3", "directed", A, B, Some(h), "x"));
    let (r, _) = run(&l);
    for m in mails(&r) {
        assert!(
            m.failures().contains(&MALFORMED_HEADERS.to_string()),
            "{m:?}"
        );
    }
}

#[test]
fn replayed_seq_and_gaps_per_signer() {
    let mut l = base();
    l.extend(signed("m1", 1, "a"));
    l.extend(signed("m2", 1, "b"));
    l.extend(signed("m3", 4, "c"));
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    let m = mails(&r);
    assert!(m[0].failures().is_empty());
    assert_eq!(m[1].failures(), vec![REPLAYED_SEQ]);
    assert!(m[2].failures().is_empty());
    let gaps: Vec<_> = r.iter().filter(|x| x.word == "SEQ_GAP").collect();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0].get("from"), Some(&json!(A)));
    assert_eq!(gaps[0].get("missing_first"), Some(&json!(2)));
    assert_eq!(gaps[0].get("missing_last"), Some(&json!(3)));
}

#[test]
fn first_seq_above_one_is_a_gap() {
    let mut l = base();
    l.extend(signed("m1", 2, "a"));
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    let gap = r.iter().find(|x| x.word == "SEQ_GAP").unwrap();
    assert_eq!(gap.get("missing_first"), Some(&json!(1)));
    assert_eq!(gap.get("missing_last"), Some(&json!(1)));
}

#[test]
fn reordered_seqs_are_not_replays() {
    let mut l = base();
    l.extend(signed("m2", 2, "b"));
    l.extend(signed("m1", 1, "a"));
    let (r, status) = run(&l);
    assert_eq!(status, 0, "{r:?}");
}

#[test]
fn broadcast_copies_verify_with_star_and_share_one_seq() {
    let mut l = base();
    l.push(decl_line(C, &declare(C, "n-c"), 3));
    let h = headers(2, "ka", A, "*", "all", 1, "news");
    for to in [B, C] {
        l.extend(mail_lines(
            &format!("all/{to}"),
            "broadcast",
            A,
            to,
            Some(h.clone()),
            "news",
        ));
    }
    let (r, status) = run(&l);
    assert_eq!(status, 0, "{r:?}");
    // Signing with the recipient's Address instead of `*` does not verify.
    let mut l = base();
    let h = headers(2, "ka", A, B, "all", 1, "news");
    l.extend(mail_lines(
        &format!("all/{B}"),
        "broadcast",
        A,
        B,
        Some(h),
        "news",
    ));
    let (r, _) = run(&l);
    assert_eq!(mails(&r)[0].failures(), vec![INVALID_SIGNATURE]);
}

#[test]
fn sender_must_hold_a_verified_declaration_unless_allowed() {
    let mut l = vec![decl_line(B, &declare(B, "n-b"), 2)];
    l.extend(signed("m1", 1, "a"));
    let (r, _) = run(&l);
    assert_eq!(mails(&r)[0].failures(), vec![SENDER_UNDECLARED]);
    let (_, status) = run_opts(
        &l,
        Options {
            allow_undeclared: true,
        },
    );
    assert_eq!(status, 0);

    // Cleared by an UNDECLARED line.
    let mut l = base();
    l.push(json!({"type":"UNDECLARED","address":A,"at_ms":1,"line":3}));
    l.extend(signed("m1", 1, "a"));
    let (r, _) = run(&l);
    assert_eq!(mails(&r)[0].failures(), vec![SENDER_UNDECLARED]);

    // A failed declaration.
    let mut l = vec![decl_line(A, &declare_with(A, "n", 9, "auth"), 1)];
    l.extend(signed("m1", 1, "a"));
    let (r, _) = run(&l);
    assert_eq!(mails(&r)[0].failures(), vec![SENDER_DECLARATION_INVALID]);
}

#[test]
fn declaration_failure_classes() {
    let other_address = declare(A, "n1");
    let mut wrong_scheme: Value = serde_json::from_str(&declare(B, "n2")).unwrap();
    wrong_scheme["scheme"] = json!("roger-declare/9");
    let mut tampered: Value = serde_json::from_str(&declare(B, "n3")).unwrap();
    tampered["facts"]["role"] = json!("lead");
    let mut missing: Value = serde_json::from_str(&declare(B, "n4")).unwrap();
    missing.as_object_mut().unwrap().remove("issued_at_ms");
    let l = vec![
        decl_line(B, &other_address, 1),
        decl_line(B, &wrong_scheme.to_string(), 2),
        decl_line(B, &declare_with(B, "n5", 1, "nobody"), 3),
        decl_line(B, &tampered.to_string(), 4),
        decl_line(B, "not json", 5),
        decl_line(B, &missing.to_string(), 6),
        decl_line(B, &declare(B, "n-a"), 7),
        decl_line(A, &declare(A, "n-a"), 8),
        decl_line(B, r#"{"kid":"nobody","kid":"nobody"}"#, 9),
    ];
    let (r, status) = run(&l);
    assert_eq!(status, 1);
    let d = decls(&r);
    assert_eq!(d[0].failures(), vec![DECLARE_ADDRESS_MISMATCH]);
    // The scheme is in the signed form, so changing it breaks the signature too.
    assert_eq!(
        d[1].failures(),
        vec![DECLARE_UNKNOWN_SCHEME, DECLARE_INVALID_SIGNATURE]
    );
    assert_eq!(d[2].failures(), vec![DECLARE_UNKNOWN_KEY]);
    assert_eq!(d[3].failures(), vec![DECLARE_INVALID_SIGNATURE]);
    assert_eq!(d[4].failures(), vec![DECLARE_MALFORMED]);
    assert!(d[5].failures().contains(&DECLARE_MALFORMED.to_string()));
    assert!(d[6].failures().is_empty());
    assert_eq!(d[7].failures(), vec![DECLARE_REPLAYED_NONCE]);
    assert!(d[8].failures().contains(&DECLARE_MALFORMED.to_string()));
    assert!(d[8]
        .get("detail")
        .unwrap()
        .as_str()
        .unwrap()
        .contains("twice"));
}

#[test]
fn store_and_daemon_lines_are_passed_over() {
    let mut l = base();
    l.extend(signed("m1", 1, "a"));
    l.push(json!({"type":"STORE","path":"s","record":"r","mail":"m1","from":A,"kind":"directed","line":1}));
    l.push(json!({"type":"HEADERS","mail":"m1","headers":null}));
    l.push(json!({"type":"BODY","mail":"m1","body":"different"}));
    let (r, status) = run(&l);
    assert_eq!(status, 0, "{r:?}");
    assert_eq!(mails(&r).len(), 1);
}

#[test]
fn text_and_json_renderings() {
    let mut l = base();
    l.extend(signed("m1", 1, "a"));
    let (r, _) = run(&l);
    let m = mails(&r)[0];
    assert!(m
        .text()
        .starts_with(&format!("MAIL mail=m1 kind=directed from={A} to={B}")));
    assert!(m.text().contains("status=verified failures=none"));
    let j: Value = serde_json::from_str(&m.json()).unwrap();
    assert_eq!(j["type"], "MAIL");
    assert_eq!(j["failures"], json!([]));
    assert!(m.json().starts_with("{\"type\":\"MAIL\",\"mail\":\"m1\""));
}

#[test]
fn non_json_input_is_an_error() {
    let err = verify_text("MAIL mail=m1\n", &roster(), Options::default()).unwrap_err();
    assert!(err.contains("not JSON"));
}

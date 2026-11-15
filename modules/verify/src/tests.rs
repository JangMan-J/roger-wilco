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

#[test]
fn group_articles_verify_with_group_target_optional_fields_and_positions() {
    let mut l = base();
    l.push(json!({
        "type": "POSITION",
        "group": "desk.intake",
        "session": B,
        "subscribed": true,
        "position": 0,
        "record": null,
        "receipted_at_ms": null
    }));

    // Standard audit MAIL row for a Group article (`to` = Group name, `article` present).
    let h1 = headers(2, "ka", A, "desk.intake", "g-1", 1, "post one");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-1",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "article": 1,
        "accepted_at_ms": 10,
        "receipted_at_ms": 12,
        "latency_ms": 2,
        "record": "r-1",
        "receipt_line": 4,
        "body_bytes": 8,
        "line": 3
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-1", "headers": h1}));
    l.push(json!({"type": "BODY", "mail": "g-1", "body": "post one"}));
    l.push(json!({
        "type": "POSITION",
        "group": "desk.intake",
        "session": B,
        "subscribed": true,
        "position": 1,
        "record": "r-1",
        "receipted_at_ms": 12
    }));

    // `MAIL.group` overrides `MAIL.to` when present, and a slash in `mail` or `group` stays verbatim.
    let h2 = headers(2, "ka", A, "ops/alerts", "g-2/ops/alerts", 2, "post two");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-2/ops/alerts",
        "kind": "group",
        "from": A,
        "to": B,
        "group": "ops/alerts",
        "article": 2,
        "accepted_at_ms": 20,
        "line": 6
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-2/ops/alerts", "headers": h2}));
    l.push(json!({"type": "BODY", "mail": "g-2/ops/alerts", "body": "post two"}));

    // `MAIL` with `group` only (no `to` field) and directed `MAIL` with null optional fields.
    let h3 = headers(2, "ka", A, "desk.intake", "g-3", 3, "post three");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-3",
        "kind": "group",
        "from": A,
        "group": "desk.intake",
        "article": 3,
        "line": 8
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-3", "headers": h3}));
    l.push(json!({"type": "BODY", "mail": "g-3", "body": "post three"}));

    let h4 = headers(2, "ka", A, B, "d-4", 4, "direct four");
    l.push(json!({
        "type": "MAIL",
        "mail": "d-4",
        "kind": "directed",
        "from": A,
        "to": B,
        "group": null,
        "article": null,
        "line": 9
    }));
    l.push(json!({"type": "HEADERS", "mail": "d-4", "headers": h4}));
    l.push(json!({"type": "BODY", "mail": "d-4", "body": "direct four"}));
    l.push(json!({
        "type": "STORE",
        "path": "b.store",
        "record": "r-1",
        "mail": "g-1",
        "from": A,
        "kind": "group",
        "group": "desk.intake",
        "article": 1,
        "line": 1
    }));

    let (r, status) = run(&l);
    assert_eq!(status, 0, "{r:?}");
    let ms = mails(&r);
    assert_eq!(ms.len(), 4);
    assert!(ms.iter().all(|m| m.failures().is_empty()));
    assert!(!r.iter().any(|x| x.word == "SEQ_GAP"));
}

#[test]
fn group_tampered_group_or_body_fails_verification() {
    let mut l = base();
    // Signed for `desk.intake`, committed under `other.group` in `to`.
    let h1 = headers(2, "ka", A, "desk.intake", "g-bad-to", 1, "body-1");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-bad-to",
        "kind": "group",
        "from": A,
        "to": "other.group",
        "article": 1,
        "line": 3
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-bad-to", "headers": h1}));
    l.push(json!({"type": "BODY", "mail": "g-bad-to", "body": "body-1"}));

    // Signed for `desk.intake`, tampered in explicit `group` field.
    let h2 = headers(2, "ka", A, "desk.intake", "g-bad-group", 2, "body-2");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-bad-group",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "group": "other.group",
        "article": 2,
        "line": 4
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-bad-group", "headers": h2}));
    l.push(json!({"type": "BODY", "mail": "g-bad-group", "body": "body-2"}));

    // Signed with `*` instead of the Group name.
    let h3 = headers(2, "ka", A, "*", "g-star", 3, "body-3");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-star",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "article": 3,
        "line": 5
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-star", "headers": h3}));
    l.push(json!({"type": "BODY", "mail": "g-star", "body": "body-3"}));

    // Signed for `desk.intake` with body `clean`, delivered with tampered body.
    let h4 = headers(2, "ka", A, "desk.intake", "g-bad-body", 4, "clean");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-bad-body",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "article": 4,
        "line": 6
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-bad-body", "headers": h4}));
    l.push(json!({"type": "BODY", "mail": "g-bad-body", "body": "tampered"}));

    let (r, status) = run(&l);
    assert_eq!(status, 1);
    let ms = mails(&r);
    assert_eq!(ms.len(), 4);
    assert_eq!(ms[0].failures(), vec![INVALID_SIGNATURE]);
    assert_eq!(ms[1].failures(), vec![INVALID_SIGNATURE]);
    assert_eq!(ms[2].failures(), vec![INVALID_SIGNATURE]);
    assert_eq!(ms[3].failures(), vec![BODY_HASH_MISMATCH]);
}

#[test]
fn group_multi_reader_rows_share_seq_while_distinct_mail_id_is_replayed_seq() {
    let mut l = base();
    l.push(decl_line(C, &declare(C, "n-c"), 3));
    let h_art1 = headers(2, "ka", A, "desk.intake", "g-art-1", 1, "shared article");

    // Reader B's audit row for g-art-1.
    l.push(json!({
        "type": "MAIL",
        "mail": "g-art-1",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "article": 1,
        "record": "r-b-1",
        "line": 10
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-art-1", "headers": h_art1.clone()}));
    l.push(json!({"type": "BODY", "mail": "g-art-1", "body": "shared article"}));
    l.push(json!({
        "type": "POSITION",
        "group": "desk.intake",
        "session": B,
        "subscribed": true,
        "position": 1,
        "record": "r-b-1"
    }));

    // Reader C's audit row for the same g-art-1 (`seq` = 1, `mail` = "g-art-1").
    l.push(json!({
        "type": "MAIL",
        "mail": "g-art-1",
        "kind": "group",
        "from": A,
        "to": C,
        "group": "desk.intake",
        "article": 1,
        "record": "r-c-1",
        "line": 10
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-art-1", "headers": h_art1}));
    l.push(json!({"type": "BODY", "mail": "g-art-1", "body": "shared article"}));

    let (r_ok, status_ok) = run(&l);
    assert_eq!(status_ok, 0, "{r_ok:?}");
    assert_eq!(mails(&r_ok).len(), 2);
    assert!(mails(&r_ok).iter().all(|m| m.failures().is_empty()));

    // A distinct `mail_id` reusing `seq` = 1 from A fails with `replayed_seq`.
    let h_dup = headers(2, "ka", A, "desk.intake", "g-art-2", 1, "distinct article");
    l.push(json!({
        "type": "MAIL",
        "mail": "g-art-2",
        "kind": "group",
        "from": A,
        "to": "desk.intake",
        "article": 2,
        "record": "r-b-2",
        "line": 15
    }));
    l.push(json!({"type": "HEADERS", "mail": "g-art-2", "headers": h_dup}));
    l.push(json!({"type": "BODY", "mail": "g-art-2", "body": "distinct article"}));

    let (r_bad, status_bad) = run(&l);
    assert_eq!(status_bad, 1);
    let ms = mails(&r_bad);
    assert_eq!(ms.len(), 3);
    assert!(ms[0].failures().is_empty());
    assert!(ms[1].failures().is_empty());
    assert_eq!(ms[2].failures(), vec![REPLAYED_SEQ]);
}

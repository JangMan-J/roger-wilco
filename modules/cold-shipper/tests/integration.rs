use roger_cold_shipper::archive::{load_manifest, MANIFEST_SCHEME};
use roger_cold_shipper::format::{encode_cold_bytes, sha256_hex, verify_cold_file, ColdRecord};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const BIN: &str = env!("CARGO_BIN_EXE_roger-cold-shipper");
static TEST_SEQ: AtomicU64 = AtomicU64::new(1);

fn temp_dir(name: &str) -> PathBuf {
    let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!(
        "roger-cold-shipper-it-{}-{seq}-{name}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn run_cli(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .output()
        .expect("running roger-cold-shipper")
}

type RawColdRow<'a> = (
    &'a str,
    &'a str,
    i64,
    &'a str,
    &'a str,
    &'a str,
    Option<&'a str>,
    &'a str,
    Option<i64>,
);

/// Hand-build exact `src/cold.rs` wire bytes (`RWCOLD01\n` + exact field order + `SHA256:<hex>\n`)
/// to prove 100% round-trip compatibility with the core's cold file writer and reader.
fn build_core_rwcold01_file(records: &[RawColdRow<'_>]) -> Vec<u8> {
    let mut payload = String::from("RWCOLD01\n");
    for (mail, stream, article, kind, from, to, hdrs, body, accepted_at_ms) in records {
        let body_digest = sha256_hex(body.as_bytes());
        let hdr_json = match hdrs {
            Some(h) => serde_json::to_string(h).unwrap(),
            None => "null".to_string(),
        };
        let ts_json = match accepted_at_ms {
            Some(ts) => ts.to_string(),
            None => "null".to_string(),
        };
        let line = format!(
            "{{\"mail\":{mail_j},\"stream\":{stream_j},\"article\":{article},\"kind\":{kind_j},\"from\":{from_j},\"to\":{to_j},\"headers\":{hdr_json},\"body\":{body_j},\"body_sha256\":\"{body_digest}\",\"accepted_at_ms\":{ts_json}}}",
            mail_j = serde_json::to_string(mail).unwrap(),
            stream_j = serde_json::to_string(stream).unwrap(),
            kind_j = serde_json::to_string(kind).unwrap(),
            from_j = serde_json::to_string(from).unwrap(),
            to_j = serde_json::to_string(to).unwrap(),
            body_j = serde_json::to_string(body).unwrap(),
        );
        payload.push_str(&line);
        payload.push('\n');
    }
    let trailer = sha256_hex(payload.as_bytes());
    payload.push_str("SHA256:");
    payload.push_str(&trailer);
    payload.push('\n');
    payload.into_bytes()
}

fn write_file(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, bytes).unwrap();
}

#[test]
fn intact_ship_verify_and_idempotent_reship_cli() {
    let root = temp_dir("ship-verify");
    let slices = root.join("slices");
    let dest = root.join("archive");

    let f0 = build_core_rwcold01_file(&[
        (
            "m-100",
            "m:dest-1",
            1,
            "directed",
            "s-1",
            "dest-1",
            Some(r#"{"x":"1"}"#),
            "hello cold 1",
            Some(1_700_000_000_001),
        ),
        (
            "m-101",
            "m:dest-1",
            2,
            "directed",
            "s-1",
            "dest-1",
            None,
            "hello cold 2",
            None,
        ),
    ]);
    let f1 = build_core_rwcold01_file(&[(
        "m-200",
        "g:news",
        1,
        "group",
        "s-2",
        "news",
        Some(r#"{"tag":"v1"}"#),
        "group article body ✓",
        Some(1_700_000_100_000),
    )]);
    let f2 = build_core_rwcold01_file(&[(
        "m-300",
        "m:dest-2",
        1,
        "broadcast",
        "s-3",
        "dest-2",
        None,
        "broadcast copy",
        Some(1_700_000_200_000),
    )]);

    write_file(&slices.join("slice-00/cold/day-19000.cold"), &f0);
    write_file(&slices.join("slice-00/cold/day-19001.cold"), &f1);
    write_file(&slices.join("slice-01/cold/day-19000.cold"), &f2);

    let out1 = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        out1.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let v1: Value = serde_json::from_slice(&out1.stdout).unwrap();
    assert_eq!(v1["type"], "ship");
    assert_eq!(v1["status"], "ok");
    assert_eq!(v1["shipped"], 3);
    assert_eq!(v1["skipped"], 0);
    assert_eq!(v1["slices"], 2);
    assert_eq!(v1["total_files"], 3);
    assert_eq!(v1["total_records"], 4);
    assert_eq!(v1["total_bytes"], (f0.len() + f1.len() + f2.len()) as u64);

    let manifest = load_manifest(&dest).unwrap();
    assert_eq!(manifest.scheme, MANIFEST_SCHEME);
    assert_eq!(manifest.slices.len(), 2);
    assert_eq!(manifest.slices["slice-00"].len(), 2);
    assert_eq!(manifest.slices["slice-01"].len(), 1);
    assert_eq!(manifest.slices["slice-00"][0].sha256, sha256_hex(&f0));

    for rel in [
        "slice-00/day-19000.cold",
        "slice-00/day-19001.cold",
        "slice-01/day-19000.cold",
    ] {
        let p = dest.join(rel);
        assert!(
            fs::metadata(&p).unwrap().permissions().readonly(),
            "{rel} must be read-only"
        );
    }

    // Verify archive returns exit 0 and intact=true.
    let ver = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(ver.status.code(), Some(0));
    let vv: Value = serde_json::from_slice(&ver.stdout).unwrap();
    assert_eq!(vv["intact"], true);
    assert_eq!(vv["files_verified"], 3);
    assert_eq!(vv["files_failed"], 0);

    // Idempotent reship skips all 3 verified files.
    let out2 = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(out2.status.code(), Some(0));
    let v2: Value = serde_json::from_slice(&out2.stdout).unwrap();
    assert_eq!(v2["shipped"], 0);
    assert_eq!(v2["skipped"], 3);

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn tampered_local_cold_file_rejected_before_shipping() {
    let root = temp_dir("tampered-src");
    let slices = root.join("slices");
    let dest = root.join("archive");

    let valid = build_core_rwcold01_file(&[(
        "m-ok",
        "m:r",
        1,
        "directed",
        "s",
        "r",
        None,
        "intact body",
        Some(1_700_000_000_000),
    )]);
    write_file(&slices.join("slice-00/cold/day-19000.cold"), &valid);

    // Case 1: Tamper with record body and recompute trailer so ONLY per-record body_sha256 fails.
    let bad_rec = ColdRecord {
        mail: "m-bad".to_string(),
        stream: "m:r".to_string(),
        article: 1,
        kind: "directed".to_string(),
        from: "s".to_string(),
        to: "r".to_string(),
        headers: None,
        body: "tampered payload".to_string(),
        body_sha256: sha256_hex(b"original payload"),
        accepted_at_ms: None,
    };
    let bad_bytes = encode_cold_bytes(&[bad_rec]).unwrap();
    write_file(&slices.join("slice-01/cold/day-19001.cold"), &bad_bytes);

    let out = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
    ]);
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("body digest"),
        "expected body digest error, saw: {stderr}"
    );
    assert!(
        !dest.exists(),
        "nothing should be written to archive when source verification fails"
    );

    // Case 2: Corrupt trailer checksum on local cold file.
    let mut bad_trailer = valid.clone();
    let s = String::from_utf8(bad_trailer)
        .unwrap()
        .replacen("intact body", "broken body", 1);
    bad_trailer = s.into_bytes();
    write_file(&slices.join("slice-01/cold/day-19001.cold"), &bad_trailer);

    let out2 = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
    ]);
    assert_eq!(out2.status.code(), Some(1));
    assert!(!dest.exists());

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn verify_catches_corrupted_missing_and_unlisted_archive_files() {
    let root = temp_dir("verify-corrupt");
    let slices = root.join("slices");
    let dest = root.join("archive");

    let orig = build_core_rwcold01_file(&[(
        "m-1",
        "m:r",
        1,
        "directed",
        "s",
        "r",
        None,
        "original archive body",
        Some(1_700_000_000_000),
    )]);
    write_file(&slices.join("slice-00/cold/day-19000.cold"), &orig);

    let ship_out = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
    ]);
    assert_eq!(ship_out.status.code(), Some(0));

    let archived = dest.join("slice-00/day-19000.cold");

    // Subcase A: Corrupt bytes inside archived cold file -> exit 1 (corrupt_file).
    let mut perms = fs::metadata(&archived).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    fs::set_permissions(&archived, perms).unwrap();
    let corrupted_text = String::from_utf8(orig.clone()).unwrap().replacen(
        "original archive body",
        "corrupt! archive body",
        1,
    );
    fs::write(&archived, corrupted_text.as_bytes()).unwrap();

    let va = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(va.status.code(), Some(1));
    let va_json: Value = serde_json::from_slice(&va.stdout).unwrap();
    assert_eq!(va_json["intact"], false);
    assert_eq!(va_json["issues"][0]["kind"], "corrupt_file");

    // Subcase B: Replace archived file with a different valid RWCOLD01 file so only manifest.json digest disagrees -> exit 1.
    let swapped = build_core_rwcold01_file(&[(
        "m-1",
        "m:r",
        1,
        "directed",
        "s",
        "r",
        None,
        "different valid cold file",
        Some(1_700_000_000_000),
    )]);
    fs::write(&archived, &swapped).unwrap();
    let vb = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(vb.status.code(), Some(1));
    let vb_json: Value = serde_json::from_slice(&vb.stdout).unwrap();
    assert_eq!(vb_json["issues"][0]["kind"], "manifest_mismatch");

    // Restore original bytes, then add an unlisted .cold file -> exit 1 (unlisted_file).
    fs::write(&archived, &orig).unwrap();
    let extra = dest.join("slice-00/day-99999.cold");
    fs::write(&extra, &orig).unwrap();
    let vc = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(vc.status.code(), Some(1));
    let vc_json: Value = serde_json::from_slice(&vc.stdout).unwrap();
    assert_eq!(vc_json["issues"][0]["kind"], "unlisted_file");
    fs::remove_file(&extra).unwrap();

    // Subcase D: Delete archived cold file -> exit 1 (missing_file).
    fs::remove_file(&archived).unwrap();
    let vd = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(vd.status.code(), Some(1));
    let vd_json: Value = serde_json::from_slice(&vd.stdout).unwrap();
    assert_eq!(vd_json["issues"][0]["kind"], "missing_file");

    // Subcase E: Missing manifest -> exit 1 (missing_manifest).
    fs::remove_file(dest.join("manifest.json")).unwrap();
    let ve = run_cli(&["verify", "--dest", dest.to_str().unwrap(), "--json"]);
    assert_eq!(ve.status.code(), Some(1));
    let ve_json: Value = serde_json::from_slice(&ve.stdout).unwrap();
    assert_eq!(ve_json["issues"][0]["kind"], "missing_manifest");

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn restore_round_trip_matches_core_rwcold01_format_and_rejects_corrupt_archive() {
    let root = temp_dir("restore-rt");
    let single_slice = root.join("coord-single");
    let dest = root.join("archive");
    let restored_cold = root.join("restored-cold");

    let composite_addr = "[[3:BUILDER:1]]3b0d5b6e-7e3d-4a0e-9b1c-2a7f1c9d0e11";
    let hdr_raw =
        r#"{"z-last":"4417","note":"héllo ✓ 日本語","a-first":"[[153:Meteor Shower:7]]"}"#;
    let day0 = build_core_rwcold01_file(&[
        (
            "m-a",
            "m:target",
            1,
            "directed",
            composite_addr,
            "target",
            Some(hdr_raw),
            "line 1\nline 2\t✓ 日本語",
            Some(1_700_000_000_111),
        ),
        (
            "m-b",
            "g:eng",
            4,
            "group",
            composite_addr,
            "eng",
            None,
            "group payload",
            None,
        ),
    ]);
    let day1 = build_core_rwcold01_file(&[(
        "m-c",
        "m:target",
        2,
        "directed",
        "sender-2",
        composite_addr,
        None,
        "day 19001 body",
        Some(1_700_086_400_000),
    )]);

    write_file(&single_slice.join("cold/day-19000.cold"), &day0);
    write_file(&single_slice.join("cold/day-19001.cold"), &day1);

    // Ship single-slice directory (maps to slice-00).
    let s = run_cli(&[
        "ship",
        "--slices-dir",
        single_slice.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
    ]);
    assert_eq!(s.status.code(), Some(0));

    // Restore ONLY day 19000 first via --day 19000.
    let r1 = run_cli(&[
        "restore",
        "--dest",
        dest.to_str().unwrap(),
        "--slice",
        "slice-00",
        "--cold-dir",
        restored_cold.to_str().unwrap(),
        "--day",
        "19000",
        "--json",
    ]);
    assert_eq!(
        r1.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&r1.stderr)
    );
    let r1_json: Value = serde_json::from_slice(&r1.stdout).unwrap();
    assert_eq!(r1_json["restored"], 1);
    assert_eq!(r1_json["skipped"], 0);
    assert!(restored_cold.join("day-19000.cold").exists());
    assert!(!restored_cold.join("day-19001.cold").exists());

    // Restore all of slice-00: day-19000 is skipped (already intact), day-19001 is restored.
    let r2 = run_cli(&[
        "restore",
        "--dest",
        dest.to_str().unwrap(),
        "--slice",
        "slice-00",
        "--cold-dir",
        restored_cold.to_str().unwrap(),
        "--json",
    ]);
    assert_eq!(r2.status.code(), Some(0));
    let r2_json: Value = serde_json::from_slice(&r2.stdout).unwrap();
    assert_eq!(r2_json["restored"], 1);
    assert_eq!(r2_json["skipped"], 1);

    // Byte-for-byte match with core RWCOLD01 files and read-only permissions preserved.
    let restored_0 = fs::read(restored_cold.join("day-19000.cold")).unwrap();
    let restored_1 = fs::read(restored_cold.join("day-19001.cold")).unwrap();
    assert_eq!(restored_0, day0);
    assert_eq!(restored_1, day1);
    assert!(fs::metadata(restored_cold.join("day-19000.cold"))
        .unwrap()
        .permissions()
        .readonly());
    let (_, parsed0) = verify_cold_file(&restored_cold.join("day-19000.cold")).unwrap();
    assert_eq!(parsed0.records[0].from, composite_addr);
    assert_eq!(parsed0.records[0].headers.as_deref(), Some(hdr_raw));

    // Corrupting archived file causes restore into a fresh dir to reject before writing.
    let archived_1 = dest.join("slice-00/day-19001.cold");
    let mut perms = fs::metadata(&archived_1).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    fs::set_permissions(&archived_1, perms).unwrap();
    fs::write(&archived_1, &day0).unwrap(); // Valid RWCOLD01 file, wrong manifest sha256!

    let clean_cold = root.join("clean-cold");
    let r_bad = run_cli(&[
        "restore",
        "--dest",
        dest.to_str().unwrap(),
        "--slice",
        "slice-00",
        "--cold-dir",
        clean_cold.to_str().unwrap(),
    ]);
    assert_eq!(r_bad.status.code(), Some(1));
    assert!(
        !clean_cold.exists(),
        "corrupt archive must abort restore before creating files"
    );

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn watch_stdin_triggers_incremental_passes_on_each_line_without_polling() {
    let root = temp_dir("watch-stdin");
    let slices = root.join("slices");
    let dest = root.join("archive");
    fs::create_dir_all(&slices).unwrap();

    let mut child = Command::new(BIN)
        .args([
            "watch-stdin",
            "--slices-dir",
            slices.to_str().unwrap(),
            "--dest",
            dest.to_str().unwrap(),
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawning watch-stdin");

    let mut child_stdin = child.stdin.take().unwrap();
    let mut child_stdout = BufReader::new(child.stdout.take().unwrap());

    // Place first cold file; before any stdin trigger line is sent, dest has not been created.
    let d0 = build_core_rwcold01_file(&[(
        "m-w1",
        "m:r",
        1,
        "directed",
        "s",
        "r",
        None,
        "event pass 1",
        Some(1_700_000_000_000),
    )]);
    write_file(&slices.join("slice-00/cold/day-19000.cold"), &d0);
    assert!(!dest.exists());

    // Trigger pass 1 via one stdin line; response arrives immediately on stdout.
    writeln!(child_stdin, "day-19000-sealed").unwrap();
    child_stdin.flush().unwrap();

    let mut line1 = String::new();
    child_stdout.read_line(&mut line1).unwrap();
    let j1: Value = serde_json::from_str(&line1).unwrap();
    assert_eq!(j1["shipped"], 1);
    assert_eq!(j1["skipped"], 0);
    assert!(dest.join("slice-00/day-19000.cold").exists());

    // Add second cold file and trigger pass 2 via second stdin line.
    let d1 = build_core_rwcold01_file(&[(
        "m-w2",
        "m:r",
        2,
        "directed",
        "s",
        "r",
        None,
        "event pass 2",
        Some(1_700_086_400_000),
    )]);
    write_file(&slices.join("slice-00/cold/day-19001.cold"), &d1);
    writeln!(child_stdin, "day-19001-sealed").unwrap();
    child_stdin.flush().unwrap();

    let mut line2 = String::new();
    child_stdout.read_line(&mut line2).unwrap();
    let j2: Value = serde_json::from_str(&line2).unwrap();
    assert_eq!(j2["shipped"], 1);
    assert_eq!(j2["skipped"], 1);
    assert_eq!(j2["total_files"], 2);

    // Close stdin: watch-stdin exits 0 cleanly.
    drop(child_stdin);
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(0));

    let _ = fs::remove_dir_all(&root);
}
#[test]
fn cli_text_output_and_usage_exit_codes() {
    let help = run_cli(&["--help"]);
    assert_eq!(help.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&help.stdout).contains("watch-stdin"));

    let bad_sub = run_cli(&["unknown-sub"]);
    assert_eq!(bad_sub.status.code(), Some(2));

    let missing_flags = run_cli(&["ship", "--slices-dir"]);
    assert_eq!(missing_flags.status.code(), Some(2));

    let bad_day = run_cli(&[
        "restore",
        "--dest",
        "/tmp/x",
        "--slice",
        "slice-00",
        "--cold-dir",
        "/tmp/y",
        "--day",
        "not-an-int",
    ]);
    assert_eq!(bad_day.status.code(), Some(2));

    let root = temp_dir("text-mode");
    let slices = root.join("slices");
    let dest = root.join("archive");
    let local = root.join("local");
    let d0 = build_core_rwcold01_file(&[(
        "m-t1",
        "m:r",
        1,
        "directed",
        "s",
        "r",
        None,
        "text mode check",
        Some(1_700_000_000_000),
    )]);
    write_file(&slices.join("slice-00/cold/day-19000.cold"), &d0);

    let s_out = run_cli(&[
        "ship",
        "--slices-dir",
        slices.to_str().unwrap(),
        "--dest",
        dest.to_str().unwrap(),
    ]);
    assert_eq!(s_out.status.code(), Some(0));
    let s_txt = String::from_utf8(s_out.stdout).unwrap();
    assert!(s_txt.contains("SHIP_FILE slice=slice-00 day=19000 action=shipped"));
    assert!(s_txt.contains("SHIP status=ok shipped=1 skipped=0 slices=1 files=1"));

    let v_out = run_cli(&["verify", "--dest", dest.to_str().unwrap()]);
    assert_eq!(v_out.status.code(), Some(0));
    let v_txt = String::from_utf8(v_out.stdout).unwrap();
    assert!(v_txt.contains("VERIFY_FILE slice=slice-00 day=19000 status=verified"));
    assert!(v_txt.contains("VERIFY status=ok slices=1 files=1 verified=1 failed=0 issues=0"));

    let r_out = run_cli(&[
        "restore",
        "--dest",
        dest.to_str().unwrap(),
        "--slice",
        "slice-00",
        "--cold-dir",
        local.to_str().unwrap(),
    ]);
    assert_eq!(r_out.status.code(), Some(0));
    let r_txt = String::from_utf8(r_out.stdout).unwrap();
    assert!(r_txt.contains("RESTORE_FILE slice=slice-00 day=19000 action=restored"));
    assert!(r_txt.contains("RESTORE status=ok slice=slice-00 restored=1 skipped=0"));

    let _ = fs::remove_dir_all(&root);
}

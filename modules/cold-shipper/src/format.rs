use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub const COLD_HEADER: &str = "RWCOLD01\n";
pub const TRAILER_PREFIX: &str = "SHA256:";

/// One exported Mail record inside a `day-<bucket>.cold` file, matching `src/cold.rs`.
/// Opaque fields (`headers`, `from`, `to`, `body`) are carried and verified as raw values
/// without inspecting any token or header entry.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColdRecord {
    pub mail: String,
    pub stream: String,
    pub article: i64,
    pub kind: String,
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub headers: Option<String>,
    pub body: String,
    pub digest: String,
    #[serde(default)]
    pub accepted_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedColdFile {
    pub byte_size: u64,
    pub record_count: usize,
    pub file_sha256: String,
    pub trailer_sha256: String,
    pub records: Vec<ColdRecord>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

pub fn is_lower_hex_64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Parse strictly canonical `day-<i64>.cold` filenames (ignoring `.tmp` or unrelated files).
pub fn parse_day_filename(path: &Path) -> Option<i64> {
    if path.extension().and_then(|e| e.to_str()) != Some("cold") {
        return None;
    }
    let file_name = path.file_name()?.to_str()?;
    let stem = path.file_stem()?.to_str()?;
    let bucket = stem.strip_prefix("day-")?.parse::<i64>().ok()?;
    if file_name != format!("day-{bucket}.cold") {
        return None;
    }
    Some(bucket)
}

/// Verify `RWCOLD01\n` header, `SHA256:<64-hex>\n` trailer digest over all preceding bytes,
/// and every JSON record's body digest against its `body`.
pub fn verify_cold_bytes(raw: &[u8], origin: &Path) -> Result<VerifiedColdFile, String> {
    let text = std::str::from_utf8(raw).map_err(|e| {
        format!(
            "expected UTF-8 cold file at {}, saw invalid UTF-8: {e}",
            origin.display()
        )
    })?;
    if !text.starts_with(COLD_HEADER) {
        return Err(format!(
            "expected cold header RWCOLD01 in {}, saw invalid header",
            origin.display()
        ));
    }
    let trimmed = text.strip_suffix('\n').ok_or_else(|| {
        format!(
            "expected trailing newline in {}, saw truncated file",
            origin.display()
        )
    })?;
    let (before_trailer, last_line) = trimmed
        .rsplit_once('\n')
        .ok_or_else(|| format!("expected SHA256 trailer in {}, saw none", origin.display()))?;
    let expected_trailer = last_line.strip_prefix(TRAILER_PREFIX).ok_or_else(|| {
        format!(
            "expected SHA256 trailer in {}, saw {last_line}",
            origin.display()
        )
    })?;
    if !is_lower_hex_64(expected_trailer) {
        return Err(format!(
            "expected 64 lowercase hex digits in SHA256 trailer of {}, saw {expected_trailer}",
            origin.display()
        ));
    }

    let payload_len = before_trailer.len() + 1;
    let payload = &text[..payload_len];
    let actual_trailer = sha256_hex(payload.as_bytes());
    if actual_trailer != expected_trailer {
        return Err(format!(
            "expected cold checksum {expected_trailer} in {}, saw {actual_trailer}",
            origin.display()
        ));
    }

    let mut records = Vec::new();
    for (idx, line) in payload[COLD_HEADER.len()..].lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        if !line.starts_with('{') {
            return Err(format!(
                "expected JSON object record at {}:{}, saw non-object line",
                origin.display(),
                idx + 2
            ));
        }
        let rec: ColdRecord = serde_json::from_str(line).map_err(|e| {
            format!(
                "expected valid JSON record at {}:{}, saw {e}",
                origin.display(),
                idx + 2
            )
        })?;
        if !is_lower_hex_64(&rec.digest) {
            return Err(format!(
                "expected 64-hex body digest for mail {} in {}, saw {}",
                rec.mail,
                origin.display(),
                rec.digest
            ));
        }
        let actual_body_digest = sha256_hex(rec.body.as_bytes());
        if actual_body_digest != rec.digest {
            return Err(format!(
                "expected body digest {} for mail {} in {}, saw {actual_body_digest}",
                rec.digest,
                rec.mail,
                origin.display()
            ));
        }
        records.push(rec);
    }

    Ok(VerifiedColdFile {
        byte_size: raw.len() as u64,
        record_count: records.len(),
        file_sha256: sha256_hex(raw),
        trailer_sha256: actual_trailer,
        records,
    })
}

pub fn verify_cold_file(path: &Path) -> Result<(Vec<u8>, VerifiedColdFile), String> {
    let raw = fs::read(path)
        .map_err(|e| format!("expected readable cold file {}, saw {e}", path.display()))?;
    let verified = verify_cold_bytes(&raw, path)?;
    Ok((raw, verified))
}

/// Encode records into the exact `RWCOLD01\n` + sorted-by-mail JSONL + `SHA256:<hex>\n`
/// representation produced by `src/cold.rs`.
pub fn encode_cold_bytes(records: &[ColdRecord]) -> Result<Vec<u8>, String> {
    let mut sorted: BTreeMap<&str, &ColdRecord> = BTreeMap::new();
    for rec in records {
        sorted.insert(rec.mail.as_str(), rec);
    }
    let mut payload = String::from(COLD_HEADER);
    for rec in sorted.values() {
        let line =
            serde_json::to_string(rec).map_err(|e| format!("encoding cold record failed: {e}"))?;
        payload.push_str(&line);
        payload.push('\n');
    }
    let digest = sha256_hex(payload.as_bytes());
    payload.push_str(TRAILER_PREFIX);
    payload.push_str(&digest);
    payload.push('\n');
    Ok(payload.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn sample_rec(mail: &str, art: i64, body: &str, hdrs: Option<&str>) -> ColdRecord {
        ColdRecord {
            mail: mail.to_string(),
            stream: "m:dest".to_string(),
            article: art,
            kind: "directed".to_string(),
            from: "sender-a".to_string(),
            to: "recipient-b".to_string(),
            headers: hdrs.map(str::to_string),
            body: body.to_string(),
            digest: sha256_hex(body.as_bytes()),
            accepted_at_ms: Some(1_700_000_000_000 + art),
        }
    }

    #[test]
    fn valid_rwcold01_verifies_header_records_and_trailer() {
        let r2 = sample_rec(
            "m-02",
            2,
            "second payload ✓ 日本語\nline2",
            Some(r#"{"x":"1"}"#),
        );
        let mut r1 = sample_rec("m-01", 1, "first payload", None);
        r1.accepted_at_ms = None;

        let encoded = encode_cold_bytes(&[r2.clone(), r1.clone()]).unwrap();
        let verified = verify_cold_bytes(&encoded, Path::new("day-19000.cold")).unwrap();

        assert_eq!(verified.byte_size, encoded.len() as u64);
        assert_eq!(verified.record_count, 2);
        assert_eq!(verified.file_sha256, sha256_hex(&encoded));
        assert_eq!(verified.records, vec![r1, r2]);
    }

    #[test]
    fn rejects_bad_header_missing_trailer_and_trailer_mismatch() {
        let p = Path::new("day-19000.cold");
        let err_hdr = verify_cold_bytes(b"RWCOLD00\nSHA256:00\n", p).unwrap_err();
        assert!(err_hdr.contains("RWCOLD01"), "{err_hdr}");

        let err_nl = verify_cold_bytes(b"RWCOLD01\nSHA256:00", p).unwrap_err();
        assert!(err_nl.contains("trailing newline"), "{err_nl}");

        let err_no_trailer = verify_cold_bytes(b"RWCOLD01\n", p).unwrap_err();
        assert!(
            err_no_trailer.contains("SHA256 trailer"),
            "{err_no_trailer}"
        );

        let rec = sample_rec("m-01", 1, "hello", None);
        let mut bytes = encode_cold_bytes(&[rec]).unwrap();
        let text = String::from_utf8(bytes)
            .unwrap()
            .replacen("hello", "hallo", 1);
        bytes = text.into_bytes();
        let err_ck = verify_cold_bytes(&bytes, p).unwrap_err();
        assert!(err_ck.contains("checksum"), "{err_ck}");
    }

    #[test]
    fn rejects_tampered_record_body_digest_even_with_matching_trailer() {
        let mut rec = sample_rec("m-tamper", 1, "original body", None);
        rec.body = "modified body".to_string();
        let forged = encode_cold_bytes(&[rec]).unwrap();
        let err = verify_cold_bytes(&forged, Path::new("day-19001.cold")).unwrap_err();
        assert!(err.contains("body digest"), "{err}");
    }

    #[test]
    fn parse_day_filename_accepts_canonical_and_ignores_temp_files() {
        assert_eq!(parse_day_filename(&PathBuf::from("day-0.cold")), Some(0));
        assert_eq!(
            parse_day_filename(&PathBuf::from("/tmp/slice-00/cold/day-19500.cold")),
            Some(19_500)
        );
        assert_eq!(
            parse_day_filename(&PathBuf::from("day-19500.cold.tmp.1234")),
            None
        );
        assert_eq!(
            parse_day_filename(&PathBuf::from(".day-19500.cold.tmp.1234.1")),
            None
        );
        assert_eq!(parse_day_filename(&PathBuf::from("day-01.cold")), None);
        assert_eq!(parse_day_filename(&PathBuf::from("day-abc.cold")), None);
    }
}

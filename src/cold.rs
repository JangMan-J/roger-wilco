//! Sealed, checksummed per-slice cold day files (`day-<bucket>.cold`) and
//! in-memory Mail-id Bloom filters.
//!
//! File layout:
//!   line 1:   `RWCOLD01`
//!   lines 2..N: one JSON object per exported Mail record (sorted by mail_id)
//!   last line: `SHA256:<64-hex-digest-of-all-preceding-bytes>`
//!
//! Every cold file is written aside (`.tmp`), fsynced, atomically renamed into
//! place, marked read-only, and directory-fsynced before hot bodies are removed.
//! Reading any cold file verifies its SHA-256 trailer first, and `read_body`
//! consults the file's in-memory `Filter` before touching the filesystem.

use crate::durlog;
use crate::json::{self, obj, s, Value};
use crate::sha256::{hash_pair, hex_digest};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub const COLD_HEADER: &str = "RWCOLD01\n";
const TRAILER_PREFIX: &str = "SHA256:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub mail_id: String,
    pub stream: String,
    pub article_num: i64,
    pub kind: String,
    pub sender: String,
    pub recipient: String,
    pub headers_json: Option<String>,
    pub body: String,
    pub body_sha256: String,
    pub accepted_at_ms: Option<i64>,
}

/// Compact 6-probe Bloom filter over a cold day file's Mail ids.
/// Zero false negatives; a cold file is opened on disk only on a filter hit.
#[derive(Clone, Debug)]
pub struct Filter {
    bits: Vec<u64>,
    num_bits: u64,
}

impl Filter {
    pub fn build<'a>(ids: impl IntoIterator<Item = &'a str>, count_hint: usize) -> Self {
        // At least 2048 bits (256 B), or ~16 bits per entry rounded up to 64-bit words.
        let words = ((count_hint.saturating_mul(16)).div_ceil(64)).max(32);
        let num_bits = (words as u64) * 64;
        let mut filter = Self {
            bits: vec![0u64; words],
            num_bits,
        };
        for id in ids {
            filter.insert(id);
        }
        filter
    }

    pub fn insert(&mut self, mail_id: &str) {
        let (h1, h2) = hash_pair(mail_id.as_bytes());
        for i in 0..6u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            self.bits[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }

    pub fn may_contain(&self, mail_id: &str) -> bool {
        let (h1, h2) = hash_pair(mail_id.as_bytes());
        for i in 0..6u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits;
            if (self.bits[(bit / 64) as usize] & (1u64 << (bit % 64))) == 0 {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug)]
pub struct Day {
    pub day_bucket: i64,
    pub path: PathBuf,
    pub filter: Filter,
}

pub struct Index {
    days: RwLock<BTreeMap<i64, Day>>,
    opens: AtomicU64,
}

impl Index {
    pub fn load(cold_dir: &Path) -> io::Result<Self> {
        let mut map = BTreeMap::new();
        if cold_dir.exists() {
            let mut entries: Vec<PathBuf> = Vec::new();
            for entry in fs::read_dir(cold_dir)? {
                let p = entry?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("cold") {
                    entries.push(p);
                }
            }
            entries.sort();
            for path in entries {
                if let Some(day) = parse_day(&path) {
                    let records = read_file(&path)?;
                    let filter =
                        Filter::build(records.iter().map(|r| r.mail_id.as_str()), records.len());
                    map.insert(
                        day,
                        Day {
                            day_bucket: day,
                            path,
                            filter,
                        },
                    );
                }
            }
        }
        Ok(Self {
            days: RwLock::new(map),
            opens: AtomicU64::new(0),
        })
    }

    /// Number of cold file disk opens performed through `read_body` / `contains_id`.
    pub fn file_opens(&self) -> u64 {
        self.opens.load(Ordering::SeqCst)
    }

    /// Seal `records` into `<cold_dir>/day-<day_bucket>.cold`. If that day file
    /// already exists (for instance after a crash between cold-file rename and
    /// hot-row deletion), existing records are verified and merged idempotently
    /// by `mail_id` before sealing the replacement file.
    pub fn seal_day(
        &self,
        cold_dir: &Path,
        day_bucket: i64,
        records: &[Record],
    ) -> io::Result<PathBuf> {
        fs::create_dir_all(cold_dir)?;
        let final_path = file_path(cold_dir, day_bucket);
        let mut merged: BTreeMap<String, Record> = BTreeMap::new();
        if final_path.exists() {
            for existing in read_file(&final_path)? {
                merged.insert(existing.mail_id.clone(), existing);
            }
        }
        for rec in records {
            merged.insert(rec.mail_id.clone(), rec.clone());
        }

        let mut payload = String::from(COLD_HEADER);
        for rec in merged.values() {
            let line = encode_record(rec);
            payload.push_str(&json::stringify(&line));
            payload.push('\n');
        }
        let digest = hex_digest(payload.as_bytes());
        let mut full = payload;
        full.push_str(TRAILER_PREFIX);
        full.push_str(&digest);
        full.push('\n');

        let tmp_path = cold_dir.join(format!("day-{day_bucket}.cold.tmp.{}", std::process::id()));
        // If an older read-only final_path exists on Windows, make it writable before rename.
        #[cfg(windows)]
        if final_path.exists() {
            if let Ok(meta) = fs::metadata(&final_path) {
                let mut perms = meta.permissions();
                #[allow(clippy::permissions_set_readonly_false)]
                perms.set_readonly(false);
                let _ = fs::set_permissions(&final_path, perms);
            }
        }
        let write_res = (|| -> io::Result<()> {
            let mut f = File::create(&tmp_path)?;
            f.write_all(full.as_bytes())?;
            f.sync_all()?;
            drop(f);
            let mut perms = fs::metadata(&tmp_path)?.permissions();
            perms.set_readonly(true);
            fs::set_permissions(&tmp_path, perms)?;
            fs::rename(&tmp_path, &final_path)?;
            durlog::sync_dir(cold_dir)
        })();
        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }

        let filter = Filter::build(merged.keys().map(String::as_str), merged.len());
        self.days.write().unwrap().insert(
            day_bucket,
            Day {
                day_bucket,
                path: final_path.clone(),
                filter,
            },
        );
        Ok(final_path)
    }

    /// Look up `mail_id`'s body in `day_bucket` (or across all cold days if
    /// `day_bucket` is not in the index), opening a cold file only when its
    /// in-memory `Filter` reports a hit.
    pub fn read_body(&self, day_bucket: i64, mail_id: &str) -> io::Result<Option<String>> {
        let candidates: Vec<PathBuf> = {
            let guard = self.days.read().unwrap();
            if let Some(entry) = guard.get(&day_bucket) {
                if entry.filter.may_contain(mail_id) {
                    vec![entry.path.clone()]
                } else {
                    Vec::new()
                }
            } else {
                guard
                    .values()
                    .filter(|e| e.filter.may_contain(mail_id))
                    .map(|e| e.path.clone())
                    .collect()
            }
        };
        for path in candidates {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let records = read_file(&path)?;
            for rec in records {
                if rec.mail_id == mail_id {
                    return Ok(Some(rec.body));
                }
            }
        }
        Ok(None)
    }

    /// Check exact membership of `mail_id` across all sealed cold day files,
    /// opening a cold file on disk only when its `Filter` hits.
    pub fn find_record(&self, mail_id: &str) -> io::Result<Option<Record>> {
        let candidates: Vec<PathBuf> = {
            let guard = self.days.read().unwrap();
            guard
                .values()
                .filter(|e| e.filter.may_contain(mail_id))
                .map(|e| e.path.clone())
                .collect()
        };
        for path in candidates {
            self.opens.fetch_add(1, Ordering::SeqCst);
            for rec in read_file(&path)? {
                if rec.mail_id == mail_id {
                    return Ok(Some(rec));
                }
            }
        }
        Ok(None)
    }

    pub fn day_paths(&self) -> Vec<(i64, PathBuf)> {
        self.days
            .read()
            .unwrap()
            .iter()
            .map(|(d, e)| (*d, e.path.clone()))
            .collect()
    }
}

pub fn file_path(cold_dir: &Path, day_bucket: i64) -> PathBuf {
    cold_dir.join(format!("day-{day_bucket}.cold"))
}

pub fn parse_day(path: &Path) -> Option<i64> {
    let stem = path.file_stem()?.to_str()?;
    let rest = stem.strip_prefix("day-")?;
    rest.parse::<i64>().ok()
}

/// Verify the `RWCOLD01` header, the whole-payload `SHA256:` trailer, and each
/// record's `body_sha256` before returning any record.
pub fn read_file(path: &Path) -> io::Result<Vec<Record>> {
    let raw = fs::read_to_string(path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("expected readable cold file {}, saw {e}", path.display()),
        )
    })?;
    if !raw.starts_with(COLD_HEADER) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected cold header RWCOLD01 in {}, saw invalid header",
                path.display()
            ),
        ));
    }
    let trimmed = raw.strip_suffix('\n').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected trailing newline in {}, saw truncated file",
                path.display()
            ),
        )
    })?;
    let (body_part, last_line) = match trimmed.rsplit_once('\n') {
        Some((before, last)) => (format!("{before}\n"), last),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected SHA256 trailer in {}, saw none", path.display()),
            ))
        }
    };
    let expected_hex = last_line.strip_prefix(TRAILER_PREFIX).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected SHA256 trailer in {}, saw {last_line}",
                path.display()
            ),
        )
    })?;
    let actual_hex = hex_digest(body_part.as_bytes());
    if actual_hex != expected_hex {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected cold checksum {expected_hex} in {}, saw {actual_hex}",
                path.display()
            ),
        ));
    }

    let mut out = Vec::new();
    for (idx, line) in body_part[COLD_HEADER.len()..].lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let v = json::parse(line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected valid JSON record at {}:{} , saw {e}",
                    path.display(),
                    idx + 2
                ),
            )
        })?;
        let rec = decode_record(&v, path)?;
        if hex_digest(rec.body.as_bytes()) != rec.body_sha256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected body digest {} for mail {} in {}, saw mismatch",
                    rec.body_sha256,
                    rec.mail_id,
                    path.display()
                ),
            ));
        }
        out.push(rec);
    }
    Ok(out)
}

fn encode_record(r: &Record) -> Value {
    obj(vec![
        ("mail", s(&r.mail_id)),
        ("stream", s(&r.stream)),
        ("article", Value::Int(r.article_num)),
        ("kind", s(&r.kind)),
        ("from", s(&r.sender)),
        ("to", s(&r.recipient)),
        (
            "headers",
            match &r.headers_json {
                Some(h) => s(h),
                None => Value::Null,
            },
        ),
        ("body", s(&r.body)),
        ("digest", s(&r.body_sha256)),
        (
            "accepted_at_ms",
            match r.accepted_at_ms {
                Some(ts) => Value::Int(ts),
                None => Value::Null,
            },
        ),
    ])
}

fn req_str<'a>(v: &'a Value, key: &str, path: &Path) -> io::Result<&'a str> {
    v.get(key).and_then(|x| x.as_str()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected string field {key} in {}, saw missing",
                path.display()
            ),
        )
    })
}

fn decode_record(v: &Value, path: &Path) -> io::Result<Record> {
    let mail_id = req_str(v, "mail", path)?.to_string();
    let stream = req_str(v, "stream", path)?.to_string();
    let article_num = v.get("article").and_then(|x| x.as_i64()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected integer article in {}, saw missing",
                path.display()
            ),
        )
    })?;
    let kind = req_str(v, "kind", path)?.to_string();
    let sender = req_str(v, "from", path)?.to_string();
    let recipient = req_str(v, "to", path)?.to_string();
    let headers_json = match v.get("headers") {
        Some(Value::Str(h)) => Some(h.clone()),
        Some(Value::Null) | None => None,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected string or null headers in {}", path.display()),
            ))
        }
    };
    let body = req_str(v, "body", path)?.to_string();
    let body_sha256 = req_str(v, "digest", path)?.to_string();
    let accepted_at_ms = match v.get("accepted_at_ms") {
        Some(Value::Int(ts)) => Some(*ts),
        Some(Value::Null) | None => None,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected integer or null accepted_at_ms in {}",
                    path.display()
                ),
            ))
        }
    };
    Ok(Record {
        mail_id,
        stream,
        article_num,
        kind,
        sender,
        recipient,
        headers_json,
        body,
        body_sha256,
        accepted_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(id: &str, art: i64, body: &str) -> Record {
        Record {
            mail_id: id.to_string(),
            stream: "m:dest".to_string(),
            article_num: art,
            kind: "directed".to_string(),
            sender: "sender-1".to_string(),
            recipient: "dest".to_string(),
            headers_json: Some(r#"{"x":"1"}"#.to_string()),
            body: body.to_string(),
            body_sha256: hex_digest(body.as_bytes()),
            accepted_at_ms: Some(1_700_000_000_000),
        }
    }

    #[test]
    fn seal_verify_filter_skip_and_tamper_detection() {
        let dir = std::env::temp_dir().join(format!("roger-cold-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let idx = Index::load(&dir).unwrap();
        let recs = vec![
            sample_record("cold-m1", 1, "hello cold tier"),
            sample_record("cold-m2", 2, "second payload ✓"),
        ];
        let path = idx.seal_day(&dir, 19_000, &recs).unwrap();
        assert!(path.exists());

        // Filter miss never opens the file on disk.
        assert_eq!(idx.file_opens(), 0);
        for n in 0..50 {
            let miss = format!("definitely-not-in-cold-{n}");
            let _ = idx.read_body(19_000, &miss).unwrap();
        }
        assert!(
            idx.file_opens() <= 2,
            "Bloom filter should reject almost all misses without opening disk, saw {} opens",
            idx.file_opens()
        );

        // Filter hit reads and verifies the body.
        assert_eq!(
            idx.read_body(19_000, "cold-m2").unwrap().as_deref(),
            Some("second payload ✓")
        );

        // Tampering with a single byte fails checksum verification.
        let mut perms = fs::metadata(&path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(&path, perms).unwrap();
        let mut content = fs::read_to_string(&path).unwrap();
        content = content.replacen("second payload", "tamper payload", 1);
        fs::write(&path, content).unwrap();
        let err = idx.read_body(19_000, "cold-m2").unwrap_err();
        assert!(err.to_string().contains("checksum"), "{err}");

        let _ = fs::remove_dir_all(&dir);
    }
}

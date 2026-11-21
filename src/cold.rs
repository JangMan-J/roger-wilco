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
//!
//! Each day file may have a derived sidecar `day-<bucket>.idx`:
//!   line 1:   `RWCIDX01`
//!   line 2:   JSON `{"size","trailer","bloom","spans"}` holding the cold file's
//!             byte size, its trailer SHA-256 hex, the Bloom filter words, and
//!             `[mail_id, offset, length]` spans of each record's JSON line.
//! A sidecar is used only when the cold file's current size and trailer match;
//! otherwise the cold file is parsed. It is rebuildable and never trusted for
//! body reads, which always verify the whole cold file.

use crate::durlog;
use crate::json::{self, obj, s, Value};
use crate::sha256::{hash_pair, hex_digest};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub const COLD_HEADER: &str = "RWCOLD01\n";
const TRAILER_PREFIX: &str = "SHA256:";
const IDX_HEADER: &str = "RWCIDX01\n";

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
    parses: AtomicU64,
}

impl Index {
    /// Read-only load: uses a valid sidecar when present, else parses the
    /// cold file in memory. Never writes.
    pub fn load(cold_dir: &Path) -> io::Result<Self> {
        Self::load_with(cold_dir, false)
    }

    /// Like `load`, but writes or rebuilds a missing or stale sidecar per day.
    /// Sidecar write failures are ignored: it is derived data.
    #[allow(dead_code)]
    pub fn load_indexed(cold_dir: &Path) -> io::Result<Self> {
        Self::load_with(cold_dir, true)
    }

    fn load_with(cold_dir: &Path, write_index: bool) -> io::Result<Self> {
        let mut map = BTreeMap::new();
        let mut parses = 0u64;
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
                    let filter = match load_sidecar(&path) {
                        Some(filter) => filter,
                        None => {
                            parses += 1;
                            let parsed = parse_cold(&path)?;
                            let filter = Filter::build(
                                parsed.records.iter().map(|r| r.mail_id.as_str()),
                                parsed.records.len(),
                            );
                            if write_index {
                                let spans: Vec<(&str, usize, usize)> = parsed
                                    .records
                                    .iter()
                                    .zip(&parsed.spans)
                                    .map(|(r, &(off, len))| (r.mail_id.as_str(), off, len))
                                    .collect();
                                let _ = write_sidecar(
                                    &path,
                                    parsed.size,
                                    &parsed.trailer,
                                    &filter,
                                    &spans,
                                );
                            }
                            filter
                        }
                    };
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
            parses: AtomicU64::new(parses),
        })
    }

    /// Number of cold file disk opens performed through `read_body` / `contains_id`.
    pub fn file_opens(&self) -> u64 {
        self.opens.load(Ordering::SeqCst)
    }

    /// Number of full cold file parses done through this Index, load included.
    #[allow(dead_code)]
    pub fn file_parses(&self) -> u64 {
        self.parses.load(Ordering::SeqCst)
    }

    fn parse(&self, path: &Path) -> io::Result<Vec<Record>> {
        self.parses.fetch_add(1, Ordering::SeqCst);
        read_file(path)
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
            for existing in self.parse(&final_path)? {
                merged.insert(existing.mail_id.clone(), existing);
            }
        }
        for rec in records {
            merged.insert(rec.mail_id.clone(), rec.clone());
        }

        let mut payload = String::from(COLD_HEADER);
        let mut spans: Vec<(&str, usize, usize)> = Vec::with_capacity(merged.len());
        for rec in merged.values() {
            let line = json::stringify(&encode_record(rec));
            spans.push((rec.mail_id.as_str(), payload.len(), line.len()));
            payload.push_str(&line);
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
        let _ = write_sidecar(&final_path, full.len(), &digest, &filter, &spans);
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
            let records = self.parse(&path)?;
            for rec in records {
                if rec.mail_id == mail_id {
                    return Ok(Some(rec.body));
                }
            }
        }
        Ok(None)
    }

    /// Bodies for `(day_bucket, mail_id)` pairs, keyed by mail_id. Wants are
    /// grouped by day so each candidate file is read, verified and parsed at
    /// most once per call; an unindexed day falls back across every day's
    /// filter, as `read_body` does. Filter misses never open a file.
    // ## YAGNI: reads parse the whole verified file; sidecar spans are not
    // consulted here because the whole-file SHA-256 read dominates anyway.
    #[allow(dead_code)]
    pub fn read_bodies(&self, wants: &[(i64, &str)]) -> io::Result<HashMap<String, String>> {
        let mut by_day: BTreeMap<i64, (PathBuf, HashSet<&str>)> = BTreeMap::new();
        {
            let guard = self.days.read().unwrap();
            let mut add = |e: &Day, id| {
                by_day
                    .entry(e.day_bucket)
                    .or_insert_with(|| (e.path.clone(), HashSet::new()))
                    .1
                    .insert(id);
            };
            for &(day, id) in wants {
                match guard.get(&day) {
                    Some(e) if e.filter.may_contain(id) => add(e, id),
                    Some(_) => {}
                    None => guard
                        .values()
                        .filter(|e| e.filter.may_contain(id))
                        .for_each(|e| add(e, id)),
                }
            }
        }
        let mut out = HashMap::new();
        for (path, mut ids) in by_day.into_values() {
            ids.retain(|id| !out.contains_key(*id));
            if ids.is_empty() {
                continue;
            }
            self.opens.fetch_add(1, Ordering::SeqCst);
            for rec in self.parse(&path)? {
                if ids.contains(rec.mail_id.as_str()) {
                    out.insert(rec.mail_id, rec.body);
                }
            }
        }
        Ok(out)
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
            for rec in self.parse(&path)? {
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
    parse_cold(path).map(|p| p.records)
}

/// A verified cold file: records, each record line's `(offset, length)`,
/// the file's byte size, and its trailer digest hex.
struct Parsed {
    records: Vec<Record>,
    spans: Vec<(usize, usize)>,
    size: usize,
    trailer: String,
}

fn parse_cold(path: &Path) -> io::Result<Parsed> {
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
    let mut spans = Vec::new();
    let mut offset = COLD_HEADER.len();
    for (idx, chunk) in body_part[COLD_HEADER.len()..]
        .split_inclusive('\n')
        .enumerate()
    {
        let start = offset;
        offset += chunk.len();
        let line = chunk.strip_suffix('\n').unwrap_or(chunk);
        let line = line.strip_suffix('\r').unwrap_or(line);
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
        spans.push((start, line.len()));
    }
    Ok(Parsed {
        records: out,
        spans,
        size: raw.len(),
        trailer: expected_hex.to_string(),
    })
}

fn sidecar_path(cold_path: &Path) -> PathBuf {
    cold_path.with_extension("idx")
}

/// Best-effort write of the derived sidecar for `cold_path` (tmp, fsync, rename).
fn write_sidecar(
    cold_path: &Path,
    size: usize,
    trailer: &str,
    filter: &Filter,
    spans: &[(&str, usize, usize)],
) -> io::Result<()> {
    let to_int = |n: usize| Value::Int(i64::try_from(n).unwrap_or(i64::MAX));
    let body = obj(vec![
        ("size", to_int(size)),
        ("trailer", s(trailer)),
        (
            "bloom",
            Value::Arr(filter.bits.iter().map(|&w| Value::Int(w as i64)).collect()),
        ),
        (
            "spans",
            Value::Arr(
                spans
                    .iter()
                    .map(|&(id, off, len)| Value::Arr(vec![s(id), to_int(off), to_int(len)]))
                    .collect(),
            ),
        ),
    ]);
    let mut text = String::from(IDX_HEADER);
    text.push_str(&json::stringify(&body));
    text.push('\n');

    let final_path = sidecar_path(cold_path);
    let dir = cold_path.parent().unwrap_or(Path::new("."));
    let name = final_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("day.idx");
    let tmp_path = dir.join(format!("{name}.tmp.{}", std::process::id()));
    let res = (|| -> io::Result<()> {
        let mut f = File::create(&tmp_path)?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp_path, &final_path)?;
        durlog::sync_dir(dir)
    })();
    if res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    res
}

/// The Bloom filter from `cold_path`'s sidecar, if the sidecar parses and the
/// cold file's current size and trailer digest match it; `None` means stale.
fn load_sidecar(cold_path: &Path) -> Option<Filter> {
    let text = fs::read_to_string(sidecar_path(cold_path)).ok()?;
    let v = json::parse(text.strip_prefix(IDX_HEADER)?.strip_suffix('\n')?).ok()?;
    let size = u64::try_from(v.get("size")?.as_i64()?).ok()?;
    let trailer = v.get("trailer")?.as_str()?;
    let Some(Value::Arr(words)) = v.get("bloom") else {
        return None;
    };
    let bits = words
        .iter()
        .map(|w| w.as_i64().map(|w| w as u64))
        .collect::<Option<Vec<u64>>>()?;
    if bits.is_empty() || !matches!(v.get("spans"), Some(Value::Arr(_))) {
        return None;
    }

    let expect = format!("{TRAILER_PREFIX}{trailer}\n");
    let mut f = File::open(cold_path).ok()?;
    if f.metadata().ok()?.len() != size || size < expect.len() as u64 {
        return None;
    }
    f.seek(SeekFrom::End(-(expect.len() as i64))).ok()?;
    let mut tail = vec![0u8; expect.len()];
    f.read_exact(&mut tail).ok()?;
    if tail != expect.as_bytes() {
        return None;
    }
    let num_bits = bits.len() as u64 * 64;
    Some(Filter { bits, num_bits })
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

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("roger-cold-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn make_writable(path: &Path) {
        let mut perms = fs::metadata(path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(path, perms).unwrap();
    }

    #[test]
    fn read_bodies_opens_each_day_once_and_detects_tamper() {
        let dir = temp_dir("batch");
        let idx = Index::load(&dir).unwrap();
        let day1: Vec<Record> = (0..8)
            .map(|n| sample_record(&format!("b-{n}"), n, &format!("body {n}")))
            .collect();
        let path = idx.seal_day(&dir, 1, &day1).unwrap();
        idx.seal_day(&dir, 2, &[sample_record("other", 9, "other body")])
            .unwrap();

        let (opens, parses) = (idx.file_opens(), idx.file_parses());
        let wants: Vec<(i64, &str)> = day1.iter().map(|r| (1, r.mail_id.as_str())).collect();
        let got = idx.read_bodies(&wants).unwrap();
        assert_eq!(idx.file_opens() - opens, 1);
        assert_eq!(idx.file_parses() - parses, 1);
        assert_eq!(got.len(), 8);
        for r in &day1 {
            assert_eq!(got.get(&r.mail_id), Some(&r.body));
        }

        // Unindexed day falls back across all filters; misses open nothing.
        let got = idx.read_bodies(&[(999, "other")]).unwrap();
        assert_eq!(got.get("other").map(String::as_str), Some("other body"));
        let opens = idx.file_opens();
        let misses: Vec<String> = (0..50).map(|n| format!("absent-{n}")).collect();
        let wants: Vec<(i64, &str)> = misses.iter().map(|m| (1, m.as_str())).collect();
        assert!(idx.read_bodies(&wants).unwrap().is_empty());
        assert!(idx.file_opens() - opens <= 1);

        make_writable(&path);
        let content = fs::read_to_string(&path)
            .unwrap()
            .replacen("body 3", "bodx 3", 1);
        fs::write(&path, content).unwrap();
        let err = idx.read_bodies(&[(1, "b-3")]).unwrap_err();
        assert!(err.to_string().contains("checksum"), "{err}");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sidecar_written_used_and_rebuilt_when_stale() {
        let dir = temp_dir("sidecar");
        let recs = vec![
            sample_record("s-1", 1, "first"),
            sample_record("s-2", 2, "second"),
        ];
        let cold = Index::load(&dir).unwrap().seal_day(&dir, 7, &recs).unwrap();
        let idx_path = dir.join("day-7.idx");
        assert!(idx_path.exists(), "seal_day writes the sidecar");

        // `load` never writes: without a sidecar it parses and leaves none.
        fs::remove_file(&idx_path).unwrap();
        let plain = Index::load(&dir).unwrap();
        assert_eq!(plain.file_parses(), 1);
        assert!(!idx_path.exists());

        let built = Index::load_indexed(&dir).unwrap();
        assert_eq!(built.file_parses(), 1);
        assert!(idx_path.exists());

        let fast = Index::load(&dir).unwrap();
        assert_eq!(fast.file_parses(), 0);
        let got = fast.read_bodies(&[(7, "s-1"), (7, "s-2")]).unwrap();
        assert_eq!(got.get("s-1").map(String::as_str), Some("first"));
        assert_eq!(got.get("s-2").map(String::as_str), Some("second"));

        // Corrupt sidecar: parse, then rebuild.
        fs::write(&idx_path, "RWCIDX01\n{not json\n").unwrap();
        assert_eq!(Index::load(&dir).unwrap().file_parses(), 1);
        assert_eq!(Index::load_indexed(&dir).unwrap().file_parses(), 1);
        assert_eq!(Index::load(&dir).unwrap().file_parses(), 0);

        // Same size, different trailer: stale, so the cold file is parsed
        // (and fails verification) rather than trusting the sidecar.
        let original = fs::read(&cold).unwrap();
        make_writable(&cold);
        let mut bad = original.clone();
        let n = bad.len();
        bad[n - 2] = if bad[n - 2] == b'0' { b'1' } else { b'0' };
        fs::write(&cold, &bad).unwrap();
        let err = Index::load(&dir)
            .err()
            .expect("stale sidecar must not be used");
        assert!(err.to_string().contains("checksum"), "{err}");

        // Different content and size (sealed elsewhere, copied in): rebuild.
        let other = temp_dir("sidecar-other");
        let new_recs = vec![sample_record("s-3", 3, "third body, longer")];
        let other_cold = Index::load(&other)
            .unwrap()
            .seal_day(&other, 7, &new_recs)
            .unwrap();
        fs::remove_file(&cold).unwrap();
        fs::copy(&other_cold, &cold).unwrap();
        let rebuilt = Index::load_indexed(&dir).unwrap();
        assert_eq!(rebuilt.file_parses(), 1);
        let fast = Index::load(&dir).unwrap();
        assert_eq!(fast.file_parses(), 0);
        let got = fast.read_bodies(&[(7, "s-3"), (7, "s-1")]).unwrap();
        assert_eq!(
            got.get("s-3").map(String::as_str),
            Some("third body, longer")
        );
        assert!(!got.contains_key("s-1"));

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&other);
    }
}

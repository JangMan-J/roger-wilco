use crate::format::{is_lower_hex_64, parse_day, verify_file};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const MANIFEST_SCHEME: &str = "roger-cold-manifest/1";
pub const MANIFEST_FILE: &str = "manifest.json";

static TMP_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub scheme: String,
    pub total_files: usize,
    pub total_bytes: u64,
    pub total_records: usize,
    pub slices: BTreeMap<String, Vec<ManifestDay>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestDay {
    pub day_bucket: i64,
    pub file: String,
    pub bytes: u64,
    pub records: usize,
    pub sha256: String,
    pub trailer_sha256: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShipAction {
    Shipped,
    Skipped,
}

impl ShipAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Shipped => "shipped",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShipFile {
    pub slice: String,
    pub day_bucket: i64,
    pub action: ShipAction,
    pub bytes: u64,
    pub records: usize,
    pub sha256: String,
    pub trailer_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShipSummary {
    pub shipped: usize,
    pub skipped: usize,
    pub slices: usize,
    pub total_files: usize,
    pub total_bytes: u64,
    pub total_records: usize,
    pub files: Vec<ShipFile>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestoreAction {
    Restored,
    Skipped,
}

impl RestoreAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Restored => "restored",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreFile {
    pub slice: String,
    pub day_bucket: i64,
    pub action: RestoreAction,
    pub bytes: u64,
    pub records: usize,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreSummary {
    pub slice: String,
    pub restored: usize,
    pub skipped: usize,
    pub files: Vec<RestoreFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyFile {
    pub slice: String,
    pub day_bucket: i64,
    pub status: String,
    pub bytes: u64,
    pub records: usize,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Issue {
    pub kind: String,
    pub file: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifySummary {
    pub intact: bool,
    pub slices: usize,
    pub files_checked: usize,
    pub files_verified: usize,
    pub files_failed: usize,
    pub bytes: u64,
    pub records: usize,
    pub files: Vec<VerifyFile>,
    pub issues: Vec<Issue>,
}

pub fn is_safe_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

fn sync_dir(dir: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("syncing directory {}: {e}", dir.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

fn write_atomic(dest: &Path, bytes: &[u8], readonly: bool) -> Result<(), String> {
    let parent = dest
        .parent()
        .ok_or_else(|| format!("expected parent directory for {}", dest.display()))?;
    fs::create_dir_all(parent)
        .map_err(|e| format!("creating directory {}: {e}", parent.display()))?;
    let base_name = dest
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("expected UTF-8 filename for {}", dest.display()))?;
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_path = parent.join(format!(".{base_name}.tmp.{}.{seq}", std::process::id()));

    #[cfg(windows)]
    if dest.exists() {
        if let Ok(meta) = fs::metadata(dest) {
            let mut perms = meta.permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            let _ = fs::set_permissions(dest, perms);
        }
    }

    let res = (|| -> Result<(), String> {
        let mut f = File::create(&tmp_path)
            .map_err(|e| format!("creating temp file {}: {e}", tmp_path.display()))?;
        f.write_all(bytes)
            .map_err(|e| format!("writing temp file {}: {e}", tmp_path.display()))?;
        f.sync_all()
            .map_err(|e| format!("syncing temp file {}: {e}", tmp_path.display()))?;
        drop(f);

        if readonly {
            let mut perms = fs::metadata(&tmp_path)
                .map_err(|e| format!("stat temp file {}: {e}", tmp_path.display()))?
                .permissions();
            perms.set_readonly(true);
            fs::set_permissions(&tmp_path, perms)
                .map_err(|e| format!("setting read-only on {}: {e}", tmp_path.display()))?;
        }

        fs::rename(&tmp_path, dest).map_err(|e| {
            format!(
                "renaming {} into {}: {e}",
                tmp_path.display(),
                dest.display()
            )
        })?;
        sync_dir(parent)
    })();

    if res.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    res
}

fn ensure_readonly(path: &Path) -> Result<(), String> {
    let meta =
        fs::metadata(path).map_err(|e| format!("stat archived file {}: {e}", path.display()))?;
    let mut perms = meta.permissions();
    if !perms.readonly() {
        perms.set_readonly(true);
        fs::set_permissions(path, perms)
            .map_err(|e| format!("setting read-only on {}: {e}", path.display()))?;
    }
    Ok(())
}

fn collect_cold_dir(
    cold_dir: &Path,
    slice_name: &str,
    out: &mut BTreeMap<(String, i64), PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(cold_dir).map_err(|e| {
        format!(
            "expected readable cold directory {}, saw {e}",
            cold_dir.display()
        )
    })?;
    for item in entries {
        let path = item
            .map_err(|e| format!("reading cold directory {}: {e}", cold_dir.display()))?
            .path();
        if !path.is_file() {
            continue;
        }
        if let Some(day) = parse_day(&path) {
            out.insert((slice_name.to_string(), day), path);
        }
    }
    Ok(())
}

/// Discover `slice-*/cold/day-*.cold` and/or single-slice `cold/day-*.cold` under `slices_dir`.
pub fn scan_shards(slices_dir: &Path) -> Result<Vec<(String, i64, PathBuf)>, String> {
    let meta = fs::metadata(slices_dir).map_err(|e| {
        format!(
            "expected readable slices directory {}, saw {e}",
            slices_dir.display()
        )
    })?;
    if !meta.is_dir() {
        return Err(format!(
            "expected directory at --slices-dir {}, saw non-directory",
            slices_dir.display()
        ));
    }

    let mut found: BTreeMap<(String, i64), PathBuf> = BTreeMap::new();

    let single_cold = slices_dir.join("cold");
    if single_cold.is_dir() {
        let slice_name = slices_dir
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| n.starts_with("slice-") && is_safe_component(n))
            .unwrap_or("slice-00");
        collect_cold_dir(&single_cold, slice_name, &mut found)?;
    }

    let entries = fs::read_dir(slices_dir)
        .map_err(|e| format!("reading slices directory {}: {e}", slices_dir.display()))?;
    for item in entries {
        let entry = item.map_err(|e| format!("reading entry in {}: {e}", slices_dir.display()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with("slice-") || !is_safe_component(name) {
            continue;
        }
        let cold_dir = path.join("cold");
        if cold_dir.is_dir() {
            collect_cold_dir(&cold_dir, name, &mut found)?;
        }
    }

    Ok(found
        .into_iter()
        .map(|((slice, day), path)| (slice, day, path))
        .collect())
}

fn validate_manifest_structure(manifest: &Manifest, origin: &Path) -> Result<(), String> {
    if manifest.scheme != MANIFEST_SCHEME {
        return Err(format!(
            "expected manifest scheme {MANIFEST_SCHEME} in {}, saw {}",
            origin.display(),
            manifest.scheme
        ));
    }
    let mut calc_files = 0usize;
    let mut calc_bytes = 0u64;
    let mut calc_records = 0usize;

    for (slice, days) in &manifest.slices {
        if !is_safe_component(slice) {
            return Err(format!(
                "expected safe slice name in {}, saw {slice:?}",
                origin.display()
            ));
        }
        let mut prev_day: Option<i64> = None;
        for d in days {
            if let Some(prev) = prev_day {
                if d.day_bucket <= prev {
                    return Err(format!(
                        "expected strictly ascending day_bucket in {} slice {slice}, saw {} after {prev}",
                        origin.display(),
                        d.day_bucket
                    ));
                }
            }
            prev_day = Some(d.day_bucket);

            let expected_rel = format!("{slice}/day-{}.cold", d.day_bucket);
            if d.file != expected_rel {
                return Err(format!(
                    "expected manifest file path {expected_rel} in {}, saw {}",
                    origin.display(),
                    d.file
                ));
            }
            if !is_lower_hex_64(&d.sha256) {
                return Err(format!(
                    "expected 64-hex sha256 for {expected_rel} in {}, saw {}",
                    origin.display(),
                    d.sha256
                ));
            }
            if !is_lower_hex_64(&d.trailer_sha256) {
                return Err(format!(
                    "expected 64-hex trailer_sha256 for {expected_rel} in {}, saw {}",
                    origin.display(),
                    d.trailer_sha256
                ));
            }
            calc_files += 1;
            calc_bytes = calc_bytes.saturating_add(d.bytes);
            calc_records = calc_records.saturating_add(d.records);
        }
    }

    if manifest.total_files != calc_files
        || manifest.total_bytes != calc_bytes
        || manifest.total_records != calc_records
    {
        return Err(format!(
            "expected manifest totals ({calc_files}, {calc_bytes}, {calc_records}) in {}, saw ({}, {}, {})",
            origin.display(),
            manifest.total_files,
            manifest.total_bytes,
            manifest.total_records
        ));
    }
    Ok(())
}

pub fn load_manifest(dest_dir: &Path) -> Result<Manifest, String> {
    let manifest_path = dest_dir.join(MANIFEST_FILE);
    let raw = fs::read_to_string(&manifest_path).map_err(|e| {
        format!(
            "expected readable manifest at {}, saw {e}",
            manifest_path.display()
        )
    })?;
    let manifest: Manifest = serde_json::from_str(&raw).map_err(|e| {
        format!(
            "expected valid JSON manifest at {}, saw {e}",
            manifest_path.display()
        )
    })?;
    validate_manifest_structure(&manifest, &manifest_path)?;
    Ok(manifest)
}

fn build_manifest_from_map(map: BTreeMap<(String, i64), ManifestDay>) -> Manifest {
    let mut slices: BTreeMap<String, Vec<ManifestDay>> = BTreeMap::new();
    let mut total_files = 0usize;
    let mut total_bytes = 0u64;
    let mut total_records = 0usize;
    for ((slice, _day), entry) in map {
        total_files += 1;
        total_bytes += entry.bytes;
        total_records += entry.records;
        slices.entry(slice).or_default().push(entry);
    }
    Manifest {
        scheme: MANIFEST_SCHEME.to_string(),
        total_files,
        total_bytes,
        total_records,
        slices,
    }
}

/// Verify all discovered cold day files under `slices_dir`, atomically copy unshipped or
/// mismatched files into `dest_dir/<slice>/day-<bucket>.cold` with read-only permissions,
/// and atomically update `dest_dir/manifest.json`.
pub fn ship(slices_dir: &Path, dest_dir: &Path) -> Result<ShipSummary, String> {
    let candidates = scan_shards(slices_dir)?;

    // Verify EVERY candidate source cold file before mutating dest_dir so any tampered
    // local cold file is rejected cleanly with zero partial archive writes.
    let mut verified_sources = Vec::with_capacity(candidates.len());
    for (slice, day_bucket, src_path) in candidates {
        let (raw, verified) = verify_file(&src_path)?;
        verified_sources.push((slice, day_bucket, raw, verified));
    }

    fs::create_dir_all(dest_dir)
        .map_err(|e| format!("creating archive directory {}: {e}", dest_dir.display()))?;

    let manifest_path = dest_dir.join(MANIFEST_FILE);
    let existing_raw =
        if manifest_path.exists() {
            Some(fs::read(&manifest_path).map_err(|e| {
                format!("reading existing manifest {}: {e}", manifest_path.display())
            })?)
        } else {
            None
        };

    let mut manifest_map: BTreeMap<(String, i64), ManifestDay> = BTreeMap::new();
    if manifest_path.exists() {
        let existing = load_manifest(dest_dir)?;
        for (slice, days) in existing.slices {
            for d in days {
                manifest_map.insert((slice.clone(), d.day_bucket), d);
            }
        }
    }

    let mut shipped = 0usize;
    let mut skipped = 0usize;
    let mut reports = Vec::with_capacity(verified_sources.len());

    for (slice, day_bucket, raw, verified) in verified_sources {
        let rel_file = format!("{slice}/day-{day_bucket}.cold");
        let dest_file = dest_dir.join(&slice).join(format!("day-{day_bucket}.cold"));

        let already_matches = if dest_file.exists() {
            match verify_file(&dest_file) {
                Ok((_, dest_verified)) => dest_verified.file_sha256 == verified.file_sha256,
                Err(_) => false,
            }
        } else {
            false
        };

        let action = if already_matches {
            ensure_readonly(&dest_file)?;
            skipped += 1;
            ShipAction::Skipped
        } else {
            write_atomic(&dest_file, &raw, true)?;
            shipped += 1;
            ShipAction::Shipped
        };

        manifest_map.insert(
            (slice.clone(), day_bucket),
            ManifestDay {
                day_bucket,
                file: rel_file,
                bytes: verified.byte_size,
                records: verified.record_count,
                sha256: verified.file_sha256.clone(),
                trailer_sha256: verified.trailer_sha256.clone(),
            },
        );

        reports.push(ShipFile {
            slice,
            day_bucket,
            action,
            bytes: verified.byte_size,
            records: verified.record_count,
            sha256: verified.file_sha256,
            trailer_sha256: verified.trailer_sha256,
        });
    }

    let updated_manifest = build_manifest_from_map(manifest_map);
    let mut encoded = serde_json::to_vec_pretty(&updated_manifest)
        .map_err(|e| format!("serializing manifest: {e}"))?;
    encoded.push(b'\n');

    if existing_raw.as_deref() != Some(encoded.as_slice()) {
        write_atomic(&manifest_path, &encoded, false)?;
    }

    Ok(ShipSummary {
        shipped,
        skipped,
        slices: updated_manifest.slices.len(),
        total_files: updated_manifest.total_files,
        total_bytes: updated_manifest.total_bytes,
        total_records: updated_manifest.total_records,
        files: reports,
    })
}

fn collect_archive_cold_files(dir: &Path, acc: &mut BTreeSet<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("reading archive directory {}: {e}", dir.display()))?;
    for item in entries {
        let path = item
            .map_err(|e| format!("reading entry in {}: {e}", dir.display()))?
            .path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect_archive_cold_files(&path, acc)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("cold") {
            acc.insert(path);
        }
    }
    Ok(())
}

/// Verify `<dest_dir>/manifest.json` and every archived `day-*.cold` file's whole-file digest,
/// `SHA256:` trailer, and per-record `digest`, plus detect any missing or unlisted cold file.
pub fn verify_archive(dest_dir: &Path) -> VerifySummary {
    let manifest_path = dest_dir.join(MANIFEST_FILE);
    let mut issues = Vec::new();
    let mut files = Vec::new();

    if !dest_dir.exists() {
        issues.push(Issue {
            kind: "missing_archive".to_string(),
            file: dest_dir.display().to_string(),
            detail: format!(
                "expected archive directory at {}, saw missing",
                dest_dir.display()
            ),
        });
        return VerifySummary {
            intact: false,
            slices: 0,
            files_checked: 0,
            files_verified: 0,
            files_failed: 0,
            bytes: 0,
            records: 0,
            files,
            issues,
        };
    }

    if !manifest_path.exists() {
        issues.push(Issue {
            kind: "missing_manifest".to_string(),
            file: MANIFEST_FILE.to_string(),
            detail: format!(
                "expected manifest at {}, saw missing",
                manifest_path.display()
            ),
        });
        return VerifySummary {
            intact: false,
            slices: 0,
            files_checked: 0,
            files_verified: 0,
            files_failed: 0,
            bytes: 0,
            records: 0,
            files,
            issues,
        };
    }

    let manifest = match load_manifest(dest_dir) {
        Ok(m) => m,
        Err(detail) => {
            issues.push(Issue {
                kind: "invalid_manifest".to_string(),
                file: MANIFEST_FILE.to_string(),
                detail,
            });
            return VerifySummary {
                intact: false,
                slices: 0,
                files_checked: 0,
                files_verified: 0,
                files_failed: 0,
                bytes: 0,
                records: 0,
                files,
                issues,
            };
        }
    };

    let mut expected_paths = BTreeSet::new();
    let mut files_checked = 0usize;
    let mut files_verified = 0usize;
    let mut files_failed = 0usize;
    let mut verified_bytes = 0u64;
    let mut verified_records = 0usize;

    for (slice, days) in &manifest.slices {
        for entry in days {
            files_checked += 1;
            let file_path = dest_dir
                .join(slice)
                .join(format!("day-{}.cold", entry.day_bucket));
            expected_paths.insert(file_path.clone());

            if !file_path.exists() {
                let detail = format!(
                    "expected archived cold file {}, saw missing",
                    file_path.display()
                );
                files_failed += 1;
                issues.push(Issue {
                    kind: "missing_file".to_string(),
                    file: entry.file.clone(),
                    detail: detail.clone(),
                });
                files.push(VerifyFile {
                    slice: slice.clone(),
                    day_bucket: entry.day_bucket,
                    status: "failed".to_string(),
                    bytes: entry.bytes,
                    records: entry.records,
                    sha256: entry.sha256.clone(),
                    detail: Some(detail),
                });
                continue;
            }

            match verify_file(&file_path) {
                Err(detail) => {
                    files_failed += 1;
                    issues.push(Issue {
                        kind: "corrupt_file".to_string(),
                        file: entry.file.clone(),
                        detail: detail.clone(),
                    });
                    files.push(VerifyFile {
                        slice: slice.clone(),
                        day_bucket: entry.day_bucket,
                        status: "failed".to_string(),
                        bytes: entry.bytes,
                        records: entry.records,
                        sha256: entry.sha256.clone(),
                        detail: Some(detail),
                    });
                }
                Ok((_, verified)) => {
                    let mut mismatches = Vec::new();
                    if verified.file_sha256 != entry.sha256 {
                        mismatches.push(format!(
                            "expected file sha256 {}, saw {}",
                            entry.sha256, verified.file_sha256
                        ));
                    }
                    if verified.trailer_sha256 != entry.trailer_sha256 {
                        mismatches.push(format!(
                            "expected trailer sha256 {}, saw {}",
                            entry.trailer_sha256, verified.trailer_sha256
                        ));
                    }
                    if verified.byte_size != entry.bytes {
                        mismatches.push(format!(
                            "expected byte size {}, saw {}",
                            entry.bytes, verified.byte_size
                        ));
                    }
                    if verified.record_count != entry.records {
                        mismatches.push(format!(
                            "expected record count {}, saw {}",
                            entry.records, verified.record_count
                        ));
                    }

                    if mismatches.is_empty() {
                        files_verified += 1;
                        verified_bytes += verified.byte_size;
                        verified_records += verified.record_count;
                        files.push(VerifyFile {
                            slice: slice.clone(),
                            day_bucket: entry.day_bucket,
                            status: "verified".to_string(),
                            bytes: verified.byte_size,
                            records: verified.record_count,
                            sha256: verified.file_sha256,
                            detail: None,
                        });
                    } else {
                        let detail = mismatches.join("; ");
                        files_failed += 1;
                        issues.push(Issue {
                            kind: "manifest_mismatch".to_string(),
                            file: entry.file.clone(),
                            detail: detail.clone(),
                        });
                        files.push(VerifyFile {
                            slice: slice.clone(),
                            day_bucket: entry.day_bucket,
                            status: "failed".to_string(),
                            bytes: verified.byte_size,
                            records: verified.record_count,
                            sha256: verified.file_sha256,
                            detail: Some(detail),
                        });
                    }
                }
            }
        }
    }

    let mut disk_cold_files = BTreeSet::new();
    match collect_archive_cold_files(dest_dir, &mut disk_cold_files) {
        Ok(()) => {
            for extra in disk_cold_files.difference(&expected_paths) {
                let rel = extra
                    .strip_prefix(dest_dir)
                    .unwrap_or(extra.as_path())
                    .display()
                    .to_string();
                issues.push(Issue {
                    kind: "unlisted_file".to_string(),
                    file: rel.clone(),
                    detail: format!("archived cold file {rel} is absent from {MANIFEST_FILE}"),
                });
            }
        }
        Err(detail) => {
            issues.push(Issue {
                kind: "scan_failed".to_string(),
                file: dest_dir.display().to_string(),
                detail,
            });
        }
    }

    let intact = issues.is_empty() && files_failed == 0;
    VerifySummary {
        intact,
        slices: manifest.slices.len(),
        files_checked,
        files_verified,
        files_failed,
        bytes: verified_bytes,
        records: verified_records,
        files,
        issues,
    }
}

/// Verify archived cold day files for `slice_name` (optionally restricted to `day_filter`)
/// against `manifest.json`, `SHA256:` trailer, and per-record `digest`, then atomically
/// restore missing `day-<bucket>.cold` files into `cold_dir`.
pub fn restore(
    dest_dir: &Path,
    slice_name: &str,
    cold_dir: &Path,
    day_filter: Option<i64>,
) -> Result<RestoreSummary, String> {
    if !is_safe_component(slice_name) {
        return Err(format!("expected safe --slice name, saw {slice_name:?}"));
    }
    let manifest = load_manifest(dest_dir)?;
    let slice_days = manifest.slices.get(slice_name).ok_or_else(|| {
        format!(
            "expected slice {slice_name} in {}/{MANIFEST_FILE}, saw missing",
            dest_dir.display()
        )
    })?;

    let targets: Vec<&ManifestDay> = match day_filter {
        Some(day) => {
            let entry = slice_days
                .iter()
                .find(|d| d.day_bucket == day)
                .ok_or_else(|| {
                    format!(
                        "expected day {day} for slice {slice_name} in {}/{MANIFEST_FILE}, saw missing",
                        dest_dir.display()
                    )
                })?;
            vec![entry]
        }
        None => slice_days.iter().collect(),
    };

    // Verify all requested archived cold files against manifest + trailer + body digests
    // before writing anything into cold_dir.
    let mut verified_targets = Vec::with_capacity(targets.len());
    for entry in targets {
        let archive_path = dest_dir
            .join(slice_name)
            .join(format!("day-{}.cold", entry.day_bucket));
        let (raw, verified) = verify_file(&archive_path)?;
        if verified.file_sha256 != entry.sha256
            || verified.trailer_sha256 != entry.trailer_sha256
            || verified.byte_size != entry.bytes
            || verified.record_count != entry.records
        {
            return Err(format!(
                "expected archived {} to match manifest (sha256={}, trailer={}, bytes={}, records={}), saw (sha256={}, trailer={}, bytes={}, records={})",
                archive_path.display(),
                entry.sha256,
                entry.trailer_sha256,
                entry.bytes,
                entry.records,
                verified.file_sha256,
                verified.trailer_sha256,
                verified.byte_size,
                verified.record_count
            ));
        }
        verified_targets.push((entry, raw));
    }

    fs::create_dir_all(cold_dir)
        .map_err(|e| format!("creating local cold directory {}: {e}", cold_dir.display()))?;

    let mut restored = 0usize;
    let mut skipped = 0usize;
    let mut reports = Vec::with_capacity(verified_targets.len());

    for (entry, raw) in verified_targets {
        let local_path = cold_dir.join(format!("day-{}.cold", entry.day_bucket));
        let action = if local_path.exists() {
            let (_, local_verified) = verify_file(&local_path)?;
            if local_verified.file_sha256 != entry.sha256 {
                return Err(format!(
                    "expected existing local cold file {} to match archived sha256 {}, saw {}",
                    local_path.display(),
                    entry.sha256,
                    local_verified.file_sha256
                ));
            }
            skipped += 1;
            RestoreAction::Skipped
        } else {
            write_atomic(&local_path, &raw, true)?;
            restored += 1;
            RestoreAction::Restored
        };

        reports.push(RestoreFile {
            slice: slice_name.to_string(),
            day_bucket: entry.day_bucket,
            action,
            bytes: entry.bytes,
            records: entry.records,
            sha256: entry.sha256.clone(),
        });
    }

    Ok(RestoreSummary {
        slice: slice_name.to_string(),
        restored,
        skipped,
        files: reports,
    })
}

pub fn format_ship(summary: &ShipSummary, json_out: bool) -> Result<String, String> {
    if json_out {
        let v = serde_json::json!({
            "type": "ship",
            "status": "ok",
            "shipped": summary.shipped,
            "skipped": summary.skipped,
            "slices": summary.slices,
            "total_files": summary.total_files,
            "total_bytes": summary.total_bytes,
            "total_records": summary.total_records,
            "files": summary.files,
        });
        return serde_json::to_string(&v).map_err(|e| format!("encoding ship JSON: {e}"));
    }
    let mut lines = Vec::with_capacity(summary.files.len() + 1);
    for f in &summary.files {
        lines.push(format!(
            "SHIP_FILE slice={} day={} action={} bytes={} records={} sha256={}",
            f.slice,
            f.day_bucket,
            f.action.as_str(),
            f.bytes,
            f.records,
            f.sha256
        ));
    }
    lines.push(format!(
        "SHIP status=ok shipped={} skipped={} slices={} files={} bytes={} records={}",
        summary.shipped,
        summary.skipped,
        summary.slices,
        summary.total_files,
        summary.total_bytes,
        summary.total_records
    ));
    Ok(lines.join("\n"))
}

pub fn format_restore(summary: &RestoreSummary, json_out: bool) -> Result<String, String> {
    if json_out {
        let v = serde_json::json!({
            "type": "restore",
            "status": "ok",
            "slice": summary.slice,
            "restored": summary.restored,
            "skipped": summary.skipped,
            "files": summary.files,
        });
        return serde_json::to_string(&v).map_err(|e| format!("encoding restore JSON: {e}"));
    }
    let mut lines = Vec::with_capacity(summary.files.len() + 1);
    for f in &summary.files {
        lines.push(format!(
            "RESTORE_FILE slice={} day={} action={} bytes={} records={} sha256={}",
            f.slice,
            f.day_bucket,
            f.action.as_str(),
            f.bytes,
            f.records,
            f.sha256
        ));
    }
    lines.push(format!(
        "RESTORE status=ok slice={} restored={} skipped={}",
        summary.slice, summary.restored, summary.skipped
    ));
    Ok(lines.join("\n"))
}

pub fn format_verify(summary: &VerifySummary, json_out: bool) -> Result<String, String> {
    let status_word = if summary.intact { "ok" } else { "failed" };
    if json_out {
        let v = serde_json::json!({
            "type": "verify",
            "status": status_word,
            "intact": summary.intact,
            "slices": summary.slices,
            "files_checked": summary.files_checked,
            "files_verified": summary.files_verified,
            "files_failed": summary.files_failed,
            "bytes": summary.bytes,
            "records": summary.records,
            "files": summary.files,
            "issues": summary.issues,
        });
        return serde_json::to_string(&v).map_err(|e| format!("encoding verify JSON: {e}"));
    }
    let mut lines = Vec::with_capacity(summary.files.len() + summary.issues.len() + 1);
    for f in &summary.files {
        match &f.detail {
            Some(d) => lines.push(format!(
                "VERIFY_FILE slice={} day={} status={} bytes={} records={} sha256={} detail={d}",
                f.slice, f.day_bucket, f.status, f.bytes, f.records, f.sha256
            )),
            None => lines.push(format!(
                "VERIFY_FILE slice={} day={} status={} bytes={} records={} sha256={}",
                f.slice, f.day_bucket, f.status, f.bytes, f.records, f.sha256
            )),
        }
    }
    for issue in &summary.issues {
        lines.push(format!(
            "VERIFY_ISSUE kind={} file={} detail={}",
            issue.kind, issue.file, issue.detail
        ));
    }
    lines.push(format!(
        "VERIFY status={status_word} slices={} files={} verified={} failed={} issues={} bytes={} records={}",
        summary.slices,
        summary.files_checked,
        summary.files_verified,
        summary.files_failed,
        summary.issues.len(),
        summary.bytes,
        summary.records
    ));
    Ok(lines.join("\n"))
}

/// Event-driven line stream reader: each line read from `reader` triggers one incremental
/// `ship` pass with zero timers and zero polling.
pub fn watch_stdin<R: BufRead, W: Write>(
    reader: R,
    mut out: W,
    slices_dir: &Path,
    dest_dir: &Path,
    json_out: bool,
) -> Result<usize, String> {
    let mut passes = 0usize;
    for line_res in reader.lines() {
        let _trigger = line_res.map_err(|e| format!("reading trigger line from stdin: {e}"))?;
        let summary = ship(slices_dir, dest_dir)?;
        let rendered = if json_out {
            format_ship(&summary, true)?
        } else {
            format!(
                "SHIP status=ok pass={} shipped={} skipped={} slices={} files={} bytes={} records={}",
                passes + 1,
                summary.shipped,
                summary.skipped,
                summary.slices,
                summary.total_files,
                summary.total_bytes,
                summary.total_records
            )
        };
        writeln!(out, "{rendered}").map_err(|e| format!("writing watch output: {e}"))?;
        out.flush()
            .map_err(|e| format!("flushing watch output: {e}"))?;
        passes += 1;
    }
    Ok(passes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{encode_bytes, sha256_hex, Record};

    fn temp_dir(suffix: &str) -> PathBuf {
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "roger-cold-shipper-unit-{}-{seq}-{suffix}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_sample_cold(path: &Path, mail: &str, body: &str) -> Vec<u8> {
        if let Some(p) = path.parent() {
            fs::create_dir_all(p).unwrap();
        }
        let rec = Record {
            mail: mail.to_string(),
            stream: "g:ops".to_string(),
            article: 1,
            kind: "group".to_string(),
            from: "sender-1".to_string(),
            to: "ops".to_string(),
            headers: None,
            body: body.to_string(),
            digest: sha256_hex(body.as_bytes()),
            accepted_at_ms: Some(1_700_000_123_000),
        };
        let bytes = encode_bytes(&[rec]).unwrap();
        fs::write(path, &bytes).unwrap();
        bytes
    }

    #[test]
    fn scan_supports_multi_slice_and_single_slice_layouts() {
        let multi = temp_dir("scan-multi");
        write_sample_cold(&multi.join("slice-01/cold/day-19002.cold"), "m2", "b2");
        write_sample_cold(&multi.join("slice-00/cold/day-19001.cold"), "m1", "b1");
        fs::write(
            multi.join("slice-00/cold/day-19003.cold.tmp.99"),
            b"partial",
        )
        .unwrap();

        let scanned = scan_shards(&multi).unwrap();
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].0, "slice-00");
        assert_eq!(scanned[0].1, 19_001);
        assert_eq!(scanned[1].0, "slice-01");
        assert_eq!(scanned[1].1, 19_002);

        let single = temp_dir("scan-single");
        write_sample_cold(&single.join("cold/day-19010.cold"), "m10", "b10");
        let scanned_single = scan_shards(&single).unwrap();
        assert_eq!(scanned_single.len(), 1);
        assert_eq!(scanned_single[0].0, "slice-00");
        assert_eq!(scanned_single[0].1, 19_010);

        let _ = fs::remove_dir_all(&multi);
        let _ = fs::remove_dir_all(&single);
    }

    #[test]
    fn ship_reship_idempotency_and_dest_sha256_recheck() {
        let root = temp_dir("reship");
        let slices = root.join("slices");
        let dest = root.join("archive");

        let b0 = write_sample_cold(&slices.join("slice-00/cold/day-19000.cold"), "m0", "alpha");
        let b1 = write_sample_cold(&slices.join("slice-01/cold/day-19001.cold"), "m1", "beta");

        let s1 = ship(&slices, &dest).unwrap();
        assert_eq!(s1.shipped, 2);
        assert_eq!(s1.skipped, 0);
        assert_eq!(s1.slices, 2);
        assert_eq!(s1.total_files, 2);
        assert_eq!(s1.total_records, 2);
        assert_eq!(s1.total_bytes, (b0.len() + b1.len()) as u64);

        let archived_0 = dest.join("slice-00/day-19000.cold");
        assert!(fs::metadata(&archived_0).unwrap().permissions().readonly());

        // Second pass skips both because destination SHA-256 digests match.
        let s2 = ship(&slices, &dest).unwrap();
        assert_eq!(s2.shipped, 0);
        assert_eq!(s2.skipped, 2);

        // Corrupt the destination copy on disk: ship must NOT skip merely because the
        // destination path exists; it must detect the SHA-256 mismatch and re-ship it.
        let mut perms = fs::metadata(&archived_0).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(&archived_0, perms).unwrap();
        fs::write(&archived_0, b"corrupted destination copy\n").unwrap();

        let s3 = ship(&slices, &dest).unwrap();
        assert_eq!(s3.shipped, 1);
        assert_eq!(s3.skipped, 1);
        assert_eq!(fs::read(&archived_0).unwrap(), b0);
        assert!(fs::metadata(&archived_0).unwrap().permissions().readonly());
        assert!(verify_archive(&dest).intact);

        let _ = fs::remove_dir_all(&root);
    }
}

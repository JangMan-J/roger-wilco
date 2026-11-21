//! `roger backup`: a live, crash-safe copy of a coordinator or host daemon data
//! set. Each SQLite database is copied with `VACUUM INTO` on a read-only
//! connection (one consistent snapshot, live writers are not blocked) and the
//! copy must pass `PRAGMA integrity_check`. Sealed cold day files are copied
//! after their slice's hot database, so a concurrent sweep can only add
//! overlap between hot and cold, never lose a record.

use roger_wire::durlog::sync_dir;
use roger_wire::sha256::hex_digest;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Coordinator,
    Daemon,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Coordinator => "coordinator",
            Kind::Daemon => "daemon",
        }
    }
}

pub fn run(args: &[String]) -> io::Result<()> {
    let (mut data, mut dest, mut kind) = (None, None, None);
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        if !matches!(flag, "--data" | "--dest" | "--kind") {
            return Err(bad(format!(
                "expected --data, --dest, or --kind, saw {flag}"
            )));
        }
        let value = args.get(i + 1).ok_or_else(|| {
            bad(format!(
                "expected a value after {flag}, saw end of arguments"
            ))
        })?;
        match flag {
            "--data" => data = Some(PathBuf::from(value)),
            "--dest" => dest = Some(PathBuf::from(value)),
            _ => {
                kind = Some(match value.as_str() {
                    "coordinator" => Kind::Coordinator,
                    "daemon" => Kind::Daemon,
                    other => {
                        return Err(bad(format!("expected coordinator or daemon, saw {other}")))
                    }
                })
            }
        }
        i += 2;
    }
    let data = data.ok_or_else(|| bad("missing --data".into()))?;
    let dest = dest.ok_or_else(|| bad("missing --dest".into()))?;
    let kind = match kind {
        Some(k) => k,
        None => detect(&data)?,
    };
    prepare_dest(&dest)?;
    let mut out = io::stdout().lock();
    let files = match kind {
        Kind::Coordinator => backup_coordinator(&data, &dest, &mut out)?,
        Kind::Daemon => backup_daemon(&data, &dest, &mut out)?,
    };
    writeln!(
        out,
        "BACKUP kind={} files={files} dest={}",
        kind.name(),
        dest.display()
    )
}

/// Slice directories under a coordinator root: the root itself when it holds
/// `hot.sqlite`, else its `slice-NN` directories holding `hot.sqlite`, sorted.
fn slices(root: &Path) -> io::Result<Vec<PathBuf>> {
    if root.join("hot.sqlite").is_file() {
        return Ok(vec![root.to_path_buf()]);
    }
    let Ok(entries) = fs::read_dir(root) else {
        return Ok(Vec::new());
    };
    let mut dirs = Vec::new();
    for e in entries {
        let p = e?.path();
        let is_slice = p
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("slice-"))
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
        if is_slice && p.join("hot.sqlite").is_file() {
            dirs.push(p);
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn detect(data: &Path) -> io::Result<Kind> {
    let root = crate::sql::coord_root(data);
    if !slices(&root)?.is_empty() {
        return Ok(Kind::Coordinator);
    }
    let db = crate::sql::daemon_db_path(data);
    if crate::sql::is_sqlite_file(&db) {
        return Ok(Kind::Daemon);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "expected coordinator slices under {} or a daemon SQLite file at {}, saw neither",
            root.display(),
            db.display()
        ),
    ))
}

fn prepare_dest(dest: &Path) -> io::Result<()> {
    match fs::read_dir(dest) {
        Ok(mut entries) => {
            if let Some(e) = entries.next() {
                return Err(bad(format!(
                    "expected --dest {} to be absent or an empty directory, saw {}",
                    dest.display(),
                    e?.path().display()
                )));
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => fs::create_dir_all(dest)?,
        Err(e) => {
            return Err(bad(format!(
                "expected --dest {} to be absent or an empty directory, saw {e}",
                dest.display()
            )))
        }
    }
    Ok(())
}

/// Name under which `data` is mirrored in dest, so `--data <dest>/<name>`
/// restores to the same layout.
fn base_name(data: &Path) -> io::Result<PathBuf> {
    let abs = fs::canonicalize(data)
        .or_else(|_| std::path::absolute(data))
        .map_err(|e| bad(format!("expected a resolvable --data path, saw {e}")))?;
    abs.file_name().map(PathBuf::from).ok_or_else(|| {
        bad(format!(
            "expected --data with a file name, saw {}",
            data.display()
        ))
    })
}

fn report(out: &mut impl Write, rel: &Path, dst: &Path) -> io::Result<()> {
    let bytes = fs::read(dst)?;
    writeln!(
        out,
        "BACKUP_FILE path={} bytes={} sha256={}",
        rel.display(),
        bytes.len(),
        hex_digest(&bytes)
    )
}

fn backup_coordinator(data: &Path, dest: &Path, out: &mut impl Write) -> io::Result<usize> {
    let root = crate::sql::coord_root(data);
    let slice_dirs = slices(&root)?;
    if slice_dirs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "expected hot.sqlite or slice-NN/hot.sqlite under {}, saw none",
                root.display()
            ),
        ));
    }
    let base = base_name(data)?;
    // Root relative to the mirrored name: `data` itself, a child of it, or `<data>.slices`.
    let root_rel = match root.strip_prefix(data) {
        Ok(rest) => base.join(rest),
        Err(_) => PathBuf::from(root.file_name().unwrap_or_default()),
    };
    let mut files = 0;
    if data.is_file() && !crate::sql::is_sqlite_file(data) {
        let dst = dest.join(&base);
        copy_verified(data, &dst)?;
        report(out, &base, &dst)?;
        files += 1;
    }
    for dir in slice_dirs {
        let rel_dir = root_rel.join(dir.strip_prefix(&root).unwrap_or(Path::new("")));
        fs::create_dir_all(dest.join(&rel_dir))?;
        let rel = rel_dir.join("hot.sqlite");
        let dst = dest.join(&rel);
        snapshot(&dir.join("hot.sqlite"), &dst)?;
        report(out, &rel, &dst)?;
        files += 1;
        let cold = dir.join("cold");
        let mut days = Vec::new();
        if let Ok(entries) = fs::read_dir(&cold) {
            for e in entries {
                let p = e?.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name.starts_with("day-") && name.ends_with(".cold") && !name.contains(".tmp.") {
                    days.push(p);
                }
            }
        }
        days.sort();
        if !days.is_empty() {
            fs::create_dir_all(dest.join(&rel_dir).join("cold"))?;
        }
        for day in days {
            let rel = rel_dir
                .join("cold")
                .join(day.file_name().unwrap_or_default());
            let dst = dest.join(&rel);
            copy_verified(&day, &dst)?;
            report(out, &rel, &dst)?;
            files += 1;
        }
    }
    Ok(files)
}

fn backup_daemon(data: &Path, dest: &Path, out: &mut impl Write) -> io::Result<usize> {
    let db = crate::sql::daemon_db_path(data);
    if !crate::sql::is_sqlite_file(&db) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "expected a daemon SQLite file at {}, saw none",
                db.display()
            ),
        ));
    }
    let rel = PathBuf::from(db.file_name().unwrap_or_default());
    let dst = dest.join(&rel);
    snapshot(&db, &dst)?;
    report(out, &rel, &dst)?;
    Ok(1)
}

/// Copy a sealed file through a temp name, fsync, compare SHA-256 of source and
/// copy, then rename into place and sync the directory.
fn copy_verified(src: &Path, dst: &Path) -> io::Result<()> {
    let bytes = fs::read(src)?;
    let mut tmp_name = dst.as_os_str().to_owned();
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp = PathBuf::from(tmp_name);
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    drop(f);
    let (want, got) = (hex_digest(&bytes), hex_digest(&fs::read(&tmp)?));
    if want != got {
        let _ = fs::remove_file(&tmp);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected copy of {} with sha256 {want}, saw {got}",
                src.display()
            ),
        ));
    }
    fs::rename(&tmp, dst)?;
    sync_dir(dst.parent().unwrap_or(Path::new(".")))
}

/// Consistent snapshot of a live SQLite database into `dst`, then require
/// `PRAGMA integrity_check` == "ok" on the copy.
// ## YAGNI: rusqlite/SQL kept here, not in src/sql/, because src/sql/ is out of scope for this change.
fn snapshot(src: &Path, dst: &Path) -> io::Result<()> {
    use rusqlite::{Connection, OpenFlags};
    let ro = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let sql_err = |what: &str, p: &Path, e: rusqlite::Error| {
        io::Error::other(format!("expected {what} of {}, saw {e}", p.display()))
    };
    let dst_str = dst.to_str().ok_or_else(|| {
        bad(format!(
            "expected a UTF-8 --dest path, saw {}",
            dst.display()
        ))
    })?;
    let conn =
        Connection::open_with_flags(src, ro).map_err(|e| sql_err("read-only open", src, e))?;
    // no-poll: one bounded SQLite lock wait for the snapshot, not a scan for work.
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| sql_err("lock timeout", src, e))?;
    conn.execute("VACUUM INTO ?1", [dst_str])
        .map_err(|e| sql_err("VACUUM INTO snapshot", src, e))?;
    drop(conn);
    fs::File::open(dst)?.sync_all()?;
    sync_dir(dst.parent().unwrap_or(Path::new(".")))?;
    let copy = Connection::open_with_flags(dst, ro).map_err(|e| sql_err("open", dst, e))?;
    let check: String = copy
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(|e| sql_err("integrity_check", dst, e))?;
    if check != "ok" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected integrity_check ok for {}, saw {check}",
                dst.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Tmp(PathBuf);
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tmp(tag: &str) -> Tmp {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "roger-backup-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        Tmp(p)
    }
    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }
    fn s(p: &Path) -> &str {
        p.to_str().unwrap()
    }

    /// Live WAL writer with `committed` rows left in the -wal, plus a second
    /// connection holding an open uncommitted write transaction.
    fn live_db(path: &Path, committed: i64) -> (Connection, Connection) {
        let w = Connection::open(path).unwrap();
        w.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(x INTEGER);",
        )
        .unwrap();
        for i in 0..committed {
            w.execute("INSERT INTO t(x) VALUES (?1)", [i]).unwrap();
        }
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        assert!(fs::metadata(PathBuf::from(wal)).unwrap().len() > 0);
        let u = Connection::open(path).unwrap();
        u.execute_batch("BEGIN IMMEDIATE; INSERT INTO t(x) VALUES (999);")
            .unwrap();
        (w, u)
    }
    fn count(path: &Path) -> i64 {
        let c = Connection::open(path).unwrap();
        let ok: String = c
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ok, "ok");
        c.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn coordinator_live_slices_and_cold() {
        let t = tmp("coord");
        let data = t.0.join("coordinator.log");
        fs::write(&data, b"{\"legacy\":1}\n").unwrap();
        let root = t.0.join("coordinator.log.slices");
        let mut live = Vec::new();
        for (i, n) in [(0, 3), (1, 5)] {
            let dir = root.join(format!("slice-{i:02}"));
            fs::create_dir_all(dir.join("cold")).unwrap();
            live.push(live_db(&dir.join("hot.sqlite"), n));
        }
        let cold = root.join("slice-00/cold");
        fs::write(cold.join("day-19000.cold"), b"sealed-bytes\x00\x01").unwrap();
        fs::write(cold.join("day-19001.cold.tmp.42"), b"partial").unwrap();
        let dest = t.0.join("out");
        run(&args(&[
            "--data",
            s(&data),
            "--dest",
            s(&dest),
            "--kind",
            "coordinator",
        ]))
        .unwrap();
        let b = dest.join("coordinator.log.slices");
        assert_eq!(count(&b.join("slice-00/hot.sqlite")), 3);
        assert_eq!(count(&b.join("slice-01/hot.sqlite")), 5);
        assert_eq!(
            fs::read(b.join("slice-00/cold/day-19000.cold")).unwrap(),
            b"sealed-bytes\x00\x01"
        );
        assert!(!b.join("slice-00/cold/day-19001.cold.tmp.42").exists());
        assert_eq!(
            fs::read(dest.join("coordinator.log")).unwrap(),
            b"{\"legacy\":1}\n"
        );
        drop(live);
    }

    #[test]
    fn daemon_live_and_auto_detect() {
        let t = tmp("daemon");
        let data = t.0.join("daemon");
        fs::write(crate::sql::daemon_lock_path(&data), b"").unwrap();
        let live = live_db(&crate::sql::daemon_db_path(&data), 4);
        assert_eq!(detect(&data).unwrap(), Kind::Daemon);
        let dest = t.0.join("out");
        run(&args(&["--data", s(&data), "--dest", s(&dest)])).unwrap();
        assert_eq!(count(&dest.join("daemon.sqlite")), 4);
        assert!(!dest.join("daemon.lock").exists());
        drop(live);
    }

    #[test]
    fn auto_detect_coordinator_root_and_neither() {
        let t = tmp("detect");
        let data = t.0.join("coord");
        fs::create_dir_all(&data).unwrap();
        let live = live_db(&data.join("hot.sqlite"), 2);
        assert_eq!(detect(&data).unwrap(), Kind::Coordinator);
        let dest = t.0.join("out");
        run(&args(&["--data", s(&data), "--dest", s(&dest)])).unwrap();
        assert_eq!(count(&dest.join("coord/hot.sqlite")), 2);
        drop(live);
        let err = detect(&t.0.join("nothing")).unwrap_err().to_string();
        assert!(
            err.contains("nothing.slices") && err.contains("nothing.sqlite"),
            "{err}"
        );
    }

    #[test]
    fn non_empty_dest_refused() {
        let t = tmp("dest");
        let data = t.0.join("daemon");
        let live = live_db(&crate::sql::daemon_db_path(&data), 1);
        let dest = t.0.join("out");
        fs::create_dir_all(&dest).unwrap();
        fs::write(dest.join("stale"), b"x").unwrap();
        let err = run(&args(&["--data", s(&data), "--dest", s(&dest)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("empty directory") && err.contains("stale"),
            "{err}"
        );
        drop(live);
    }

    #[test]
    fn parser_errors() {
        let e = |v: &[&str]| run(&args(v)).unwrap_err();
        let err = e(&["--bogus", "x"]);
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            err.to_string(),
            "expected --data, --dest, or --kind, saw --bogus"
        );
        assert_eq!(e(&["--dest", "d"]).to_string(), "missing --data");
        assert_eq!(e(&["--data", "--dest"]).to_string(), "missing --dest");
        assert_eq!(
            e(&["--data", "x", "--dest", "y", "--kind", "relay"]).to_string(),
            "expected coordinator or daemon, saw relay"
        );
    }
}

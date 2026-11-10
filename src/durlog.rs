//! Append-only durable line log: each append is written and fsynced before
//! it returns. Replay yields complete lines only, one at a time, so opening a
//! log never holds the whole file in memory; a trailing fragment left by a
//! crash mid-append is dropped.
//!
//! The file is locked exclusively for as long as the log is open, so a
//! second process (a second adapter on one Store, a second daemon or
//! coordinator on one data file) fails at open instead of numbering records
//! from the same replay. The kernel drops the lock when the process dies.
//!
//! One failed append poisons the log: the bytes may have reached the disk
//! even though the caller saw an error and left memory unchanged, so no
//! later append may commit on top of that split. Every later append returns
//! an error naming the first failure; the owner restarts and replays.
//!
//! A log can be rotated: its owner hands over the lines that restate what it
//! still needs, and they become the start of a fresh file under the same
//! path, between a `{"snapshot":<n>,"prior":"<file>"}` line and a
//! `{"snapshot_end":<n>}` line. The file as it was stays beside it, whole,
//! under `<path>.<n>`: nothing a log ever held is removed by this program.
//! The path names a complete log at every instant, the old one or the new.

use crate::json::{self, obj, s, Value};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// When a log rotates and what its owner may leave out when it does. Both
/// come from flags; with neither, a log only ever grows.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Policy {
    /// Rotate once the log has grown by this many bytes since its last
    /// snapshot (and by at least the snapshot's own size, so a large snapshot
    /// is not rewritten for a small gain). 0: never.
    pub rotate_bytes: u64,
    /// At a rotation, what is settled and older than this keeps only its id.
    /// None: everything is kept.
    pub cull_after_ms: Option<i64>,
}

impl Policy {
    /// `--rotate-bytes <n>` and `--cull-after-ms <n>` from a command line.
    /// Culling happens at a rotation, so it is refused without one.
    pub fn from_args(args: &[String]) -> io::Result<Policy> {
        let number = |name: &str| -> io::Result<Option<u64>> {
            match args.windows(2).find(|w| w[0] == name) {
                None => Ok(None),
                Some(w) => w[1].parse().map(Some).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{name} expects a whole number, saw {}", w[1]),
                    )
                }),
            }
        };
        let rotate_bytes = number("--rotate-bytes")?.unwrap_or(0);
        let cull_after_ms =
            number("--cull-after-ms")?.map(|ms| i64::try_from(ms).unwrap_or(i64::MAX));
        if cull_after_ms.is_some() && rotate_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected --rotate-bytes above 0 with --cull-after-ms (culling happens when a log rotates), saw none",
            ));
        }
        Ok(Policy {
            rotate_bytes,
            cull_after_ms,
        })
    }
}

/// Test hook (feature `test-hooks` only): end the process, as a crash would,
/// at one step of a rotation: `written` (the new file is complete and not
/// yet in place), `linked` (the old file also has its archive name) or
/// `renamed` (the new file is in place).
#[cfg(feature = "test-hooks")]
pub static CRASH_AT_ROTATE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn crash_at(_step: &str) {
    #[cfg(feature = "test-hooks")]
    if CRASH_AT_ROTATE.get().map(String::as_str) == Some(_step) {
        std::process::abort();
    }
}

/// What a log's replay tells its owner about the file, once read.
#[derive(Default)]
struct Extent {
    /// Bytes of whole lines the file holds.
    len: AtomicU64,
    /// Where the snapshot at the start of the file ends; 0 with none.
    snapshot_len: AtomicU64,
    /// The number of the snapshot the file starts with: the number of the
    /// last archive, even when that archive is no longer beside the file.
    seq: AtomicU64,
}

pub struct DurLog {
    file: File,
    path: PathBuf,
    /// The first append failure, kept so every later append refuses.
    poison: Option<String>,
    /// Set by `Replay` when it has read to the end and cut any torn tail. An
    /// append before that could glue onto a fragment, so it is refused.
    replayed: Arc<AtomicBool>,
    extent: Arc<Extent>,
    /// After a rotation that failed, the size at which the next is tried.
    retry_at: u64,
    /// The files this log was before each rotation, kept open, and so
    /// locked, for as long as the log is. A second process that opened the
    /// path just before a rotation holds the old file: it must find that
    /// file still locked, not take the lock and believe the log is its own.
    retired: Vec<File>,
}

/// The complete lines a log held when it was opened, in order, empty lines
/// left out. Read it to its end before the first append.
pub struct Replay {
    reader: BufReader<File>,
    /// How many bytes of whole lines have been read: where a torn tail starts.
    keep: u64,
    done: bool,
    replayed: Arc<AtomicBool>,
    extent: Arc<Extent>,
}

/// How a rotation's opening and closing lines start.
const SNAPSHOT: &str = "{\"snapshot\":";
const SNAPSHOT_END: &str = "{\"snapshot_end\":";

impl Iterator for Replay {
    type Item = io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = Vec::new();
        while !self.done {
            line.clear();
            let n = match self.reader.read_until(b'\n', &mut line) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            if line.last() != Some(&b'\n') {
                // End of file, or a crash mid-append left a fragment with no
                // terminating newline. Drop it from the file, not just from
                // the replay: an append in append mode would otherwise glue
                // onto it and corrupt a record.
                self.done = true;
                if n > 0 {
                    if let Err(e) = self.reader.get_ref().set_len(self.keep) {
                        return Some(Err(e));
                    }
                }
                self.extent.len.store(self.keep, Ordering::SeqCst);
                self.replayed.store(true, Ordering::SeqCst);
                break;
            }
            self.keep += n as u64;
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.starts_with(SNAPSHOT_END.as_bytes()) {
                self.extent.snapshot_len.store(self.keep, Ordering::SeqCst);
            } else if let Some(rest) = line.strip_prefix(SNAPSHOT.as_bytes()) {
                let digits: Vec<u8> = rest
                    .iter()
                    .copied()
                    .take_while(u8::is_ascii_digit)
                    .collect();
                if let Some(n) = std::str::from_utf8(&digits)
                    .ok()
                    .and_then(|d| d.parse().ok())
                {
                    self.extent.seq.fetch_max(n, Ordering::SeqCst);
                }
            }
            if !line.is_empty() {
                return Some(Ok(String::from_utf8_lossy(&line).into_owned()));
            }
        }
        None
    }
}

impl DurLog {
    pub fn open(path: &Path) -> io::Result<(DurLog, Replay)> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let existed = path.exists();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        // no-poll: one attempt, never retried; a held lock is an error.
        file.try_lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "expected {} unlocked, saw it held by another process: {e}",
                    path.display()
                ),
            )
        })?;
        if !existed {
            // Make the new file itself durable.
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    File::open(parent)?.sync_all()?;
                }
            }
        }
        // The file itself, not a link that names it: a rotation puts a new
        // file in this one's place and its archive beside it, and a link
        // given as the path must go on naming the log.
        let path = path.canonicalize()?;
        let replayed = Arc::new(AtomicBool::new(false));
        let extent = Arc::new(Extent::default());
        // The same open file, so the lock above covers the replay too.
        let replay = Replay {
            reader: BufReader::new(file.try_clone()?),
            keep: 0,
            done: false,
            replayed: Arc::clone(&replayed),
            extent: Arc::clone(&extent),
        };
        Ok((
            DurLog {
                file,
                path,
                poison: None,
                replayed,
                extent,
                retry_at: 0,
                retired: Vec::new(),
            },
            replay,
        ))
    }

    /// True when the log has grown enough since its last snapshot to rotate
    /// under `policy`.
    pub fn due(&self, policy: &Policy) -> bool {
        let len = self.extent.len.load(Ordering::SeqCst);
        let snapshot = self.extent.snapshot_len.load(Ordering::SeqCst);
        policy.rotate_bytes > 0
            && self.replayed.load(Ordering::SeqCst)
            && self.poison.is_none()
            && len >= self.retry_at
            && len.saturating_sub(snapshot) >= policy.rotate_bytes.max(snapshot)
    }

    /// The owner could not prepare a rotation that was due (it could not read
    /// what to restate). The next is not due until the log has grown further,
    /// as after a rotation that failed here.
    pub fn put_off(&mut self) {
        let len = self.extent.len.load(Ordering::SeqCst);
        self.retry_at = len.saturating_add(len / 2).saturating_add(1);
    }

    /// Start a fresh file under this log's path that holds `lines` between a
    /// `snapshot` line (its number, the archive's file name, then `extra`)
    /// and a `snapshot_end` line, and keep the file as it was under
    /// `<path>.<n>`. `lines` must restate everything the owner still needs:
    /// after this, the replay of the path is those lines.
    ///
    /// The new file is written beside the path, flushed and locked before it
    /// takes the path's place in one rename, so the path names the old log or
    /// the new one, never part of either, whenever this stops. An error
    /// before the rename leaves the old log in use and unharmed.
    pub fn rotate(&mut self, extra: Vec<(&str, Value)>, lines: &[String]) -> io::Result<u64> {
        use std::io::Write;
        use std::os::unix::fs::MetadataExt;
        let len = self.extent.len.load(Ordering::SeqCst);
        let fail = |this: &mut DurLog, e: io::Error| {
            this.retry_at = len.saturating_add(len / 2).saturating_add(1);
            e
        };
        let name = self
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "expected a log path with a file name, saw {}",
                        self.path.display()
                    ),
                )
            })?
            .to_string();
        let dir = match self.path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        };
        // The next archive number: one past the highest ever used. That is
        // the number this file's own snapshot carries, and the highest beside
        // the path (`<name>.<n>`, also with a further suffix such as `.gz`),
        // whichever is larger, so a number is not taken again after its
        // archive was compressed, moved or removed. A rotation that stopped
        // after naming its archive left that name on this very file; it is
        // taken up again instead of naming a second.
        let mut highest = self.extent.seq.load(Ordering::SeqCst);
        for entry in std::fs::read_dir(&dir).map_err(|e| fail(self, e))? {
            let entry = entry.map_err(|e| fail(self, e))?;
            if let Some(n) = entry
                .file_name()
                .to_str()
                .and_then(|f| f.strip_prefix(&format!("{name}.")))
                .and_then(|rest| rest.split('.').next())
                .and_then(|n| n.parse::<u64>().ok())
            {
                highest = highest.max(n);
            }
        }
        let live = self.file.metadata().map_err(|e| fail(self, e))?;
        let resumed = highest > 0
            && std::fs::metadata(dir.join(format!("{name}.{highest}")))
                .is_ok_and(|m| m.ino() == live.ino() && m.dev() == live.dev());
        let seq = if resumed { highest } else { highest + 1 };
        let archive_name = format!("{name}.{seq}");
        let archive = dir.join(&archive_name);
        let next_path = dir.join(format!("{name}.next"));

        let written = (|| -> io::Result<(File, u64)> {
            let _ = std::fs::remove_file(&next_path);
            let next = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&next_path)?;
            // no-poll: one attempt on a file this call just created; a held lock is an error.
            next.try_lock()?;
            let mut head = vec![
                ("snapshot", Value::Int(seq as i64)),
                ("prior", s(&archive_name)),
            ];
            head.extend(extra);
            let mut out = io::BufWriter::new(&next);
            let mut bytes = 0u64;
            let mut put = |line: &str| -> io::Result<()> {
                debug_assert!(!line.contains('\n'));
                out.write_all(line.as_bytes())?;
                out.write_all(b"\n")?;
                bytes += line.len() as u64 + 1;
                Ok(())
            };
            put(&json::stringify(&obj(head)))?;
            for line in lines {
                put(line)?;
            }
            put(&json::stringify(&obj(vec![(
                "snapshot_end",
                Value::Int(seq as i64),
            )])))?;
            out.flush()?;
            drop(out);
            next.sync_all()?;
            Ok((next, bytes))
        })();
        let (next, bytes) = match written {
            Ok(done) => done,
            Err(e) => {
                let _ = std::fs::remove_file(&next_path);
                return Err(fail(self, e));
            }
        };
        crash_at("written");
        if !resumed {
            if let Err(e) = std::fs::hard_link(&self.path, &archive) {
                let _ = std::fs::remove_file(&next_path);
                return Err(fail(self, e));
            }
        }
        crash_at("linked");
        if let Err(e) = std::fs::rename(&next_path, &self.path) {
            let _ = std::fs::remove_file(&next_path);
            return Err(fail(self, e));
        }
        crash_at("renamed");
        // From here the new file is the log. The old file stays open, and
        // locked, as its archive (see `retired`).
        self.retired.push(std::mem::replace(&mut self.file, next));
        self.extent.len.store(bytes, Ordering::SeqCst);
        self.extent.snapshot_len.store(bytes, Ordering::SeqCst);
        self.extent.seq.store(seq, Ordering::SeqCst);
        self.retry_at = 0;
        // Make both names durable. A failure here cannot be undone and must
        // not be built on: the log is poisoned, and a restart replays whichever
        // file the path names.
        if let Err(e) = File::open(&dir).and_then(|d| d.sync_all()) {
            self.poison = Some(e.to_string());
            return Err(e);
        }
        Ok(seq)
    }

    #[cfg(test)]
    fn from_file(file: File) -> DurLog {
        DurLog {
            file,
            path: PathBuf::new(),
            poison: None,
            replayed: Arc::new(AtomicBool::new(true)),
            extent: Arc::new(Extent::default()),
            retry_at: 0,
            retired: Vec::new(),
        }
    }

    /// True once an append has failed: nothing appended since has committed.
    pub fn is_poisoned(&self) -> bool {
        self.poison.is_some()
    }

    pub fn append(&mut self, line: &str) -> io::Result<()> {
        debug_assert!(!line.contains('\n'));
        if !self.replayed.load(Ordering::SeqCst) {
            return Err(io::Error::other(
                "expected the log replayed to its end before an append, saw the replay unfinished",
            ));
        }
        if let Some(first) = &self.poison {
            return Err(io::Error::other(format!(
                "expected a durable log, saw one poisoned by an earlier append failure: {first}"
            )));
        }
        self.write_line(line)
            .inspect_err(|e| self.poison = Some(e.to_string()))?;
        self.extent
            .len
            .fetch_add(line.len() as u64 + 1, Ordering::SeqCst);
        Ok(())
    }

    fn write_line(&mut self, line: &str) -> io::Result<()> {
        use std::io::Write;
        self.file.write_all(line.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    /// Open `path` and read its replay to the end.
    fn open(path: &Path) -> io::Result<(DurLog, Vec<String>)> {
        let (log, replay) = DurLog::open(path)?;
        Ok((log, replay.collect::<io::Result<Vec<String>>>()?))
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("roger-durlog-{}-{tag}.log", std::process::id()));
        let _ = fs::remove_file(&p);
        p
    }

    #[test]
    fn appended_lines_survive_reopen_in_order() {
        let path = temp_path("order");
        {
            let (mut log, prior) = open(&path).unwrap();
            assert!(prior.is_empty());
            log.append("{\"a\":1}").unwrap();
            log.append("{\"b\":2}").unwrap();
        }
        let (_log, lines) = open(&path).unwrap();
        assert_eq!(
            lines,
            vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn one_failed_append_poisons_every_later_append() {
        // /dev/full accepts the open and fails every write with ENOSPC, so
        // the first append fails for a real filesystem reason and the second
        // must fail for the poison alone.
        let file = fs::OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .unwrap();
        let mut log = DurLog::from_file(file);
        assert!(!log.is_poisoned());
        let first = log.append("{\"a\":1}").unwrap_err();
        assert!(log.is_poisoned());
        let second = log.append("{\"b\":2}").unwrap_err();
        let msg = second.to_string();
        assert!(msg.contains("poisoned"), "{msg}");
        assert!(msg.contains(&first.to_string()), "{msg} / {first}");
    }

    #[test]
    fn a_second_open_of_a_held_log_fails_until_the_first_closes() {
        let path = temp_path("lock");
        let (first, _) = open(&path).unwrap();
        let err = open(&path).err().expect("second open must fail");
        assert!(err.to_string().contains("held by another process"), "{err}");
        drop(first);
        open(&path).unwrap();
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn an_append_before_the_replay_is_read_to_its_end_is_refused() {
        let path = temp_path("unreplayed");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"{\"a\":1}\n{\"b\":2}\n{\"c\":3").unwrap();
        }
        let (mut log, mut replay) = DurLog::open(&path).unwrap();
        assert_eq!(replay.next().unwrap().unwrap(), "{\"a\":1}");
        let e = log.append("{\"d\":4}").unwrap_err();
        assert!(e.to_string().contains("replay unfinished"), "{e}");
        assert!(!log.is_poisoned(), "a refused append wrote nothing");
        assert_eq!(replay.next().unwrap().unwrap(), "{\"b\":2}");
        assert!(replay.next().is_none());
        log.append("{\"d\":4}").unwrap();
        drop((log, replay));
        let raw = fs::read_to_string(&path).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(raw, "{\"a\":1}\n{\"b\":2}\n{\"d\":4}\n");
    }

    #[test]
    fn replay_skips_empty_lines_and_reads_a_log_larger_than_its_buffer() {
        let path = temp_path("large");
        let long = "x".repeat(100_000);
        {
            let mut f = File::create(&path).unwrap();
            for n in 0..50 {
                writeln!(f, "{{\"n\":{n},\"body\":\"{long}\"}}\n").unwrap();
            }
            f.write_all(b"\r\n").unwrap();
        }
        let (_log, lines) = open(&path).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(lines.len(), 50);
        for (n, line) in lines.iter().enumerate() {
            assert!(line.starts_with(&format!("{{\"n\":{n},")), "{n}");
            assert_eq!(
                line.len(),
                long.len() + format!("{{\"n\":{n},\"body\":\"\"}}").len()
            );
        }
    }

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_policy_comes_from_its_flags_and_culling_needs_a_rotation() {
        assert_eq!(Policy::from_args(&args(&[])).unwrap(), Policy::default());
        assert_eq!(
            Policy::from_args(&args(&["--rotate-bytes", "4096"])).unwrap(),
            Policy {
                rotate_bytes: 4096,
                cull_after_ms: None
            }
        );
        assert_eq!(
            Policy::from_args(&args(&["--cull-after-ms", "7", "--rotate-bytes", "1"])).unwrap(),
            Policy {
                rotate_bytes: 1,
                cull_after_ms: Some(7)
            }
        );
        for (bad, says) in [
            (
                vec!["--cull-after-ms", "7"],
                "expected --rotate-bytes above 0",
            ),
            (
                vec!["--cull-after-ms", "7", "--rotate-bytes", "0"],
                "expected --rotate-bytes above 0",
            ),
            (
                vec!["--rotate-bytes", "lots"],
                "expects a whole number, saw lots",
            ),
            (
                vec!["--rotate-bytes", "9", "--cull-after-ms", "-1"],
                "expects a whole number, saw -1",
            ),
        ] {
            let e = Policy::from_args(&args(&bad)).unwrap_err();
            assert!(e.to_string().contains(says), "{bad:?}: {e}");
        }
    }

    /// A fresh directory for one test, so archive names are its own.
    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("roger-durlog-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_rotation_keeps_the_old_file_whole_and_starts_the_new_one_with_the_snapshot() {
        let dir = temp_dir("rotate");
        let path = dir.join("x.log");
        let (mut log, _) = open(&path).unwrap();
        log.append("{\"a\":1}").unwrap();
        log.append("{\"b\":2}").unwrap();
        let seq = log
            .rotate(
                vec![("at_ms", Value::Int(5))],
                &["{\"keep\":1}".to_string(), "{\"keep\":2}".to_string()],
            )
            .unwrap();
        assert_eq!(seq, 1);
        log.append("{\"c\":3}").unwrap();
        assert_eq!(
            fs::read_to_string(dir.join("x.log.1")).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
        let fresh = "{\"snapshot\":1,\"prior\":\"x.log.1\",\"at_ms\":5}\n{\"keep\":1}\n{\"keep\":2}\n{\"snapshot_end\":1}\n{\"c\":3}\n";
        assert_eq!(fs::read_to_string(&path).unwrap(), fresh);
        assert!(!dir.join("x.log.next").exists());
        // Both stay locked while the log is open: a process that opened the
        // path just before the rotation holds the old file, and must not get
        // its lock.
        assert!(DurLog::open(&path).is_err());
        assert!(DurLog::open(&dir.join("x.log.1")).is_err());
        drop(log);
        drop(DurLog::open(&dir.join("x.log.1")).unwrap());
        let (mut log, lines) = open(&path).unwrap();
        assert_eq!(lines.len(), 5);
        // A second rotation takes the next number, even past a gap, and a
        // stale `.next` left by a rotation that stopped is written over.
        fs::write(dir.join("x.log.6"), "someone else").unwrap();
        fs::write(dir.join("x.log.next"), "half a snapshot").unwrap();
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 7);
        assert_eq!(fs::read_to_string(dir.join("x.log.7")).unwrap(), fresh);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "{\"snapshot\":7,\"prior\":\"x.log.7\"}\n{\"snapshot_end\":7}\n"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_archive_number_is_not_taken_again_after_its_archive_is_gone_or_compressed() {
        let dir = temp_dir("numbers");
        let path = dir.join("x.log");
        let (mut log, _) = open(&path).unwrap();
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 1);
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 2);
        // The operator compresses one archive and removes the other.
        fs::rename(dir.join("x.log.1"), dir.join("x.log.1.gz")).unwrap();
        fs::remove_file(dir.join("x.log.2")).unwrap();
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 3);
        // The same after a reopen with every archive gone: the file's own
        // snapshot line carries the last number.
        fs::remove_file(dir.join("x.log.3")).unwrap();
        fs::remove_file(dir.join("x.log.1.gz")).unwrap();
        drop(log);
        let (mut log, lines) = open(&path).unwrap();
        assert_eq!(lines[0], "{\"snapshot\":3,\"prior\":\"x.log.3\"}");
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 4);
        // A compressed archive alone also holds its number.
        fs::rename(dir.join("x.log.4"), dir.join("x.log.9.zst")).unwrap();
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 10);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_log_opened_through_a_link_rotates_the_file_and_leaves_the_link() {
        let dir = temp_dir("link");
        fs::create_dir_all(dir.join("real")).unwrap();
        let (real, link) = (dir.join("real/x.log"), dir.join("named.log"));
        fs::write(&real, "{\"a\":1}\n").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let (mut log, lines) = open(&link).unwrap();
        assert_eq!(lines, ["{\"a\":1}"]);
        log.rotate(Vec::new(), &[]).unwrap();
        log.append("{\"b\":2}").unwrap();
        // The link still names the log; the archive is beside the real file.
        assert!(fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(
            fs::read_to_string(dir.join("real/x.log.1")).unwrap(),
            "{\"a\":1}\n"
        );
        assert!(fs::read_to_string(&link).unwrap().ends_with("{\"b\":2}\n"));
        assert!(!dir.join("named.log.1").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_rotation_that_stopped_after_naming_its_archive_is_taken_up_under_that_name() {
        let dir = temp_dir("resume");
        let path = dir.join("x.log");
        let (mut log, _) = open(&path).unwrap();
        log.append("{\"a\":1}").unwrap();
        // What a crash between the link and the rename leaves behind.
        fs::hard_link(&path, dir.join("x.log.1")).unwrap();
        assert_eq!(log.rotate(Vec::new(), &[]).unwrap(), 1);
        assert_eq!(
            fs::read_to_string(dir.join("x.log.1")).unwrap(),
            "{\"a\":1}\n"
        );
        assert!(!dir.join("x.log.2").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_log_is_due_once_it_has_grown_by_the_bound_and_by_its_own_snapshot() {
        let dir = temp_dir("due");
        let path = dir.join("x.log");
        let line = format!("{{\"pad\":\"{}\"}}", "x".repeat(89)); // 100 bytes with its newline
        assert_eq!(line.len(), 99);
        let policy = Policy {
            rotate_bytes: 300,
            cull_after_ms: None,
        };
        let (mut log, _) = open(&path).unwrap();
        assert!(!log.due(&Policy::default()), "never without a bound");
        for grown in [100, 200] {
            log.append(&line).unwrap();
            assert!(!log.due(&policy), "{grown}");
        }
        log.append(&line).unwrap();
        assert!(log.due(&policy));
        assert!(!log.due(&Policy::default()));
        // A snapshot larger than the bound: due only after as much again.
        let kept: Vec<String> = (0..10).map(|_| line.clone()).collect();
        log.rotate(Vec::new(), &kept).unwrap();
        let snapshot = fs::metadata(&path).unwrap().len();
        assert!(snapshot > 1000, "{snapshot}");
        for _ in 0..10 {
            assert!(!log.due(&policy));
            log.append(&line).unwrap();
        }
        assert!(
            !log.due(&policy),
            "1000 bytes past a {snapshot} byte snapshot"
        );
        log.append(&line).unwrap();
        assert!(log.due(&policy));
        // The same answer after a reopen: the replay finds where the snapshot ends.
        drop(log);
        let (log, mut replay) = DurLog::open(&path).unwrap();
        assert!(!log.due(&policy), "not before the replay is read");
        assert_eq!(replay.by_ref().count(), 23);
        assert!(log.due(&policy));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_drops_trailing_partial_line() {
        let path = temp_path("partial");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"{\"a\":1}\n{\"b\":2").unwrap(); // crash mid-append
        }
        let (_log, lines) = open(&path).unwrap();
        assert_eq!(lines, vec!["{\"a\":1}".to_string()]);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn append_after_a_crash_fragment_does_not_glue_onto_it() {
        let path = temp_path("glue");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"{\"a\":1}\n{\"b\":2").unwrap(); // crash mid-append
        }
        {
            let (mut log, _) = open(&path).unwrap();
            log.append("{\"c\":3}").unwrap();
        }
        let (_log, lines) = open(&path).unwrap();
        let raw = fs::read_to_string(&path).unwrap();
        let _ = fs::remove_file(&path);
        assert_eq!(
            lines,
            vec!["{\"a\":1}".to_string(), "{\"c\":3}".to_string()],
            "raw file: {raw:?}"
        );
    }
}

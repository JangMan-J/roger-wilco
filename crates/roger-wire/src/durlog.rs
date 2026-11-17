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

use std::fs::{self, File, Metadata};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendSpan {
    pub pre_offset: u64,
    pub bytes: u64,
    pub file_id: Option<(u64, u64)>,
}

pub struct DurLog {
    file: File,
    file_id: Option<(u64, u64)>,
    /// The first append failure, kept so every later append refuses.
    poison: Option<String>,
    /// Set by `Replay` when it has read to the end and cut any torn tail. An
    /// append before that could glue onto a fragment, so it is refused.
    replayed: Arc<AtomicBool>,
    /// Byte offset recorded by `Replay` when it finishes, then advanced by each append.
    replayed_offset: Arc<AtomicU64>,
    written: Option<u64>,
}

/// The complete lines a log held when it was opened, in order, empty lines
/// left out. Read it to its end before the first append.
pub struct Replay {
    reader: BufReader<File>,
    /// How many bytes of whole lines have been read: where a torn tail starts.
    keep: u64,
    done: bool,
    replayed: Arc<AtomicBool>,
    replayed_offset: Arc<AtomicU64>,
}

impl Replay {
    /// Verified byte offset of the end of all complete lines read so far.
    pub fn offset(&self) -> u64 {
        self.keep
    }
}

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
                    if let Err(e) = self
                        .reader
                        .get_ref()
                        .set_len(self.keep)
                        .and_then(|()| self.reader.get_ref().sync_all())
                    {
                        return Some(Err(e));
                    }
                }
                self.replayed_offset.store(self.keep, Ordering::SeqCst);
                self.replayed.store(true, Ordering::SeqCst);
                break;
            }
            self.keep += n as u64;
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if !line.is_empty() {
                return Some(Ok(String::from_utf8_lossy(&line).into_owned()));
            }
        }
        None
    }
}

/// Flush a directory entry on Unix; on non-Unix platforms opening a directory
/// with `File::open` is unsupported and metadata follows the file flush.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Acquire a non-blocking exclusive process lock on `path`, creating parent
/// directories when needed. Held for the lifetime of the returned `File`.
pub fn open_exclusive_lock(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let existed = path.exists();
    let file = open_exclusive(path)?;
    if !existed {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                sync_dir(parent)?;
            }
        }
    }
    Ok(file)
}

fn open_exclusive(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        let file = fs::OpenOptions::new()
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
        Ok(file)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_SHARE_READ (1) only: refuses a second writer at open while still
        // letting read-only callers (`roger mcp` inbox, `roger audit`) read the
        // file without hitting a mandatory byte-range lock violation.
        fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .share_mode(1)
            .open(path)
            .map_err(|e| {
                if e.raw_os_error() == Some(32) {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "expected {} unlocked, saw it held by another process: {e}",
                            path.display()
                        ),
                    )
                } else {
                    e
                }
            })
    }
}

impl DurLog {
    pub fn open(path: &Path) -> io::Result<(DurLog, Replay)> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let existed = path.exists();
        let file = open_exclusive(path)?;
        if !existed {
            // Make the new file itself durable.
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    sync_dir(parent)?;
                }
            }
        }
        let file_id = file.metadata().ok().and_then(|m| meta_file_id(&m));
        let replayed = Arc::new(AtomicBool::new(false));
        let replayed_offset = Arc::new(AtomicU64::new(0));
        // The same open file, so the lock above covers the replay too.
        let replay = Replay {
            reader: BufReader::new(file.try_clone()?),
            keep: 0,
            done: false,
            replayed: Arc::clone(&replayed),
            replayed_offset: Arc::clone(&replayed_offset),
        };
        Ok((
            DurLog {
                file,
                file_id,
                poison: None,
                replayed,
                replayed_offset,
                written: None,
            },
            replay,
        ))
    }

    #[cfg(test)]
    fn from_file(file: File) -> DurLog {
        DurLog {
            file,
            file_id: None,
            poison: None,
            replayed: Arc::new(AtomicBool::new(true)),
            replayed_offset: Arc::new(AtomicU64::new(0)),
            written: Some(0),
        }
    }

    /// True once an append has failed: nothing appended since has committed.
    pub fn is_poisoned(&self) -> bool {
        self.poison.is_some()
    }

    pub fn append(&mut self, line: &str) -> io::Result<()> {
        self.append_at(line).map(|_| ())
    }

    /// Append `line` and return its `AppendSpan` once `sync_all()` succeeds.
    pub fn append_at(&mut self, line: &str) -> io::Result<AppendSpan> {
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
        let pre = *self
            .written
            .get_or_insert_with(|| self.replayed_offset.load(Ordering::SeqCst));
        let bytes = (line.len() + 1) as u64;
        self.write_line(line)
            .inspect_err(|e| self.poison = Some(e.to_string()))?;
        self.written = Some(pre + bytes);
        Ok(AppendSpan {
            pre_offset: pre,
            bytes,
            file_id: self.file_id,
        })
    }

    fn write_line(&mut self, line: &str) -> io::Result<()> {
        self.file.write_all(line.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.sync_all()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailEvent<'a> {
    Reset,
    Line(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailDelta {
    pub reset: bool,
    pub bytes_read: u64,
    pub lines_parsed: usize,
}
#[cfg(unix)]
fn meta_file_id(meta: &Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn meta_file_id(_meta: &Metadata) -> Option<(u64, u64)> {
    None
}

/// Incremental byte-offset tail reader for append-only JSONL Store files.
/// Reads only newly appended `\n`-terminated lines from `offset..EOF`, ignores
/// any trailing fragment without `\n`, and resets cleanly if the file is
/// truncated, deleted, or replaced.
pub struct TailReader {
    path: PathBuf,
    offset: u64,
    initialized: bool,
    file_id: Option<(u64, u64)>,
}

impl TailReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            initialized: false,
            file_id: None,
        }
    }

    pub fn from_replayed(path: PathBuf, offset: u64) -> io::Result<Self> {
        match fs::metadata(&path) {
            Ok(meta) => Ok(Self {
                path,
                offset,
                initialized: true,
                file_id: meta_file_id(&meta),
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::new(path)),
            Err(e) => Err(io::Error::new(
                e.kind(),
                format!("expected to read {}, saw {e}", path.display()),
            )),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Advance the tail offset in-memory only when this reader is already
    /// initialized on the exact same `file_id` at `expected_offset`.
    pub fn advance_from(&mut self, span: AppendSpan) -> bool {
        if self.initialized && self.offset == span.pre_offset && self.file_id == span.file_id {
            self.offset += span.bytes;
            true
        } else {
            false
        }
    }

    pub fn refresh(&mut self, mut on_event: impl FnMut(TailEvent<'_>)) -> io::Result<TailDelta> {
        let meta = match fs::metadata(&self.path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let reset = self.initialized;
                if reset {
                    on_event(TailEvent::Reset);
                }
                self.offset = 0;
                self.initialized = false;
                self.file_id = None;
                return Ok(TailDelta {
                    reset,
                    bytes_read: 0,
                    lines_parsed: 0,
                });
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("expected to read {}, saw {e}", self.path.display()),
                ))
            }
        };
        let id = meta_file_id(&meta);
        let len = meta.len();
        if self.initialized && id == self.file_id && len == self.offset {
            return Ok(TailDelta {
                reset: false,
                bytes_read: 0,
                lines_parsed: 0,
            });
        }

        let mut f = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let reset = self.initialized;
                if reset {
                    on_event(TailEvent::Reset);
                }
                self.offset = 0;
                self.initialized = false;
                self.file_id = None;
                return Ok(TailDelta {
                    reset,
                    bytes_read: 0,
                    lines_parsed: 0,
                });
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("expected to read {}, saw {e}", self.path.display()),
                ))
            }
        };
        let meta = f.metadata()?;
        let id = meta_file_id(&meta);
        let len = meta.len();
        let mut reset = false;
        if !self.initialized || id != self.file_id || len < self.offset {
            if self.initialized {
                reset = true;
                on_event(TailEvent::Reset);
            }
            self.offset = 0;
        }
        self.file_id = id;
        self.initialized = true;
        if len == self.offset {
            return Ok(TailDelta {
                reset,
                bytes_read: 0,
                lines_parsed: 0,
            });
        }

        f.seek(SeekFrom::Start(self.offset))?;
        let mut buf = Vec::with_capacity((len - self.offset) as usize);
        f.read_to_end(&mut buf)?;
        let end = buf.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
        if end == 0 {
            return Ok(TailDelta {
                reset,
                bytes_read: 0,
                lines_parsed: 0,
            });
        }
        let mut lines_parsed = 0usize;
        for raw in buf[..end].split(|&b| b == b'\n') {
            let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
            if raw.is_empty() {
                continue;
            }
            let text = String::from_utf8_lossy(raw);
            on_event(TailEvent::Line(&text));
            lines_parsed += 1;
        }
        self.offset += end as u64;
        Ok(TailDelta {
            reset,
            bytes_read: end as u64,
            lines_parsed,
        })
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

    fn temp_path(tag: &str) -> PathBuf {
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

    #[test]
    fn replay_drops_trailing_partial_line() {
        let path = temp_path("partial");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"{\"a\":1}\n{\"b\":2").unwrap();
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
            f.write_all(b"{\"a\":1}\n{\"b\":2").unwrap();
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

    #[test]
    fn tail_reader_reads_incrementally_ignores_torn_tail_and_resets_on_truncate_or_replace() {
        let path = temp_path("tail");
        let mut tail = TailReader::new(path.clone());
        let mut items: Vec<String> = Vec::new();
        let apply = |items: &mut Vec<String>, ev: TailEvent<'_>| match ev {
            TailEvent::Reset => items.clear(),
            TailEvent::Line(l) => items.push(l.to_string()),
        };

        // Missing file -> 0 bytes, no reset.
        let d0 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert_eq!(
            d0,
            TailDelta {
                reset: false,
                bytes_read: 0,
                lines_parsed: 0
            }
        );

        // Initial write with 2 complete lines + torn tail.
        let l1 = "{\"record\":\"r-1\"}\n";
        let l2 = "{\"record\":\"r-2\"}\n";
        fs::write(&path, format!("{l1}{l2}{{\"record\":\"r-3\"")).unwrap();
        let d1 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert_eq!(
            d1,
            TailDelta {
                reset: false,
                bytes_read: (l1.len() + l2.len()) as u64,
                lines_parsed: 2
            }
        );
        assert_eq!(items, vec!["{\"record\":\"r-1\"}", "{\"record\":\"r-2\"}"]);

        // No change -> 0 bytes read, 0 lines parsed.
        let d2 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert_eq!(
            d2,
            TailDelta {
                reset: false,
                bytes_read: 0,
                lines_parsed: 0
            }
        );

        // DurLog::open truncates torn tail and appends r-3 -> only r-3 is read & parsed.
        {
            let (mut log, replay) = DurLog::open(&path).unwrap();
            let replayed: Vec<String> = replay.collect::<io::Result<Vec<_>>>().unwrap();
            assert_eq!(replayed.len(), 2);
            log.append("{\"record\":\"r-3\"}").unwrap();
        }
        let d3 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert_eq!(
            d3,
            TailDelta {
                reset: false,
                bytes_read: "{\"record\":\"r-3\"}\n".len() as u64,
                lines_parsed: 1
            }
        );
        assert_eq!(items.len(), 3);

        // Truncate file -> resets cleanly and re-reads from 0.
        fs::write(&path, "{\"record\":\"r-new\"}\n").unwrap();
        let d4 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert!(d4.reset);
        assert_eq!(d4.lines_parsed, 1);
        assert_eq!(items, vec!["{\"record\":\"r-new\"}"]);

        // Atomic replace with same-length file -> inode change triggers reset.
        let tmp = temp_path("tail-replace");
        fs::write(&tmp, "{\"record\":\"r-rep\"}\n").unwrap();
        fs::rename(&tmp, &path).unwrap();
        let d5 = tail.refresh(|ev| apply(&mut items, ev)).unwrap();
        assert!(d5.reset);
        assert_eq!(d5.lines_parsed, 1);
        assert_eq!(items, vec!["{\"record\":\"r-rep\"}"]);

        let _ = fs::remove_file(&path);
    }
}

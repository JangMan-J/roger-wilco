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

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct DurLog {
    file: File,
    /// The first append failure, kept so every later append refuses.
    poison: Option<String>,
    /// Set by `Replay` when it has read to the end and cut any torn tail. An
    /// append before that could glue onto a fragment, so it is refused.
    replayed: Arc<AtomicBool>,
}

/// The complete lines a log held when it was opened, in order, empty lines
/// left out. Read it to its end before the first append.
pub struct Replay {
    reader: BufReader<File>,
    /// How many bytes of whole lines have been read: where a torn tail starts.
    keep: u64,
    done: bool,
    replayed: Arc<AtomicBool>,
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
                    if let Err(e) = self.reader.get_ref().set_len(self.keep) {
                        return Some(Err(e));
                    }
                }
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
        let replayed = Arc::new(AtomicBool::new(false));
        // The same open file, so the lock above covers the replay too.
        let replay = Replay {
            reader: BufReader::new(file.try_clone()?),
            keep: 0,
            done: false,
            replayed: Arc::clone(&replayed),
        };
        Ok((
            DurLog {
                file,
                poison: None,
                replayed,
            },
            replay,
        ))
    }

    #[cfg(test)]
    fn from_file(file: File) -> DurLog {
        DurLog {
            file,
            poison: None,
            replayed: Arc::new(AtomicBool::new(true)),
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
            .inspect_err(|e| self.poison = Some(e.to_string()))
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

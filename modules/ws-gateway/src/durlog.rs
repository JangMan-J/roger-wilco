//! Durable append-only Store log and SHA-256 Store naming.
//!
//! Store files written here are byte-for-byte compatible with `roger` (`src/durlog.rs`,
//! `src/mcp.rs`, and `roger audit --store`).

use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Collision-free Store file path for `address` inside `store_dir`:
/// `<hex_sha256_of_address>.store`.
pub fn store_path(store_dir: &Path, address: &str) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(address.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{b:02x}");
    }
    store_dir.join(format!("{hex}.store"))
}

/// Run a short blocking filesystem operation safely whether called from a
/// multi-thread Tokio worker or a single-threaded unit test.
pub fn blocking_io<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

pub struct DurLog {
    file: File,
    poison: Option<String>,
    replayed: Arc<AtomicBool>,
}

pub struct Replay {
    reader: BufReader<File>,
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

fn open_exclusive(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        // no-poll: single non-blocking lock check on Store open; never retried.
        let lock_res = file.try_lock();
        lock_res.map_err(|e| {
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
        std::fs::OpenOptions::new()
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
                std::fs::create_dir_all(parent)?;
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
        let replayed = Arc::new(AtomicBool::new(false));
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
        self.file.write_all(line.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.sync_all()
    }
}

fn fill_random(b: &mut [u8; 16]) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Read as _;
        File::open("/dev/urandom")?.read_exact(b)
    }
    #[cfg(windows)]
    {
        #[link(name = "bcrypt", kind = "raw-dylib")]
        unsafe extern "system" {
            fn BCryptGenRandom(
                algorithm: *mut std::ffi::c_void,
                buffer: *mut u8,
                bytes: u32,
                flags: u32,
            ) -> i32;
        }
        let status = unsafe { BCryptGenRandom(std::ptr::null_mut(), b.as_mut_ptr(), 16, 2) };
        if status < 0 {
            Err(io::Error::other(format!(
                "expected BCryptGenRandom to succeed, saw NTSTATUS {status:#x}"
            )))
        } else {
            Ok(())
        }
    }
}

/// Generate a fresh RFC 4122 UUIDv4 using the OS cryptographic RNG.
pub fn gen_uuid() -> io::Result<String> {
    let mut b = [0u8; 16];
    fill_random(&mut b)?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_path_hashes_composite_and_slash_addresses_deterministically() {
        let dir = Path::new("/tmp/stores");
        let a = "[[153:Meteor Shower:7]]11111111-1111-4111-8111-111111111111";
        let b = "team/alpha/node:1";
        let pa = store_path(dir, a);
        let pb = store_path(dir, b);
        assert_eq!(pa, store_path(dir, a));
        assert_ne!(pa, pb);
        assert!(pa
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".store"));
        assert_eq!(pa.file_name().unwrap().to_str().unwrap().len(), 64 + 6);
    }

    #[test]
    fn durlog_truncates_torn_tail_and_preserves_header_key_order() {
        let dir = std::env::temp_dir().join(format!("roger-ws-durlog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.store");
        std::fs::write(
            &path,
            "{\"record\":\"r-1\",\"mail\":\"m1\",\"from\":\"a\",\"body\":\"hi\",\"kind\":\"directed\",\"headers\":{\"z\":\"1\",\"a\":\"2\",\"é\":\"3\"}}\n{\"record\":\"r-2\"",
        )
        .unwrap();

        {
            let (mut log, replay) = DurLog::open(&path).unwrap();
            // A second open while held must fail immediately.
            assert!(DurLog::open(&path).is_err());
            let lines: Vec<String> = replay.collect::<io::Result<Vec<_>>>().unwrap();
            assert_eq!(lines.len(), 1);
            log.append("{\"record\":\"r-2\",\"mail\":\"m2\",\"from\":\"a\",\"body\":\"ok\",\"kind\":\"directed\",\"headers\":{}}")
                .unwrap();
        }

        let (_log, replay) = DurLog::open(&path).unwrap();
        let lines: Vec<String> = replay.collect::<io::Result<Vec<_>>>().unwrap();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains(r#""headers":{"z":"1","a":"2","é":"3"}"#));
        assert!(lines[1].contains(r#""record":"r-2""#));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

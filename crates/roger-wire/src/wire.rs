//! Shared wire constants, header bounds, opaque token helpers, and Store/cursor utilities.

use crate::durlog::sync_dir;
use crate::sha256::hex_digest;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub const MAX_HEADERS: usize = 32;
pub const MAX_KEY_BYTES: usize = 128;
pub const MAX_VALUE_BYTES: usize = 4096;

/// Joins a Broadcast id to a recipient Address in that recipient's copy id.
pub const COPY_SEP: char = '/';

/// The Kind of a Mail sent to one Address.
pub const DIRECTED: &str = "directed";
/// The Kind of a copy of a Broadcast.
pub const BROADCAST: &str = "broadcast";
/// The Kind of an article delivered to a subscribed reader of a Group.
pub const GROUP: &str = "group";

/// The longest first line a TCP peer may send before it has shown its token.
pub const HELLO_MAX: usize = 64 * 1024;

/// Compare two secrets in constant time without stopping at the first differing byte.
pub fn same_secret_bytes(seen: &[u8], expected: &[u8]) -> bool {
    seen.len() == expected.len()
        && seen
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// Compare two secret strings without stopping at the first differing byte.
pub fn same_secret(seen: &str, expected: &str) -> bool {
    same_secret_bytes(seen.as_bytes(), expected.as_bytes())
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub fn format_iso8601_utc(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let day_secs = secs.rem_euclid(86_400);
    let hour = day_secs / 3600;
    let min = (day_secs % 3600) / 60;
    let sec = day_secs % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
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
        // BCRYPT_USE_SYSTEM_PREFERRED_RNG = 0x00000002
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

/// Collision-free Store file path for `address` inside `store_dir`:
/// `<hex_sha256_of_address>.store`.
pub fn store_path(store_dir: &Path, address: &str) -> PathBuf {
    store_dir.join(format!("{}.store", hex_digest(address.as_bytes())))
}

/// Cursor file path alongside `store`: `<store>.cursor`.
pub fn cursor_path(store: &Path) -> PathBuf {
    let mut p = store.as_os_str().to_owned();
    p.push(".cursor");
    PathBuf::from(p)
}

/// Read the saved Cursor from `<store>.cursor` if present.
pub fn load_cursor(pos_file: &Path) -> io::Result<Option<String>> {
    match std::fs::read_to_string(pos_file) {
        Ok(text) => Ok(Some(text.trim().to_string()).filter(|c| !c.is_empty())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("expected to read {}, saw {e}", pos_file.display()),
        )),
    }
}

/// Atomically persist the read position via `<cursor>.tmp` -> `sync_all` -> `rename` -> dir `sync_all`.
pub fn save_cursor(pos_file: &Path, record: &str) -> io::Result<()> {
    let mut tmp = pos_file.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut f = File::create(&tmp)?;
    f.write_all(format!("{record}\n").as_bytes())?;
    f.sync_all()?;
    std::fs::rename(&tmp, pos_file)?;
    if let Some(dir) = pos_file.parent().filter(|d| !d.as_os_str().is_empty()) {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Publish the bound port at `path` whole: write it beside the file, flush it,
/// then rename it into place, so a reader that sees `path` reads a port and
/// never an empty file.
pub fn publish_port(path: &Path, port: u16) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut aside = path.as_os_str().to_owned();
    aside.push(".tmp");
    let aside = PathBuf::from(aside);
    let wrote = (|| {
        let mut f = File::create(&aside)?;
        f.write_all(port.to_string().as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&aside, path)?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(())
    })();
    wrote.map_err(|e: io::Error| {
        let _ = std::fs::remove_file(&aside);
        io::Error::new(
            e.kind(),
            format!(
                "expected to publish port {port} at {} through {}, saw {e}",
                path.display(),
                aside.display()
            ),
        )
    })
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
    fn constant_time_secret_and_iso8601_format() {
        assert!(same_secret("token-abc", "token-abc"));
        assert!(!same_secret("token-abc", "token-abd"));
        assert!(!same_secret("token-abc", "token-ab"));
        assert_eq!(format_iso8601_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_iso8601_utc(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
    }
}

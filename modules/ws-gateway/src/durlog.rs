//! Durable append-only Store log and SHA-256 Store naming, backed by `roger-wire`.

pub use roger_wire::durlog::{sync_dir, DurLog, Replay};
pub use roger_wire::wire::{gen_uuid, store_path};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::path::Path;

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

//! Durable append-only Store log, atomic Cursor file, and `serde_json` Store helpers,
//! backed by `roger-wire`.

pub use roger_wire::durlog::{
    sync_dir, AppendSpan, DurLog, Replay, TailDelta, TailEvent, TailReader,
};
pub use roger_wire::wire::{cursor_path, gen_uuid, load_cursor, save_cursor, store_path};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io;
use std::path::Path;
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
/// Every complete entry in `store`, oldest first. Any trailing fragment without `\n` is ignored.
pub fn read_store_entries(store: &Path) -> io::Result<Vec<Value>> {
    let bytes = match std::fs::read(store) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("expected to read {}, saw {e}", store.display()),
            ))
        }
    };
    let end = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    Ok(String::from_utf8_lossy(&bytes[..end])
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|e| e.get("record").and_then(Value::as_str).is_some())
        .collect())
}

/// Incremental in-memory index over a Store file, synced via `TailReader` and
/// updated directly on `DurLog::append`.
pub struct StoreCache {
    tail: TailReader,
    pub entries: Vec<Value>,
    pub record_index: HashMap<String, usize>,
    pub generation: u64,
    #[cfg(test)]
    pub parses: usize,
}

impl StoreCache {
    pub fn open(store: &Path) -> io::Result<Self> {
        let mut cache = Self {
            tail: TailReader::new(store.to_path_buf()),
            entries: Vec::new(),
            record_index: HashMap::new(),
            generation: 0,
            #[cfg(test)]
            parses: 0,
        };
        cache.refresh()?;
        Ok(cache)
    }

    pub fn from_replayed(store: &Path, offset: u64, entries: Vec<Value>) -> io::Result<Self> {
        let mut record_index = HashMap::with_capacity(entries.len());
        for (idx, entry) in entries.iter().enumerate() {
            if let Some(rec) = entry.get("record").and_then(Value::as_str) {
                record_index.entry(rec.to_string()).or_insert(idx);
            }
        }
        Ok(Self {
            tail: TailReader::from_replayed(store.to_path_buf(), offset)?,
            entries,
            record_index,
            generation: 0,
            #[cfg(test)]
            parses: 0,
        })
    }

    pub fn refresh(&mut self) -> io::Result<TailDelta> {
        let entries = &mut self.entries;
        let record_index = &mut self.record_index;
        let generation = &mut self.generation;
        #[cfg(test)]
        let parses = &mut self.parses;
        self.tail.refresh(|ev| match ev {
            TailEvent::Reset => {
                entries.clear();
                record_index.clear();
                *generation += 1;
            }
            TailEvent::Line(line) => {
                #[cfg(test)]
                {
                    *parses += 1;
                }
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    if let Some(rec) = v.get("record").and_then(Value::as_str) {
                        if !record_index.contains_key(rec) {
                            let idx = entries.len();
                            record_index.insert(rec.to_string(), idx);
                            entries.push(v);
                        }
                    }
                }
            }
        })
    }

    pub fn push_appended(&mut self, entry: Value, span: AppendSpan) -> io::Result<()> {
        if self.tail.advance_from(span) {
            if let Some(rec) = entry.get("record").and_then(Value::as_str) {
                if !self.record_index.contains_key(rec) {
                    self.record_index
                        .insert(rec.to_string(), self.entries.len());
                    self.entries.push(entry);
                }
            }
            Ok(())
        } else {
            self.refresh().map(|_| ())
        }
    }

    pub fn entries_after(&self, consumed: Option<&str>, limit: usize) -> Vec<Value> {
        entries_after_indexed(&self.entries, &self.record_index, consumed, limit)
    }

    pub fn is_empty(&mut self) -> io::Result<bool> {
        self.refresh()?;
        Ok(self.entries.is_empty())
    }
}

pub fn entries_after_indexed(
    all: &[Value],
    record_index: &HashMap<String, usize>,
    consumed: Option<&str>,
    limit: usize,
) -> Vec<Value> {
    let start = consumed
        .and_then(|c| record_index.get(c).copied())
        .map_or(0, |i| i + 1);
    all[start..]
        .iter()
        .take(limit)
        .map(shape_mail_entry)
        .collect()
}

/// Entries after `consumed`, up to `limit`, shaped identically to `roger mcp`.
pub fn entries_after(all: &[Value], consumed: Option<&str>, limit: usize) -> Vec<Value> {
    let mut index = HashMap::new();
    for (i, e) in all.iter().enumerate() {
        if let Some(rec) = e.get("record").and_then(Value::as_str) {
            index.entry(rec.to_string()).or_insert(i);
        }
    }
    entries_after_indexed(all, &index, consumed, limit)
}
/// Shape one Store entry keeping keys in `["record", "mail", "from", "body", "kind", "headers"]` order.
pub fn shape_mail_entry(entry: &Value) -> Value {
    let mut out = Map::new();
    for key in [
        "record", "mail", "from", "body", "kind", "headers", "group", "article",
    ] {
        if let Some(val) = entry.get(key) {
            out.insert(key.to_string(), val.clone());
        }
    }
    Value::Object(out)
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
        let dir = std::env::temp_dir().join(format!("roger-gw-durlog-{}", std::process::id()));
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

        let entries = read_store_entries(&path).unwrap();
        assert_eq!(entries.len(), 2);
        let keys: Vec<&str> = entries[0]["headers"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["z", "a", "é"]);

        let pos = cursor_path(&path);
        save_cursor(&pos, "r-1").unwrap();
        assert_eq!(load_cursor(&pos).unwrap().as_deref(), Some("r-1"));
        let page = entries_after(&entries, Some("r-1"), 50);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0]["record"], "r-2");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_cache_updates_on_append_tails_incrementally_and_resets_on_truncate() {
        let dir = std::env::temp_dir().join(format!("roger-gw-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cache.store");
        std::fs::write(
            &path,
            "{\"record\":\"r-1\",\"mail\":\"m1\",\"from\":\"a\",\"body\":\"hi\",\"kind\":\"directed\",\"headers\":{}}\n{\"record\":\"r-torn\"",
        )
        .unwrap();

        let (mut log, mut replay) = DurLog::open(&path).unwrap();
        let mut initial = Vec::new();
        for line in &mut replay {
            let v: Value = serde_json::from_str(&line.unwrap()).unwrap();
            initial.push(v);
        }
        let mut cache = StoreCache::from_replayed(&path, replay.offset(), initial).unwrap();
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.parses, 0);

        // Append via DurLog and push directly into cache -> refresh reads 0 bytes and parses 0 lines.
        let entry2 = serde_json::json!({"record":"r-2","mail":"m2","from":"a","body":"ok","kind":"directed","headers":{}});
        let line2 = serde_json::to_string(&entry2).unwrap();
        let span2 = log.append_at(&line2).unwrap();
        cache.push_appended(entry2, span2).unwrap();
        let d = cache.refresh().unwrap();
        assert_eq!(d.bytes_read, 0);
        assert_eq!(cache.parses, 0);
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.entries_after(Some("r-1"), 10).len(), 1);
        drop(replay);
        drop(log);

        // External append followed by misaligned push_appended -> falls back to refresh without offset drift.
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"record\":\"r-3\",\"mail\":\"m3\",\"from\":\"a\",\"body\":\"ext\",\"kind\":\"directed\",\"headers\":{{}}}}").unwrap();
        }
        let (mut log2, replay2) = DurLog::open(&path).unwrap();
        let _: Vec<String> = replay2.collect::<io::Result<Vec<_>>>().unwrap();
        let entry4 = serde_json::json!({"record":"r-4","mail":"m4","from":"a","body":"sync","kind":"directed","headers":{}});
        let line4 = serde_json::to_string(&entry4).unwrap();
        let span4 = log2.append_at(&line4).unwrap();
        // cache.tail.offset is still before r-3, so span4.pre_offset != cache.tail.offset -> refreshes both r-3 and r-4 cleanly!
        cache.push_appended(entry4, span4).unwrap();
        assert_eq!(cache.entries.len(), 4);
        assert_eq!(cache.entries[2]["record"], "r-3");
        assert_eq!(cache.entries[3]["record"], "r-4");
        drop(log2);

        // Truncate/replace -> resets generation and re-reads cleanly.
        std::fs::write(
            &path,
            "{\"record\":\"r-9\",\"mail\":\"m9\",\"from\":\"a\",\"body\":\"new\",\"kind\":\"directed\",\"headers\":{}}\n",
        )
        .unwrap();
        let d3 = cache.refresh().unwrap();
        assert!(d3.reset);
        assert_eq!(cache.generation, 1);
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.entries[0]["record"], "r-9");

        let _ = std::fs::remove_dir_all(&dir);
    }
}

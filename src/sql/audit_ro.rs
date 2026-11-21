//! Concurrent read-only audit snapshots: every coordinator slice `hot.sqlite`
//! and the host daemon database open with `SQLITE_OPEN_READ_ONLY`, each read
//! inside one deferred transaction (one WAL snapshot), so any number of
//! auditors run beside a live writer and none of them blocks it. Bodies moved
//! to cold day files are read from the slice's sealed `cold/` files.

use super::{coord_root, daemon_db_path, is_sqlite_file};
use crate::cold::Index;
use crate::json::Value;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Event {
    pub seq: i64,
    pub kind: String,
    pub addr: Option<String>,
    pub host: Option<String>,
    pub old_host: Option<String>,
    pub label: Option<String>,
    pub declared: Option<String>,
    pub registration: Option<String>,
    pub claim_key: Option<String>,
    pub expires_ms: Option<i64>,
    pub mail_id: Option<String>,
    pub at_ms: Option<i64>,
    pub line: i64,
}

#[derive(Clone, Debug)]
pub struct Receipt {
    pub mail_id: String,
    pub reader: String,
    pub record_id: String,
    pub receipted_at_ms: Option<i64>,
    pub receipt_line: i64,
}

#[derive(Clone, Debug)]
pub struct Envelope {
    pub seq: i64,
    pub line: i64,
    pub mail_id: String,
    pub stream: String,
    pub article_num: i64,
    pub kind: String,
    pub sender: String,
    pub recipient: String,
    pub headers_json: Option<String>,
    pub body_len: i64,
    pub body: Option<String>,
    pub accepted_at_ms: Option<i64>,
    pub receipts: Vec<Receipt>,
    /// Readers holding a position on this envelope's stream (Group articles).
    pub subscribers: Vec<String>,
    /// A Group article at or below its stream's prune floor.
    pub pruned: bool,
}

#[derive(Clone, Debug)]
pub enum Item {
    Timeline(Event),
    Envelope(Envelope),
}

impl Item {
    fn seq(&self) -> i64 {
        match self {
            Item::Timeline(t) => t.seq,
            Item::Envelope(e) => e.seq,
        }
    }
}

#[derive(Default)]
pub struct Coord {
    pub record_count: usize,
    pub items: Vec<Item>,
}

#[derive(Default)]
pub struct Daemon {
    pub record_count: usize,
    pub pending: Vec<(i64, super::daemon::Delivery)>,
    pub unconfirmed_receipts: Vec<(i64, super::daemon::Receipt)>,
}

pub fn has_coord(data: &Path) -> bool {
    let root = coord_root(data);
    root.join("slice-00").join("hot.sqlite").exists()
        || root.join("hot.sqlite").exists()
        || is_sqlite_file(data)
}

pub fn has_daemon(data: &Path) -> bool {
    let db = daemon_db_path(data);
    db.exists() && is_sqlite_file(&db)
}

fn sql_err(what: &Path, e: rusqlite::Error) -> io::Error {
    io::Error::other(format!(
        "expected a readable SQLite database at {}, saw {e}",
        what.display()
    ))
}

/// A read-only, no-mutex connection: it never takes a write lock, so it
/// neither waits on nor holds up the one writer of the file.
fn open_ro(db: &Path) -> io::Result<Connection> {
    Connection::open_with_flags(
        db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| sql_err(db, e))
}

/// Every slice database and its slice directory, in slice order.
fn slice_dbs(data: &Path) -> io::Result<Vec<(PathBuf, PathBuf)>> {
    if is_sqlite_file(data) {
        let dir = data.parent().unwrap_or(Path::new(".")).to_path_buf();
        return Ok(vec![(data.to_path_buf(), dir)]);
    }
    let root = coord_root(data);
    if root.join("hot.sqlite").exists() {
        return Ok(vec![(root.join("hot.sqlite"), root)]);
    }
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&root)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("slice-"))
                && p.join("hot.sqlite").exists()
        })
        .collect();
    dirs.sort();
    Ok(dirs
        .into_iter()
        .map(|d| (d.join("hot.sqlite"), d))
        .collect())
}

fn read_slice(db: &Path, dir: &Path, include_bodies: bool, out: &mut Coord) -> io::Result<()> {
    let conn = open_ro(db)?;
    let e = |err| sql_err(db, err);
    // One deferred read transaction: every query below sees one snapshot.
    conn.execute_batch("BEGIN DEFERRED").map_err(e)?;

    let mut stmt = conn
        .prepare(
            "SELECT seq, kind, addr, host, old_host, label, declared, registration, \
             claim_key, expires_ms, mail_id, at_ms, raw_line FROM timeline ORDER BY seq",
        )
        .map_err(e)?;
    let timeline = stmt
        .query_map([], |r| {
            let seq: i64 = r.get(0)?;
            Ok(Event {
                seq,
                kind: r.get(1)?,
                addr: r.get(2)?,
                host: r.get(3)?,
                old_host: r.get(4)?,
                label: r.get(5)?,
                declared: r.get(6)?,
                registration: r.get(7)?,
                claim_key: r.get(8)?,
                expires_ms: r.get(9)?,
                mail_id: r.get(10)?,
                at_ms: r.get(11)?,
                line: r.get::<_, Option<i64>>(12)?.unwrap_or(seq),
            })
        })
        .map_err(e)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(e)?;
    drop(stmt);

    let mut receipts: HashMap<String, Vec<Receipt>> = HashMap::new();
    let mut stmt = conn
        .prepare(
            "SELECT mail_id, reader, record_id, receipted_at_ms, receipt_seq, raw_line \
             FROM receipts ORDER BY receipt_seq",
        )
        .map_err(e)?;
    let rows = stmt
        .query_map([], |r| {
            let receipt_seq: i64 = r.get(4)?;
            Ok(Receipt {
                mail_id: r.get(0)?,
                reader: r.get(1)?,
                record_id: r.get(2)?,
                receipted_at_ms: r.get(3)?,
                receipt_line: r.get::<_, Option<i64>>(5)?.unwrap_or(receipt_seq),
            })
        })
        .map_err(e)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(e)?;
    drop(stmt);
    let receipt_count = rows.len();
    for row in rows {
        receipts.entry(row.mail_id.clone()).or_default().push(row);
    }

    let mut subscribers: HashMap<String, Vec<String>> = HashMap::new();
    let mut stmt = conn
        .prepare("SELECT stream, reader FROM positions ORDER BY stream, reader")
        .map_err(e)?;
    for row in stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(e)?
    {
        let (stream, reader) = row.map_err(e)?;
        subscribers.entry(stream).or_default().push(reader);
    }
    drop(stmt);

    // Older stores lack the prune floor column; then nothing is pruned.
    let mut stmt = conn.prepare("PRAGMA table_info(streams)").map_err(e)?;
    let has_floor = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .map_err(e)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(e)?
        .iter()
        .any(|name| name == "pruned_through");
    drop(stmt);
    let pruned_expr = if has_floor {
        "(e.kind = 'group' AND e.article_num <= COALESCE((SELECT s.pruned_through \
         FROM streams s WHERE s.stream = e.stream), 0))"
    } else {
        "0"
    };
    let mut stmt = conn
        .prepare(&format!(
            "SELECT e.seq, e.raw_line, e.mail_id, e.stream, e.article_num, e.kind, e.sender, \
             e.recipient, e.headers_json, e.body_len, e.accepted_at_ms, e.day_bucket, e.in_cold, \
             CASE WHEN ?1 THEN h.body END, {pruned_expr} \
             FROM envelopes e LEFT JOIN hot_bodies h ON h.mail_id = e.mail_id ORDER BY e.seq"
        ))
        .map_err(e)?;
    let mut envelopes = Vec::new();
    for row in stmt
        .query_map([include_bodies], |r| {
            let seq: i64 = r.get(0)?;
            let stream: String = r.get(3)?;
            let mail_id: String = r.get(2)?;
            let pruned = r.get::<_, i64>(14)? != 0;
            Ok((
                Envelope {
                    seq,
                    line: r.get::<_, Option<i64>>(1)?.unwrap_or(seq),
                    receipts: receipts.remove(&mail_id).unwrap_or_default(),
                    subscribers: subscribers.get(&stream).cloned().unwrap_or_default(),
                    mail_id,
                    stream,
                    article_num: r.get(4)?,
                    kind: r.get(5)?,
                    sender: r.get(6)?,
                    recipient: r.get(7)?,
                    headers_json: r.get(8)?,
                    body_len: r.get(9)?,
                    accepted_at_ms: r.get(10)?,
                    body: if pruned { None } else { r.get(13)? },
                    pruned,
                },
                r.get::<_, i64>(11)?,
                r.get::<_, i64>(12)?,
            ))
        })
        .map_err(e)?
    {
        envelopes.push(row.map_err(e)?);
    }
    drop(stmt);

    // Loaded inside the snapshot, after it is fixed: any row this snapshot
    // marks cold names a day file that was sealed before that commit.
    if include_bodies && envelopes.iter().any(|(_, _, cold)| *cold != 0) {
        let cold = Index::load(&dir.join("cold"))?;
        for (env, day, in_cold) in envelopes.iter_mut() {
            if *in_cold != 0 && !env.pruned {
                env.body = cold.read_body(*day, &env.mail_id)?;
            }
        }
    }
    conn.execute_batch("COMMIT").map_err(e)?;

    out.record_count += timeline.len() + envelopes.len() + receipt_count;
    out.items.extend(timeline.into_iter().map(Item::Timeline));
    out.items
        .extend(envelopes.into_iter().map(|(env, _, _)| Item::Envelope(env)));
    Ok(())
}

/// Timeline rows and envelopes (with their receipts, oldest first) of every
/// slice under `data`, merged in global `seq` order.
pub fn coord(data: &Path, include_bodies: bool) -> io::Result<Coord> {
    let slices = slice_dbs(data)?;
    if slices.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "expected a coordinator slice hot.sqlite under {}, saw none",
                data.display()
            ),
        ));
    }
    let mut snapshot = Coord::default();
    for (db, dir) in &slices {
        read_slice(db, dir, include_bodies, &mut snapshot)?;
    }
    snapshot.items.sort_by_key(Item::seq);
    Ok(snapshot)
}

/// The host daemon's pending deliveries and unconfirmed receipts, in order.
pub fn daemon(data: &Path) -> io::Result<Daemon> {
    let db = daemon_db_path(data);
    let conn = open_ro(&db)?;
    let e = |err| sql_err(&db, err);
    conn.execute_batch("BEGIN DEFERRED").map_err(e)?;
    let mut stmt = conn
        .prepare(
            "SELECT seq, mail_id, sender, recipient, group_name, article_num, body, kind, \
             headers_json FROM pending_deliveries ORDER BY seq",
        )
        .map_err(e)?;
    let pending = stmt
        .query_map([], |r| {
            let headers: String = r.get(8)?;
            Ok((
                r.get::<_, i64>(0)?,
                super::daemon::Delivery {
                    mail_id: r.get(1)?,
                    from: r.get(2)?,
                    to: r.get(3)?,
                    group: r.get(4)?,
                    article_num: r.get(5)?,
                    body: r.get(6)?,
                    kind: r.get(7)?,
                    headers: crate::json::parse(&headers).unwrap_or(Value::Str(headers)),
                },
            ))
        })
        .map_err(e)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(e)?;
    drop(stmt);
    let mut stmt = conn
        .prepare("SELECT seq, mail_id, session, record_id FROM unconfirmed_receipts ORDER BY seq")
        .map_err(e)?;
    let unconfirmed_receipts = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                super::daemon::Receipt {
                    mail_id: r.get(1)?,
                    session: r.get(2)?,
                    record_id: r.get(3)?,
                },
            ))
        })
        .map_err(e)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(e)?;
    drop(stmt);
    conn.execute_batch("COMMIT").map_err(e)?;
    Ok(Daemon {
        record_count: pending.len() + unconfirmed_receipts.len(),
        pending,
        unconfirmed_receipts,
    })
}

/// Test fixture: a coordinator slice under `<dir>/coord.slices/slice-00` with
/// a Register, a hot directed Mail, a directed Mail whose body is sealed in a
/// cold day file, and a Group article read by two readers; and a daemon
/// database at `<dir>/daemon.sqlite` with one pending Group delivery and one
/// unconfirmed receipt. Returns (coordinator data path, daemon data path).
#[cfg(test)]
pub fn write_test_fixture(dir: &Path) -> (PathBuf, PathBuf) {
    use super::super::cold::Record;
    use super::{COORD_SLICE_SCHEMA, DAEMON_SCHEMA};
    let data = dir.join("coord");
    let slice = coord_root(&data).join("slice-00");
    std::fs::create_dir_all(&slice).unwrap();
    let conn = Connection::open(slice.join("hot.sqlite")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    conn.execute_batch(COORD_SLICE_SCHEMA).unwrap();
    conn.execute_batch(
        "INSERT INTO timeline(seq, kind, addr, host, at_ms) VALUES (1, 'session', 'r1', 'h', 10);
         INSERT INTO envelopes VALUES (2, 'hot-1', 'd:r1', 1, 'directed', 's', 'r1', NULL, 3, 'x', 100, 0, 0, NULL);
         INSERT INTO hot_bodies VALUES ('hot-1', 'hot');
         INSERT INTO receipts VALUES ('hot-1', 'r1', 'd:r1', 1, 'r-1', 150, 3, NULL);
         INSERT INTO envelopes VALUES (4, 'cold-1', 'd:r1', 2, 'directed', 's', 'r1', '{}', 4, 'x', 200, 0, 1, NULL);
         INSERT INTO envelopes VALUES (5, 'g-1', 'g:news', 1, 'group', 's', 'news', '{\"k\":\"v\"}', 7, 'x', 300, 0, 0, NULL);
         INSERT INTO hot_bodies VALUES ('g-1', 'article');
         INSERT INTO receipts VALUES ('g-1', 'r2', 'g:news', 1, 'r-5', 320, 7, NULL);
         INSERT INTO receipts VALUES ('g-1', 'r1', 'g:news', 1, 'r-2', 310, 6, NULL);
         INSERT INTO positions(stream, reader, position) VALUES ('g:news', 'r1', 1), ('g:news', 'r2', 1), ('g:news', 'r3', 0);
         INSERT INTO timeline(seq, kind, addr, mail_id, label) VALUES (8, 'receipt', 'r1', 'lost', 'r-9');",
    )
    .unwrap();
    Index::load(&slice.join("cold"))
        .unwrap()
        .seal_day(
            &slice.join("cold"),
            0,
            &[Record {
                mail_id: "cold-1".into(),
                stream: "d:r1".into(),
                article_num: 2,
                kind: "directed".into(),
                sender: "s".into(),
                recipient: "r1".into(),
                headers_json: Some("{}".into()),
                body: "cold".into(),
                body_sha256: crate::sha256::hex_digest(b"cold"),
                accepted_at_ms: Some(200),
            }],
        )
        .unwrap();
    let daemon = dir.join("daemon.sqlite");
    let d = Connection::open(&daemon).unwrap();
    d.execute_batch(DAEMON_SCHEMA).unwrap();
    d.execute_batch(
        "INSERT INTO pending_deliveries(mail_id, sender, recipient, group_name, article_num, body, kind, headers_json)
           VALUES ('g-1', 's', 'r3', 'news', 1, 'article', 'group', '{\"k\":\"v\"}');
         INSERT INTO unconfirmed_receipts(mail_id, session, record_id) VALUES ('g-1', 'r1', 'r-2');",
    )
    .unwrap();
    (data, daemon)
}

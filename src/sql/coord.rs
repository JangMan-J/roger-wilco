//! Coordinator slice databases: one `hot.sqlite` (WAL, synchronous FULL) per
//! slice, one writer thread per slice that commits every queued command of a
//! wakeup in one `BEGIN IMMEDIATE ... COMMIT` (group commit), and an
//! off-send-path sweep that seals old bodies into cold day files.
//!
//! Streams shard by `hash_pair(stream)`, Mail-id records (`seen_ids`,
//! `broadcasts`) by `hash_pair(mail_id)`; Addresses and Group names are hashed
//! as opaque bytes, never read (#I3). A caller hears back only after the
//! transaction holding its write has committed.
//!
//! Writes that span two slices commit the envelope first and the Mail-id
//! record second. The envelope insert is idempotent for a repeat of the same
//! Mail, so a resend after a failure between the two commits completes the
//! second one instead of losing or doubling the Mail.

use super::*;
use crate::cold::{Index, Record};
use crate::json::{self, Value};
use crate::sha256::{hash_pair, hex_digest};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

type Finish = Box<dyn FnOnce(Result<(), String>) + Send>;
type Job = Box<dyn FnOnce(&Connection) -> (bool, Finish) + Send>;

fn sq(e: rusqlite::Error) -> io::Error {
    io::Error::other(e)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn resend_refusal(mail: &str, what: &str) -> String {
    format!(
        "mail {mail}: expected a resend to repeat the Mail already accepted under this id, saw a different {what}"
    )
}

#[derive(Default)]
struct QueueState {
    jobs: Vec<Job>,
    closed: bool,
}

type Queue = (Mutex<QueueState>, Condvar);

/// One shard's writer thread and its command queue.
struct Writer {
    queue: Arc<Queue>,
    commits: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

/// A queued command's answer, available once its transaction has committed.
struct Pending<R>(Receiver<io::Result<R>>);

impl<R> Pending<R> {
    fn wait(self) -> io::Result<R> {
        self.0.recv().map_err(|_| {
            io::Error::other("expected a slice transaction to commit, saw it abandoned")
        })?
    }
}

impl Writer {
    fn spawn(conn: Connection, idx: usize) -> io::Result<Self> {
        let queue: Arc<Queue> = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        let commits = Arc::new(AtomicU64::new(0));
        let (q, c) = (Arc::clone(&queue), Arc::clone(&commits));
        let thread = std::thread::Builder::new()
            .name(format!("roger-shard-{idx:02}"))
            .spawn(move || writer_loop(conn, &q, &c))?;
        Ok(Self {
            queue,
            commits,
            thread: Some(thread),
        })
    }

    fn submit<R: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<R> + Send + 'static,
    ) -> io::Result<Pending<R>> {
        let (tx, rx) = sync_channel(1);
        let job: Job = Box::new(move |c| {
            let r = f(c);
            let ok = r.is_ok();
            let fin: Finish = Box::new(move |commit| {
                let out = match commit {
                    Err(e) => Err(io::Error::other(e)),
                    Ok(()) => r.map_err(sq),
                };
                let _ = tx.send(out);
            });
            (ok, fin)
        });
        let (m, cv) = &*self.queue;
        let mut g = m.lock().unwrap();
        if g.closed {
            return Err(io::Error::other(
                "expected an open slice writer, saw it closed",
            ));
        }
        g.jobs.push(job);
        cv.notify_all();
        Ok(Pending(rx))
    }

    fn exec<R: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<R> + Send + 'static,
    ) -> io::Result<R> {
        self.submit(f)?.wait()
    }

    /// Block until at least `n` commands wait in the queue (signalled by each push).
    #[cfg(test)]
    fn wait_queued(&self, n: usize) {
        let (m, cv) = &*self.queue;
        let mut g = m.lock().unwrap();
        while g.jobs.len() < n {
            g = cv.wait(g).unwrap();
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        {
            let (m, cv) = &*self.queue;
            m.lock().unwrap().closed = true;
            cv.notify_all();
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn writer_loop(mut conn: Connection, queue: &Queue, commits: &AtomicU64) {
    loop {
        let batch = {
            let (m, cv) = queue;
            let mut g = m.lock().unwrap();
            while g.jobs.is_empty() && !g.closed {
                g = cv.wait(g).unwrap();
            }
            if g.jobs.is_empty() {
                return;
            }
            std::mem::take(&mut g.jobs)
        };
        run_batch(&mut conn, batch);
        commits.fetch_add(1, Ordering::SeqCst);
    }
}

/// Run every command of one wakeup in one transaction, each inside its own
/// savepoint so a failing command rolls back alone; answer all after COMMIT.
fn run_batch(conn: &mut Connection, batch: Vec<Job>) {
    // A failed BEGIN drops the commands, which answers each caller with an error.
    let Ok(mut tx) = conn.transaction_with_behavior(TransactionBehavior::Immediate) else {
        return;
    };
    let mut fins = Vec::with_capacity(batch.len());
    for job in batch {
        let Ok(sp) = tx.savepoint() else { continue };
        let (ok, fin) = job(&sp);
        let release = if ok {
            sp.commit().map_err(|e| e.to_string())
        } else {
            drop(sp);
            Ok(())
        };
        fins.push((fin, release));
    }
    let res = tx.commit().map_err(|e| e.to_string());
    for (fin, release) in fins {
        fin(res.clone().and(release));
    }
}

fn migrate_streams(c: &Connection) -> rusqlite::Result<()> {
    let mut st = c.prepare("PRAGMA table_info(streams)")?;
    let cols: Vec<String> = st
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (name, ty) in [
        ("keep_last", "INTEGER"),
        ("ttl_ms", "INTEGER"),
        ("retired_at_ms", "INTEGER"),
        ("pruned_through", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !cols.iter().any(|c| c == name) {
            c.execute(&format!("ALTER TABLE streams ADD COLUMN {name} {ty}"), [])?;
        }
    }
    Ok(())
}

fn migrate_seen_ids(c: &Connection) -> rusqlite::Result<()> {
    let mut st = c.prepare("PRAGMA table_info(seen_ids)")?;
    let cols: Vec<String> = st
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !cols.iter().any(|c| c == "stream") {
        c.execute("ALTER TABLE seen_ids ADD COLUMN stream TEXT", [])?;
    }
    Ok(())
}

fn open_conn(path: &Path, schema: bool) -> io::Result<Connection> {
    let c = Connection::open(path).map_err(sq)?;
    c.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0))
        .map_err(sq)?;
    c.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(sq)?;
    c.set_prepared_statement_cache_capacity(64);
    c.execute_batch("PRAGMA busy_timeout = 5000;").map_err(sq)?; // no-poll: SQLite lock wait bound, not a scan
    if schema {
        c.execute_batch(COORD_SLICE_SCHEMA).map_err(sq)?;
        migrate_streams(&c).map_err(sq)?;
        migrate_seen_ids(&c).map_err(sq)?;
    }
    Ok(c)
}

fn meta(c: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    c.query_row("SELECT val FROM meta WHERE key = ?1", [key], |r| r.get(0))
        .optional()
}

#[derive(Clone, Debug)]
pub struct Config {
    pub data: PathBuf,
    pub num_slices: usize,
    pub retain_ms: i64,
    pub day_ms: i64,
    pub sweep_enabled: bool,
}

impl Config {
    pub fn new(data: PathBuf) -> Self {
        Self {
            data,
            num_slices: 1,
            retain_ms: DEFAULT_RETAIN_MS,
            day_ms: DAY_MS,
            sweep_enabled: true,
        }
    }
}

#[derive(Default)]
pub struct Startup {
    pub sessions: HashMap<String, StoredSession>,
    pub claims: Claims,
    pub takeover_registrations: HashSet<String>,
}

#[derive(Clone, Debug)]
pub struct Directed {
    pub mail_id: String,
    pub from: String,
    pub to: String,
    pub body: String,
    pub headers: Option<Value>,
    pub accepted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Broadcast {
    pub mail_id: String,
    pub from: String,
    pub recipients: Vec<String>,
    pub body: String,
    pub headers: Option<Value>,
    pub accepted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct GroupMail {
    pub mail_id: String,
    pub from: String,
    pub group: String,
    pub body: String,
    pub headers: Option<Value>,
    pub expected_article: Option<i64>,
    pub accepted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum Status {
    Committed {
        article_num: i64,
        subscribers: Vec<String>,
    },
    DuplicateAccepted {
        article_num: i64,
    },
    Conflict(String),
}

struct Shard {
    cold_dir: PathBuf,
    cold: Arc<Index>,
    read: Mutex<Connection>,
    writer: Writer,
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
}

struct Inner {
    cfg: Config,
    slices: Vec<Shard>,
    next_seq: AtomicI64,
    in_flight: (Mutex<HashSet<String>>, Condvar),
    subscribed_groups: Mutex<HashMap<String, HashMap<usize, HashSet<String>>>>,
    last_swept_day: AtomicI64,
    next_sweep_due_ms: AtomicI64,
    sweep_running: std::sync::atomic::AtomicBool,
    sweep_lock: Mutex<()>,
    sweeps: Mutex<Vec<JoinHandle<()>>>,
    _lock: File,
}

pub struct Store {
    inner: Arc<Inner>,
}

/// Holds one Mail id against a concurrent send under the same id.
struct Flight<'a> {
    inner: &'a Inner,
    id: String,
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        let (m, cv) = &self.inner.in_flight;
        m.lock().unwrap().remove(&self.id);
        cv.notify_all();
    }
}

struct SeenRow {
    kind: String,
    sender: String,
    target: Option<String>,
    body_len: i64,
    body_sha256: String,
    article_num: i64,
}

impl SeenRow {
    /// What a send under this id differs in, if anything.
    fn differs(
        &self,
        kind: &str,
        sender: &str,
        target: Option<&str>,
        len: i64,
        sha: &str,
    ) -> Option<String> {
        let mut what = Vec::new();
        if self.sender != sender {
            what.push("sender");
        }
        if self.kind != kind || self.target.as_deref() != target {
            what.push("recipient");
        }
        if self.body_len != len || self.body_sha256 != sha {
            what.push("body");
        }
        (!what.is_empty()).then(|| what.join(", "))
    }
}

struct NewEnvelope {
    seq: i64,
    mail_id: String,
    stream: String,
    stream_kind: &'static str,
    kind: &'static str,
    sender: String,
    recipient: String,
    headers_json: Option<String>,
    body: String,
    body_sha256: String,
    accepted_at_ms: Option<i64>,
    day_bucket: i64,
    reader: Option<String>,
    expected_article: Option<i64>,
}

struct NewSeen {
    mail_id: String,
    kind: &'static str,
    sender: String,
    target: Option<String>,
    body_len: i64,
    body_sha256: String,
    stream: Option<String>,
}

enum InsertOutcome {
    Committed(i64),
    ResendConflict(String),
    HeadConflict { expected: i64, saw: i64 },
}

enum SingleWrite {
    Committed(i64, Vec<String>),
    Duplicate(i64),
    Conflict(String),
}

/// Raise `stream`'s pruned floor to `max_drop_art` and drop the hot bodies of the
/// newly pruned articles. Envelopes stay (ack and receipt still resolve); cold
/// copies stay sealed but are never served.
fn prune_hot_group(c: &Connection, stream: &str, max_drop_art: i64) -> rusqlite::Result<()> {
    let old: i64 = c
        .prepare_cached("SELECT pruned_through FROM streams WHERE stream = ?1")?
        .query_row([stream], |r| r.get(0))
        .optional()?
        .unwrap_or(0);
    if max_drop_art <= old {
        return Ok(());
    }
    c.prepare_cached(
        "DELETE FROM hot_bodies WHERE mail_id IN (
             SELECT mail_id FROM envelopes WHERE stream = ?1 AND article_num > ?2 AND article_num <= ?3
         )",
    )?
    .execute(params![stream, old, max_drop_art])?;
    c.prepare_cached("UPDATE streams SET pruned_through = ?2 WHERE stream = ?1")?
        .execute(params![stream, max_drop_art])?;
    Ok(())
}

/// Insert one envelope and its body, assigning the stream's next article.
/// A repeat of the same Mail returns its article; a different Mail under an
/// existing id returns what differs; an expected-article mismatch returns the
/// expected vs current head.
fn insert_envelope(c: &Connection, e: &NewEnvelope) -> rusqlite::Result<InsertOutcome> {
    let existing = c
        .query_row(
            "SELECT article_num, kind, sender, recipient, body_len, body_sha256 FROM envelopes WHERE mail_id = ?1",
            [&e.mail_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    if let Some((art, kind, sender, recipient, len, sha)) = existing {
        let mut what = Vec::new();
        if sender != e.sender {
            what.push("sender");
        }
        if kind != e.kind || recipient != e.recipient {
            what.push("recipient");
        }
        if len != e.body.len() as i64 || sha != e.body_sha256 {
            what.push("body");
        }
        return Ok(if what.is_empty() {
            InsertOutcome::Committed(art)
        } else {
            InsertOutcome::ResendConflict(what.join(", "))
        });
    }
    c.execute(
        "INSERT OR IGNORE INTO streams (stream, kind, next_article) VALUES (?1, ?2, 1)",
        params![e.stream, e.stream_kind],
    )?;
    let (art, keep_last): (i64, Option<i64>) = c.query_row(
        "SELECT next_article, keep_last FROM streams WHERE stream = ?1",
        [&e.stream],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if let Some(expected) = e.expected_article {
        let saw = art - 1;
        if saw != expected {
            return Ok(InsertOutcome::HeadConflict { expected, saw });
        }
    }
    c.execute(
        "UPDATE streams SET next_article = ?2 WHERE stream = ?1",
        params![e.stream, art + 1],
    )?;
    c.execute(
        "INSERT INTO envelopes (seq, mail_id, stream, article_num, kind, sender, recipient, headers_json, body_len, body_sha256, accepted_at_ms, day_bucket, in_cold)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0)",
        params![
            e.seq,
            e.mail_id,
            e.stream,
            art,
            e.kind,
            e.sender,
            e.recipient,
            e.headers_json,
            e.body.len() as i64,
            e.body_sha256,
            e.accepted_at_ms,
            e.day_bucket
        ],
    )?;
    c.execute(
        "INSERT INTO hot_bodies (mail_id, body) VALUES (?1, ?2)",
        params![e.mail_id, e.body],
    )?;
    if e.stream_kind == "group" {
        if let Some(keep) = keep_last.filter(|&k| k >= 0) {
            prune_hot_group(c, &e.stream, art - keep)?;
        }
    }
    if let Some(reader) = &e.reader {
        c.execute(
            "INSERT OR IGNORE INTO positions (stream, reader, subscribed, position, updated_at_ms) VALUES (?1, ?2, 1, 0, ?3)",
            params![e.stream, reader, e.accepted_at_ms],
        )?;
    }
    Ok(InsertOutcome::Committed(art))
}

fn insert_seen(c: &Connection, s: &NewSeen, article_num: i64) -> rusqlite::Result<()> {
    let mut st = c.prepare_cached(
        "INSERT OR IGNORE INTO seen_ids (mail_id, kind, sender, target, body_len, body_sha256, article_num, stream)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )?;
    st.execute(params![
        s.mail_id,
        s.kind,
        s.sender,
        s.target,
        s.body_len,
        s.body_sha256,
        article_num,
        s.stream
    ])?;
    Ok(())
}

fn query_seen(c: &Connection, mail_id: &str) -> rusqlite::Result<Option<SeenRow>> {
    let mut st = c.prepare_cached(
        "SELECT kind, sender, target, body_len, body_sha256, article_num FROM seen_ids WHERE mail_id = ?1",
    )?;
    st.query_row([mail_id], |r| {
        Ok(SeenRow {
            kind: r.get(0)?,
            sender: r.get(1)?,
            target: r.get(2)?,
            body_len: r.get(3)?,
            body_sha256: r.get(4)?,
            article_num: r.get(5)?,
        })
    })
    .optional()
}

fn query_broadcast(c: &Connection, mail_id: &str) -> rusqlite::Result<Option<SeenRow>> {
    let mut st = c.prepare_cached(
        "SELECT sender, body_len, body_sha256 FROM broadcasts WHERE mail_id = ?1",
    )?;
    st.query_row([mail_id], |r| {
        Ok(SeenRow {
            kind: "broadcast".into(),
            sender: r.get(0)?,
            target: None,
            body_len: r.get(1)?,
            body_sha256: r.get(2)?,
            article_num: 0,
        })
    })
    .optional()
}

fn subscribers_of(c: &Connection, stream: &str) -> rusqlite::Result<Vec<String>> {
    let mut st = c.prepare(
        "SELECT reader FROM positions WHERE stream = ?1 AND subscribed = 1 ORDER BY reader",
    )?;
    let rows = st.query_map([stream], |r| r.get(0))?;
    rows.collect()
}

/// Move `reader`'s position on `stream` to at least the stream's pruned floor,
/// then forward across every contiguous receipted article.
fn advance_position(c: &Connection, stream: &str, reader: &str, now: i64) -> rusqlite::Result<()> {
    let (pos, floor): (i64, i64) = c
        .prepare_cached(
            "SELECT p.position, COALESCE(s.pruned_through, 0) FROM positions p
             LEFT JOIN streams s ON s.stream = p.stream
             WHERE p.stream = ?1 AND p.reader = ?2",
        )?
        .query_row(params![stream, reader], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let start = pos.max(floor);
    let mut st = c.prepare_cached(
        "SELECT article_num, record_id, receipted_at_ms FROM receipts
         WHERE stream = ?1 AND reader = ?2 AND article_num > ?3 ORDER BY article_num",
    )?;
    let mut rows = st.query(params![stream, reader, start])?;
    let mut cur = start;
    let mut last: Option<(String, Option<i64>)> = None;
    while let Some(r) = rows.next()? {
        let art: i64 = r.get(0)?;
        if art != cur + 1 {
            break;
        }
        cur = art;
        last = Some((r.get(1)?, r.get(2)?));
    }
    if cur != pos {
        let (record_id, at) = last.unzip();
        c.prepare_cached(
            "UPDATE positions SET position = ?3,
                    record_id = CASE WHEN ?4 IS NULL THEN record_id ELSE ?4 END,
                    receipted_at_ms = CASE WHEN ?4 IS NULL THEN receipted_at_ms ELSE ?5 END,
                    updated_at_ms = ?6
             WHERE stream = ?1 AND reader = ?2",
        )?
        .execute(params![stream, reader, cur, record_id, at.flatten(), now])?;
    }
    Ok(())
}

const PENDING_COLS: &str = "e.seq, e.mail_id, e.sender, e.recipient, e.article_num, e.kind, e.headers_json, e.day_bucket, e.in_cold, h.body";

struct RawPending {
    seq: i64,
    mail_id: String,
    sender: String,
    recipient: String,
    article_num: i64,
    kind: String,
    headers_json: Option<String>,
    day_bucket: i64,
    in_cold: bool,
    body: Option<String>,
}

fn raw_pending(r: &Row) -> rusqlite::Result<RawPending> {
    Ok(RawPending {
        seq: r.get(0)?,
        mail_id: r.get(1)?,
        sender: r.get(2)?,
        recipient: r.get(3)?,
        article_num: r.get(4)?,
        kind: r.get(5)?,
        headers_json: r.get(6)?,
        day_bucket: r.get(7)?,
        in_cold: r.get::<_, i64>(8)? != 0,
        body: r.get(9)?,
    })
}

fn headers_value(json_text: Option<&str>) -> Value {
    json_text
        .and_then(|t| json::parse(t).ok())
        .unwrap_or_else(|| Value::Obj(Vec::new()))
}

fn day_bucket(accepted_at_ms: Option<i64>, day_ms: i64) -> i64 {
    accepted_at_ms.map(|a| a.div_euclid(day_ms)).unwrap_or(0)
}

impl Inner {
    fn shard(&self, key: &str) -> usize {
        hash_pair(key.as_bytes()).0 as usize % self.slices.len()
    }

    fn seq(&self) -> i64 {
        self.next_seq.fetch_add(1, Ordering::SeqCst)
    }

    fn hold(&self, id: &str) -> Flight<'_> {
        let (m, cv) = &self.in_flight;
        let mut g = m.lock().unwrap();
        while g.contains(id) {
            g = cv.wait(g).unwrap();
        }
        g.insert(id.to_string());
        Flight {
            inner: self,
            id: id.to_string(),
        }
    }

    fn read<R>(
        &self,
        idx: usize,
        f: impl FnOnce(&Connection) -> rusqlite::Result<R>,
    ) -> io::Result<R> {
        #[cfg(test)]
        self.slices[idx].reads.fetch_add(1, Ordering::SeqCst);
        f(&self.slices[idx].read.lock().unwrap()).map_err(sq)
    }

    fn seen(&self, mail_id: &str) -> io::Result<(Option<SeenRow>, Option<SeenRow>)> {
        self.read(self.shard(mail_id), |c| {
            Ok((query_seen(c, mail_id)?, query_broadcast(c, mail_id)?))
        })
    }

    /// Resolves a body from the hot row or from `cold`, the map one
    /// `cold_bodies` call produced for this batch.
    fn resolve_body(
        cold: &mut std::collections::HashMap<String, String>,
        mail_id: &str,
        day_bucket: i64,
        in_cold: bool,
        body: Option<String>,
    ) -> io::Result<String> {
        match body {
            Some(b) => Ok(b),
            None if in_cold => {
                cold.remove(mail_id).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "expected mail {mail_id} in cold day {day_bucket}, saw no such record",
                        ),
                    )
                })
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected a hot body for mail {mail_id}, saw none"),
            )),
        }
    }

    fn resolve(
        cold: &mut std::collections::HashMap<String, String>,
        raw: RawPending,
        reader: &str,
    ) -> io::Result<Delivery> {
        let body = Self::resolve_body(cold, &raw.mail_id, raw.day_bucket, raw.in_cold, raw.body)?;
        let group = raw.kind == "group";
        Ok(Delivery {
            seq: raw.seq,
            mail_id: raw.mail_id,
            from: raw.sender,
            to: if group {
                reader.to_string()
            } else {
                raw.recipient.clone()
            },
            group: group.then_some(raw.recipient),
            article_num: group.then_some(raw.article_num),
            body,
            kind: raw.kind,
            headers: headers_value(raw.headers_json.as_deref()),
        })
    }

    /// Cold bodies for every `(day_bucket, mail_id, in_cold, has_hot_body)`
    /// row needing one, read with a single `read_bodies` call on slice `idx`.
    fn cold_bodies<'a>(
        &self,
        idx: usize,
        rows: impl Iterator<Item = (i64, &'a str, bool, bool)>,
    ) -> io::Result<std::collections::HashMap<String, String>> {
        let wants: Vec<(i64, &str)> = rows
            .filter(|&(_, _, in_cold, hot)| in_cold && !hot)
            .map(|(day, id, _, _)| (day, id))
            .collect();
        if wants.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        self.slices[idx].cold.read_bodies(&wants)
    }

    fn resolve_all(
        &self,
        idx: usize,
        raws: Vec<RawPending>,
        reader: &str,
    ) -> io::Result<Vec<Delivery>> {
        let mut cold = self.cold_bodies(
            idx,
            raws.iter().map(|r| {
                (
                    r.day_bucket,
                    r.mail_id.as_str(),
                    r.in_cold,
                    r.body.is_some(),
                )
            }),
        )?;
        raws.into_iter()
            .map(|r| Self::resolve(&mut cold, r, reader))
            .collect()
    }

    /// Group articles above `reader`'s position on subscribed streams in slice
    /// `idx` (only `stream` when given) that `reader` has not receipted.
    fn group_pending(
        &self,
        idx: usize,
        reader: &str,
        stream: Option<&str>,
    ) -> io::Result<Vec<Delivery>> {
        let raws = self.read(idx, |c| {
            let mut st = c.prepare_cached(&format!(
                "SELECT {PENDING_COLS} FROM positions p
                 LEFT JOIN streams s ON s.stream = p.stream
                 JOIN envelopes e ON e.stream = p.stream
                   AND e.article_num > max(p.position, COALESCE(s.pruned_through, 0))
                 LEFT JOIN hot_bodies h ON h.mail_id = e.mail_id
                 WHERE p.reader = ?1 AND p.subscribed = 1 AND e.kind = 'group'
                   AND (?2 IS NULL OR p.stream = ?2)
                   AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.mail_id = e.mail_id AND r.reader = ?1)
                 ORDER BY e.seq"
            ))?;
            let rows = st.query_map(params![reader, stream], raw_pending)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })?;
        self.resolve_all(idx, raws, reader)
    }

    fn pending_for(&self, session: &str) -> io::Result<Vec<Delivery>> {
        let stream = format!("m:{session}");
        let idx = self.shard(&stream);
        let raws = self.read(idx, |c| {
            let mut st = c.prepare_cached(&format!(
                "SELECT {PENDING_COLS} FROM envelopes e
                 LEFT JOIN hot_bodies h ON h.mail_id = e.mail_id
                 WHERE e.stream = ?1 AND e.article_num > COALESCE((SELECT position FROM positions WHERE stream = ?1 AND reader = ?2), 0) AND e.kind IN ('directed', 'broadcast') AND NOT EXISTS (SELECT 1 FROM receipts r WHERE r.mail_id = e.mail_id)
                 ORDER BY e.seq"
            ))?;
            let rows = st.query_map(params![&stream, session], raw_pending)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let mut out = self.resolve_all(idx, raws, session)?;
        let group_shards: Vec<usize> = {
            let subs = self.subscribed_groups.lock().unwrap();
            let mut idxs: Vec<usize> = subs
                .get(session)
                .map(|m| m.keys().copied().collect())
                .unwrap_or_default();
            idxs.sort_unstable();
            idxs
        };
        for i in group_shards {
            out.extend(self.group_pending(i, session, None)?);
        }
        out.sort_by_key(|p| p.seq);
        Ok(out)
    }

    #[allow(clippy::type_complexity)]
    fn find_in_slice(
        &self,
        idx: usize,
        mail_id: &str,
    ) -> io::Result<
        Option<(
            usize,
            (
                String,
                i64,
                String,
                String,
                String,
                Option<String>,
                Option<i64>,
            ),
        )>,
    > {
        let row = self.read(idx, |c| {
            let mut st = c.prepare_cached(
                "SELECT stream, article_num, kind, sender, recipient, headers_json, accepted_at_ms FROM envelopes WHERE mail_id = ?1",
            )?;
            st.query_row([mail_id], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            })
            .optional()
        })?;
        Ok(row.map(|r| (idx, r)))
    }

    /// Find an envelope by Mail id: (slice, stream, article, kind, sender, recipient, headers, accepted_at).
    #[allow(clippy::type_complexity)]
    fn find(
        &self,
        mail_id: &str,
    ) -> io::Result<
        Option<(
            usize,
            (
                String,
                i64,
                String,
                String,
                String,
                Option<String>,
                Option<i64>,
            ),
        )>,
    > {
        if self.slices.len() == 1 {
            return self.find_in_slice(0, mail_id);
        }
        let seen_stream: Option<Option<String>> = self.read(self.shard(mail_id), |c| {
            c.query_row(
                "SELECT stream FROM seen_ids WHERE mail_id = ?1",
                [mail_id],
                |r| r.get(0),
            )
            .optional()
        })?;
        match seen_stream {
            Some(Some(stream)) => self.find_in_slice(self.shard(&stream), mail_id),
            Some(None) => {
                for idx in 0..self.slices.len() {
                    if let Some(found) = self.find_in_slice(idx, mail_id)? {
                        return Ok(Some(found));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Seal every hot body accepted at or before `now - retain_ms` into its
    /// slice's cold day file, then mark it cold and drop the hot copy.
    fn sweep(&self, now: i64) -> io::Result<()> {
        let _g = self.sweep_lock.lock().unwrap();
        self.next_sweep_due_ms.store(i64::MIN, Ordering::SeqCst);
        let mut due = i64::MAX;
        let default_retain = self.cfg.retain_ms;
        for (idx, slice) in self.slices.iter().enumerate() {
            let rows = self.read(idx, |c| {
                let mut st = c.prepare(
                    "SELECT e.day_bucket, e.mail_id, e.stream, e.article_num, e.kind, e.sender, e.recipient,
                            e.headers_json, h.body, e.body_sha256, e.accepted_at_ms
                     FROM envelopes e
                     JOIN hot_bodies h ON h.mail_id = e.mail_id
                     LEFT JOIN streams s ON s.stream = e.stream
                     WHERE e.in_cold = 0
                       AND e.accepted_at_ms IS NOT NULL
                       AND e.accepted_at_ms <= max(0, ?1 - COALESCE(s.ttl_ms, ?2))
                     ORDER BY e.seq",
                )?;
                let rows = st.query_map(params![now, default_retain], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        Record {
                            mail_id: r.get(1)?,
                            stream: r.get(2)?,
                            article_num: r.get(3)?,
                            kind: r.get(4)?,
                            sender: r.get(5)?,
                            recipient: r.get(6)?,
                            headers_json: r.get(7)?,
                            body: r.get(8)?,
                            body_sha256: r.get(9)?,
                            accepted_at_ms: r.get(10)?,
                        },
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })?;
            let mut days: BTreeMap<i64, Vec<Record>> = BTreeMap::new();
            for (day, rec) in rows {
                days.entry(day).or_default().push(rec);
            }
            let mut ids = Vec::new();
            for (day, recs) in &days {
                slice.cold.seal_day(&slice.cold_dir, *day, recs)?;
                ids.extend(recs.iter().map(|r| r.mail_id.clone()));
            }
            if !ids.is_empty() {
                slice.writer.exec(move |c| {
                    for id in &ids {
                        c.execute("UPDATE envelopes SET in_cold = 1 WHERE mail_id = ?1", [id])?;
                        c.execute("DELETE FROM hot_bodies WHERE mail_id = ?1", [id])?;
                    }
                    Ok(())
                })?;
            }
            let (hot_due, shortest_ttl): (Option<i64>, Option<i64>) = self.read(idx, |c| {
                c.query_row(
                    "SELECT CAST(MIN(e.accepted_at_ms + COALESCE(s.ttl_ms, ?1)) AS INTEGER),
                            (SELECT MIN(ttl_ms) FROM streams)
                     FROM hot_bodies h JOIN envelopes e ON e.mail_id = h.mail_id
                     LEFT JOIN streams s ON s.stream = e.stream",
                    [default_retain],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
            })?;
            let ttl = default_retain.min(shortest_ttl.unwrap_or(default_retain));
            due = due
                .min(hot_due.unwrap_or(i64::MAX))
                .min(now.saturating_add(ttl));
        }
        self.next_sweep_due_ms.store(due, Ordering::SeqCst);
        Ok(())
    }
}

fn insert_timeline(c: &Connection, seq: i64, ev: &Event, now: i64) -> rusqlite::Result<()> {
    // (kind, addr, host, old_host, label, declared, registration, claim_key, expires_ms, mail_id, at_ms)
    type Cols<'a> = (
        &'a str,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<i64>,
        Option<&'a str>,
        Option<i64>,
    );
    let cols: Cols = match ev {
        Event::Session {
            session,
            host,
            at_ms,
        } => {
            c.execute(
                "INSERT INTO live_sessions (address, host, label, declared, at_ms) VALUES (?1, ?2, '', NULL, ?3)
                 ON CONFLICT(address) DO UPDATE SET host = excluded.host, at_ms = excluded.at_ms",
                params![session, host, at_ms],
            )?;
            (
                "session",
                Some(session),
                Some(host),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                *at_ms,
            )
        }
        Event::Unregister { session, at_ms } => {
            c.execute("DELETE FROM live_sessions WHERE address = ?1", [session])?;
            (
                "unregister",
                Some(session),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                *at_ms,
            )
        }
        Event::Label {
            session,
            label,
            at_ms,
        } => {
            c.execute(
                "UPDATE live_sessions SET label = ?2 WHERE address = ?1",
                params![session, label],
            )?;
            (
                "label",
                Some(session),
                None,
                None,
                Some(label),
                None,
                None,
                None,
                None,
                None,
                *at_ms,
            )
        }
        Event::Declared {
            session,
            declared,
            at_ms,
        } => {
            c.execute(
                "UPDATE live_sessions SET declared = ?2 WHERE address = ?1",
                params![session, declared],
            )?;
            (
                "declared",
                Some(session),
                None,
                None,
                None,
                declared.as_deref(),
                None,
                None,
                None,
                None,
                *at_ms,
            )
        }
        Event::Takeover {
            session,
            old_host,
            host,
            at_ms,
            registration,
        } => (
            "takeover",
            Some(session),
            Some(host),
            Some(old_host),
            None,
            None,
            registration.as_deref(),
            None,
            None,
            None,
            *at_ms,
        ),
        Event::Claim {
            key,
            owner,
            expires_ms,
        } => {
            c.execute(
                "INSERT OR REPLACE INTO live_claims (claim_key, owner, expires_ms) VALUES (?1, ?2, ?3)",
                params![key, owner, expires_ms],
            )?;
            (
                "claim",
                Some(owner),
                None,
                None,
                None,
                None,
                None,
                Some(key),
                Some(*expires_ms),
                None,
                Some(now),
            )
        }
        Event::Release { key } => {
            c.execute("DELETE FROM live_claims WHERE claim_key = ?1", [key])?;
            (
                "release",
                None,
                None,
                None,
                None,
                None,
                None,
                Some(key),
                None,
                None,
                Some(now),
            )
        }
        // A receipt for Mail the coordinator never accepted: the record id rides in `label`.
        Event::StrayReceipt {
            mail_id,
            session,
            record_id,
        } => (
            "receipt",
            session.as_deref(),
            None,
            None,
            Some(record_id),
            None,
            None,
            None,
            None,
            Some(mail_id),
            Some(now),
        ),
    };
    c.execute(
        "INSERT INTO timeline (seq, kind, addr, host, old_host, label, declared, registration, claim_key, expires_ms, mail_id, at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![seq, cols.0, cols.1, cols.2, cols.3, cols.4, cols.5, cols.6, cols.7, cols.8, cols.9, cols.10],
    )?;
    Ok(())
}

fn load_startup(c: &Connection) -> rusqlite::Result<Startup> {
    let mut out = Startup::default();
    let mut st = c.prepare("SELECT address, host, label, declared FROM live_sessions")?;
    let rows = st.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            StoredSession {
                host: r.get(1)?,
                label: r.get(2)?,
                declared: r.get(3)?,
            },
        ))
    })?;
    for row in rows {
        let (a, s) = row?;
        out.sessions.insert(a, s);
    }
    let mut st = c.prepare("SELECT claim_key, owner, expires_ms FROM live_claims")?;
    let rows = st.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        let (key, owner, expires_ms) = row?;
        out.claims.apply(
            &key,
            &crate::claims::Change::Grant(crate::claims::Held { owner, expires_ms }),
        );
    }
    let mut st = c.prepare(
        "SELECT registration FROM timeline WHERE kind = 'takeover' AND registration IS NOT NULL AND registration != ''",
    )?;
    let rows = st.query_map([], |r| r.get::<_, String>(0))?;
    for row in rows {
        out.takeover_registrations.insert(row?);
    }
    Ok(out)
}

fn max_seq(c: &Connection) -> rusqlite::Result<i64> {
    c.query_row(
        "SELECT MAX(COALESCE((SELECT MAX(seq) FROM timeline), 0),
                    COALESCE((SELECT MAX(seq) FROM envelopes), 0),
                    COALESCE((SELECT MAX(receipt_seq) FROM receipts), 0),
                    COALESCE((SELECT MAX(seq) FROM broadcasts), 0))",
        [],
        |r| r.get(0),
    )
}

/// The slice a stream or Mail id lives on; the same hash as `Inner::shard`.
fn slice_of(key: &str, n: usize) -> usize {
    hash_pair(key.as_bytes()).0 as usize % n
}

/// Validate (or record) the slice count before any further slice is created.
/// An existing store keeps `num_slices` in slice 0's meta table; a store from
/// before that key backfills it from the slice directories on disk.
fn check_slice_count(root: &Path, want: usize) -> io::Result<()> {
    let hot0 = shard_dir(root, 0).join("hot.sqlite");
    if !hot0.exists() {
        let dir = shard_dir(root, 0);
        fs::create_dir_all(&dir)?;
        let c = open_conn(&dir.join("hot.sqlite"), true)?;
        c.execute(
            "INSERT OR REPLACE INTO meta (key, val) VALUES ('num_slices', ?1)",
            [want.to_string()],
        )
        .map_err(sq)?;
        return Ok(());
    }
    let c = open_conn(&hot0, true)?;
    let stored = match meta(&c, "num_slices").map_err(sq)? {
        Some(v) => v.parse::<usize>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "expected a slice count in meta num_slices of {}, saw {v:?}",
                    hot0.display()
                ),
            )
        })?,
        None => {
            let on_disk = (0..)
                .take_while(|&i| shard_dir(root, i).join("hot.sqlite").exists())
                .count();
            c.execute(
                "INSERT OR REPLACE INTO meta (key, val) VALUES ('num_slices', ?1)",
                [on_disk.to_string()],
            )
            .map_err(sq)?;
            on_disk
        }
    };
    if stored != want {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "expected --slices {stored} for existing coordinator store {}, saw --slices {want}",
                root.display()
            ),
        ));
    }
    Ok(())
}

/// Restore Mail-id records lost between a cross-slice envelope commit and its
/// `seen_ids` commit. Each slice scans only envelopes past its
/// `seen_reconciled_seq` watermark, so the cost tracks recent writes.
fn reconcile_seen(conns: &[Connection]) -> rusqlite::Result<()> {
    let n = conns.len();
    if n < 2 {
        return Ok(());
    }
    // mail_id, kind, sender, target, body_len, body_sha256, article_num, stream
    type SeenFields = (String, String, String, String, i64, String, i64, String);
    for (st_idx, c) in conns.iter().enumerate() {
        let mark: i64 = meta(c, "seen_reconciled_seq")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut st = c.prepare(
            "SELECT seq, mail_id, kind, sender, recipient, body_len, body_sha256, article_num, stream
             FROM envelopes WHERE seq > ?1 AND kind IN ('directed', 'group') ORDER BY seq",
        )?;
        let mut by_slice: Vec<Vec<SeenFields>> = vec![Vec::new(); n];
        let mut top = mark;
        let mut rows = st.query([mark])?;
        while let Some(r) = rows.next()? {
            top = top.max(r.get(0)?);
            let mail_id: String = r.get(1)?;
            let seen_idx = slice_of(&mail_id, n);
            if seen_idx != st_idx {
                by_slice[seen_idx].push((
                    mail_id,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ));
            }
        }
        if top == mark {
            continue;
        }
        for (seen_idx, rows) in by_slice.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let tx = conns[seen_idx].unchecked_transaction()?;
            for (mail_id, kind, sender, target, len, sha, art, stream) in rows {
                tx.execute(
                    "INSERT OR IGNORE INTO seen_ids (mail_id, kind, sender, target, body_len, body_sha256, article_num, stream)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![mail_id, kind, sender, target, len, sha, art, stream],
                )?;
            }
            tx.commit()?;
        }
        c.execute(
            "INSERT OR REPLACE INTO meta (key, val) VALUES ('seen_reconciled_seq', ?1)",
            [top.to_string()],
        )?;
    }
    Ok(())
}

impl Store {
    pub fn open(cfg: Config) -> io::Result<(Self, Startup)> {
        if cfg.num_slices == 0 || cfg.day_ms <= 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "expected num_slices >= 1 and day_ms > 0, saw num_slices={} day_ms={}",
                    cfg.num_slices, cfg.day_ms
                ),
            ));
        }
        let root = coord_root(&cfg.data);
        fs::create_dir_all(&root)?;
        let lock = crate::durlog::open_exclusive_lock(&root.join("LOCK"))?;
        check_slice_count(&root, cfg.num_slices)?;
        let mut dirs = Vec::with_capacity(cfg.num_slices);
        let mut conns = Vec::with_capacity(cfg.num_slices);
        for idx in 0..cfg.num_slices {
            let dir = shard_dir(&root, idx);
            fs::create_dir_all(dir.join("cold"))?;
            conns.push(open_conn(&dir.join("hot.sqlite"), true)?);
            dirs.push(dir);
        }
        if cfg.data.is_file()
            && !is_sqlite_file(&cfg.data)
            && meta(&conns[0], "imported_legacy").map_err(sq)?.as_deref() != Some("1")
            && super::import::coord_log(&cfg.data, &mut conns)?
        {
            conns[0]
                .execute(
                    "INSERT OR REPLACE INTO meta (key, val) VALUES ('imported_legacy', '1')",
                    [],
                )
                .map_err(sq)?;
        }
        reconcile_seen(&conns).map_err(sq)?;
        let startup = load_startup(&conns[0]).map_err(sq)?;
        let mut top = 0;
        let mut sub_map: HashMap<String, HashMap<usize, HashSet<String>>> = HashMap::new();
        for (idx, c) in conns.iter().enumerate() {
            top = top.max(max_seq(c).map_err(sq)?);
            let mut st = c
                .prepare(
                    "SELECT reader, stream FROM positions WHERE subscribed = 1 AND substr(stream, 1, 2) = 'g:'",
                )
                .map_err(sq)?;
            let rows = st
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(sq)?;
            for row in rows {
                let (reader, stream) = row.map_err(sq)?;
                sub_map
                    .entry(reader)
                    .or_default()
                    .entry(idx)
                    .or_default()
                    .insert(stream);
            }
        }
        let mut slices = Vec::with_capacity(cfg.num_slices);
        for (idx, (conn, dir)) in conns.into_iter().zip(dirs).enumerate() {
            let cold_dir = dir.join("cold");
            slices.push(Shard {
                cold: Arc::new(Index::load_indexed(&cold_dir)?),
                cold_dir,
                read: Mutex::new(open_conn(&dir.join("hot.sqlite"), false)?),
                writer: Writer::spawn(conn, idx)?,
                #[cfg(test)]
                reads: std::sync::atomic::AtomicUsize::new(0),
            });
        }
        let inner = Inner {
            cfg,
            slices,
            next_seq: AtomicI64::new(top + 1),
            in_flight: (Mutex::new(HashSet::new()), Condvar::new()),
            subscribed_groups: Mutex::new(sub_map),
            last_swept_day: AtomicI64::new(i64::MIN),
            next_sweep_due_ms: AtomicI64::new(i64::MIN),
            sweep_running: std::sync::atomic::AtomicBool::new(false),
            sweep_lock: Mutex::new(()),
            sweeps: Mutex::new(Vec::new()),
            _lock: lock,
        };
        Ok((
            Self {
                inner: Arc::new(inner),
            },
            startup,
        ))
    }

    pub fn commit_timeline(&self, events: Vec<Event>, now_ms: i64) -> io::Result<()> {
        let rows: Vec<(i64, Event)> = events.into_iter().map(|e| (self.inner.seq(), e)).collect();
        self.inner.slices[0].writer.exec(move |c| {
            for (seq, ev) in &rows {
                insert_timeline(c, *seq, ev, now_ms)?;
            }
            Ok(())
        })
    }

    /// A committed Mail can start a due sweep, off the send path.
    fn after_send(&self, accepted_at_ms: Option<i64>) {
        let Some(at) = accepted_at_ms else { return };
        if !self.inner.cfg.sweep_enabled {
            return;
        }
        let day = at.div_euclid(self.inner.cfg.day_ms);
        if day <= self.inner.last_swept_day.load(Ordering::SeqCst)
            && now_ms() < self.inner.next_sweep_due_ms.load(Ordering::SeqCst)
        {
            return;
        }
        if self.inner.sweep_running.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let spawned = std::thread::Builder::new()
            .name("roger-cold-sweep".into())
            .spawn(move || {
                match inner.sweep(now_ms()) {
                    Ok(()) => {
                        inner.last_swept_day.fetch_max(day, Ordering::SeqCst);
                    }
                    Err(e) => eprintln!("cold sweep failed: {e}"),
                }
                inner.sweep_running.store(false, Ordering::SeqCst);
            });
        match spawned {
            Ok(h) => {
                let mut g = self.inner.sweeps.lock().unwrap();
                g.retain(|h| !h.is_finished());
                g.push(h);
            }
            Err(e) => {
                self.inner.sweep_running.store(false, Ordering::SeqCst);
                eprintln!("cold sweep not started: {e}");
            }
        }
    }

    #[cfg(test)]
    fn join_sweeps(&self) {
        let handles = std::mem::take(&mut *self.inner.sweeps.lock().unwrap());
        for h in handles {
            h.join().unwrap();
        }
    }

    /// Write one single-stream envelope plus its Mail-id record; atomic when
    /// both live in the same slice.
    fn write_single(&self, env: NewEnvelope, seen: NewSeen) -> io::Result<SingleWrite> {
        let inner = &self.inner;
        let st_idx = inner.shard(&env.stream);
        let seen_idx = inner.shard(&seen.mail_id);
        let same = st_idx == seen_idx;
        let target = env.recipient.clone();
        let mail_id = env.mail_id.clone();
        if same {
            return inner.slices[st_idx].writer.exec(move |c| {
                if let Some(s) = query_seen(c, &mail_id)? {
                    return Ok(
                        match s.differs(
                            env.kind,
                            &env.sender,
                            Some(&env.recipient),
                            seen.body_len,
                            &seen.body_sha256,
                        ) {
                            None => SingleWrite::Duplicate(s.article_num),
                            Some(w) => SingleWrite::Conflict(resend_refusal(&mail_id, &w)),
                        },
                    );
                }
                if query_broadcast(c, &mail_id)?.is_some() {
                    return Ok(SingleWrite::Conflict(resend_refusal(&mail_id, "recipient")));
                }
                let art = match insert_envelope(c, &env)? {
                    InsertOutcome::Committed(a) => a,
                    InsertOutcome::ResendConflict(w) => {
                        return Ok(SingleWrite::Conflict(resend_refusal(&mail_id, &w)));
                    }
                    InsertOutcome::HeadConflict { expected, saw } => {
                        return Ok(SingleWrite::Conflict(format!(
                            "expected group {target} at article {expected}, saw {saw}"
                        )));
                    }
                };
                insert_seen(c, &seen, art)?;
                let subs = if env.kind == "group" {
                    subscribers_of(c, &env.stream)?
                } else {
                    vec![env.recipient.clone()]
                };
                Ok(SingleWrite::Committed(art, subs))
            });
        }
        let res = inner.slices[st_idx].writer.exec(move |c| {
            let art = match insert_envelope(c, &env)? {
                InsertOutcome::Committed(a) => a,
                InsertOutcome::ResendConflict(w) => return Ok(Err(resend_refusal(&mail_id, &w))),
                InsertOutcome::HeadConflict { expected, saw } => {
                    return Ok(Err(format!(
                        "expected group {target} at article {expected}, saw {saw}"
                    )));
                }
            };
            let subs = if env.kind == "group" {
                subscribers_of(c, &env.stream)?
            } else {
                vec![env.recipient.clone()]
            };
            Ok(Ok((art, subs)))
        })?;
        match res {
            Err(reason) => Ok(SingleWrite::Conflict(reason)),
            Ok((art, subs)) => {
                inner.slices[seen_idx]
                    .writer
                    .exec(move |c| insert_seen(c, &seen, art))?;
                Ok(SingleWrite::Committed(art, subs))
            }
        }
    }

    pub fn send_directed(&self, req: Directed) -> io::Result<Status> {
        self.send_single(
            req.mail_id,
            req.from,
            req.to,
            req.body,
            req.headers,
            req.accepted_at_ms,
            false,
            None,
        )
    }

    pub fn send_group(&self, req: GroupMail) -> io::Result<Status> {
        self.send_single(
            req.mail_id,
            req.from,
            req.group,
            req.body,
            req.headers,
            req.accepted_at_ms,
            true,
            req.expected_article,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn send_single(
        &self,
        mail_id: String,
        from: String,
        target: String,
        body: String,
        headers: Option<Value>,
        accepted_at_ms: Option<i64>,
        group: bool,
        expected_article: Option<i64>,
    ) -> io::Result<Status> {
        let inner = &self.inner;
        let _flight = inner.hold(&mail_id);
        let kind = if group { "group" } else { "directed" };
        let sha = hex_digest(body.as_bytes());
        let len = body.len() as i64;
        let stream = format!("{}:{target}", if group { "g" } else { "m" });
        if inner.shard(&stream) != inner.shard(&mail_id) {
            let (seen, bcast) = inner.seen(&mail_id)?;
            if let Some(s) = seen {
                return Ok(match s.differs(kind, &from, Some(&target), len, &sha) {
                    None => Status::DuplicateAccepted {
                        article_num: s.article_num,
                    },
                    Some(w) => Status::Conflict(resend_refusal(&mail_id, &w)),
                });
            }
            if bcast.is_some() {
                return Ok(Status::Conflict(resend_refusal(&mail_id, "recipient")));
            }
        }
        let env = NewEnvelope {
            seq: inner.seq(),
            mail_id: mail_id.clone(),
            stream: stream.clone(),
            stream_kind: if group { "group" } else { "mail" },
            kind,
            sender: from.clone(),
            recipient: target.clone(),
            headers_json: headers.as_ref().map(json::stringify),
            body,
            body_sha256: sha.clone(),
            accepted_at_ms,
            day_bucket: day_bucket(accepted_at_ms, inner.cfg.day_ms),
            reader: (!group).then(|| target.clone()),
            expected_article,
        };
        let seen = NewSeen {
            mail_id: mail_id.clone(),
            kind,
            sender: from,
            target: Some(target),
            body_len: len,
            body_sha256: sha,
            stream: Some(stream),
        };
        let status = match self.write_single(env, seen)? {
            SingleWrite::Conflict(reason) => return Ok(Status::Conflict(reason)),
            SingleWrite::Duplicate(article_num) => {
                return Ok(Status::DuplicateAccepted { article_num });
            }
            SingleWrite::Committed(article_num, subscribers) => Status::Committed {
                article_num,
                subscribers,
            },
        };
        drop(_flight);
        self.after_send(accepted_at_ms);
        Ok(status)
    }
    pub fn groups(
        &self,
        session: Option<&str>,
        prefix: Option<&str>,
        include_retired: bool,
    ) -> io::Result<Vec<Group>> {
        let mut out = Vec::new();
        for idx in 0..self.inner.slices.len() {
            out.extend(self.inner.read(idx, |c| {
                let mut st = c.prepare(
                    "SELECT s.stream, s.next_article - 1,
                            (SELECT COUNT(*) FROM positions p WHERE p.stream = s.stream AND p.subscribed = 1),
                            q.position, q.subscribed, q.record_id,
                            s.retired_at_ms IS NOT NULL, s.keep_last, s.ttl_ms
                     FROM streams s
                     LEFT JOIN positions q ON q.stream = s.stream AND q.reader = ?1
                     WHERE s.kind = 'group'
                       AND (?2 = 1 OR s.retired_at_ms IS NULL)",
                )?;
                let rows = st.query_map(params![session, i64::from(include_retired)], |r| {
                    let stream: String = r.get(0)?;
                    let name = stream.strip_prefix("g:").unwrap_or(&stream).to_string();
                    Ok(Group {
                        group: name,
                        high_water: r.get(1)?,
                        subscribers: r.get::<_, i64>(2)? as usize,
                        position: r.get(3)?,
                        subscribed: r.get::<_, Option<i64>>(4)? == Some(1),
                        record_id: r.get(5)?,
                        retired: r.get::<_, i64>(6)? != 0,
                        keep_last: r.get(7)?,
                        ttl_ms: r.get(8)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })?);
        }
        if let Some(p) = prefix.filter(|p| !p.is_empty()) {
            out.retain(|g| g.group.starts_with(p));
        }
        out.sort_by(|a, b| a.group.cmp(&b.group));
        Ok(out)
    }

    /// Stateless read of Group articles strictly after `after`, up to `limit`,
    /// without creating a stream row, position, or receipt.
    pub fn articles(&self, group: &str, after: i64, limit: usize) -> io::Result<ArticlesPage> {
        let stream = format!("g:{group}");
        let idx = self.inner.shard(&stream);
        let meta: Option<(i64, bool)> = self.inner.read(idx, |c| {
            c.query_row(
                "SELECT next_article - 1, retired_at_ms IS NOT NULL FROM streams WHERE stream = ?1 AND kind = 'group'",
                [&stream],
                |r| Ok((r.get(0)?, r.get::<_, i64>(1)? != 0)),
            )
            .optional()
        })?;
        let Some((high_water, retired)) = meta else {
            return Ok(ArticlesPage {
                group: group.to_string(),
                high_water: 0,
                retired: false,
                articles: Vec::new(),
            });
        };
        let cap = i64::try_from(limit).unwrap_or(i64::MAX);
        let raws = self.inner.read(idx, |c| {
            let mut st = c.prepare(
                "SELECT e.article_num, e.mail_id, e.sender, e.headers_json, e.day_bucket, e.in_cold, h.body, e.accepted_at_ms
                 FROM envelopes e
                 LEFT JOIN hot_bodies h ON h.mail_id = e.mail_id
                 WHERE e.stream = ?1 AND e.kind = 'group'
                   AND e.article_num > max(?2, COALESCE((SELECT pruned_through FROM streams WHERE stream = ?1), 0))
                 ORDER BY e.article_num
                 LIMIT ?3",
            )?;
            let rows = st.query_map(params![&stream, after, cap], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)? != 0,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, Option<i64>>(7)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })?;
        let mut cold = self.inner.cold_bodies(
            idx,
            raws.iter().map(|(_, id, _, _, day, in_cold, body, _)| {
                (*day, id.as_str(), *in_cold, body.is_some())
            }),
        )?;
        let mut articles = Vec::with_capacity(raws.len());
        for (article_num, mail_id, from, headers_json, day_bucket, in_cold, body, accepted_at_ms) in
            raws
        {
            let body = Inner::resolve_body(&mut cold, &mail_id, day_bucket, in_cold, body)?;
            articles.push(Article {
                article_num,
                mail_id,
                from,
                body,
                headers: headers_value(headers_json.as_deref()),
                accepted_at_ms,
            });
        }
        Ok(ArticlesPage {
            group: group.to_string(),
            high_water,
            retired,
            articles,
        })
    }

    pub fn group_policy(
        &self,
        group: &str,
        keep_last: Option<Option<i64>>,
        ttl_ms: Option<Option<i64>>,
        retired: Option<bool>,
        now_ms: i64,
    ) -> io::Result<Group> {
        let _policy_guard = {
            let guard = self.inner.sweep_lock.lock().unwrap();
            self.inner
                .next_sweep_due_ms
                .store(i64::MIN, Ordering::SeqCst);
            guard
        };
        let stream = format!("g:{group}");
        let idx = self.inner.shard(&stream);
        let group_name = group.to_string();
        self.inner.slices[idx].writer.exec(move |c| {
            c.execute(
                "INSERT OR IGNORE INTO streams (stream, kind, next_article) VALUES (?1, 'group', 1)",
                [&stream],
            )?;
            if let Some(kl) = keep_last {
                c.execute(
                    "UPDATE streams SET keep_last = ?2 WHERE stream = ?1",
                    params![&stream, kl],
                )?;
            }
            if let Some(ttl) = ttl_ms {
                c.execute(
                    "UPDATE streams SET ttl_ms = ?2 WHERE stream = ?1",
                    params![&stream, ttl],
                )?;
            }
            if let Some(ret) = retired {
                if ret {
                    c.execute(
                        "UPDATE streams SET retired_at_ms = COALESCE(retired_at_ms, ?2) WHERE stream = ?1",
                        params![&stream, now_ms],
                    )?;
                } else {
                    c.execute(
                        "UPDATE streams SET retired_at_ms = NULL WHERE stream = ?1",
                        [&stream],
                    )?;
                }
            }
            let (hw, subs, is_ret, kl, ttl): (i64, i64, i64, Option<i64>, Option<i64>) = c
                .query_row(
                    "SELECT next_article - 1,
                            (SELECT COUNT(*) FROM positions p WHERE p.stream = s.stream AND p.subscribed = 1),
                            s.retired_at_ms IS NOT NULL, s.keep_last, s.ttl_ms
                     FROM streams s WHERE s.stream = ?1",
                    [&stream],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )?;
            if let Some(k) = kl.filter(|&k| k >= 0) {
                prune_hot_group(c, &stream, hw - k)?;
            }
            Ok(Group {
                group: group_name,
                high_water: hw,
                subscribers: subs as usize,
                position: None,
                subscribed: false,
                record_id: None,
                retired: is_ret != 0,
                keep_last: kl,
                ttl_ms: ttl,
            })
        })
    }

    fn broadcast_flights(&self, req: &Broadcast) -> Vec<Flight<'_>> {
        let mut ids: Vec<_> = req
            .recipients
            .iter()
            .map(|r| format!("{}/{r}", req.mail_id))
            .collect();
        ids.push(req.mail_id.clone());
        ids.sort_unstable();
        ids.dedup();
        ids.iter().map(|id| self.inner.hold(id)).collect()
    }

    fn copy_conflict(
        c: &Connection,
        list: &[NewEnvelope],
    ) -> rusqlite::Result<Option<(String, String)>> {
        let mut st = c.prepare("SELECT kind, sender, recipient, body_len, body_sha256, article_num FROM envelopes WHERE mail_id = ?1")?;
        for e in list {
            let existing = st
                .query_row([&e.mail_id], |r| {
                    Ok(SeenRow {
                        kind: r.get(0)?,
                        sender: r.get(1)?,
                        target: r.get(2)?,
                        body_len: r.get(3)?,
                        body_sha256: r.get(4)?,
                        article_num: r.get(5)?,
                    })
                })
                .optional()?;
            if let Some(w) = existing.and_then(|s| {
                s.differs(
                    e.kind,
                    &e.sender,
                    Some(&e.recipient),
                    e.body.len() as i64,
                    &e.body_sha256,
                )
            }) {
                return Ok(Some((e.mail_id.clone(), w)));
            }
        }
        Ok(None)
    }

    pub fn send_broadcast(&self, req: Broadcast) -> io::Result<Status> {
        let inner = &self.inner;
        let _flight = self.broadcast_flights(&req);
        let sha = hex_digest(req.body.as_bytes());
        let len = req.body.len() as i64;
        let (seen, bcast) = inner.seen(&req.mail_id)?;
        if let Some(b) = bcast {
            return Ok(match b.differs("broadcast", &req.from, None, len, &sha) {
                None => Status::DuplicateAccepted { article_num: 0 },
                Some(w) => Status::Conflict(resend_refusal(&req.mail_id, &w)),
            });
        }
        if seen.is_some() {
            return Ok(Status::Conflict(resend_refusal(&req.mail_id, "recipient")));
        }
        let headers_json = req.headers.as_ref().map(json::stringify);
        let n = inner.slices.len();
        let mut envs: Vec<Vec<NewEnvelope>> = (0..n).map(|_| Vec::new()).collect();
        let mut seens: Vec<Vec<NewSeen>> = (0..n).map(|_| Vec::new()).collect();
        for rcpt in &req.recipients {
            let copy = format!("{}/{rcpt}", req.mail_id);
            let (copy_seen, copy_bcast) = inner.seen(&copy)?;
            if copy_bcast.is_some() {
                return Ok(Status::Conflict(resend_refusal(&copy, "recipient")));
            }
            // A copy already recorded by this same broadcast is a resume after a
            // partial write; anything else under the copy id is a conflict.
            if let Some(s) = copy_seen {
                if let Some(w) = s.differs("broadcast", &req.from, Some(rcpt), len, &sha) {
                    return Ok(Status::Conflict(resend_refusal(&copy, &w)));
                }
            }
            let stream = format!("m:{rcpt}");
            envs[inner.shard(&stream)].push(NewEnvelope {
                seq: inner.seq(),
                mail_id: copy.clone(),
                stream: stream.clone(),
                stream_kind: "mail",
                kind: "broadcast",
                sender: req.from.clone(),
                recipient: rcpt.clone(),
                headers_json: headers_json.clone(),
                body: req.body.clone(),
                body_sha256: sha.clone(),
                accepted_at_ms: req.accepted_at_ms,
                day_bucket: day_bucket(req.accepted_at_ms, inner.cfg.day_ms),
                reader: Some(rcpt.clone()),
                expected_article: None,
            });
            seens[inner.shard(&copy)].push(NewSeen {
                mail_id: copy,
                kind: "broadcast",
                sender: req.from.clone(),
                target: Some(rcpt.clone()),
                body_len: len,
                body_sha256: sha.clone(),
                stream: Some(stream),
            });
        }
        // Check every stream slice before submitting any writes. The flights keep these
        // ids reserved until both the envelopes and the Mail-id records are committed.
        for (idx, list) in envs.iter().enumerate().filter(|(_, l)| !l.is_empty()) {
            if let Some((id, what)) = inner.read(idx, |c| Self::copy_conflict(c, list))? {
                return Ok(Status::Conflict(resend_refusal(&id, &what)));
            }
        }
        // Phase 1: every copy's envelope, all slices at once.
        let mut waits = Vec::new();
        for (idx, list) in envs.into_iter().enumerate().filter(|(_, l)| !l.is_empty()) {
            waits.push(inner.slices[idx].writer.submit(move |c| {
                if let Some((id, what)) = Self::copy_conflict(c, &list)? {
                    return Ok(vec![(id, InsertOutcome::ResendConflict(what))]);
                }
                let mut out = Vec::new();
                for e in &list {
                    out.push((e.mail_id.clone(), insert_envelope(c, e)?));
                }
                Ok(out)
            })?);
        }
        let mut articles: HashMap<String, i64> = HashMap::new();
        let mut conflict = None;
        for w in waits {
            for (id, r) in w.wait()? {
                match r {
                    InsertOutcome::Committed(a) => {
                        articles.insert(id, a);
                    }
                    InsertOutcome::ResendConflict(what) => {
                        conflict = Some(resend_refusal(&id, &what));
                    }
                    InsertOutcome::HeadConflict { expected, saw } => {
                        conflict = Some(format!("expected {id} at article {expected}, saw {saw}"));
                    }
                }
            }
        }
        if let Some(c) = conflict {
            return Ok(Status::Conflict(c));
        }
        // Phase 2: Mail-id records for every copy, then the broadcast itself.
        let bseq = inner.seq();
        let b_idx = inner.shard(&req.mail_id);
        let recipients_json = json::stringify(&Value::Arr(
            req.recipients
                .iter()
                .map(|r| Value::Str(r.clone()))
                .collect(),
        ));
        let mut brow = Some((
            req.mail_id.clone(),
            bseq,
            req.from.clone(),
            len,
            sha.clone(),
            headers_json,
            recipients_json,
            req.accepted_at_ms,
        ));
        let mut waits = Vec::new();
        for (idx, list) in seens.into_iter().enumerate() {
            let b = if idx == b_idx { brow.take() } else { None };
            if list.is_empty() && b.is_none() {
                continue;
            }
            let arts: Vec<i64> = list
                .iter()
                .map(|s| articles.get(&s.mail_id).copied().unwrap_or(0))
                .collect();
            waits.push(inner.slices[idx].writer.submit(move |c| {
                for (s, a) in list.iter().zip(arts) {
                    insert_seen(c, s, a)?;
                }
                if let Some((id, seq, sender, len, sha, hj, rj, at)) = b {
                    c.execute(
                        "INSERT OR IGNORE INTO broadcasts (mail_id, seq, sender, body_len, body_sha256, headers_json, recipients_json, accepted_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![id, seq, sender, len, sha, hj, rj, at],
                    )?;
                }
                Ok(())
            })?);
        }
        for w in waits {
            w.wait()?;
        }
        drop(_flight);
        self.after_send(req.accepted_at_ms);
        Ok(Status::Committed {
            article_num: 0,
            subscribers: req.recipients,
        })
    }

    pub fn ack(
        &self,
        mail_id: &str,
        session: &str,
        record_id: &str,
        now_ms: i64,
    ) -> io::Result<AckResult> {
        let inner = &self.inner;
        let Some((idx, (stream, article_num, kind, _sender, recipient, _, _))) =
            inner.find(mail_id)?
        else {
            return Ok(AckResult::UnknownMail);
        };
        let group = kind == "group";
        let reader = if group {
            session.to_string()
        } else {
            recipient
        };
        let receipt_seq = inner.seq();
        let (mail_id, record_id) = (mail_id.to_string(), record_id.to_string());
        let newly = inner.slices[idx].writer.exec(move |c| {
            if !group {
                let mut st = c.prepare_cached("SELECT 1 FROM receipts WHERE mail_id = ?1")?;
                if st.query_row([&mail_id], |_| Ok(())).optional()?.is_some() {
                    return Ok(false);
                }
            }
            let mut st = c.prepare_cached(
                "INSERT OR IGNORE INTO receipts (mail_id, reader, stream, article_num, record_id, receipted_at_ms, receipt_seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            let n = st.execute(params![
                mail_id,
                reader,
                stream,
                article_num,
                record_id,
                now_ms,
                receipt_seq
            ])?;
            if n == 0 {
                return Ok(false);
            }
            let mut st = c.prepare_cached(
                "INSERT OR IGNORE INTO positions (stream, reader, subscribed, position, updated_at_ms) VALUES (?1, ?2, ?3, 0, ?4)",
            )?;
            st.execute(params![stream, reader, i64::from(!group), now_ms])?;
            advance_position(c, &stream, &reader, now_ms)?;
            Ok(true)
        })?;
        Ok(AckResult::Recorded {
            newly_receipted: newly,
        })
    }

    pub fn pending_session(&self, session: &str) -> io::Result<Vec<Delivery>> {
        self.inner.pending_for(session)
    }

    pub fn pending_host(&self, sessions: &[String]) -> io::Result<Vec<Delivery>> {
        let mut out = Vec::new();
        for s in sessions {
            out.extend(self.inner.pending_for(s)?);
        }
        out.sort_by_key(|p| p.seq);
        Ok(out)
    }

    pub fn receipt(&self, mail_id: &str, reader: Option<&str>) -> io::Result<Option<Receipt>> {
        let inner = &self.inner;
        let Some((
            idx,
            (_stream, article_num, kind, sender, recipient, headers_json, accepted_at_ms),
        )) = inner.find(mail_id)?
        else {
            return Ok(None);
        };
        let receipt: Option<(String, Option<i64>)> = inner.read(idx, |c| match reader {
            Some(r) => {
                let mut st = c.prepare_cached(
                    "SELECT record_id, receipted_at_ms FROM receipts WHERE mail_id = ?1 AND reader = ?2",
                )?;
                st.query_row(params![mail_id, r], |row| Ok((row.get(0)?, row.get(1)?)))
                    .optional()
            }
            None => {
                let mut st = c.prepare_cached(
                    "SELECT record_id, receipted_at_ms FROM receipts WHERE mail_id = ?1 ORDER BY receipt_seq LIMIT 1",
                )?;
                st.query_row([mail_id], |row| Ok((row.get(0)?, row.get(1)?)))
                    .optional()
            }
        })?;
        let group = kind == "group";
        let (record_id, receipted_at_ms) = match receipt {
            Some((r, at)) => (Some(r), at),
            None => (None, None),
        };
        Ok(Some(Receipt {
            mail_id: mail_id.to_string(),
            from: sender,
            to: recipient.clone(),
            group: group.then_some(recipient),
            article_num: group.then_some(article_num),
            kind,
            headers: headers_json.as_deref().and_then(|t| json::parse(t).ok()),
            accepted_at_ms,
            record_id,
            receipted_at_ms,
        }))
    }

    pub fn subscribe(
        &self,
        session: &str,
        group: &str,
        after_article: Option<i64>,
        from_latest: bool,
        now_ms: i64,
    ) -> io::Result<(i64, i64, Vec<Delivery>)> {
        let inner = &self.inner;
        let stream = format!("g:{group}");
        let idx = inner.shard(&stream);
        let (s, reader) = (stream.clone(), session.to_string());
        let (position, high_water) = inner.slices[idx].writer.exec(move |c| {
            c.execute(
                "INSERT OR IGNORE INTO streams (stream, kind, next_article) VALUES (?1, 'group', 1)",
                [&s],
            )?;
            let hw: i64 = c.query_row(
                "SELECT next_article - 1 FROM streams WHERE stream = ?1",
                [&s],
                |r| r.get(0),
            )?;
            let existing: Option<i64> = c
                .query_row(
                    "SELECT position FROM positions WHERE stream = ?1 AND reader = ?2",
                    params![s, reader],
                    |r| r.get(0),
                )
                .optional()?;
            let pos = match (after_article, from_latest) {
                (Some(a), _) => a.max(0),
                (None, true) => hw,
                (None, false) => existing.unwrap_or(0),
            };
            c.execute(
                "INSERT INTO positions (stream, reader, subscribed, position, updated_at_ms) VALUES (?1, ?2, 1, ?3, ?4)
                 ON CONFLICT(stream, reader) DO UPDATE SET subscribed = 1, position = excluded.position, updated_at_ms = excluded.updated_at_ms",
                params![s, reader, pos, now_ms],
            )?;
            Ok((pos, hw))
        })?;
        {
            let mut subs = inner.subscribed_groups.lock().unwrap();
            subs.entry(session.to_string())
                .or_default()
                .entry(idx)
                .or_default()
                .insert(stream.clone());
        }
        let pending = inner.group_pending(idx, session, Some(&stream))?;
        Ok((position, high_water, pending))
    }

    pub fn unsubscribe(&self, session: &str, group: &str, now_ms: i64) -> io::Result<i64> {
        let stream = format!("g:{group}");
        let idx = self.inner.shard(&stream);
        let reader = session.to_string();
        let pos = self.inner.slices[idx].writer.exec(move |c| {
            c.execute(
                "UPDATE positions SET subscribed = 0, updated_at_ms = ?3 WHERE stream = ?1 AND reader = ?2",
                params![stream, reader, now_ms],
            )?;
            Ok(c.query_row(
                "SELECT position FROM positions WHERE stream = ?1 AND reader = ?2",
                params![stream, reader],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
        })?;
        {
            let mut subs = self.inner.subscribed_groups.lock().unwrap();
            if let Some(by_shard) = subs.get_mut(session) {
                let stream_key = format!("g:{group}");
                if let Some(streams) = by_shard.get_mut(&idx) {
                    streams.remove(&stream_key);
                    if streams.is_empty() {
                        by_shard.remove(&idx);
                    }
                }
                if by_shard.is_empty() {
                    subs.remove(session);
                }
            }
        }
        Ok(pos)
    }

    /// Wait for every background sweep started so far, then sweep once more now.
    pub fn flush_cold(&self) -> io::Result<()> {
        let handles = std::mem::take(&mut *self.inner.sweeps.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
        self.inner.sweep(now_ms())
    }

    #[cfg(test)]
    pub(crate) fn gate_slice0(
        &self,
    ) -> (
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<io::Result<()>>,
    ) {
        let (entered_tx, entered_rx) = sync_channel::<()>(1);
        let (gate_tx, gate_rx) = sync_channel::<()>(1);
        let pending = self.inner.slices[0]
            .writer
            .submit(move |_| {
                let _ = entered_tx.send(());
                let _ = gate_rx.recv();
                Ok(())
            })
            .unwrap();
        entered_rx.recv().unwrap();
        (gate_tx, pending.0)
    }

    #[cfg(test)]
    pub(crate) fn wait_slice0_queued(&self, n: usize) {
        self.inner.slices[0].writer.wait_queued(n);
    }

    #[cfg(test)]
    pub(crate) fn timeline_rows(&self) -> Vec<(i64, String, Option<String>, Option<String>)> {
        self.inner
            .read(0, |c| {
                let mut st =
                    c.prepare("SELECT seq, kind, addr, claim_key FROM timeline ORDER BY seq")?;
                let rows =
                    st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct TempDir(PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir(name: &str) -> TempDir {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "rw-coord-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn cfg(dir: &TempDir, slices: usize) -> Config {
        let mut c = Config::new(dir.0.join("coordinator.db"));
        c.num_slices = slices;
        c
    }
    fn directed(id: &str, from: &str, to: &str, body: &str) -> Directed {
        Directed {
            mail_id: id.into(),
            from: from.into(),
            to: to.into(),
            body: body.into(),
            headers: None,
            accepted_at_ms: Some(now_ms()),
        }
    }
    fn group(id: &str, from: &str, g: &str, body: &str) -> GroupMail {
        GroupMail {
            mail_id: id.into(),
            from: from.into(),
            group: g.into(),
            body: body.into(),
            headers: None,
            expected_article: None,
            accepted_at_ms: Some(now_ms()),
        }
    }
    fn bcast(id: &str, from: &str, to: &[&str], body: &str) -> Broadcast {
        Broadcast {
            mail_id: id.into(),
            from: from.into(),
            recipients: to.iter().map(|s| s.to_string()).collect(),
            body: body.into(),
            headers: None,
            accepted_at_ms: Some(now_ms()),
        }
    }
    fn count(store: &Store, idx: usize, sql: &str) -> i64 {
        store
            .inner
            .read(idx, |c| c.query_row(sql, [], |r| r.get(0)))
            .unwrap()
    }
    fn committed(s: Status) -> i64 {
        match s {
            Status::Committed { article_num, .. } => article_num,
            other => panic!("expected Committed, saw {other:?}"),
        }
    }
    fn conflict(s: Status) -> String {
        match s {
            Status::Conflict(r) => r,
            other => panic!("expected Conflict, saw {other:?}"),
        }
    }

    #[test]
    fn queued_sends_commit_in_one_transaction_and_answer_after_it() {
        let dir = tempdir("batch");
        let (store, _) = Store::open(cfg(&dir, 1)).unwrap();
        let store = Arc::new(store);
        let w = &store.inner.slices[0].writer;
        let (entered_tx, entered_rx) = sync_channel::<()>(1);
        let (gate_tx, gate_rx) = sync_channel::<()>(1);
        let gate = w
            .submit(move |_| {
                entered_tx.send(()).unwrap();
                gate_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
        entered_rx.recv().unwrap();
        let before = w.commits.load(Ordering::SeqCst);
        let answered = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for i in 0..8 {
            let (store, answered) = (Arc::clone(&store), Arc::clone(&answered));
            threads.push(std::thread::spawn(move || {
                let s = store
                    .send_directed(directed(&format!("m{i}"), "s", "r", &format!("b{i}")))
                    .unwrap();
                answered.fetch_add(1, Ordering::SeqCst);
                committed(s)
            }));
        }
        w.wait_queued(8);
        assert_eq!(
            answered.load(Ordering::SeqCst),
            0,
            "no answer before COMMIT"
        );
        gate_tx.send(()).unwrap();
        gate.wait().unwrap();
        let mut arts: Vec<i64> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        arts.sort();
        assert_eq!(arts, (1..=8).collect::<Vec<_>>());
        assert_eq!(
            w.commits.load(Ordering::SeqCst) - before,
            2,
            "gate transaction plus one group commit for all eight sends"
        );
        assert_eq!(store.pending_session("r").unwrap().len(), 8);
    }

    #[test]
    fn mail_ids_are_never_reused_across_kinds_slices_or_restarts() {
        let dir = tempdir("ids");
        for round in 0..2 {
            let (store, _) = Store::open(cfg(&dir, 4)).unwrap();
            let r = store
                .send_directed(directed("m1", "a", "b", "hello"))
                .unwrap();
            if round == 0 {
                assert_eq!(committed(r), 1);
            } else {
                assert!(
                    matches!(r, Status::DuplicateAccepted { article_num: 1 }),
                    "{r:?}"
                );
            }
            let r = store
                .send_directed(directed("m1", "a", "b", "hello"))
                .unwrap();
            assert!(
                matches!(r, Status::DuplicateAccepted { article_num: 1 }),
                "{r:?}"
            );
            let c = conflict(
                store
                    .send_directed(directed("m1", "a", "b", "other"))
                    .unwrap(),
            );
            assert_eq!(
                c,
                "mail m1: expected a resend to repeat the Mail already accepted under this id, saw a different body"
            );
            assert!(conflict(
                store
                    .send_directed(directed("m1", "x", "c", "hello"))
                    .unwrap()
            )
            .ends_with("different sender, recipient"));
            conflict(store.send_group(group("m1", "a", "b", "hello")).unwrap());
            conflict(
                store
                    .send_broadcast(bcast("m1", "a", &["b"], "hello"))
                    .unwrap(),
            );

            let r = store
                .send_broadcast(bcast("b1", "a", &["x", "y"], "all"))
                .unwrap();
            if round == 0 {
                committed(r);
            } else {
                assert!(matches!(r, Status::DuplicateAccepted { .. }), "{r:?}");
            }
            conflict(
                store
                    .send_broadcast(bcast("b1", "a", &["x"], "changed"))
                    .unwrap(),
            );
            conflict(
                store
                    .send_directed(directed("b1", "a", "x", "all"))
                    .unwrap(),
            );
            conflict(
                store
                    .send_directed(directed("b1/x", "a", "x", "all"))
                    .unwrap(),
            );

            let r = store.send_group(group("g1", "a", "G", "post")).unwrap();
            if round == 0 {
                assert_eq!(committed(r), 1);
            }
            let r = store.send_group(group("g1", "a", "G", "post")).unwrap();
            assert!(
                matches!(r, Status::DuplicateAccepted { article_num: 1 }),
                "{r:?}"
            );
            conflict(store.send_group(group("g1", "z", "G", "post")).unwrap());
            conflict(
                store
                    .send_directed(directed("g1", "a", "G", "post"))
                    .unwrap(),
            );

            assert_eq!(store.pending_session("b").unwrap().len(), 1);
            let total: i64 = (0..4)
                .map(|i| count(&store, i, "SELECT COUNT(*) FROM envelopes"))
                .sum();
            assert_eq!(total, 4, "m1, b1/x, b1/y, g1 once each");
        }
    }

    #[test]
    fn conflicting_broadcast_copy_leaves_other_slices_unchanged() {
        let dir = tempdir("broadcast-conflict");
        let (store, _) = Store::open(cfg(&dir, 4)).unwrap();
        let mut shards = HashSet::new();
        let recipients: Vec<_> = (0..100)
            .map(|n| format!("r{n}"))
            .filter(|r| shards.insert(store.inner.shard(&format!("m:{r}"))))
            .take(3)
            .collect();
        let rs: Vec<_> = recipients.iter().map(String::as_str).collect();
        assert_eq!(rs.len(), 3);
        let copy = format!("B/{}", rs[1]);
        committed(
            store
                .send_directed(directed(&copy, "s", rs[1], "other"))
                .unwrap(),
        );
        let shard = &store.inner.slices[store.inner.shard(&copy)];
        let db = Connection::open(shard.cold_dir.parent().unwrap().join("hot.sqlite")).unwrap();
        assert_eq!(
            db.execute("DELETE FROM seen_ids WHERE mail_id = ?1", [&copy])
                .unwrap(),
            1
        );
        let reason = conflict(store.send_broadcast(bcast("B", "s", &rs, "all")).unwrap());
        assert!(reason.contains(&copy), "{reason}");
        for i in 0..4 {
            assert_eq!(
                count(
                    &store,
                    i,
                    &format!(
                        "SELECT COUNT(*) FROM envelopes WHERE mail_id IN ('B/{}','B/{}')",
                        rs[0], rs[2]
                    )
                ),
                0
            );
            assert_eq!(
                count(
                    &store,
                    i,
                    "SELECT COUNT(*) FROM broadcasts WHERE mail_id = 'B'"
                ),
                0
            );
        }
    }

    #[test]
    fn identical_broadcast_is_accepted_once() {
        let dir = tempdir("broadcast-repeat");
        let (store, _) = Store::open(cfg(&dir, 4)).unwrap();
        let send = || {
            store
                .send_broadcast(bcast("B", "s", &["r1", "r2"], "all"))
                .unwrap()
        };
        committed(send());
        assert!(matches!(send(), Status::DuplicateAccepted { .. }));
    }

    #[test]
    fn old_bodies_move_to_cold_and_restore_on_pending() {
        let dir = tempdir("cold");
        let mut c = cfg(&dir, 2);
        c.retain_ms = 60_000;
        c.day_ms = 1_000;
        let (store, _) = Store::open(c).unwrap();
        store.subscribe("reader", "G", None, false, 1).unwrap();
        let mut old = directed("old", "s", "a", "ancient body");
        old.accepted_at_ms = Some(5_000);
        committed(store.send_directed(old).unwrap());
        let mut post = group("post", "s", "G", "ancient post");
        post.accepted_at_ms = Some(5_500);
        committed(store.send_group(post).unwrap());
        committed(
            store
                .send_directed(directed("new", "s", "a", "fresh body"))
                .unwrap(),
        );
        store.flush_cold().unwrap();

        let cold: i64 = (0..2)
            .map(|i| {
                count(
                    &store,
                    i,
                    "SELECT COUNT(*) FROM envelopes WHERE in_cold = 1",
                )
            })
            .sum();
        let hot: i64 = (0..2)
            .map(|i| count(&store, i, "SELECT COUNT(*) FROM hot_bodies"))
            .sum();
        assert_eq!(
            (cold, hot),
            (2, 1),
            "two old bodies cold, the fresh one hot"
        );
        assert!(store
            .inner
            .slices
            .iter()
            .any(|s| s.cold_dir.join("day-5.cold").exists()));

        let a = store.pending_session("a").unwrap();
        let bodies: Vec<&str> = a.iter().map(|p| p.body.as_str()).collect();
        assert_eq!(bodies, ["ancient body", "fresh body"]);
        let r = store.pending_session("reader").unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].body, "ancient post");
        assert_eq!(r[0].group.as_deref(), Some("G"));

        // A late subscriber restores the cold article too.
        let (_, hw, late) = store.subscribe("late", "G", None, false, 2).unwrap();
        assert_eq!(hw, 1);
        assert_eq!(late[0].body, "ancient post");
        drop(store);
        let (store, _) = Store::open({
            let mut c = cfg(&dir, 2);
            c.day_ms = 1_000;
            c
        })
        .unwrap();
        assert_eq!(store.pending_session("a").unwrap()[0].body, "ancient body");
    }

    #[test]
    fn failed_sweep_retries_on_same_day() {
        let dir = tempdir("cold-retry");
        let mut c = cfg(&dir, 1);
        c.day_ms = i64::MAX;
        c.retain_ms = 1_000;
        let (store, _) = Store::open(c).unwrap();
        let cold_dir = &store.inner.slices[0].cold_dir;
        fs::create_dir_all(cold_dir).unwrap();
        fs::remove_dir(cold_dir).unwrap();
        fs::write(cold_dir, []).unwrap();
        let mut old = directed("old", "s", "a", "old body");
        old.accepted_at_ms = Some(now_ms() - 10_000);
        committed(store.send_directed(old).unwrap());
        store.join_sweeps();
        let query = "SELECT in_cold FROM envelopes WHERE mail_id = 'old'";
        assert_eq!(count(&store, 0, query), 0);
        fs::remove_file(cold_dir).unwrap();
        fs::create_dir(cold_dir).unwrap();
        committed(
            store
                .send_directed(directed("new", "s", "a", "new body"))
                .unwrap(),
        );
        store.join_sweeps();
        assert_eq!(count(&store, 0, query), 1);
    }

    #[test]
    fn short_group_ttl_triggers_same_day_sweep() {
        let dir = tempdir("cold-ttl");
        let (store, _) = Store::open(cfg(&dir, 1)).unwrap();
        let at = now_ms() - 10_000;
        let mut post = group("post", "s", "G", "body");
        post.accepted_at_ms = Some(at);
        committed(store.send_group(post).unwrap());
        store.join_sweeps();
        store
            .group_policy("G", None, Some(Some(100)), None, at)
            .unwrap();
        // Model a completed sweep just before expiry, without waiting for a clock tick.
        store.inner.sweep(at + 1).unwrap();
        assert_eq!(
            store.inner.next_sweep_due_ms.load(Ordering::SeqCst),
            at + 100
        );
        let query = "SELECT in_cold FROM envelopes WHERE mail_id = 'post'";
        assert_eq!(count(&store, 0, query), 0);
        let mut fresh = directed("new", "s", "a", "new body");
        fresh.accepted_at_ms = Some(at);
        committed(store.send_directed(fresh).unwrap());
        store.join_sweeps();
        assert_eq!(count(&store, 0, query), 1);
    }

    #[test]
    fn group_article_has_one_body_and_one_position_per_reader() {
        let dir = tempdir("group");
        let (store, _) = Store::open(cfg(&dir, 3)).unwrap();
        for r in ["r1", "r2"] {
            assert_eq!(store.subscribe(r, "G", None, false, 1).unwrap().0, 0);
        }
        committed(store.send_group(group("p1", "s", "G", "one")).unwrap());
        let (pos, hw, pend) = store.subscribe("r3", "G", None, true, 2).unwrap();
        assert_eq!((pos, hw, pend.len()), (1, 1, 0));
        committed(store.send_group(group("p2", "s", "G", "two")).unwrap());
        match store.send_group(group("p3", "s", "G", "three")).unwrap() {
            Status::Committed {
                article_num,
                subscribers,
            } => {
                assert_eq!(article_num, 3);
                assert_eq!(subscribers, ["r1", "r2", "r3"]);
            }
            other => panic!("{other:?}"),
        }
        let idx = store.inner.shard("g:G");
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM envelopes"), 3);
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM hot_bodies"), 3);

        let pos = |r: &str| {
            store
                .groups(Some(r), None, false)
                .unwrap()
                .into_iter()
                .find(|g| g.group == "G")
                .unwrap()
                .position
                .unwrap()
        };
        assert_eq!(
            store.ack("p2", "r1", "r-2", 10).unwrap(),
            AckResult::Recorded {
                newly_receipted: true
            }
        );
        assert_eq!(pos("r1"), 0, "a gap holds the position");
        store.ack("p1", "r1", "r-1", 11).unwrap();
        assert_eq!(pos("r1"), 2);
        assert_eq!(
            store.ack("p1", "r1", "r-9", 12).unwrap(),
            AckResult::Recorded {
                newly_receipted: false
            }
        );
        store.ack("p1", "r2", "r-1", 13).unwrap();
        assert_eq!(pos("r2"), 1);
        assert_eq!(
            store.ack("nope", "r1", "r-1", 1).unwrap(),
            AckResult::UnknownMail
        );
        let arts = |r: &str| -> Vec<i64> {
            store
                .pending_session(r)
                .unwrap()
                .iter()
                .map(|p| p.article_num.unwrap())
                .collect()
        };
        assert_eq!(arts("r1"), [3]);
        assert_eq!(arts("r2"), [2, 3]);
        assert_eq!(arts("r3"), [2, 3]);
        let q = store.receipt("p1", Some("r1")).unwrap().unwrap();
        assert_eq!(
            (q.record_id.as_deref(), q.group.as_deref()),
            (Some("r-1"), Some("G"))
        );

        assert_eq!(store.unsubscribe("r3", "G", 20).unwrap(), 1);
        assert!(arts("r3").is_empty());
        let g = &store.groups(None, None, false).unwrap()[0];
        assert_eq!((g.high_water, g.subscribers), (3, 2));
        let (p, _, back) = store.subscribe("r3", "G", None, false, 21).unwrap();
        assert_eq!((p, back.len()), (1, 2), "re-subscribe keeps the position");
        let (_, _, all) = store.subscribe("r3", "G", Some(0), false, 22).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn streams_and_ids_shard_across_slices_and_survive_reopen() {
        let dir = tempdir("shard");
        let n = 4;
        let addrs: Vec<String> = (0..24).map(|i| format!("[[x:{i}]]addr-{i}")).collect();
        {
            let (store, _) = Store::open(cfg(&dir, n)).unwrap();
            for (i, a) in addrs.iter().enumerate() {
                committed(
                    store
                        .send_directed(directed(&format!("id-{i}"), "s", a, a))
                        .unwrap(),
                );
            }
            let mut used = HashSet::new();
            for (i, a) in addrs.iter().enumerate() {
                let st = hash_pair(format!("m:{a}").as_bytes()).0 as usize % n;
                let id = format!("id-{i}");
                let sd = hash_pair(id.as_bytes()).0 as usize % n;
                used.insert(st);
                let here = |idx: usize, table: &str| {
                    store
                        .inner
                        .read(idx, |c| {
                            c.query_row(
                                &format!("SELECT COUNT(*) FROM {table} WHERE mail_id = ?1"),
                                [&id],
                                |r| r.get::<_, i64>(0),
                            )
                        })
                        .unwrap()
                };
                for idx in 0..n {
                    assert_eq!(
                        here(idx, "envelopes"),
                        i64::from(idx == st),
                        "{id} envelope"
                    );
                    assert_eq!(here(idx, "seen_ids"), i64::from(idx == sd), "{id} seen");
                }
            }
            assert!(used.len() > 1, "streams spread over slices");
        }
        let (store, _) = Store::open(cfg(&dir, n)).unwrap();
        let all = store.pending_host(&addrs).unwrap();
        assert_eq!(all.len(), addrs.len());
        assert!(all.windows(2).all(|w| w[0].seq < w[1].seq));
        let top = all.last().unwrap().seq;
        committed(
            store
                .send_directed(directed("after", "s", &addrs[0], "x"))
                .unwrap(),
        );
        let p = store.pending_session(&addrs[0]).unwrap();
        assert!(
            p.last().unwrap().seq > top,
            "sequence continues after reopen"
        );
        store.ack("id-0", &addrs[0], "r-1", 5).unwrap();
        assert_eq!(store.pending_session(&addrs[0]).unwrap().len(), 1);
        let q = store.receipt("id-0", None).unwrap().unwrap();
        assert_eq!(q.record_id.as_deref(), Some("r-1"));
    }

    #[test]
    fn slice_count_is_recorded_and_a_mismatch_is_refused_before_creating_slices() {
        let dir = tempdir("slicecount");
        let root = coord_root(&cfg(&dir, 2).data);
        drop(Store::open(cfg(&dir, 2)).unwrap());
        let refused = |n: usize| {
            let Err(e) = Store::open(cfg(&dir, n)) else {
                panic!("expected --slices {n} to be refused");
            };
            assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
            let msg = e.to_string();
            assert!(
                msg.contains("--slices 2") && msg.contains(&format!("--slices {n}")),
                "{msg}"
            );
        };
        refused(3);
        assert!(
            !root.join("slice-02").exists(),
            "no slice created on refusal"
        );
        drop(Store::open(cfg(&dir, 2)).unwrap());

        let hot0 = shard_dir(&root, 0).join("hot.sqlite");
        let stored = |want: Option<&str>| {
            let c = Connection::open(&hot0).unwrap();
            assert_eq!(meta(&c, "num_slices").unwrap().as_deref(), want);
            c
        };
        stored(Some("2"))
            .execute("DELETE FROM meta WHERE key = 'num_slices'", [])
            .unwrap();
        stored(None);
        drop(Store::open(cfg(&dir, 2)).unwrap());
        stored(Some("2"));
        refused(1);
        refused(3);
        assert!(
            !root.join("slice-02").exists(),
            "no slice created on refusal"
        );
    }

    #[test]
    fn lost_cross_slice_seen_record_is_restored_on_open() {
        let dir = tempdir("reconcile");
        let n = 4;
        let root = coord_root(&cfg(&dir, n).data);
        let to = "[[x:r]]rcpt";
        let st = slice_of(&format!("m:{to}"), n);
        let id = (0..)
            .map(|i| format!("split-{i}"))
            .find(|id| slice_of(id, n) != st)
            .unwrap();
        {
            let (store, _) = Store::open(cfg(&dir, n)).unwrap();
            committed(store.send_directed(directed(&id, "s", to, "body")).unwrap());
            store.flush_cold().unwrap(); // a background sweep holds the store lock until it ends
        }
        let seen_hot = shard_dir(&root, slice_of(&id, n)).join("hot.sqlite");
        let gone = Connection::open(&seen_hot)
            .unwrap()
            .execute("DELETE FROM seen_ids WHERE mail_id = ?1", [&id])
            .unwrap();
        assert_eq!(gone, 1, "seen record lived on the id's slice");
        let (store, _) = Store::open(cfg(&dir, n)).unwrap();
        assert!(matches!(
            store.ack(&id, to, "rec-1", 5).unwrap(),
            AckResult::Recorded { .. }
        ));
        assert!(store.receipt(&id, None).unwrap().is_some());
        conflict(
            store
                .send_directed(directed(&id, "s", to, "other"))
                .unwrap(),
        );
        assert!(matches!(
            store.send_directed(directed(&id, "s", to, "body")).unwrap(),
            Status::DuplicateAccepted { .. }
        ));
    }

    #[test]
    fn timeline_reloads_sessions_claims_and_takeovers_under_one_lock() {
        let dir = tempdir("timeline");
        {
            let (store, _) = Store::open(cfg(&dir, 2)).unwrap();
            assert!(Store::open(cfg(&dir, 2)).is_err(), "second open is refused");
            store
                .commit_timeline(
                    vec![
                        Event::Session {
                            session: "a".into(),
                            host: "h1".into(),
                            at_ms: Some(1),
                        },
                        Event::Label {
                            session: "a".into(),
                            label: "L".into(),
                            at_ms: Some(2),
                        },
                        Event::Declared {
                            session: "a".into(),
                            declared: Some("d".into()),
                            at_ms: Some(3),
                        },
                        Event::Session {
                            session: "b".into(),
                            host: "h1".into(),
                            at_ms: Some(4),
                        },
                        Event::Unregister {
                            session: "b".into(),
                            at_ms: Some(5),
                        },
                        Event::Claim {
                            key: "k".into(),
                            owner: "a".into(),
                            expires_ms: 1_000,
                        },
                        Event::Claim {
                            key: "k2".into(),
                            owner: "a".into(),
                            expires_ms: 1_000,
                        },
                        Event::Release { key: "k2".into() },
                        Event::Takeover {
                            session: "a".into(),
                            old_host: "h1".into(),
                            host: "h2".into(),
                            at_ms: Some(6),
                            registration: Some("reg-1".into()),
                        },
                    ],
                    7,
                )
                .unwrap();
        }
        let (_store, st) = Store::open(cfg(&dir, 2)).unwrap();
        let a = &st.sessions["a"];
        assert_eq!(
            (a.host.as_str(), a.label.as_str(), a.declared.as_deref()),
            ("h1", "L", Some("d"))
        );
        assert!(!st.sessions.contains_key("b"));
        assert_eq!(st.claims.live("k", 0).unwrap().owner, "a");
        assert!(st.claims.live("k2", 0).is_none());
        assert!(st.takeover_registrations.contains("reg-1"));
    }

    #[test]
    fn articles_stateless_reads_without_creating_stream_or_positions_and_restores_cold() {
        let dir = tempdir("articles");
        let mut c = cfg(&dir, 2);
        c.retain_ms = 1_000;
        c.day_ms = 10_000;
        c.sweep_enabled = false;
        let (store, _) = Store::open(c).unwrap();
        let idx = store.inner.shard("g:card.1");

        let empty = store.articles("card.1", 0, 10).unwrap();
        assert_eq!(
            (empty.high_water, empty.retired, empty.articles.len()),
            (0, false, 0)
        );
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM streams"), 0);
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM positions"), 0);

        let mut m1 = group("m1", "s", "card.1", "first");
        m1.accepted_at_ms = Some(100);
        let mut m2 = group("m2", "s", "card.1", "second");
        m2.accepted_at_ms = Some(2_000);
        let mut m3 = group("m3", "s", "card.1", "third");
        m3.accepted_at_ms = Some(2_500);
        assert_eq!(committed(store.send_group(m1).unwrap()), 1);
        assert_eq!(committed(store.send_group(m2).unwrap()), 2);
        assert_eq!(committed(store.send_group(m3).unwrap()), 3);

        store.inner.sweep(2_600).unwrap();
        assert_eq!(
            count(
                &store,
                idx,
                "SELECT COUNT(*) FROM envelopes WHERE in_cold = 1"
            ),
            1
        );

        let page1 = store.articles("card.1", 0, 2).unwrap();
        assert_eq!(page1.high_water, 3);
        assert_eq!(page1.articles.len(), 2);
        assert_eq!(page1.articles[0].article_num, 1);
        assert_eq!(page1.articles[0].body, "first");
        assert_eq!(page1.articles[1].article_num, 2);
        assert_eq!(page1.articles[1].body, "second");

        let page2 = store.articles("card.1", 2, 10).unwrap();
        assert_eq!(page2.articles.len(), 1);
        assert_eq!(page2.articles[0].article_num, 3);
        assert_eq!(page2.articles[0].body, "third");
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM positions"), 0);
        assert_eq!(count(&store, idx, "SELECT COUNT(*) FROM receipts"), 0);
    }

    fn cold_files(dir: &std::path::Path, ext: &str, out: &mut Vec<std::path::PathBuf>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                cold_files(&p, ext, out);
            } else if p.extension().and_then(|x| x.to_str()) == Some(ext)
                && p.file_name().unwrap().to_string_lossy().starts_with("day-")
            {
                out.push(p);
            }
        }
    }

    fn batch_cfg(dir: &TempDir) -> Config {
        let mut c = cfg(dir, 1);
        c.retain_ms = 1_000;
        c.day_ms = 1_000;
        c.sweep_enabled = false;
        c
    }

    #[test]
    fn pending_restores_cold_day_with_one_file_open_and_reopen_writes_sidecar() {
        let dir = tempdir("batch-pending");
        let (store, _) = Store::open(batch_cfg(&dir)).unwrap();
        for i in 0..5 {
            let mut m = directed(&format!("d{i}"), "s", "r", &format!("body {i}"));
            m.accepted_at_ms = Some(100 + i * 100);
            store.send_directed(m).unwrap();
        }
        store.inner.sweep(10_000).unwrap();
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM envelopes WHERE in_cold = 1"
            ),
            5
        );
        let before = store.inner.slices[0].cold.file_opens();
        let got = store.pending_session("r").unwrap();
        let bodies: Vec<&str> = got.iter().map(|d| d.body.as_str()).collect();
        assert_eq!(bodies, ["body 0", "body 1", "body 2", "body 3", "body 4"]);
        assert_eq!(store.inner.slices[0].cold.file_opens() - before, 1);
        drop(store);

        let mut idx_files = Vec::new();
        cold_files(&dir.0, "idx", &mut idx_files);
        for p in &idx_files {
            fs::remove_file(p).unwrap();
        }
        let mut days = Vec::new();
        cold_files(&dir.0, "cold", &mut days);
        assert!(!days.is_empty(), "expected a cold day file, saw none");
        let (store, _) = Store::open(batch_cfg(&dir)).unwrap();
        let mut idx_files = Vec::new();
        cold_files(&dir.0, "idx", &mut idx_files);
        let names: Vec<String> = idx_files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n == "day-0.idx"), "{names:?}");
        assert_eq!(store.pending_session("r").unwrap().len(), 5);
    }

    #[test]
    fn articles_restore_cold_day_with_one_file_open() {
        let dir = tempdir("batch-articles");
        let (store, _) = Store::open(batch_cfg(&dir)).unwrap();
        for i in 0..4 {
            let mut m = group(&format!("a{i}"), "s", "card.2", &format!("art {i}"));
            m.accepted_at_ms = Some(100 + i * 100);
            store.send_group(m).unwrap();
        }
        store.inner.sweep(10_000).unwrap();
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM envelopes WHERE in_cold = 1"
            ),
            4
        );
        let before = store.inner.slices[0].cold.file_opens();
        let page = store.articles("card.2", 0, 10).unwrap();
        let bodies: Vec<&str> = page.articles.iter().map(|a| a.body.as_str()).collect();
        assert_eq!(bodies, ["art 0", "art 1", "art 2", "art 3"]);
        assert_eq!(store.inner.slices[0].cold.file_opens() - before, 1);
    }

    #[test]
    fn expected_article_cas_fences_group_head_and_preserves_idempotent_resend() {
        let dir = tempdir("cas");
        let (store, _) = Store::open(cfg(&dir, 1)).unwrap();
        let mut g1 = group("cas-1", "s", "tx.1", "a1");
        g1.expected_article = Some(0);
        assert_eq!(committed(store.send_group(g1).unwrap()), 1);

        let mut stale = group("cas-stale", "s", "tx.1", "nope");
        stale.expected_article = Some(0);
        let err = conflict(store.send_group(stale).unwrap());
        assert!(
            err.contains("expected group tx.1 at article 0, saw 1"),
            "{err}"
        );

        let mut g2 = group("cas-2", "s", "tx.1", "a2");
        g2.expected_article = Some(1);
        assert_eq!(committed(store.send_group(g2).unwrap()), 2);

        let mut resend = group("cas-1", "s", "tx.1", "a1");
        resend.expected_article = Some(0);
        match store.send_group(resend).unwrap() {
            Status::DuplicateAccepted { article_num } => assert_eq!(article_num, 1),
            other => panic!("expected DuplicateAccepted, saw {other:?}"),
        }
    }

    #[test]
    fn group_policy_keep_last_ttl_retire_and_prefix_preserve_monotonic_numbers() {
        let dir = tempdir("policy");
        let mut c = cfg(&dir, 1);
        c.retain_ms = 100_000;
        c.sweep_enabled = false;
        let (store, _) = Store::open(c).unwrap();

        for i in 1..=5 {
            let mut gm = group(&format!("p-{i}"), "s", "card.a", &format!("b-{i}"));
            gm.accepted_at_ms = Some(i * 100);
            assert_eq!(committed(store.send_group(gm).unwrap()), i);
        }
        let pol = store
            .group_policy("card.a", Some(Some(2)), Some(Some(500)), Some(false), 1_000)
            .unwrap();
        assert_eq!(
            (pol.high_water, pol.keep_last, pol.ttl_ms, pol.retired),
            (5, Some(2), Some(500), false)
        );
        let page = store.articles("card.a", 0, 10).unwrap();
        let nums: Vec<i64> = page.articles.iter().map(|a| a.article_num).collect();
        assert_eq!(nums, [4, 5]);

        assert_eq!(
            committed(
                store
                    .send_group(group("p-6", "s", "card.a", "b-6"))
                    .unwrap()
            ),
            6
        );
        let nums: Vec<i64> = store
            .articles("card.a", 0, 10)
            .unwrap()
            .articles
            .iter()
            .map(|a| a.article_num)
            .collect();
        assert_eq!(nums, [5, 6]);
        // seen_ids stays intact for pruned article p-1
        match store
            .send_group(group("p-1", "s", "card.a", "b-1"))
            .unwrap()
        {
            Status::DuplicateAccepted { article_num } => assert_eq!(article_num, 1),
            other => panic!("expected DuplicateAccepted, saw {other:?}"),
        }

        let mut tx1 = group("tx-1", "s", "tx.d1", "go");
        tx1.accepted_at_ms = Some(1_900);
        committed(store.send_group(tx1).unwrap());
        store
            .group_policy("card.a", None, None, Some(true), 2_000)
            .unwrap();
        let active: Vec<String> = store
            .groups(None, None, false)
            .unwrap()
            .into_iter()
            .map(|g| g.group)
            .collect();
        assert_eq!(active, ["tx.d1"]);
        let by_prefix: Vec<String> = store
            .groups(None, Some("card."), true)
            .unwrap()
            .into_iter()
            .map(|g| g.group)
            .collect();
        assert_eq!(by_prefix, ["card.a"]);

        // Per-group ttl_ms sweeps earlier than global retain_ms.
        let mut tx2 = group("tx-2", "s", "tx.d1", "done");
        tx2.accepted_at_ms = Some(2_000);
        committed(store.send_group(tx2).unwrap());
        store
            .group_policy("tx.d1", None, Some(Some(100)), None, 2_000)
            .unwrap();
        store.inner.sweep(2_200).unwrap();
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM envelopes WHERE stream = 'g:tx.d1' AND in_cold = 1"
            ),
            2
        );
    }

    fn reset_reads(store: &Store) {
        for s in &store.inner.slices {
            s.reads.store(0, Ordering::SeqCst);
        }
    }

    fn slice_reads(store: &Store) -> Vec<usize> {
        store
            .inner
            .slices
            .iter()
            .map(|s| s.reads.load(Ordering::SeqCst))
            .collect()
    }

    #[test]
    fn multi_slice_ack_receipt_and_pending_avoid_linear_slice_scans() {
        let dir = tempdir("o1-slices");
        let mut c = cfg(&dir, 4);
        c.sweep_enabled = false;
        let (store, _) = Store::open(c).unwrap();
        let rcpt = "rcpt-o1";
        let m_idx = store.inner.shard(&format!("m:{rcpt}"));
        let mail_id = (0..100)
            .map(|i| format!("mail-o1-{i}"))
            .find(|id| store.inner.shard(id) != m_idx)
            .unwrap();
        let id_idx = store.inner.shard(&mail_id);
        let grp = (0..100)
            .map(|i| format!("grp-o1-{i}"))
            .find(|g| {
                let gi = store.inner.shard(&format!("g:{g}"));
                gi != m_idx && gi != id_idx
            })
            .unwrap();
        let g_idx = store.inner.shard(&format!("g:{grp}"));
        let unused_idx = (0..4)
            .find(|&i| i != m_idx && i != id_idx && i != g_idx)
            .unwrap();

        committed(
            store
                .send_directed(directed(&mail_id, "sender", rcpt, "hello"))
                .unwrap(),
        );

        // Unsubscribed session: pending_session executes zero group queries.
        reset_reads(&store);
        let pend = store.pending_session(rcpt).unwrap();
        assert_eq!(pend.len(), 1);
        let reads = slice_reads(&store);
        assert_eq!(reads[m_idx], 1);
        for (i, &r) in reads.iter().enumerate() {
            if i != m_idx {
                assert_eq!(
                    r, 0,
                    "slice {i} should not be queried for unsubscribed session"
                );
            }
        }

        // receipt and ack resolve via seen_ids.stream in O(1) without touching other slices.
        reset_reads(&store);
        let rec = store.receipt(&mail_id, None).unwrap().unwrap();
        assert_eq!(rec.record_id, None);
        let reads = slice_reads(&store);
        assert_eq!(reads[id_idx], 1);
        assert_eq!(reads[m_idx], 2);
        assert_eq!(reads[g_idx], 0);
        assert_eq!(reads[unused_idx], 0);

        reset_reads(&store);
        assert_eq!(
            store.ack(&mail_id, rcpt, "r-1", 10).unwrap(),
            AckResult::Recorded {
                newly_receipted: true
            }
        );
        let reads = slice_reads(&store);
        assert_eq!(reads[id_idx], 1);
        assert_eq!(reads[m_idx], 1);
        assert_eq!(reads[g_idx], 0);
        assert_eq!(reads[unused_idx], 0);

        // Subscribe to a group on g_idx: pending_session queries only m_idx and g_idx.
        store.subscribe(rcpt, &grp, None, false, 11).unwrap();
        committed(
            store
                .send_group(group("gpost-1", "sender", &grp, "post"))
                .unwrap(),
        );
        reset_reads(&store);
        let pend = store.pending_session(rcpt).unwrap();
        assert_eq!(pend.len(), 1);
        assert_eq!(pend[0].mail_id, "gpost-1");
        let reads = slice_reads(&store);
        assert_eq!(reads[m_idx], 1);
        assert_eq!(reads[g_idx], 1);
        assert_eq!(reads[id_idx], 0);
        assert_eq!(reads[unused_idx], 0);

        // Reload Store::open: subscribed_groups and seen_ids.stream survive restart.
        drop(store);
        let mut c = cfg(&dir, 4);
        c.sweep_enabled = false;
        let (store, _) = Store::open(c).unwrap();
        reset_reads(&store);
        assert_eq!(store.pending_session(rcpt).unwrap().len(), 1);
        let reads = slice_reads(&store);
        assert_eq!(reads[m_idx], 1);
        assert_eq!(reads[g_idx], 1);
        assert_eq!(reads[id_idx], 0);
        assert_eq!(reads[unused_idx], 0);

        reset_reads(&store);
        let rec = store.receipt(&mail_id, None).unwrap().unwrap();
        assert_eq!(rec.record_id.as_deref(), Some("r-1"));
        let reads = slice_reads(&store);
        assert_eq!(reads[id_idx], 1);
        assert_eq!(reads[m_idx], 2);
        assert_eq!(reads[g_idx], 0);
        assert_eq!(reads[unused_idx], 0);

        // Unsubscribe removes the shard from subscribed_groups.
        store.unsubscribe(rcpt, &grp, 12).unwrap();
        reset_reads(&store);
        assert!(store.pending_session(rcpt).unwrap().is_empty());
        let reads = slice_reads(&store);
        assert_eq!(reads[m_idx], 1);
        assert_eq!(reads[g_idx], 0);

        // Legacy row with seen_ids.stream = NULL falls back to scanning slices.
        let mid = mail_id.clone();
        store.inner.slices[id_idx]
            .writer
            .exec(move |c| {
                c.execute(
                    "UPDATE seen_ids SET stream = NULL WHERE mail_id = ?1",
                    [&mid],
                )?;
                Ok(())
            })
            .unwrap();
        let rec = store.receipt(&mail_id, None).unwrap().unwrap();
        assert_eq!(rec.record_id.as_deref(), Some("r-1"));
        assert_eq!(
            store.ack(&mail_id, rcpt, "r-1", 13).unwrap(),
            AckResult::Recorded {
                newly_receipted: false
            }
        );
        // Unknown mail_id returns None without scanning all slices.
        reset_reads(&store);
        assert!(store.receipt("never-existed", None).unwrap().is_none());
        assert_eq!(slice_reads(&store).iter().sum::<usize>(), 1);

        // Pre-migration database without seen_ids.stream upgrades idempotently on Store::open.
        drop(store);
        let legacy_dir = tempdir("o1-legacy-schema");
        let root = coord_root(&legacy_dir.0.join("coordinator.db"));
        let s0 = shard_dir(&root, 0);
        fs::create_dir_all(s0.join("cold")).unwrap();
        {
            let c = Connection::open(s0.join("hot.sqlite")).unwrap();
            c.execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, val TEXT NOT NULL);
                 CREATE TABLE live_sessions (address TEXT PRIMARY KEY, host TEXT NOT NULL, label TEXT NOT NULL DEFAULT '', declared TEXT, at_ms INTEGER);
                 CREATE TABLE live_claims (claim_key TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_ms INTEGER NOT NULL);
                 CREATE TABLE timeline (seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, addr TEXT, host TEXT, old_host TEXT, label TEXT, declared TEXT, registration TEXT, claim_key TEXT, expires_ms INTEGER, mail_id TEXT, at_ms INTEGER, raw_line INTEGER);
                 CREATE TABLE streams (stream TEXT PRIMARY KEY, kind TEXT NOT NULL, next_article INTEGER NOT NULL);
                 CREATE TABLE seen_ids (mail_id TEXT PRIMARY KEY, kind TEXT NOT NULL, sender TEXT NOT NULL, target TEXT, body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL, article_num INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE broadcasts (mail_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, sender TEXT NOT NULL, body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL, headers_json TEXT, recipients_json TEXT NOT NULL, accepted_at_ms INTEGER, raw_line INTEGER);
                 CREATE TABLE envelopes (seq INTEGER PRIMARY KEY, mail_id TEXT NOT NULL UNIQUE, stream TEXT NOT NULL, article_num INTEGER NOT NULL, kind TEXT NOT NULL, sender TEXT NOT NULL, recipient TEXT NOT NULL, headers_json TEXT, body_len INTEGER NOT NULL, body_sha256 TEXT NOT NULL, accepted_at_ms INTEGER, day_bucket INTEGER NOT NULL, in_cold INTEGER NOT NULL DEFAULT 0, raw_line INTEGER, UNIQUE(stream, article_num));
                 CREATE TABLE hot_bodies (mail_id TEXT PRIMARY KEY, body TEXT NOT NULL);
                 CREATE TABLE positions (stream TEXT NOT NULL, reader TEXT NOT NULL, subscribed INTEGER NOT NULL DEFAULT 1, position INTEGER NOT NULL DEFAULT 0, record_id TEXT, receipted_at_ms INTEGER, updated_at_ms INTEGER, PRIMARY KEY(stream, reader));
                 CREATE TABLE receipts (mail_id TEXT NOT NULL, reader TEXT NOT NULL, stream TEXT NOT NULL, article_num INTEGER NOT NULL, record_id TEXT NOT NULL, receipted_at_ms INTEGER, receipt_seq INTEGER NOT NULL, raw_line INTEGER, PRIMARY KEY(mail_id, reader));
                 INSERT INTO seen_ids (mail_id, kind, sender, target, body_len, body_sha256, article_num) VALUES ('leg-1', 'directed', 'a', 'b', 2, 'xx', 1);
                 INSERT INTO envelopes (seq, mail_id, stream, article_num, kind, sender, recipient, body_len, body_sha256, day_bucket) VALUES (1, 'leg-1', 'm:b', 1, 'directed', 'a', 'b', 2, 'xx', 0);
                 INSERT INTO hot_bodies (mail_id, body) VALUES ('leg-1', 'hi');",
            )
            .unwrap();
        }
        let (leg_store, _) = Store::open(cfg(&legacy_dir, 1)).unwrap();
        assert!(leg_store.receipt("leg-1", None).unwrap().is_some());
    }

    #[test]
    fn pending_session_skips_acked_prefix_and_keeps_out_of_order_gap() {
        let dir = tempdir("pending-prefix");
        let (store, _) = Store::open(cfg(&dir, 2)).unwrap();
        for i in 1..=6 {
            committed(
                store
                    .send_directed(directed(&format!("p{i}"), "s", "r", "b"))
                    .unwrap(),
            );
        }
        for i in 1..=4 {
            store.ack(&format!("p{i}"), "r", "rec", 10).unwrap();
        }
        let ids = |s: &Store| -> Vec<String> {
            s.pending_session("r")
                .unwrap()
                .into_iter()
                .map(|d| d.mail_id)
                .collect()
        };
        assert_eq!(ids(&store), ["p5", "p6"]);
        store.ack("p6", "r", "rec", 11).unwrap();
        assert_eq!(ids(&store), ["p5"]);
    }

    #[test]
    fn single_slice_fast_path_skips_preread_and_catches_pruned_and_cross_kind_conflicts() {
        let dir = tempdir("fast-1slice");
        let (store, _) = Store::open(cfg(&dir, 1)).unwrap();
        store
            .group_policy("G", Some(Some(1)), None, None, 1)
            .unwrap();

        reset_reads(&store);
        assert_eq!(
            committed(store.send_group(group("g1", "s", "G", "one")).unwrap()),
            1
        );
        assert_eq!(
            committed(store.send_group(group("g2", "s", "G", "two")).unwrap()),
            2
        );
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM envelopes WHERE mail_id = 'g1'"
            ),
            1,
            "g1's envelope stays after keep_last=1 prunes it"
        );
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM hot_bodies WHERE mail_id = 'g1'"
            ),
            0,
            "g1's hot body was dropped by keep_last=1"
        );
        // Reset after count() helper; verify send_single itself performs zero read-conn queries.
        reset_reads(&store);
        assert!(matches!(
            store.send_group(group("g1", "s", "G", "one")).unwrap(),
            Status::DuplicateAccepted { article_num: 1 }
        ));
        assert_eq!(
            conflict(store.send_group(group("g1", "s", "G", "changed")).unwrap()),
            "mail g1: expected a resend to repeat the Mail already accepted under this id, saw a different body"
        );
        assert!(
            conflict(store.send_group(group("g1", "other", "G", "one")).unwrap())
                .ends_with("different sender")
        );
        assert!(conflict(
            store
                .send_directed(directed("g1", "s", "G", "one"))
                .unwrap()
        )
        .ends_with("different recipient"));
        assert_eq!(
            slice_reads(&store)[0],
            0,
            "1-slice send_single must not perform separate inner.seen pre-reads"
        );

        // Cross-kind conflict against broadcast parent id and directed->group id on 1-slice.
        committed(
            store
                .send_broadcast(bcast("b1", "s", &["a", "b"], "all"))
                .unwrap(),
        );
        reset_reads(&store);
        assert!(conflict(
            store
                .send_directed(directed("b1", "s", "a", "all"))
                .unwrap()
        )
        .ends_with("different recipient"));
        assert!(
            conflict(store.send_group(group("b1", "s", "G", "all")).unwrap())
                .ends_with("different recipient")
        );
        committed(store.send_directed(directed("d1", "s", "G", "hi")).unwrap());
        assert!(
            conflict(store.send_group(group("d1", "s", "G", "hi")).unwrap())
                .ends_with("different recipient")
        );
        assert_eq!(slice_reads(&store)[0], 0);
    }

    #[test]
    fn keep_last_prune_keeps_envelope_receipt_metadata_and_ack_records() {
        let dir = tempdir("prune-ack");
        let (store, _) = Store::open(cfg(&dir, 2)).unwrap();
        let idx = store.inner.shard("g:K");
        store.subscribe("r", "K", None, false, 1).unwrap();
        for i in 1..=3 {
            let id = format!("k{i}");
            assert_eq!(
                committed(store.send_group(group(&id, "s", "K", "b")).unwrap()),
                i
            );
        }
        assert_eq!(store.pending_session("r").unwrap().len(), 3);
        store
            .group_policy("K", Some(Some(1)), None, None, 2)
            .unwrap();
        assert_eq!(
            count(
                &store,
                idx,
                "SELECT COUNT(*) FROM envelopes WHERE stream = 'g:K'"
            ),
            3
        );
        assert_eq!(
            count(
                &store,
                idx,
                "SELECT pruned_through FROM streams WHERE stream = 'g:K'"
            ),
            2
        );
        assert_eq!(
            count(&store, idx, "SELECT COUNT(*) FROM hot_bodies"),
            1,
            "only k3's hot body remains"
        );
        assert_eq!(
            store.ack("k1", "r", "rec-1", 3).unwrap(),
            AckResult::Recorded {
                newly_receipted: true
            }
        );
        let rc = store
            .receipt("k1", Some("r"))
            .unwrap()
            .expect("receipt for k1");
        assert_eq!(rc.article_num, Some(1));
        assert_eq!(rc.record_id.as_deref(), Some("rec-1"));
        assert!(store.receipt("k2", None).unwrap().is_some());
        let left: Vec<i64> = store
            .pending_session("r")
            .unwrap()
            .iter()
            .filter_map(|d| d.article_num)
            .collect();
        assert_eq!(left, [3]);
    }

    #[test]
    fn articles_skip_pruned_across_hot_and_cold_and_floor_survives_reopen() {
        let dir = tempdir("prune-cold");
        let mk = || {
            let mut c = cfg(&dir, 1);
            c.retain_ms = 500;
            c.day_ms = 10_000;
            c.sweep_enabled = false;
            c
        };
        let (store, _) = Store::open(mk()).unwrap();
        for (i, at) in [(1, 100), (2, 200), (3, 300), (4, 5_000), (5, 5_100)] {
            let mut gm = group(&format!("c{i}"), "s", "C", &format!("b{i}"));
            gm.accepted_at_ms = Some(at);
            assert_eq!(committed(store.send_group(gm).unwrap()), i);
        }
        store.inner.sweep(1_000).unwrap();
        assert_eq!(
            count(
                &store,
                0,
                "SELECT COUNT(*) FROM envelopes WHERE in_cold = 1"
            ),
            3
        );
        store
            .group_policy("C", Some(Some(3)), None, None, 6_000)
            .unwrap();
        let nums = |s: &Store, after: i64, limit: usize| -> Vec<(i64, String)> {
            s.articles("C", after, limit)
                .unwrap()
                .articles
                .into_iter()
                .map(|a| (a.article_num, a.body))
                .collect()
        };
        let want = vec![(3, "b3".to_string()), (4, "b4".into()), (5, "b5".into())];
        assert_eq!(nums(&store, 0, 10), want);
        assert_eq!(nums(&store, 0, 1), want[..1]);
        assert_eq!(nums(&store, 3, 10), want[1..]);
        drop(store);
        let (store, _) = Store::open(mk()).unwrap();
        assert_eq!(
            count(
                &store,
                0,
                "SELECT pruned_through FROM streams WHERE stream = 'g:C'"
            ),
            2
        );
        assert_eq!(nums(&store, 0, 10), want);
    }

    #[test]
    fn reader_below_floor_gets_unpruned_and_advances_past_gap() {
        let dir = tempdir("prune-gap");
        let (store, _) = Store::open(cfg(&dir, 1)).unwrap();
        store.subscribe("r", "Q", None, false, 1).unwrap();
        for i in 1..=5 {
            committed(
                store
                    .send_group(group(&format!("q{i}"), "s", "Q", "b"))
                    .unwrap(),
            );
        }
        store
            .group_policy("Q", Some(Some(2)), None, None, 2)
            .unwrap();
        let pend: Vec<i64> = store
            .pending_session("r")
            .unwrap()
            .iter()
            .filter_map(|d| d.article_num)
            .collect();
        assert_eq!(pend, [4, 5]);
        let pos = |s: &Store| {
            count(
                s,
                0,
                "SELECT position FROM positions WHERE stream = 'g:Q' AND reader = 'r'",
            )
        };
        assert_eq!(pos(&store), 0);
        store.ack("q4", "r", "rec-4", 3).unwrap();
        assert_eq!(pos(&store), 4);
        assert_eq!(
            count(
                &store,
                0,
                "SELECT record_id = 'rec-4' FROM positions WHERE stream = 'g:Q' AND reader = 'r'",
            ),
            1
        );
        store.ack("q5", "r", "rec-5", 4).unwrap();
        assert_eq!(pos(&store), 5);
        assert!(store.pending_session("r").unwrap().is_empty());
    }
}

//! Isolated SQLite storage layer (#I6): all SQL statements and `rusqlite` calls
//! in the core binary live inside `src/sql/`.
//!
//! Submodules own disjoint concerns:
//! - `coord`: coordinator slice database(s), WAL FULL group commit, hot/cold sweep, positions
//! - `daemon`: host daemon small SQLite database for pending deliveries and unconfirmed receipts
//! - `import`: read-only import of pre-cutover JSONL `coordinator.log` and `daemon.log`
//! - `audit_ro`: concurrent read-only snapshot queries across hot slice DBs and cold day files

pub mod audit_ro;
pub mod coord;
pub mod daemon;
pub mod import;

use crate::claims::Claims;
use crate::json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub const DAY_MS: i64 = 86_400_000;
pub const DEFAULT_RETAIN_DAYS: i64 = 30;
pub const DEFAULT_RETAIN_MS: i64 = DEFAULT_RETAIN_DAYS * DAY_MS;

/// Resolve the coordinator slice root directory from `--data <path>`.
/// Never overwrites `<path>` itself so a pre-cutover JSONL `coordinator.log`
/// at `<path>` remains intact on disk.
pub fn coord_root(data: &Path) -> PathBuf {
    if data.is_dir() {
        if data.join("slice-00").exists()
            || data.extension().and_then(|s| s.to_str()) == Some("slices")
        {
            return data.to_path_buf();
        }
        if data.join("hot.sqlite").exists() {
            return data.to_path_buf();
        }
        return data.join("slices");
    }
    let mut os = data.as_os_str().to_owned();
    os.push(".slices");
    PathBuf::from(os)
}

pub fn shard_dir(root: &Path, idx: usize) -> PathBuf {
    if root.join("hot.sqlite").exists() && idx == 0 {
        return root.to_path_buf();
    }
    root.join(format!("slice-{idx:02}"))
}

pub fn daemon_db_path(data: &Path) -> PathBuf {
    if matches!(
        data.extension().and_then(|e| e.to_str()),
        Some("sqlite" | "db")
    ) {
        return data.to_path_buf();
    }
    let mut os = data.as_os_str().to_owned();
    os.push(".sqlite");
    PathBuf::from(os)
}

pub fn daemon_lock_path(data: &Path) -> PathBuf {
    let mut os = data.as_os_str().to_owned();
    os.push(".lock");
    PathBuf::from(os)
}

pub fn is_sqlite_file(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 16];
    f.read_exact(&mut magic).is_ok() && &magic == b"SQLite format 3\0"
}

#[derive(Clone, Debug, Default)]
pub struct StoredSession {
    pub host: String,
    pub label: String,
    pub declared: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Event {
    Session {
        session: String,
        host: String,
        at_ms: Option<i64>,
    },
    Unregister {
        session: String,
        at_ms: Option<i64>,
    },
    Label {
        session: String,
        label: String,
        at_ms: Option<i64>,
    },
    Declared {
        session: String,
        declared: Option<String>,
        at_ms: Option<i64>,
    },
    Takeover {
        session: String,
        old_host: String,
        host: String,
        at_ms: Option<i64>,
        registration: Option<String>,
    },
    Claim {
        key: String,
        owner: String,
        expires_ms: i64,
    },
    Release {
        key: String,
    },
    StrayReceipt {
        mail_id: String,
        session: Option<String>,
        record_id: String,
    },
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub seq: i64,
    pub mail_id: String,
    pub from: String,
    pub to: String,
    pub group: Option<String>,
    pub article_num: Option<i64>,
    pub body: String,
    pub kind: String,
    pub headers: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckResult {
    Recorded { newly_receipted: bool },
    UnknownMail,
}

#[derive(Clone, Debug)]
pub struct Receipt {
    pub mail_id: String,
    pub from: String,
    pub to: String,
    pub group: Option<String>,
    pub article_num: Option<i64>,
    pub kind: String,
    pub headers: Option<Value>,
    pub accepted_at_ms: Option<i64>,
    pub record_id: Option<String>,
    pub receipted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Group {
    pub group: String,
    pub high_water: i64,
    pub subscribers: usize,
    pub position: Option<i64>,
    pub subscribed: bool,
    pub record_id: Option<String>,
    pub retired: bool,
    pub keep_last: Option<i64>,
    pub ttl_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Article {
    pub article_num: i64,
    pub mail_id: String,
    pub from: String,
    pub body: String,
    pub headers: Value,
    pub accepted_at_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct ArticlesPage {
    pub group: String,
    pub high_water: i64,
    pub retired: bool,
    pub articles: Vec<Article>,
}
/// Shared schema DDL for each coordinator slice `hot.sqlite` so `coord`,
/// `import`, and `audit_ro` agree on exact table and column names.
pub const COORD_SLICE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    val TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS live_sessions (
    address TEXT PRIMARY KEY,
    host TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    declared TEXT,
    at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS live_claims (
    claim_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS timeline (
    seq INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    addr TEXT,
    host TEXT,
    old_host TEXT,
    label TEXT,
    declared TEXT,
    registration TEXT,
    claim_key TEXT,
    expires_ms INTEGER,
    mail_id TEXT,
    at_ms INTEGER,
    raw_line INTEGER
);
CREATE TABLE IF NOT EXISTS streams (
    stream TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    next_article INTEGER NOT NULL,
    keep_last INTEGER,
    ttl_ms INTEGER,
    retired_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS seen_ids (
    mail_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    sender TEXT NOT NULL,
    target TEXT,
    body_len INTEGER NOT NULL,
    body_sha256 TEXT NOT NULL,
    article_num INTEGER NOT NULL DEFAULT 0,
    stream TEXT
);
CREATE TABLE IF NOT EXISTS broadcasts (
    mail_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    sender TEXT NOT NULL,
    body_len INTEGER NOT NULL,
    body_sha256 TEXT NOT NULL,
    headers_json TEXT,
    recipients_json TEXT NOT NULL,
    accepted_at_ms INTEGER,
    raw_line INTEGER
);
CREATE TABLE IF NOT EXISTS envelopes (
    seq INTEGER PRIMARY KEY,
    mail_id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    article_num INTEGER NOT NULL,
    kind TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    headers_json TEXT,
    body_len INTEGER NOT NULL,
    body_sha256 TEXT NOT NULL,
    accepted_at_ms INTEGER,
    day_bucket INTEGER NOT NULL,
    in_cold INTEGER NOT NULL DEFAULT 0,
    raw_line INTEGER,
    UNIQUE(stream, article_num)
);
CREATE INDEX IF NOT EXISTS idx_envelopes_stream_art ON envelopes(stream, article_num);
CREATE INDEX IF NOT EXISTS idx_envelopes_cold_day ON envelopes(in_cold, day_bucket);
CREATE INDEX IF NOT EXISTS idx_envelopes_sender ON envelopes(sender);
CREATE INDEX IF NOT EXISTS idx_envelopes_recipient ON envelopes(recipient);
CREATE TABLE IF NOT EXISTS hot_bodies (
    mail_id TEXT PRIMARY KEY,
    body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions (
    stream TEXT NOT NULL,
    reader TEXT NOT NULL,
    subscribed INTEGER NOT NULL DEFAULT 1,
    position INTEGER NOT NULL DEFAULT 0,
    record_id TEXT,
    receipted_at_ms INTEGER,
    updated_at_ms INTEGER,
    PRIMARY KEY(stream, reader)
);
CREATE INDEX IF NOT EXISTS idx_positions_reader ON positions(reader, subscribed);
CREATE TABLE IF NOT EXISTS receipts (
    mail_id TEXT NOT NULL,
    reader TEXT NOT NULL,
    stream TEXT NOT NULL,
    article_num INTEGER NOT NULL,
    record_id TEXT NOT NULL,
    receipted_at_ms INTEGER,
    receipt_seq INTEGER NOT NULL,
    raw_line INTEGER,
    PRIMARY KEY(mail_id, reader)
);
CREATE INDEX IF NOT EXISTS idx_receipts_reader ON receipts(reader);
";

/// Shared schema DDL for the host daemon's small `<data>.sqlite` database.
pub const DAEMON_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    val TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_deliveries (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    mail_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    recipient TEXT NOT NULL,
    group_name TEXT,
    article_num INTEGER,
    body TEXT NOT NULL,
    kind TEXT NOT NULL,
    headers_json TEXT NOT NULL,
    UNIQUE(mail_id, recipient)
);
CREATE INDEX IF NOT EXISTS idx_pending_recipient ON pending_deliveries(recipient, seq);
CREATE TABLE IF NOT EXISTS unconfirmed_receipts (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    mail_id TEXT NOT NULL,
    session TEXT NOT NULL,
    record_id TEXT NOT NULL,
    UNIQUE(mail_id, session)
);
";

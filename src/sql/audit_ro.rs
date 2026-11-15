use super::*;
use std::io;
use std::path::Path;

#[derive(Clone, Debug)]
pub struct AuditTimelineRow {
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
pub struct AuditReceipt {
    pub mail_id: String,
    pub reader: String,
    pub record_id: String,
    pub receipted_at_ms: Option<i64>,
    pub receipt_line: i64,
}

#[derive(Clone, Debug)]
pub struct AuditEnvelope {
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
    pub receipts: Vec<AuditReceipt>,
}

#[derive(Clone, Debug)]
pub enum AuditCoordItem {
    Timeline(AuditTimelineRow),
    Envelope(AuditEnvelope),
}

#[derive(Default)]
pub struct CoordAuditSnapshot {
    pub record_count: usize,
    pub items: Vec<AuditCoordItem>,
}

#[derive(Default)]
pub struct DaemonAuditSnapshot {
    pub record_count: usize,
    pub pending: Vec<(i64, DaemonDeliverRow)>,
    pub unconfirmed_receipts: Vec<(i64, DaemonReceiptRow)>,
}

pub fn has_coordinator_sqlite(data: &Path) -> bool {
    let root = coordinator_root(data);
    root.join("slice-00").join("hot.sqlite").exists()
        || root.join("hot.sqlite").exists()
        || is_sqlite_file(data)
}

pub fn has_daemon_sqlite(data: &Path) -> bool {
    let db = daemon_db_path(data);
    db.exists() && is_sqlite_file(&db)
}

pub fn read_coordinator_records(
    _data: &Path,
    _include_bodies: bool,
) -> io::Result<CoordAuditSnapshot> {
    Err(io::Error::other("audit_ro stub"))
}

pub fn read_daemon_records(_data: &Path) -> io::Result<DaemonAuditSnapshot> {
    Err(io::Error::other("audit_ro stub"))
}

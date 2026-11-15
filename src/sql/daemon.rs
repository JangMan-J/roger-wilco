use super::*;
use rusqlite::{params, Connection, OptionalExtension};
use std::io;
use std::sync::Mutex;

pub struct DaemonStore {
    conn: Mutex<Connection>,
    _lock: File,
}

fn err(e: rusqlite::Error) -> io::Error {
    io::Error::other(e)
}
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<DaemonDeliverRow> {
    let headers: String = r.get(8)?;
    Ok(DaemonDeliverRow {
        mail_id: r.get(0)?,
        from: r.get(1)?,
        to: r.get(2)?,
        group: r.get(3)?,
        article_num: r.get(4)?,
        body: r.get(5)?,
        kind: r.get(6)?,
        headers: crate::json::parse(&headers).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(io::Error::other(e)),
            )
        })?,
    })
}
impl DaemonStore {
    pub fn open(data: &Path) -> io::Result<(Self, DaemonStartup)> {
        if let Some(p) = data.parent() {
            std::fs::create_dir_all(p)?;
        }
        let lock = crate::durlog::open_exclusive_lock(&daemon_lock_path(data))?;
        let mut conn = Connection::open(daemon_db_path(data)).map_err(err)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(err)?;
        conn.execute_batch(DAEMON_SCHEMA).map_err(err)?;
        let imported: Option<String> = conn
            .query_row(
                "SELECT val FROM meta WHERE key='imported_legacy'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(err)?;
        if data.is_file() && !is_sqlite_file(data) && imported.as_deref() != Some("1") {
            conn.execute_batch("BEGIN IMMEDIATE").map_err(err)?;
            super::import::maybe_import_daemon_log(data, &mut conn)?;
            conn.execute_batch(
                "INSERT OR REPLACE INTO meta VALUES ('imported_legacy','1'); COMMIT;",
            )
            .map_err(err)?;
        }
        let pending = conn.prepare("SELECT mail_id,sender,recipient,group_name,article_num,body,kind,seq,headers_json FROM pending_deliveries ORDER BY seq").map_err(err)?.query_map([], row).map_err(err)?.collect::<Result<Vec<_>,_>>().map_err(err)?;
        let receipts = conn
            .prepare("SELECT mail_id,session,record_id FROM unconfirmed_receipts ORDER BY seq")
            .map_err(err)?
            .query_map([], |r| {
                Ok(DaemonReceiptRow {
                    mail_id: r.get(0)?,
                    session: r.get(1)?,
                    record_id: r.get(2)?,
                })
            })
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        Ok((
            Self {
                conn: Mutex::new(conn),
                _lock: lock,
            },
            DaemonStartup { pending, receipts },
        ))
    }
    pub fn insert_deliver(&self, r: &DaemonDeliverRow) -> io::Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        conn.execute_batch("PRAGMA synchronous=FULL;")
            .map_err(err)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(err)?;
        let changed = tx.execute("INSERT OR IGNORE INTO pending_deliveries (mail_id,sender,recipient,group_name,article_num,body,kind,headers_json) SELECT ?1,?2,?3,?4,?5,?6,?7,?8 WHERE NOT EXISTS (SELECT 1 FROM unconfirmed_receipts WHERE mail_id=?1 AND session=?3)", params![r.mail_id,r.from,r.to,r.group,r.article_num,r.body,r.kind,crate::json::stringify(&r.headers)]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(changed != 0)
    }
    pub fn pending_for_session(&self, session: &str) -> io::Result<Vec<DaemonDeliverRow>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.prepare("SELECT mail_id,sender,recipient,group_name,article_num,body,kind,seq,headers_json FROM pending_deliveries WHERE recipient=?1 ORDER BY seq").map_err(err)?.query_map([session], row).map_err(err)?.collect::<Result<Vec<_>,_>>().map_err(err);
        result
    }
    pub fn settle_ack(&self, mail_id: &str, session: &str, record_id: &str) -> io::Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        conn.execute_batch("PRAGMA synchronous=FULL;")
            .map_err(err)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(err)?;
        tx.execute(
            "DELETE FROM pending_deliveries WHERE mail_id=?1 AND recipient=?2",
            params![mail_id, session],
        )
        .map_err(err)?;
        let n = tx.execute("INSERT OR IGNORE INTO unconfirmed_receipts (mail_id,session,record_id) VALUES (?1,?2,?3)", params![mail_id,session,record_id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(n != 0)
    }
    pub fn confirm_receipt(&self, mail_id: &str, session: Option<&str>) -> io::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(err)?;
        tx.execute(
            "DELETE FROM unconfirmed_receipts WHERE mail_id=?1 AND (?2 IS NULL OR session=?2)",
            params![mail_id, session],
        )
        .map_err(err)?;
        tx.execute(
            "DELETE FROM pending_deliveries WHERE mail_id=?1 AND (?2 IS NULL OR recipient=?2)",
            params![mail_id, session],
        )
        .map_err(err)?;
        tx.commit().map_err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_receipt_lifecycle() {
        let dir = std::env::temp_dir().join(crate::session::gen_uuid().unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("daemon");
        let r = DaemonDeliverRow {
            mail_id: "m".into(),
            from: "a".into(),
            to: "b".into(),
            group: Some("g".into()),
            article_num: Some(7),
            body: "text".into(),
            kind: "group".into(),
            headers: crate::json::obj(vec![]),
        };
        let (s, _) = DaemonStore::open(&path).unwrap();
        assert!(s.insert_deliver(&r).unwrap());
        assert!(!s.insert_deliver(&r).unwrap());
        drop(s);
        let (s, startup) = DaemonStore::open(&path).unwrap();
        assert_eq!(startup.pending.len(), 1);
        assert_eq!(startup.pending[0].article_num, Some(7));
        assert!(s.settle_ack("m", "b", "r").unwrap());
        assert!(!s.insert_deliver(&r).unwrap());
        drop(s);
        let (s, startup) = DaemonStore::open(&path).unwrap();
        assert!(startup.pending.is_empty());
        assert_eq!(startup.receipts.len(), 1);
        s.confirm_receipt("m", Some("b")).unwrap();
        drop(s);
        let (s, startup) = DaemonStore::open(&path).unwrap();
        assert!(startup.receipts.is_empty());
        drop(s);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

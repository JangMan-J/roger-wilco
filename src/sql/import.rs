use crate::claims::{Change, Claims};
use crate::json::{self, Value};
use crate::net;
use crate::sha256::{hash_pair, hex_digest};
use rusqlite::{Connection, OptionalExtension, Savepoint};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

#[warn(dead_code)]
fn sql_err(e: rusqlite::Error) -> io::Error {
    io::Error::other(e)
}

#[warn(dead_code)]
fn stamp_of(v: &Value, key: &str) -> Option<i64> {
    v.get(key).and_then(Value::as_i64)
}

#[warn(dead_code)]
fn shard_index(bytes: &[u8], num_slices: usize) -> usize {
    (hash_pair(bytes).0 as usize) % num_slices
}

#[warn(dead_code)]
fn alloc_seq(next_seq: &mut i64) -> i64 {
    let cur = *next_seq;
    *next_seq += 1;
    cur
}

#[warn(dead_code)]
fn is_already_imported(conn: &Connection) -> io::Result<bool> {
    let val: Option<String> = conn
        .query_row(
            "SELECT val FROM meta WHERE key = 'imported_legacy'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_err)?;
    Ok(val.as_deref() == Some("1"))
}

#[warn(dead_code)]
fn initial_next_seq(txns: &[Savepoint<'_>]) -> io::Result<i64> {
    let mut max_seq: i64 = 0;
    for tx in txns {
        let s: i64 = tx
            .query_row(
                "SELECT MAX(
                    COALESCE((SELECT MAX(seq) FROM timeline), 0),
                    COALESCE((SELECT MAX(seq) FROM envelopes), 0),
                    COALESCE((SELECT MAX(receipt_seq) FROM receipts), 0),
                    COALESCE((SELECT MAX(seq) FROM broadcasts), 0)
                )",
                [],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if s > max_seq {
            max_seq = s;
        }
    }
    Ok(max_seq + 1)
}

/// Stream complete `\n`-terminated JSON lines from `legacy_log` read-only,
/// tracking 1-based physical line numbers, skipping blank or non-JSON lines,
/// and dropping any unterminated trailing byte fragment without touching the file.
#[warn(dead_code)]
fn stream_json_lines<F>(legacy_log: &Path, mut on_event: F) -> io::Result<()>
where
    F: FnMut(i64, Value) -> io::Result<()>,
{
    let file = File::open(legacy_log)?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    let mut raw_line: i64 = 0;
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 || buf.last() != Some(&b'\n') {
            break;
        }
        raw_line += 1;
        buf.pop();
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        if buf.is_empty() {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&buf) else {
            continue;
        };
        let Ok(v) = json::parse(text) else {
            continue;
        };
        on_event(raw_line, v)?;
    }
    Ok(())
}

struct ImportedMail {
    stream: String,
    stream_slice: usize,
    id_slice: usize,
    article_num: i64,
    recipient: String,
    receipted: bool,
}

struct EnvelopeInsert<'a> {
    mail_id: &'a str,
    kind: &'a str,
    sender: &'a str,
    recipient: &'a str,
    body: &'a str,
    headers_json: Option<&'a str>,
    accepted_at_ms: Option<i64>,
    raw_line: i64,
    allow_replace: bool,
}

#[warn(dead_code)]
fn insert_envelope_row(
    txns: &[Savepoint<'_>],
    num_slices: usize,
    next_seq: &mut i64,
    next_articles: &mut HashMap<String, i64>,
    imported_mails: &mut HashMap<String, ImportedMail>,
    row: EnvelopeInsert<'_>,
) -> io::Result<()> {
    if !row.allow_replace && imported_mails.contains_key(row.mail_id) {
        return Ok(());
    }
    if row.allow_replace {
        if let Some(old) = imported_mails.remove(row.mail_id) {
            txns[old.stream_slice]
                .execute(
                    "DELETE FROM envelopes WHERE mail_id = ?1",
                    rusqlite::params![row.mail_id],
                )
                .map_err(sql_err)?;
            txns[old.stream_slice]
                .execute(
                    "DELETE FROM hot_bodies WHERE mail_id = ?1",
                    rusqlite::params![row.mail_id],
                )
                .map_err(sql_err)?;
            txns[old.stream_slice]
                .execute(
                    "DELETE FROM receipts WHERE mail_id = ?1",
                    rusqlite::params![row.mail_id],
                )
                .map_err(sql_err)?;
            txns[old.id_slice]
                .execute(
                    "DELETE FROM seen_ids WHERE mail_id = ?1",
                    rusqlite::params![row.mail_id],
                )
                .map_err(sql_err)?;
        }
    }

    let stream = format!("m:{}", row.recipient);
    let stream_slice = shard_index(stream.as_bytes(), num_slices);
    let id_slice = shard_index(row.mail_id.as_bytes(), num_slices);

    let next_art_slot = match next_articles.get_mut(&stream) {
        Some(n) => n,
        None => {
            let existing: Option<i64> = txns[stream_slice]
                .query_row(
                    "SELECT next_article FROM streams WHERE stream = ?1",
                    rusqlite::params![&stream],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sql_err)?;
            next_articles
                .entry(stream.clone())
                .or_insert(existing.unwrap_or(1))
        }
    };
    let article_num = *next_art_slot;
    *next_art_slot += 1;

    txns[stream_slice]
        .execute(
            "INSERT INTO streams (stream, kind, next_article)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(stream) DO UPDATE SET next_article = excluded.next_article",
            rusqlite::params![&stream, net::DIRECTED, article_num + 1],
        )
        .map_err(sql_err)?;

    txns[stream_slice]
        .execute(
            "INSERT INTO positions (stream, reader, subscribed, position)
             VALUES (?1, ?2, 1, 0)
             ON CONFLICT(stream, reader) DO UPDATE SET subscribed = 1",
            rusqlite::params![&stream, row.recipient],
        )
        .map_err(sql_err)?;

    let body_len = i64::try_from(row.body.len()).unwrap_or(i64::MAX);
    let digest = hex_digest(row.body.as_bytes());
    let day_bucket = row
        .accepted_at_ms
        .map_or(0, |ms| ms.div_euclid(super::DAY_MS));
    let item_seq = alloc_seq(next_seq);

    txns[stream_slice]
        .execute(
            "INSERT INTO envelopes (
                seq, mail_id, stream, article_num, kind, sender, recipient,
                headers_json, body_len, body_sha256, accepted_at_ms, day_bucket, in_cold, raw_line
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0, ?13)",
            rusqlite::params![
                item_seq,
                row.mail_id,
                &stream,
                article_num,
                row.kind,
                row.sender,
                row.recipient,
                row.headers_json,
                body_len,
                &digest,
                row.accepted_at_ms,
                day_bucket,
                row.raw_line,
            ],
        )
        .map_err(sql_err)?;

    txns[stream_slice]
        .execute(
            "INSERT OR REPLACE INTO hot_bodies (mail_id, body) VALUES (?1, ?2)",
            rusqlite::params![row.mail_id, row.body],
        )
        .map_err(sql_err)?;

    txns[id_slice]
        .execute(
            "INSERT OR REPLACE INTO seen_ids (
                mail_id, kind, sender, target, body_len, body_sha256, article_num, stream
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                row.mail_id,
                row.kind,
                row.sender,
                row.recipient,
                body_len,
                &digest,
                article_num,
                &stream,
            ],
        )
        .map_err(sql_err)?;

    imported_mails.insert(
        row.mail_id.to_string(),
        ImportedMail {
            stream,
            stream_slice,
            id_slice,
            article_num,
            recipient: row.recipient.to_string(),
            receipted: false,
        },
    );
    Ok(())
}

#[warn(dead_code)]
fn advance_position(conn: &Connection, stream: &str, reader: &str) -> io::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO positions (stream, reader, subscribed, position)
         VALUES (?1, ?2, 1, 0)",
        rusqlite::params![stream, reader],
    )
    .map_err(sql_err)?;

    let cur_pos: i64 = conn
        .query_row(
            "SELECT position FROM positions WHERE stream = ?1 AND reader = ?2",
            rusqlite::params![stream, reader],
            |r| r.get(0),
        )
        .map_err(sql_err)?;

    let mut stmt = conn
        .prepare(
            "SELECT e.article_num, r.record_id, r.receipted_at_ms
             FROM envelopes e
             LEFT JOIN receipts r ON r.mail_id = e.mail_id AND r.reader = ?2
             WHERE e.stream = ?1 AND e.article_num > ?3
             ORDER BY e.article_num ASC",
        )
        .map_err(sql_err)?;

    let rows = stmt
        .query_map(rusqlite::params![stream, reader, cur_pos], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<i64>>(2)?,
            ))
        })
        .map_err(sql_err)?;

    let mut new_pos = cur_pos;
    let mut last_record: Option<String> = None;
    let mut last_at: Option<i64> = None;
    for item in rows {
        let (art, rec, at) = item.map_err(sql_err)?;
        let Some(rec_id) = rec else {
            break;
        };
        new_pos = art;
        last_record = Some(rec_id);
        last_at = at;
    }
    drop(stmt);

    if let Some(rec_id) = last_record {
        conn.execute(
            "UPDATE positions
             SET position = ?3,
                 record_id = ?4,
                 receipted_at_ms = ?5,
                 updated_at_ms = COALESCE(?5, updated_at_ms)
             WHERE stream = ?1 AND reader = ?2",
            rusqlite::params![stream, reader, new_pos, rec_id, last_at],
        )
        .map_err(sql_err)?;
    }
    Ok(())
}

/// Read-only import of a legacy JSONL `coordinator.log` into initialized slice
/// connections (slice 0..N-1). Leaves `legacy_log` untouched on disk.
pub fn coord_log(legacy_log: &Path, slices: &mut [Connection]) -> io::Result<bool> {
    if slices.is_empty() || !legacy_log.is_file() || super::is_sqlite_file(legacy_log) {
        return Ok(false);
    }
    if is_already_imported(&slices[0])? {
        return Ok(false);
    }

    let num_slices = slices.len();
    let txns: Vec<Savepoint<'_>> = slices
        .iter_mut()
        .map(|c| c.savepoint())
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_err)?;

    let mut next_seq = initial_next_seq(&txns)?;
    let mut claims = Claims::default();
    let mut seen_broadcasts: HashSet<String> = HashSet::new();
    let mut imported_mails: HashMap<String, ImportedMail> = HashMap::new();
    let mut next_articles: HashMap<String, i64> = HashMap::new();

    stream_json_lines(legacy_log, |raw_line, v| {
        match v.get("ev").and_then(Value::as_str) {
            Some("session") => {
                if let (Ok(session), Ok(host)) = (net::field(&v, "session"), net::field(&v, "host"))
                {
                    let at_ms = stamp_of(&v, "at_ms");
                    let item_seq = alloc_seq(&mut next_seq);
                    txns[0]
                        .execute(
                            "INSERT INTO timeline (seq, kind, addr, host, at_ms, raw_line)
                             VALUES (?1, 'session', ?2, ?3, ?4, ?5)",
                            rusqlite::params![item_seq, session, host, at_ms, raw_line],
                        )
                        .map_err(sql_err)?;
                    txns[0]
                        .execute(
                            "INSERT INTO live_sessions (address, host, label, declared, at_ms)
                             VALUES (?1, ?2, '', NULL, ?3)
                             ON CONFLICT(address) DO UPDATE SET
                                 host = excluded.host,
                                 at_ms = excluded.at_ms",
                            rusqlite::params![session, host, at_ms],
                        )
                        .map_err(sql_err)?;
                }
            }
            Some("label") => {
                if let (Ok(session), Ok(lbl)) = (net::field(&v, "session"), net::field(&v, "label"))
                {
                    let at_ms = stamp_of(&v, "at_ms");
                    let item_seq = alloc_seq(&mut next_seq);
                    txns[0]
                        .execute(
                            "INSERT INTO timeline (seq, kind, addr, label, at_ms, raw_line)
                             VALUES (?1, 'label', ?2, ?3, ?4, ?5)",
                            rusqlite::params![item_seq, session, lbl, at_ms, raw_line],
                        )
                        .map_err(sql_err)?;
                    txns[0]
                        .execute(
                            "UPDATE live_sessions SET label = ?1 WHERE address = ?2",
                            rusqlite::params![lbl, session],
                        )
                        .map_err(sql_err)?;
                }
            }
            Some("declared") => {
                if let Ok(session) = net::field(&v, "session") {
                    let at_ms = stamp_of(&v, "at_ms");
                    let declared = net::declared_of(&v);
                    let item_seq = alloc_seq(&mut next_seq);
                    match &declared {
                        Some(d) => {
                            txns[0]
                                .execute(
                                    "INSERT INTO timeline (seq, kind, addr, declared, at_ms, raw_line)
                                     VALUES (?1, 'declared', ?2, ?3, ?4, ?5)",
                                    rusqlite::params![item_seq, session, d, at_ms, raw_line],
                                )
                                .map_err(sql_err)?;
                        }
                        None => {
                            txns[0]
                                .execute(
                                    "INSERT INTO timeline (seq, kind, addr, at_ms, raw_line)
                                     VALUES (?1, 'undeclared', ?2, ?3, ?4)",
                                    rusqlite::params![item_seq, session, at_ms, raw_line],
                                )
                                .map_err(sql_err)?;
                        }
                    }
                    txns[0]
                        .execute(
                            "UPDATE live_sessions SET declared = ?1 WHERE address = ?2",
                            rusqlite::params![declared.as_deref(), session],
                        )
                        .map_err(sql_err)?;
                }
            }
            Some("unregister") => {
                if let Ok(session) = net::field(&v, "session") {
                    let at_ms = stamp_of(&v, "at_ms");
                    let item_seq = alloc_seq(&mut next_seq);
                    txns[0]
                        .execute(
                            "INSERT INTO timeline (seq, kind, addr, at_ms, raw_line)
                             VALUES (?1, 'unregister', ?2, ?3, ?4)",
                            rusqlite::params![item_seq, session, at_ms, raw_line],
                        )
                        .map_err(sql_err)?;
                    txns[0]
                        .execute(
                            "DELETE FROM live_sessions WHERE address = ?1",
                            rusqlite::params![session],
                        )
                        .map_err(sql_err)?;
                }
            }
            Some("takeover") => {
                if let (Ok(session), Ok(old_host), Ok(host)) = (
                    net::field(&v, "session"),
                    net::field(&v, "old_host"),
                    net::field(&v, "host"),
                ) {
                    let registration = v.get("registration").and_then(Value::as_str);
                    let at_ms = stamp_of(&v, "at_ms");
                    let item_seq = alloc_seq(&mut next_seq);
                    txns[0]
                        .execute(
                            "INSERT INTO timeline (
                                seq, kind, addr, old_host, host, registration, at_ms, raw_line
                            ) VALUES (?1, 'takeover', ?2, ?3, ?4, ?5, ?6, ?7)",
                            rusqlite::params![
                                item_seq,
                                session,
                                old_host,
                                host,
                                registration,
                                at_ms,
                                raw_line,
                            ],
                        )
                        .map_err(sql_err)?;
                }
            }
            Some("claim" | "release") => {
                let (key, change) = Change::from_event(&v)?;
                claims.apply(&key, &change);
                let at_ms = stamp_of(&v, "at_ms");
                let item_seq = alloc_seq(&mut next_seq);
                match &change {
                    Change::Grant(held) => {
                        txns[0]
                            .execute(
                                "INSERT INTO timeline (
                                    seq, kind, addr, claim_key, expires_ms, at_ms, raw_line
                                ) VALUES (?1, 'claim', ?2, ?3, ?4, ?5, ?6)",
                                rusqlite::params![
                                    item_seq,
                                    &held.owner,
                                    &key,
                                    held.expires_ms,
                                    at_ms,
                                    raw_line,
                                ],
                            )
                            .map_err(sql_err)?;
                        txns[0]
                            .execute(
                                "INSERT INTO live_claims (claim_key, owner, expires_ms)
                                 VALUES (?1, ?2, ?3)
                                 ON CONFLICT(claim_key) DO UPDATE SET
                                     owner = excluded.owner,
                                     expires_ms = excluded.expires_ms",
                                rusqlite::params![&key, &held.owner, held.expires_ms],
                            )
                            .map_err(sql_err)?;
                    }
                    Change::Release => {
                        txns[0]
                            .execute(
                                "INSERT INTO timeline (
                                    seq, kind, claim_key, at_ms, raw_line
                                ) VALUES (?1, 'release', ?2, ?3, ?4)",
                                rusqlite::params![item_seq, &key, at_ms, raw_line],
                            )
                            .map_err(sql_err)?;
                        txns[0]
                            .execute(
                                "DELETE FROM live_claims WHERE claim_key = ?1",
                                rusqlite::params![&key],
                            )
                            .map_err(sql_err)?;
                    }
                }
            }
            Some("mail") => {
                if let (Ok(mail), Ok(from), Ok(to), Ok(body)) = (
                    net::field(&v, "mail"),
                    net::field(&v, "from"),
                    net::field(&v, "to"),
                    net::field(&v, "body"),
                ) {
                    let headers_json = v.get("headers").map(json::stringify);
                    let accepted_at_ms = stamp_of(&v, "accepted_at_ms");
                    insert_envelope_row(
                        &txns,
                        num_slices,
                        &mut next_seq,
                        &mut next_articles,
                        &mut imported_mails,
                        EnvelopeInsert {
                            mail_id: mail,
                            kind: net::DIRECTED,
                            sender: from,
                            recipient: to,
                            body,
                            headers_json: headers_json.as_deref(),
                            accepted_at_ms,
                            raw_line,
                            allow_replace: true,
                        },
                    )?;
                }
            }
            Some("broadcast") => {
                if let (Ok(mail), Ok(from), Ok(body), Some(Value::Arr(to_arr))) = (
                    net::field(&v, "mail"),
                    net::field(&v, "from"),
                    net::field(&v, "body"),
                    v.get("to"),
                ) {
                    if seen_broadcasts.insert(mail.to_string()) {
                        let recipients: Vec<String> = to_arr
                            .iter()
                            .filter_map(|x| x.as_str().map(str::to_string))
                            .collect();
                        let recipients_json = json::stringify(&Value::Arr(
                            recipients.iter().map(|r| json::s(r)).collect(),
                        ));
                        let headers_json = v.get("headers").map(json::stringify);
                        let accepted_at_ms = stamp_of(&v, "accepted_at_ms");
                        let body_len = i64::try_from(body.len()).unwrap_or(i64::MAX);
                        let digest = hex_digest(body.as_bytes());
                        let bcast_seq = alloc_seq(&mut next_seq);
                        let id_slice = shard_index(mail.as_bytes(), num_slices);

                        txns[id_slice]
                            .execute(
                                "INSERT OR REPLACE INTO broadcasts (
                                    mail_id, seq, sender, body_len, body_sha256,
                                    headers_json, recipients_json, accepted_at_ms, raw_line
                                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                                rusqlite::params![
                                    mail,
                                    bcast_seq,
                                    from,
                                    body_len,
                                    &digest,
                                    headers_json.as_deref(),
                                    &recipients_json,
                                    accepted_at_ms,
                                    raw_line,
                                ],
                            )
                            .map_err(sql_err)?;

                        txns[id_slice]
                            .execute(
                                "INSERT OR REPLACE INTO seen_ids (
                                    mail_id, kind, sender, target, body_len, body_sha256, article_num
                                ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, 0)",
                                rusqlite::params![mail, net::BROADCAST, from, body_len, &digest],
                            )
                            .map_err(sql_err)?;

                        for rcpt in &recipients {
                            let copy_id = format!("{mail}/{rcpt}");
                            insert_envelope_row(
                                &txns,
                                num_slices,
                                &mut next_seq,
                                &mut next_articles,
                                &mut imported_mails,
                                EnvelopeInsert {
                                    mail_id: &copy_id,
                                    kind: net::BROADCAST,
                                    sender: from,
                                    recipient: rcpt,
                                    body,
                                    headers_json: headers_json.as_deref(),
                                    accepted_at_ms,
                                    raw_line,
                                    allow_replace: false,
                                },
                            )?;
                        }
                    }
                }
            }
            Some("receipt") => {
                if let (Ok(mail), Ok(record)) = (net::field(&v, "mail"), net::field(&v, "record")) {
                    let receipted_at_ms = stamp_of(&v, "receipted_at_ms");
                    if let Some(info) = imported_mails.get_mut(mail) {
                        if !info.receipted {
                            let reader = v
                                .get("session")
                                .and_then(Value::as_str)
                                .unwrap_or(&info.recipient);
                            let receipt_seq = alloc_seq(&mut next_seq);
                            let inserted = txns[info.stream_slice]
                                .execute(
                                    "INSERT OR IGNORE INTO receipts (
                                        mail_id, reader, stream, article_num,
                                        record_id, receipted_at_ms, receipt_seq, raw_line
                                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                                    rusqlite::params![
                                        mail,
                                        reader,
                                        &info.stream,
                                        info.article_num,
                                        record,
                                        receipted_at_ms,
                                        receipt_seq,
                                        raw_line,
                                    ],
                                )
                                .map_err(sql_err)?;
                            if inserted > 0 {
                                info.receipted = true;
                                advance_position(&txns[info.stream_slice], &info.stream, reader)?;
                            }
                        }
                    } else {
                        let session = v.get("session").and_then(Value::as_str);
                        let item_seq = alloc_seq(&mut next_seq);
                        txns[0]
                            .execute(
                                "INSERT INTO timeline (
                                    seq, kind, addr, label, mail_id, at_ms, raw_line
                                ) VALUES (?1, 'stray_receipt', ?2, ?3, ?4, ?5, ?6)",
                                rusqlite::params![
                                    item_seq,
                                    session,
                                    record,
                                    mail,
                                    receipted_at_ms,
                                    raw_line,
                                ],
                            )
                            .map_err(sql_err)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    txns[0]
        .execute(
            "INSERT INTO meta (key, val) VALUES ('imported_legacy', '1')
             ON CONFLICT(key) DO UPDATE SET val = excluded.val",
            [],
        )
        .map_err(sql_err)?;

    for tx in txns {
        tx.commit().map_err(sql_err)?;
    }
    Ok(true)
}

struct ReplayedDeliver {
    mail_id: String,
    sender: String,
    recipient: String,
    group_name: Option<String>,
    article_num: Option<i64>,
    body: String,
    kind: String,
    headers_json: String,
}

struct ReplayedReceipt {
    mail_id: String,
    session: String,
    record_id: String,
}

/// Read-only import of a legacy JSONL `daemon.log` into `conn`.
/// Leaves `legacy_log` untouched on disk.
pub fn daemon_log(legacy_log: &Path, conn: &mut Connection) -> io::Result<bool> {
    if !legacy_log.is_file() || super::is_sqlite_file(legacy_log) {
        return Ok(false);
    }
    if is_already_imported(conn)? {
        return Ok(false);
    }

    let mut seen_deliveries: HashSet<String> = HashSet::new();
    let mut pending_index: HashMap<String, usize> = HashMap::new();
    let mut pending: Vec<Option<ReplayedDeliver>> = Vec::new();
    let mut acked: HashSet<String> = HashSet::new();
    let mut receipts: Vec<ReplayedReceipt> = Vec::new();

    stream_json_lines(legacy_log, |_raw_line, v| {
        match v.get("ev").and_then(Value::as_str) {
            Some("deliver") => {
                if let (Ok(mail), Ok(from), Ok(to), Ok(body)) = (
                    net::field(&v, "mail"),
                    net::field(&v, "from"),
                    net::field(&v, "to"),
                    net::field(&v, "body"),
                ) {
                    if seen_deliveries.insert(mail.to_string()) && !acked.contains(mail) {
                        let idx = pending.len();
                        pending_index.insert(mail.to_string(), idx);
                        pending.push(Some(ReplayedDeliver {
                            mail_id: mail.to_string(),
                            sender: from.to_string(),
                            recipient: to.to_string(),
                            group_name: v.get("group").and_then(Value::as_str).map(str::to_string),
                            article_num: v.get("article").and_then(Value::as_i64),
                            body: body.to_string(),
                            kind: net::kind_of(&v),
                            headers_json: json::stringify(&net::headers_of(&v)),
                        }));
                    }
                }
            }
            Some("receipt") => {
                if let (Ok(mail), Ok(session), Ok(record)) = (
                    net::field(&v, "mail"),
                    net::field(&v, "session"),
                    net::field(&v, "record"),
                ) {
                    if acked.insert(mail.to_string()) {
                        if let Some(&idx) = pending_index.get(mail) {
                            pending[idx] = None;
                        }
                        receipts.push(ReplayedReceipt {
                            mail_id: mail.to_string(),
                            session: session.to_string(),
                            record_id: record.to_string(),
                        });
                    }
                }
            }
            _ => {}
        }
        Ok(())
    })?;

    let sp = conn.savepoint().map_err(sql_err)?;
    for d in pending.into_iter().flatten() {
        sp.execute(
            "INSERT OR IGNORE INTO pending_deliveries (
                mail_id, sender, recipient, group_name, article_num, body, kind, headers_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                d.mail_id,
                d.sender,
                d.recipient,
                d.group_name,
                d.article_num,
                d.body,
                d.kind,
                d.headers_json,
            ],
        )
        .map_err(sql_err)?;
    }
    for r in receipts {
        sp.execute(
            "INSERT OR IGNORE INTO unconfirmed_receipts (mail_id, session, record_id)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![r.mail_id, r.session, r.record_id],
        )
        .map_err(sql_err)?;
    }
    sp.execute(
        "INSERT INTO meta (key, val) VALUES ('imported_legacy', '1')
         ON CONFLICT(key) DO UPDATE SET val = excluded.val",
        [],
    )
    .map_err(sql_err)?;
    sp.commit().map_err(sql_err)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{COORD_SLICE_SCHEMA, DAEMON_SCHEMA, DAY_MS};
    use std::fs;
    use std::path::PathBuf;

    const ADDR_A: &str = "[[1a:Meteor Shower:zz]]aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const ADDR_B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const ADDR_C: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(tag: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("roger-c03-import-{}-{tag}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }

    fn coord_slices(n: usize) -> Vec<Connection> {
        (0..n)
            .map(|_| {
                let c = Connection::open_in_memory().unwrap();
                c.execute_batch(COORD_SLICE_SCHEMA).unwrap();
                c
            })
            .collect()
    }

    fn daemon_conn() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(DAEMON_SCHEMA).unwrap();
        c
    }

    #[test]
    fn guards_skip_missing_directory_sqlite_and_second_import() {
        let dir = TestDir::new("guards");
        let missing = dir.path("missing.log");
        let subdir = dir.path("sub");
        fs::create_dir_all(&subdir).unwrap();

        let mut slices = coord_slices(1);
        let mut dconn = daemon_conn();

        assert!(!coord_log(&missing, &mut slices).unwrap());
        assert!(!coord_log(&subdir, &mut slices).unwrap());
        assert!(!daemon_log(&missing, &mut dconn).unwrap());
        assert!(!daemon_log(&subdir, &mut dconn).unwrap());

        let sqlite_path = dir.path("already.sqlite");
        {
            let disk = Connection::open(&sqlite_path).unwrap();
            disk.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);")
                .unwrap();
        }
        let sqlite_before = fs::read(&sqlite_path).unwrap();
        assert!(!coord_log(&sqlite_path, &mut slices).unwrap());
        assert!(!daemon_log(&sqlite_path, &mut dconn).unwrap());
        assert_eq!(fs::read(&sqlite_path).unwrap(), sqlite_before);

        let coord_log_path = dir.path("coord.log");
        fs::write(
            &coord_log_path,
            format!(
                "{{\"ev\":\"session\",\"session\":\"{ADDR_A}\",\"host\":\"h1\",\"at_ms\":10}}\n"
            ),
        )
        .unwrap();
        assert!(!coord_log(&coord_log_path, &mut []).unwrap());
        assert!(coord_log(&coord_log_path, &mut slices).unwrap());
        assert!(!coord_log(&coord_log_path, &mut slices).unwrap());
        let timeline_count: i64 = slices[0]
            .query_row("SELECT COUNT(*) FROM timeline", [], |r| r.get(0))
            .unwrap();
        assert_eq!(timeline_count, 1);

        let daemon_log_path = dir.path("daemon.log");
        fs::write(
            &daemon_log_path,
            format!(
                "{{\"ev\":\"deliver\",\"mail\":\"m1\",\"from\":\"{ADDR_A}\",\"to\":\"{ADDR_B}\",\"body\":\"hi\"}}\n"
            ),
        )
        .unwrap();
        assert!(daemon_log(&daemon_log_path, &mut dconn).unwrap());
        assert!(!daemon_log(&daemon_log_path, &mut dconn).unwrap());
        let pending_count: i64 = dconn
            .query_row("SELECT COUNT(*) FROM pending_deliveries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pending_count, 1);
    }

    #[test]
    fn coordinator_import_preserves_locked_readonly_file_skips_garbage_and_drops_torn_tail() {
        let dir = TestDir::new("coord-readonly-torn");
        let log_path = dir.path("coord.log");
        let raw = format!(
            "{{\"ev\":\"session\",\"session\":\"{ADDR_A}\",\"host\":\"h-a\",\"at_ms\":100}}\r\n\
             \n\
             not-json-at-all\n\
             [1,2,3]\n\
             {{\"ev\":\"mail\",\"mail\":\"m-ok\",\"from\":\"{ADDR_A}\",\"to\":\"{ADDR_B}\",\"body\":\"kept\"}}\n\
             {{\"ev\":\"session\",\"session\":\"{ADDR_C}\",\"host\":\"h-c\"}}"
        );
        fs::write(&log_path, raw.as_bytes()).unwrap();

        // Hold an exclusive advisory lock on a separate handle to prove import never locks.
        let held_lock = crate::durlog::open_exclusive_lock(&log_path).unwrap();
        let mut perms = fs::metadata(&log_path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&log_path, perms).unwrap();
        let before = fs::read(&log_path).unwrap();

        let mut slices = coord_slices(1);
        assert!(coord_log(&log_path, &mut slices).unwrap());

        let after = fs::read(&log_path).unwrap();
        assert_eq!(
            after, before,
            "legacy coordinator.log must stay byte-identical"
        );

        // Make writable again before cleanup on platforms that guard read-only unlink.
        let mut cleanup_perms = fs::metadata(&log_path).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        cleanup_perms.set_readonly(false);
        fs::set_permissions(&log_path, cleanup_perms).unwrap();
        drop(held_lock);

        let sessions: Vec<(String, i64)> = slices[0]
            .prepare("SELECT addr, raw_line FROM timeline WHERE kind = 'session'")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(sessions, vec![(ADDR_A.to_string(), 1)]);

        let live_c: i64 = slices[0]
            .query_row(
                "SELECT COUNT(*) FROM live_sessions WHERE address = ?1",
                rusqlite::params![ADDR_C],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(live_c, 0, "torn trailing fragment must not be imported");

        let (mail_line, body): (i64, String) = slices[0]
            .query_row(
                "SELECT e.raw_line, b.body
                 FROM envelopes e JOIN hot_bodies b ON b.mail_id = e.mail_id
                 WHERE e.mail_id = 'm-ok'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(mail_line, 5);
        assert_eq!(body, "kept");
    }

    #[test]
    fn coordinator_import_replays_sessions_labels_declarations_takeovers_and_claims() {
        let dir = TestDir::new("coord-timeline-state");
        let bad_log = dir.path("bad-claim.log");
        fs::write(
            &bad_log,
            format!(
                "{{\"ev\":\"session\",\"session\":\"{ADDR_A}\",\"host\":\"h1\"}}\n\
                 {{\"ev\":\"claim\",\"key\":\"k1\",\"owner\":\"{ADDR_A}\"}}\n"
            ),
        )
        .unwrap();
        let mut bad_slices = coord_slices(1);
        let err = coord_log(&bad_log, &mut bad_slices).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let rolled_back: i64 = bad_slices[0]
            .query_row("SELECT COUNT(*) FROM timeline", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            rolled_back, 0,
            "failed claim event must roll back all slice writes"
        );
        assert!(!is_already_imported(&bad_slices[0]).unwrap());

        let log_path = dir.path("coord.log");
        let lines = [
            format!(r#"{{"ev":"session","session":"{ADDR_A}","host":"host-1"}}"#),
            format!(
                r#"{{"ev":"label","session":"{ADDR_A}","label":"[[0:FLEET:0]] lead","at_ms":101}}"#
            ),
            format!(r#"{{"ev":"declared","session":"{ADDR_A}","declared":"decl-v1","at_ms":102}}"#),
            // Re-registering on host-2 must preserve existing label and declared!
            format!(r#"{{"ev":"session","session":"{ADDR_A}","host":"host-2","at_ms":103}}"#),
            format!(
                r#"{{"ev":"takeover","session":"{ADDR_A}","old_host":"host-1","host":"host-2","registration":"reg-9","at_ms":103}}"#
            ),
            format!(r#"{{"ev":"session","session":"{ADDR_B}","host":"host-2","at_ms":104}}"#),
            format!(r#"{{"ev":"declared","session":"{ADDR_B}","declared":"temp","at_ms":105}}"#),
            format!(r#"{{"ev":"declared","session":"{ADDR_B}","at_ms":106}}"#),
            format!(r#"{{"ev":"session","session":"{ADDR_C}","host":"host-3"}}"#),
            format!(r#"{{"ev":"unregister","session":"{ADDR_C}","at_ms":107}}"#),
            format!(
                r#"{{"ev":"claim","key":"[[9:lock:1]]","owner":"{ADDR_A}","expires_ms":9000}}"#
            ),
            format!(
                r#"{{"ev":"claim","key":"[[9:gone:2]]","owner":"{ADDR_B}","expires_ms":8000}}"#
            ),
            r#"{"ev":"release","key":"[[9:gone:2]]"}"#.to_string(),
        ];
        fs::write(&log_path, lines.join("\n") + "\n").unwrap();

        let mut slices = coord_slices(1);
        assert!(coord_log(&log_path, &mut slices).unwrap());

        let (host_a, label_a, decl_a, at_a): (String, String, Option<String>, Option<i64>) = slices
            [0]
        .query_row(
            "SELECT host, label, declared, at_ms FROM live_sessions WHERE address = ?1",
            rusqlite::params![ADDR_A],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
        assert_eq!(host_a, "host-2");
        assert_eq!(label_a, "[[0:FLEET:0]] lead");
        assert_eq!(decl_a.as_deref(), Some("decl-v1"));
        assert_eq!(at_a, Some(103));

        let decl_b: Option<String> = slices[0]
            .query_row(
                "SELECT declared FROM live_sessions WHERE address = ?1",
                rusqlite::params![ADDR_B],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            decl_b, None,
            "undeclared event must clear live_sessions.declared"
        );

        let count_c: i64 = slices[0]
            .query_row(
                "SELECT COUNT(*) FROM live_sessions WHERE address = ?1",
                rusqlite::params![ADDR_C],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count_c, 0,
            "unregister must remove session from live_sessions"
        );

        let claims_rows: Vec<(String, String, i64)> = slices[0]
            .prepare("SELECT claim_key, owner, expires_ms FROM live_claims ORDER BY claim_key")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            claims_rows,
            vec![("[[9:lock:1]]".to_string(), ADDR_A.to_string(), 9000)]
        );

        let kinds: Vec<(String, i64)> = slices[0]
            .prepare("SELECT kind, raw_line FROM timeline ORDER BY seq ASC")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("session".into(), 1),
                ("label".into(), 2),
                ("declared".into(), 3),
                ("session".into(), 4),
                ("takeover".into(), 5),
                ("session".into(), 6),
                ("declared".into(), 7),
                ("undeclared".into(), 8),
                ("session".into(), 9),
                ("unregister".into(), 10),
                ("claim".into(), 11),
                ("claim".into(), 12),
                ("release".into(), 13),
            ]
        );

        let takeover: (String, String, String, Option<String>, Option<i64>) = slices[0]
            .query_row(
                "SELECT addr, old_host, host, registration, at_ms
                 FROM timeline WHERE kind = 'takeover'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            takeover,
            (
                ADDR_A.to_string(),
                "host-1".to_string(),
                "host-2".to_string(),
                Some("reg-9".to_string()),
                Some(103),
            )
        );
    }

    #[test]
    fn coordinator_import_distinguishes_absent_empty_and_ordered_headers_and_unstamped_mail() {
        let dir = TestDir::new("coord-mail-headers");
        let log_path = dir.path("coord.log");
        let ts_day2 = 2 * DAY_MS + 500;
        let lines = [
            format!(
                r#"{{"ev":"mail","mail":"m-old","from":"{ADDR_A}","to":"{ADDR_B}","body":"héllo"}}"#
            ),
            format!(
                r#"{{"ev":"mail","mail":"m-empty","from":"{ADDR_A}","to":"{ADDR_B}","body":"x","headers":{{}},"accepted_at_ms":{ts_day2}}}"#
            ),
            format!(
                r#"{{"ev":"mail","mail":"m-hdr","from":"{ADDR_A}","to":"{ADDR_B}","body":"v1","headers":{{"z":"last","a":"first"}},"accepted_at_ms":50}}"#
            ),
            // Duplicate mail line under same mail_id replaces envelope and body.
            format!(
                r#"{{"ev":"mail","mail":"m-hdr","from":"{ADDR_A}","to":"{ADDR_B}","body":"v2-replaced","headers":{{"z":"last","a":"first"}},"accepted_at_ms":75}}"#
            ),
        ];
        fs::write(&log_path, lines.join("\n") + "\n").unwrap();

        let mut slices = coord_slices(1);
        assert!(coord_log(&log_path, &mut slices).unwrap());

        type EnvRow = (
            String,
            Option<String>,
            Option<i64>,
            i64,
            i64,
            String,
            String,
        );
        let rows: Vec<EnvRow> = slices[0]
            .prepare(
                "SELECT e.mail_id, e.headers_json, e.accepted_at_ms, e.day_bucket,
                        e.body_len, e.body_sha256, b.body
                 FROM envelopes e
                 JOIN hot_bodies b ON b.mail_id = e.mail_id
                 ORDER BY e.seq ASC",
            )
            .unwrap()
            .query_map([], |r| {
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
            .unwrap()
            .map(Result::unwrap)
            .collect();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "m-old");
        assert_eq!(
            rows[0].1, None,
            "pre-headers log line must keep headers_json NULL"
        );
        assert_eq!(
            rows[0].2, None,
            "unstamped log line must keep accepted_at_ms NULL"
        );
        assert_eq!(rows[0].3, 0);
        assert_eq!(rows[0].4, "héllo".len() as i64);
        assert_eq!(rows[0].5, hex_digest("héllo".as_bytes()));
        assert_eq!(rows[0].6, "héllo");

        assert_eq!(rows[1].0, "m-empty");
        assert_eq!(rows[1].1.as_deref(), Some("{}"));
        assert_eq!(rows[1].2, Some(ts_day2));
        assert_eq!(rows[1].3, 2);

        assert_eq!(rows[2].0, "m-hdr");
        assert_eq!(rows[2].1.as_deref(), Some(r#"{"z":"last","a":"first"}"#));
        assert_eq!(rows[2].2, Some(75));
        assert_eq!(rows[2].6, "v2-replaced");

        let (sub, pos): (i64, i64) = slices[0]
            .query_row(
                "SELECT subscribed, position FROM positions WHERE stream = ?1 AND reader = ?2",
                rusqlite::params![format!("m:{ADDR_B}"), ADDR_B],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((sub, pos), (1, 0));
    }

    #[test]
    fn coordinator_import_fans_out_broadcasts_keeps_receipted_bodies_and_records_stray_receipts() {
        let dir = TestDir::new("coord-bcast-receipts");
        let log_path = dir.path("coord.log");
        let lines = [
            format!(
                r#"{{"ev":"broadcast","mail":"b1","from":"{ADDR_A}","body":"all hands","to":["{ADDR_B}","{ADDR_C}"],"headers":{{"k":"v"}},"accepted_at_ms":3000}}"#
            ),
            // Duplicate broadcast id is ignored like coordinator::fan_out.
            format!(
                r#"{{"ev":"broadcast","mail":"b1","from":"{ADDR_A}","body":"ignored","to":["{ADDR_A}"]}}"#
            ),
            format!(
                r#"{{"ev":"mail","mail":"m2","from":"{ADDR_A}","to":"{ADDR_B}","body":"second","accepted_at_ms":3100}}"#
            ),
            // Out-of-order receipt for article 2 on stream m:ADDR_B: position stays 0 until article 1 is receipted.
            format!(
                r#"{{"ev":"receipt","mail":"m2","session":"{ADDR_B}","record":"r-2","receipted_at_ms":3200}}"#
            ),
            // First receipt for b1/ADDR_B (article 1) wins and advances position across 1 -> 2!
            format!(
                r#"{{"ev":"receipt","mail":"b1/{ADDR_B}","session":"{ADDR_B}","record":"r-1","receipted_at_ms":3300}}"#
            ),
            // Second receipt for b1/ADDR_B is ignored (first receipt wins).
            format!(
                r#"{{"ev":"receipt","mail":"b1/{ADDR_B}","session":"{ADDR_B}","record":"r-99","receipted_at_ms":3400}}"#
            ),
            // Stray receipt for unseen mail id goes to timeline.
            format!(
                r#"{{"ev":"receipt","mail":"ghost-mail","session":"{ADDR_C}","record":"r-7"}}"#
            ),
        ];
        fs::write(&log_path, lines.join("\n") + "\n").unwrap();

        let mut slices = coord_slices(1);
        assert!(coord_log(&log_path, &mut slices).unwrap());

        let (b_sender, b_rcpts, b_at): (String, String, Option<i64>) = slices[0]
            .query_row(
                "SELECT sender, recipients_json, accepted_at_ms FROM broadcasts WHERE mail_id = 'b1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(b_sender, ADDR_A);
        assert_eq!(b_rcpts, format!(r#"["{ADDR_B}","{ADDR_C}"]"#));
        assert_eq!(b_at, Some(3000));

        let seen_b1: (String, Option<String>) = slices[0]
            .query_row(
                "SELECT kind, target FROM seen_ids WHERE mail_id = 'b1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(seen_b1, ("broadcast".to_string(), None));

        // Receipted bodies MUST stay in hot_bodies so `roger audit --bodies` and cold export preserve them.
        let hot_count: i64 = slices[0]
            .query_row("SELECT COUNT(*) FROM hot_bodies", [], |r| r.get(0))
            .unwrap();
        assert_eq!(hot_count, 3);
        let b1_b_body: String = slices[0]
            .query_row(
                "SELECT body FROM hot_bodies WHERE mail_id = ?1",
                rusqlite::params![format!("b1/{ADDR_B}")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b1_b_body, "all hands");

        // First receipt won (`r-1`, not `r-99`).
        let (rec_id, rec_at, rec_line): (String, Option<i64>, i64) = slices[0]
            .query_row(
                "SELECT record_id, receipted_at_ms, raw_line FROM receipts WHERE mail_id = ?1",
                rusqlite::params![format!("b1/{ADDR_B}")],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(rec_id, "r-1");
        assert_eq!(rec_at, Some(3300));
        assert_eq!(rec_line, 5);

        // Contiguous position on m:ADDR_B advanced to article 2 (`r-2`, 3200), while m:ADDR_C stays 0.
        let pos_b: (i64, Option<String>, Option<i64>) = slices[0]
            .query_row(
                "SELECT position, record_id, receipted_at_ms FROM positions WHERE stream = ?1 AND reader = ?2",
                rusqlite::params![format!("m:{ADDR_B}"), ADDR_B],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(pos_b, (2, Some("r-2".to_string()), Some(3200)));

        let pos_c: i64 = slices[0]
            .query_row(
                "SELECT position FROM positions WHERE stream = ?1 AND reader = ?2",
                rusqlite::params![format!("m:{ADDR_C}"), ADDR_C],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pos_c, 0);

        let stray: (String, Option<String>, Option<String>, i64) = slices[0]
            .query_row(
                "SELECT mail_id, addr, label, raw_line FROM timeline WHERE kind = 'stray_receipt'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            stray,
            (
                "ghost-mail".to_string(),
                Some(ADDR_C.to_string()),
                Some("r-7".to_string()),
                7,
            )
        );
    }

    #[test]
    fn coordinator_import_shards_streams_and_seen_ids_across_multiple_slices() {
        let dir = TestDir::new("coord-sharding");
        let log_path = dir.path("coord.log");
        let recipients = ["rcpt-0", "rcpt-1", "rcpt-2", "rcpt-3", "rcpt-4", "rcpt-5"];
        let mut lines = vec![format!(
            r#"{{"ev":"session","session":"{ADDR_A}","host":"h1","at_ms":1}}"#
        )];
        for (idx, r) in recipients.iter().enumerate() {
            lines.push(format!(
                r#"{{"ev":"mail","mail":"id-{idx}","from":"{ADDR_A}","to":"{r}","body":"b-{idx}"}}"#
            ));
            lines.push(format!(
                r#"{{"ev":"receipt","mail":"id-{idx}","session":"{r}","record":"r-1"}}"#
            ));
        }
        lines.push(format!(
            r#"{{"ev":"broadcast","mail":"bcast-multi","from":"{ADDR_A}","body":"hi","to":["rcpt-0","rcpt-3"]}}"#
        ));
        fs::write(&log_path, lines.join("\n") + "\n").unwrap();

        let mut slices = coord_slices(4);
        assert!(coord_log(&log_path, &mut slices).unwrap());
        assert!(is_already_imported(&slices[0]).unwrap());

        let mut used_stream_slices = HashSet::new();
        let mut used_id_slices = HashSet::new();
        for (idx, r) in recipients.iter().enumerate() {
            let mail_id = format!("id-{idx}");
            let stream = format!("m:{r}");
            let expected_stream_slice =
                (crate::sha256::hash_pair(stream.as_bytes()).0 as usize) % 4;
            let expected_id_slice = (crate::sha256::hash_pair(mail_id.as_bytes()).0 as usize) % 4;
            used_stream_slices.insert(expected_stream_slice);
            used_id_slices.insert(expected_id_slice);

            for (s_idx, conn) in slices.iter().enumerate() {
                let env_cnt: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM envelopes WHERE mail_id = ?1",
                        rusqlite::params![&mail_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                let rcpt_cnt: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM receipts WHERE mail_id = ?1",
                        rusqlite::params![&mail_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                let seen_cnt: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM seen_ids WHERE mail_id = ?1",
                        rusqlite::params![&mail_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(env_cnt, i64::from(s_idx == expected_stream_slice));
                assert_eq!(rcpt_cnt, i64::from(s_idx == expected_stream_slice));
                assert_eq!(seen_cnt, i64::from(s_idx == expected_id_slice));
            }
        }
        assert!(used_stream_slices.len() > 1);
        assert!(used_id_slices.len() > 1);

        let bcast_slice = (crate::sha256::hash_pair(b"bcast-multi").0 as usize) % 4;
        let bcast_cnt: i64 = slices[bcast_slice]
            .query_row(
                "SELECT COUNT(*) FROM broadcasts WHERE mail_id = 'bcast-multi'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bcast_cnt, 1);
    }

    #[test]
    fn daemon_import_replays_legacy_wire_kinds_settles_receipts_and_preserves_file() {
        let dir = TestDir::new("daemon-import");
        let log_path = dir.path("daemon.log");
        let head = |m: &str, body: &str| {
            format!(
                r#"{{"ev":"deliver","mail":"{m}","from":"{ADDR_A}","to":"{ADDR_B}","body":"{body}""#
            )
        };
        let old_bcast = format!("{},\"wake\":false}}", head("d-bcast", "b1")); // legacy-wire: old false flag
        let old_dir = format!("{},\"wake\":true}}", head("d-dir-old", "b2")); // legacy-wire: old true flag
        let lines = [
            old_bcast,
            old_dir,
            format!(
                r#"{{"ev":"deliver","mail":"d-plain","from":"{ADDR_A}","to":"{ADDR_B}","body":"first"}}"#
            ),
            // Duplicate deliver for d-plain: first occurrence wins!
            format!(
                r#"{{"ev":"deliver","mail":"d-plain","from":"{ADDR_A}","to":"{ADDR_B}","body":"second-ignored"}}"#
            ),
            format!(
                r#"{{"ev":"deliver","mail":"d-acked","from":"{ADDR_A}","to":"{ADDR_B}","body":"settled","kind":"directed","headers":{{"z":"1","a":"2"}}}}"#
            ),
            "corrupt non-json daemon line".to_string(),
            format!(r#"{{"ev":"receipt","mail":"d-acked","session":"{ADDR_B}","record":"r-10"}}"#),
            // Duplicate receipt for d-acked: first receipt wins!
            format!(r#"{{"ev":"receipt","mail":"d-acked","session":"{ADDR_B}","record":"r-99"}}"#),
            // Receipt before any deliver line: stays in unconfirmed_receipts and suppresses later deliver.
            format!(r#"{{"ev":"receipt","mail":"d-early","session":"{ADDR_C}","record":"r-20"}}"#),
            format!(
                r#"{{"ev":"deliver","mail":"d-early","from":"{ADDR_A}","to":"{ADDR_C}","body":"already-acked"}}"#
            ),
        ];
        let raw = lines.join("\n")
            + "\n"
            + &format!(
                r#"{{"ev":"deliver","mail":"d-torn","from":"{ADDR_A}","to":"{ADDR_B}","body":"cut"}}"#
            );
        fs::write(&log_path, raw.as_bytes()).unwrap();
        let before = fs::read(&log_path).unwrap();

        let mut conn = daemon_conn();
        assert!(daemon_log(&log_path, &mut conn).unwrap());
        assert_eq!(
            fs::read(&log_path).unwrap(),
            before,
            "legacy daemon.log must stay byte-identical"
        );

        let pending: Vec<(String, String, String, String)> = conn
            .prepare(
                "SELECT mail_id, body, kind, headers_json
                 FROM pending_deliveries
                 ORDER BY seq ASC",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            pending,
            vec![
                (
                    "d-bcast".into(),
                    "b1".into(),
                    "broadcast".into(),
                    "{}".into()
                ),
                (
                    "d-dir-old".into(),
                    "b2".into(),
                    "directed".into(),
                    "{}".into()
                ),
                (
                    "d-plain".into(),
                    "first".into(),
                    "directed".into(),
                    "{}".into()
                ),
            ]
        );

        let receipts: Vec<(String, String, String)> = conn
            .prepare(
                "SELECT mail_id, session, record_id
                 FROM unconfirmed_receipts
                 ORDER BY seq ASC",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            receipts,
            vec![
                ("d-acked".into(), ADDR_B.into(), "r-10".into()),
                ("d-early".into(), ADDR_C.into(), "r-20".into()),
            ]
        );
    }
}

import { DatabaseSync } from 'node:sqlite';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/** Session-address field name used by schema 2 and older packages in queued registration JSON. */
const LEGACY_SESSION_FIELD = 'piSessionId';

/**
 * Schema 3 -> 2 inside an open transaction: restore the schema 2 evidence column
 * names and the legacy session-address field in queued registrations.
 */
function downgradeSchema3(db: DatabaseSync): void {
  db.exec(`
    DROP VIEW messages;
    ALTER TABLE inbox_messages RENAME COLUMN evidence_record_id TO receipt_entry_id;
    ALTER TABLE inbox_messages RENAME COLUMN evidence_location TO receipt_session_file;
    CREATE VIEW messages AS SELECT * FROM inbox_messages WHERE 0;
  `);
  const update = db.prepare('UPDATE inbox_registrations SET registration_json = ? WHERE session_key = ?');
  for (const row of db.prepare('SELECT session_key, registration_json FROM inbox_registrations').all() as Array<{ session_key: string; registration_json: string }>) {
    let value: unknown;
    try { value = JSON.parse(row.registration_json); } catch { continue; }
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const session = (value as { session?: unknown }).session;
    if (!session || typeof session !== 'object' || Array.isArray(session)) continue;
    const { namespace, sessionId, ...rest } = session as Record<string, unknown>;
    if (sessionId === undefined || Object.keys(rest).length) continue;
    update.run(JSON.stringify({ ...(value as Record<string, unknown>), session: { namespace, [LEGACY_SESSION_FIELD]: sessionId } }), row.session_key);
  }
  db.exec('PRAGMA user_version = 2');
}

function withInbox(path: string, work: (db: DatabaseSync, version: number) => void): void {
  const lock = new DatabaseSync(`${path}.lock`);
  let db: DatabaseSync | undefined;
  try {
    lock.exec('PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE');
    db = new DatabaseSync(path);
    db.exec('PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE');
    try {
      work(db, db.prepare('PRAGMA user_version').get()!.user_version as number);
      db.exec('COMMIT');
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  } finally { db?.close(); lock.close(); }
}

/** Requires all legacy runtimes to be stopped. Preserves post-cutover rows in place. */
export function rollbackInbox(path: string) {
  withInbox(path, (db, version) => {
    if (version !== 2 && version !== 3) throw new Error('Expected inbox schema version 2 or 3');
    if (version === 3) downgradeSchema3(db);
    db.exec(`
      DROP VIEW messages;
      DROP VIEW leases;
      DROP VIEW registrations;
      DROP INDEX inbox_messages_due;
      DROP INDEX inbox_messages_acknowledged;
      DELETE FROM inbox_leases;
      ALTER TABLE inbox_messages RENAME TO messages;
      ALTER TABLE inbox_leases RENAME TO leases;
      ALTER TABLE inbox_registrations RENAME TO registrations;
      CREATE INDEX messages_due ON messages(session_key, receipt_entry_id, sequence);
      CREATE INDEX messages_acknowledged ON messages(acknowledged_at);
      PRAGMA user_version = 0;
    `);
  });
}

/** Schema 3 -> 2, for a daemon package from before the agent-neutral contract. Requires that daemon and its clients to be stopped. */
export function rollbackInboxContract(path: string) {
  withInbox(path, (db, version) => {
    if (version !== 3) throw new Error('Expected inbox schema version 3');
    downgradeSchema3(db);
    db.exec('DELETE FROM inbox_leases');
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [flag, path] = process.argv.slice(2);
  if (process.argv.length !== 4 || !path || (flag !== '--legacy-clients-stopped' && flag !== '--contract-clients-stopped'))
    throw new Error('Usage: node deploy/rollback-inbox.ts --legacy-clients-stopped|--contract-clients-stopped inbox-path');
  if (flag === '--legacy-clients-stopped') rollbackInbox(resolve(path));
  else rollbackInboxContract(resolve(path));
  console.log('Inbox rollback preserved messages, receipts, and queued registrations.');
}

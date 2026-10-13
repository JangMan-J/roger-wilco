import { DatabaseSync } from 'node:sqlite';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/** Requires all legacy runtimes to be stopped. Preserves post-cutover rows in place. */
export function rollbackInbox(path: string) {
  const lock = new DatabaseSync(`${path}.lock`);
  let db: DatabaseSync | undefined;
  try {
    lock.exec('PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE');
    db = new DatabaseSync(path);
    db.exec('PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE');
    try {
      if (db.prepare('PRAGMA user_version').get()!.user_version !== 2) throw new Error('Expected inbox schema version 2');
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
        COMMIT;
      `);
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  } finally { db?.close(); lock.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.length !== 4 || process.argv[2] !== '--legacy-clients-stopped')
    throw new Error('Usage: node deploy/rollback-inbox.ts --legacy-clients-stopped inbox-path');
  rollbackInbox(resolve(process.argv[3]!));
  console.log('Inbox rollback preserved messages, receipts, and queued registrations.');
}

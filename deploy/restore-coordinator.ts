import { DatabaseSync } from 'node:sqlite';
import { randomBytes } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * After restoring a coordinator database from a backup, with the coordinator stopped:
 * raise the message id watermark to at least `floor` (the highest `highWater` any host
 * printed from `roger-wilco-host --reset-bookmark`), so no stored id is issued again for
 * a different message, and rotate `store_config.stream_id`, so every bookmark taken
 * against the old log is refused. One FULL transaction.
 */
export function restoreCoordinator(path: string, floor: number): { tail: number; stream: string } {
  if (!Number.isSafeInteger(floor) || floor < 0) throw new Error('Sequence floor must be a safe integer >= 0');
  const db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL; BEGIN IMMEDIATE');
    try {
      if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'messages'").get())
        throw new Error('Not a coordinator database');
      const raised = db.prepare("UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = 'messages'").run(floor);
      if (Number(raised.changes) === 0) db.prepare("INSERT INTO sqlite_sequence (name, seq) VALUES ('messages', ?)").run(floor);
      const columns = db.prepare('PRAGMA table_info(store_config)').all() as { name: string }[];
      if (!columns.some(column => column.name === 'stream_id')) db.exec('ALTER TABLE store_config ADD COLUMN stream_id TEXT');
      const stream = randomBytes(16).toString('hex');
      db.prepare('UPDATE store_config SET stream_id = ?').run(stream);
      const { tail } = db.prepare("SELECT seq AS tail FROM sqlite_sequence WHERE name = 'messages'").get() as { tail: number };
      db.exec('COMMIT');
      return { tail, stream };
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  } finally { db.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [flag, value, path] = process.argv.slice(2);
  if (process.argv.length !== 5 || flag !== '--sequence-floor' || !value || !/^(0|[1-9]\d*)$/.test(value) || !path)
    throw new Error('Usage: node deploy/restore-coordinator.ts --sequence-floor N COORDINATOR-DB-PATH (coordinator stopped)');
  console.log(JSON.stringify(restoreCoordinator(resolve(path), Number(value))));
}

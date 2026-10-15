import { DatabaseSync } from 'node:sqlite';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * Host stream v2 -> v1 for one host, with that host's daemon stopped. Every message owed
 * with seq <= C is stored on the host (C <= D), so each cursor may move up to C. Deleting
 * the hosts row lets the v1 stream and v1 receipts serve the host again. Returns the
 * removed C, or null when the host had no hosts row.
 */
export function rollbackHostV2(path: string, hostId: string): { acked: number | null } {
  if (!hostId.trim()) throw new Error('Host ID is required');
  const db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL; BEGIN IMMEDIATE');
    try {
      if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hosts'").get())
        throw new Error('Coordinator database has no hosts table');
      const row = db.prepare('SELECT acked FROM hosts WHERE host_id = ?').get(hostId) as { acked: number } | undefined;
      if (row) {
        db.prepare('UPDATE sessions SET cursor = MAX(cursor, ?), offered = NULL WHERE host_id = ?').run(row.acked, hostId);
        db.prepare('DELETE FROM hosts WHERE host_id = ?').run(hostId);
      }
      db.exec('COMMIT');
      return { acked: row?.acked ?? null };
    } catch (error) { db.exec('ROLLBACK'); throw error; }
  } finally { db.close(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [flag, hostId, path] = process.argv.slice(2);
  if (process.argv.length !== 5 || flag !== '--host' || !hostId || !path)
    throw new Error('Usage: node deploy/rollback-host-v2.ts --host HOST-ID COORDINATOR-DB-PATH (coordinator and that host daemon stopped)');
  console.log(JSON.stringify({ host: hostId, ...rollbackHostV2(resolve(path), hostId) }));
}

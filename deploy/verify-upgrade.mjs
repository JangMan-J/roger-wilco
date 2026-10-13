import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

const name = process.argv[2];
if (!name || !/^[a-z0-9-]+$/.test(name)) throw new Error('Usage: node deploy/verify-upgrade.mjs snapshot-name');
const home = homedir();
const root = join(home, '.local/share/roger-wilco');
const config = JSON.parse(await readFile(join(home, '.config/roger-wilco/host.json'), 'utf8'));
const pathOf = path => path.startsWith('~/') ? join(home, path.slice(2)) : resolve(path);
const current = new DatabaseSync(pathOf(config.inboxPath), { readOnly: true });
const backup = new DatabaseSync(join(root, name, 'host.sqlite'), { readOnly: true });
let report;
try {
  assert.equal(current.prepare('PRAGMA integrity_check').get().integrity_check, 'ok');
  assert.equal(current.prepare('PRAGMA user_version').get().user_version, 3);
  for (const table of ['messages', 'leases', 'registrations']) {
    assert.equal(current.prepare('SELECT type FROM sqlite_schema WHERE name = ?').get(table).type, 'view');
    assert.equal(current.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
  }
  const backupVersion = backup.prepare('PRAGMA user_version').get().user_version;
  const oldTable = backupVersion >= 2 ? 'inbox_messages' : 'messages';
  // Schema 3 renamed the evidence columns; values are unchanged.
  const [oldRecord, oldLocation] = backupVersion >= 3 ? ['evidence_record_id', 'evidence_location'] : ['receipt_entry_id', 'receipt_session_file'];
  const before = backup.prepare(`SELECT *, ${oldRecord} AS old_record, ${oldLocation} AS old_location FROM ${oldTable}`).all();
  let preserved = 0, expired = 0, pendingBefore = 0;
  for (const row of before) {
    if (row.old_record === null) pendingBefore++;
    const after = current.prepare('SELECT * FROM inbox_messages WHERE session_key = ? AND message_id = ?')
      .get(row.session_key, row.message_id);
    if (!after && row.old_record !== null && typeof row.acknowledged_at === 'number' &&
        row.acknowledged_at < Date.now() - 7 * 86400_000) { expired++; continue; }
    assert.ok(after, `Missing retained delivery ${row.message_id}`);
    for (const key of ['sequence', 'message_json']) assert.equal(after[key], row[key], `Changed ${key} for ${row.message_id}`);
    if (row.old_record !== null) {
      assert.equal(after.evidence_record_id, row.old_record);
      assert.equal(after.evidence_location, row.old_location);
    }
    preserved++;
  }
  report = { checkedAt: new Date().toISOString(), hostId: config.hostId, snapshot: name,
    schema: 3, integrity: 'ok', legacyNames: 'empty read-only views',
    baselineMessages: before.length, pendingBefore, preservedMessages: preserved, expiredAcknowledgedMessages: expired };
} finally { current.close(); backup.close(); }
const output = join(root, 'installation-check');
await mkdir(output, { recursive: true, mode: 0o700 });
await writeFile(join(output, 'upgrade.json'), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
console.log(JSON.stringify(report));

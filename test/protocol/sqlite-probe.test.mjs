import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('Node SQLite named/positional parameters, numeric IDs, WAL reopen', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-sqlite-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'store.sqlite');
  const db = new DatabaseSync(path);
  try {
    assert.equal(db.prepare('PRAGMA journal_mode = WAL').get().journal_mode, 'wal');
    db.exec('CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, recipient TEXT NOT NULL)');
    const result = db.prepare('INSERT INTO messages (recipient) VALUES ($recipient)').run({ $recipient: 'alice' });
    assert.equal(typeof result.lastInsertRowid, 'number');
    assert.equal(result.lastInsertRowid, 1);
    assert.deepEqual({ ...db.prepare('SELECT id, recipient FROM messages WHERE id = ?').get(1) }, { id: 1, recipient: 'alice' });
    db.prepare('INSERT INTO messages (id, recipient) VALUES (?, ?)').run(2, 'bob');
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM messages').get().count, 2);
  } finally { db.close(); }
  const reopened = new DatabaseSync(path);
  try {
    assert.deepEqual(reopened.prepare('SELECT id, recipient FROM messages ORDER BY id').all().map(row => ({ ...row })), [{ id: 1, recipient: 'alice' }, { id: 2, recipient: 'bob' }]);
    console.log(JSON.stringify({ adapter: 'node:sqlite DatabaseSync', wal: true, lastInsertRowidType: 'number', reopenedRows: 2 }));
  } finally { reopened.close(); }
});

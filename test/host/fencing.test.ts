import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { createDaemonInbox as createHostInbox } from '../../src/host/index.ts';
import { rollbackInbox } from '../../deploy/rollback-inbox.ts';
import { sessionKey, type Message, type Outcome, type PiReceipt, type SessionAddress, type StreamFrame } from '../../src/protocol/index.ts';

const root = fileURLToPath(new URL('../..', import.meta.url));
const protocol = new URL('../../src/protocol/index.ts', import.meta.url).href;
const legacySource = stripTypeScriptTypes(execFileSync('git', ['show', 'edfe56e:src/host/index.ts'], {
  cwd: root, encoding: 'utf8',
})).replaceAll("'../protocol/index.ts'", JSON.stringify(protocol));
type LegacyInbox = {
  receive(frame: StreamFrame): Promise<{ inserted: boolean }>;
  bind(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>>;
  renew(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>>;
  release(session: SessionAddress, runtimeId: string): Promise<void>;
  pending(session: SessionAddress, runtimeId: string): Promise<Message[]>;
  acknowledgePi(session: SessionAddress, runtimeId: string, proof: PiReceipt): Promise<Outcome<null>>;
  enqueueRegistration(registration: { session: SessionAddress; displayName?: string }): Promise<void>;
  close(): Promise<void>;
};
const legacyModule = await import(`data:text/javascript,${encodeURIComponent(legacySource)}`) as {
  createHostInbox(path: string): LegacyInbox;
};

const session: SessionAddress = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440000' };
const key = sessionKey(session);
const message: Message = {
  version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: 123, sender: key, recipient: key,
  body: 'kept', data: null, visibility: 'private',
};
const frame: StreamFrame = { type: 'message', subscriptionId: key, message };
const proof = { messageId: message.id, piSessionId: session.piSessionId, entryId: 'entry-1', sessionFile: 'session.jsonl' };

function temporaryDir(t: { after(fn: () => void): void }): string {
  const dir = mkdtempSync(join(tmpdir(), 'roger-fence-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}
function cell(db: DatabaseSync, sql: string): unknown {
  const row = db.prepare(sql).get();
  return row ? Object.values(row)[0] : undefined;
}

test('open legacy inbox cannot mutate daemon tables after migration', async t => {
  const path = join(temporaryDir(t), 'inbox.sqlite');
  const legacy = legacyModule.createHostInbox(path);
  t.after(() => legacy.close());
  assert.deepEqual(await legacy.receive(frame), { inserted: true });
  assert.equal((await legacy.bind(session, 'old', Date.now())).ok, true);
  await legacy.enqueueRegistration({ session, displayName: 'kept' });
  const writer = new DatabaseSync(path);
  t.after(() => writer.close());
  const insert = writer.prepare('INSERT INTO messages (session_key, message_id, sequence, message_json) VALUES (?, ?, ?, ?)');
  const update = writer.prepare('UPDATE messages SET receipt_entry_id = ? WHERE message_id = ?');
  const remove = writer.prepare('DELETE FROM registrations');
  writer.exec('BEGIN');
  assert.equal(cell(writer, 'SELECT count(*) FROM messages'), 1);
  const inbox = createHostInbox(path);
  t.after(() => inbox.close());
  // An old WAL reader can retain its snapshot, but cannot upgrade it to a
  // writable transaction after the schema fence commits.
  assert.equal(cell(writer, 'SELECT count(*) FROM messages'), 1);
  assert.throws(() => update.run('stale-snapshot', message.id), /locked/);
  writer.exec('ROLLBACK');
  assert.deepEqual(await legacy.pending(session, 'old'), []);
  await assert.rejects(legacy.bind(session, 'other', Date.now()), /view/);
  assert.equal((await legacy.renew(session, 'old', Date.now())).ok, false);
  assert.equal((await legacy.acknowledgePi(session, 'old', proof)).ok, false);
  await assert.rejects(legacy.enqueueRegistration({ session, displayName: 'changed' }), /view/);
  await assert.rejects(legacy.receive({ ...frame, message: { ...message, id: 'fleet:msg:2', sequence: 2 } }), /view/);
  await assert.rejects(legacy.release(session, 'old'), /view/);
  assert.throws(() => insert.run(key, 'fleet:msg:9', 9, '{}'), /view/);
  assert.throws(() => update.run('entry-x', message.id), /view/);
  assert.throws(() => remove.run(), /view/);
  assert.throws(() => legacyModule.createHostInbox(path), /view/);
  assert.throws(() => createHostInbox(path), /locked/);
  assert.equal(cell(writer, 'PRAGMA user_version'), 2);
  assert.deepEqual(writer.prepare("SELECT name FROM sqlite_schema WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name").all()
    .map(row => Object.values(row)[0]), ['inbox_messages_acknowledged', 'inbox_messages_due']);
  for (const name of ['messages', 'leases', 'registrations']) {
    const row = writer.prepare('SELECT type, sql FROM sqlite_schema WHERE name = ?').get(name)!;
    assert.equal(row.type, 'view');
    assert.match(String(row.sql), /inbox_.* WHERE 0/);
  }
  assert.equal(cell(writer, 'SELECT COUNT(*) FROM messages'), 0);
  assert.equal(cell(writer, 'SELECT COUNT(*) FROM leases'), 0);
  assert.equal(cell(writer, 'SELECT COUNT(*) FROM registrations'), 0);
  assert.equal(cell(writer, 'SELECT COUNT(*) FROM inbox_messages'), 1);
  assert.equal(cell(writer, 'SELECT message_json FROM inbox_messages'), JSON.stringify(message));
  assert.equal(cell(writer, 'SELECT receipt_entry_id FROM inbox_messages'), null);
  assert.equal(cell(writer, 'SELECT COUNT(*) FROM inbox_leases'), 0);
  assert.equal(cell(writer, 'SELECT registration_json FROM inbox_registrations'), JSON.stringify({ session, displayName: 'kept' }));
  assert.equal(existsSync(`${path}.lock`), true);
  await inbox.close();
  const reopened = createHostInbox(path);
  t.after(() => reopened.close());
  assert.equal((await reopened.bind(session, 'new', Date.now())).ok, true);
  assert.deepEqual((await reopened.pending(session, 'new')).map(item => item.id), [message.id]);
});

test('unsupported schema version fails closed and does not rewrite', async t => {
  const path = join(temporaryDir(t), 'inbox.sqlite');
  const db = new DatabaseSync(path);
  db.exec("PRAGMA user_version = 1; CREATE TABLE messages (session_key TEXT); INSERT INTO messages (session_key) VALUES ('keep')");
  db.close();
  assert.throws(() => createHostInbox(path), /Unsupported inbox schema version: 1/);
  assert.throws(() => createHostInbox(path), /Unsupported inbox schema version: 1/);
  const check = new DatabaseSync(path, { readOnly: true });
  try {
    assert.equal(cell(check, 'PRAGMA user_version'), 1);
    assert.equal(cell(check, 'SELECT session_key FROM messages'), 'keep');
    assert.equal(cell(check, "SELECT name FROM sqlite_schema WHERE name = 'inbox_messages'"), undefined);
    assert.equal(cell(check, "SELECT type FROM sqlite_schema WHERE name = 'messages'"), 'table');
  } finally { check.close(); }
});

test('transport rollback preserves post-cutover receipts and queued work', async t => {
  const path = join(temporaryDir(t), 'inbox.sqlite');
  const inbox = createHostInbox(path);
  assert.throws(() => rollbackInbox(path), /locked/);
  await inbox.bind(session, 'owner', Date.now());
  await inbox.receive(frame);
  await inbox.receive({ ...frame, message: { ...message, id: 'fleet:msg:2', sequence: 2 } });
  assert.equal((await inbox.acknowledgePi(session, 'owner', proof)).ok, true);
  await inbox.enqueueRegistration({ session, displayName: 'post-cutover' });
  await inbox.close();
  rollbackInbox(path);
  const legacy = legacyModule.createHostInbox(path);
  try {
    assert.equal((await legacy.bind(session, 'rollback', Date.now())).ok, true);
    assert.deepEqual((await legacy.pending(session, 'rollback')).map(item => item.id), ['fleet:msg:2']);
    const check = new DatabaseSync(path, { readOnly: true });
    try {
      assert.equal(cell(check, "SELECT receipt_entry_id FROM messages WHERE message_id = 'fleet:msg:1'"), proof.entryId);
      assert.equal(cell(check, 'SELECT registration_json FROM registrations'), JSON.stringify({ session, displayName: 'post-cutover' }));
    } finally { check.close(); }
  } finally { await legacy.close(); }
});

test('a cached legacy Pi adapter cannot lazily open the new daemon writer', async t => {
  const dir = temporaryDir(t);
  const path = join(dir, 'inbox.sqlite');
  const configPath = join(dir, 'host.json');
  writeFileSync(configPath, JSON.stringify({ namespace: 'fleet', hostId: 'host-a',
    coordinatorUrl: 'http://127.0.0.1:1', credential: 'offline', inboxPath: path }));
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = configPath;
  let source = stripTypeScriptTypes(execFileSync('git', ['show', 'edfe56e:src/pi/index.ts'],
    { cwd: root, encoding: 'utf8' }));
  for (const relative of ['../protocol/index.ts', '../bin/config.ts', '../host/index.ts'])
    source = source.replaceAll(`'${relative}'`, JSON.stringify(new URL(`../../src/pi/${relative}`, import.meta.url).href));
  source = source.replaceAll("'typebox'", JSON.stringify(import.meta.resolve('typebox')));
  const legacy = await import(`data:text/javascript,${encodeURIComponent(source)}`) as { default(api: unknown): void };
  const handlers = new Map<string, (...args: any[]) => Promise<void>>();
  let status = '';
  legacy.default({
    on(name: string, handler: (...args: any[]) => Promise<void>) { handlers.set(name, handler); },
    registerTool() {}, registerCommand() {}, sendMessage() {},
  });
  const ctx = { sessionManager: { getSessionId: () => session.piSessionId,
    getSessionFile: () => join(dir, 'session.jsonl') }, hasUI: true,
    ui: { setStatus(_key: string, value: string | undefined) { status = value ?? ''; } } };
  try {
    await handlers.get('session_start')!({}, ctx);
    assert.match(status, /createHostInbox.*not a function/);
    assert.equal(existsSync(path), false, 'legacy adapter did not open or migrate SQLite');
    assert.equal(existsSync(`${path}.lock`), false, 'legacy adapter did not take the daemon lock');
    const daemon = createHostInbox(path);
    await daemon.close();
  } finally {
    await handlers.get('session_shutdown')!({}, ctx);
    if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG;
    else process.env.ROGER_WILCO_CONFIG = saved;
  }
});

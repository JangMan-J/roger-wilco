/**
 * Version selection, migration and rollback between protocol 1 and 2, against real
 * coordinators and daemons (docs/protocol-v2.md §Version negotiation, §Migration from v1;
 * test plan 33 end to end, 41 and 44). The v1 side is the tree at PRE_V2.
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createDaemonInbox } from '../../src/host/index.ts';
import { ROUTES, type Message, type StreamFrame } from '../../src/protocol/index.ts';
import {
  ROOT, TOKENS, address, bookmark, call, coordinator, coordinatorProcess, cursors, daemon, freePort, hostAcked, inboxRows, key,
  mutate, preV2Tree, query, register, send, stored, tempDir, waitFor, wire, type Wire,
} from './v2-harness.ts';

let w: Wire;
test.beforeEach(() => { w = wire(); });
test.afterEach(() => { w.restore(); });

function reports(t: test.TestContext): string[] {
  const seen: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { seen.push(args.map(String).join(' ')); });
  return seen;
}
const step = (from: number, to: number, by = 3) => Array.from({ length: Math.floor((to - from) / by) + 1 }, (_, i) => from + i * by);
const oldHost = async () => await import(join(preV2Tree(), 'src/host/index.ts')) as typeof import('../../src/host/index.ts') & {
  createDaemonInbox(path: string): { receive(frame: StreamFrame): Promise<{ inserted: boolean }>; close(): Promise<void> };
};
async function openStream(url: string, hostId: string): Promise<{ status: number; result: any }> {
  const response = await fetch(new URL(ROUTES.stream, url), { headers: { authorization: `Bearer ${TOKENS[hostId]}`, accept: 'text/event-stream' } });
  return { status: response.status, result: await response.json() };
}

test('against a v1 coordinator the daemon falls back to v1; after the coordinator upgrade it cuts over with messages in flight', async t => {
  reports(t);
  const dir = tempDir(t);
  const port = await freePort();
  const dbPath = join(dir, 'coordinator.db');
  const v1 = await coordinatorProcess(t, dir, { dbPath, port, bin: join(preV2Tree(), 'src/bin/coordinator.ts') });
  const [a1, sender] = [address(1), address(9)];
  await register(v1.url, 'host-a', a1);
  await register(v1.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', v1.url);
  const sent: Message[] = [];
  for (let i = 0; i < 3; i++) sent.push(await send(v1.url, 'host-b', sender, a1, `v1 ${i}`));
  await waitFor(() => cursors(dbPath).get(key(a1)) === 3, 'v1 receipts');
  const first = w.requests(ROUTES.streamV2, 'host-a')[0]!;
  assert.deepEqual([first.search, first.status], ['', 404], 'query-less /v2 gets 404 from a v1 coordinator');
  assert.ok(w.requests(ROUTES.stream, 'host-a').length >= 1, 'one v1 stream request followed');
  assert.deepEqual(bookmark(a.config.inboxPath), { stream: null, bookmark: null });

  // In flight: m4 stored on the host with its v1 receipt lost. v1 offers one unreceipted
  // message per session, so m5 and m6 wait at the coordinator.
  w.drop(item => item.path === ROUTES.receipt && item.host === 'host-a', 'request', Infinity);
  for (let i = 3; i < 6; i++) sent.push(await send(v1.url, 'host-b', sender, a1, `in flight ${i}`));
  await waitFor(() => (stored(a.config.inboxPath).get(key(a1)) ?? []).length === 4, 'm4 stored without a receipt');
  await delay(200);
  assert.equal(stored(a.config.inboxPath).get(key(a1))!.length, 4);
  assert.equal(cursors(dbPath).get(key(a1)), 3);

  // Coordinator first: the v2 binary opens the same file on the same port.
  await v1.stop();
  w.reset();
  const c = await coordinator(t, dir, {}, { dbPath, port });
  await waitFor(() => hostAcked(dbPath, 'host-a') === 6, 'cut-over and receipt through the replay');
  sent.push(await send(c.url, 'host-b', sender, a1, 'after the cut-over'));
  await waitFor(() => hostAcked(dbPath, 'host-a') === 7, 'v2 delivery');
  const opened = () => w.requests(ROUTES.streamV2, 'host-a').filter(item => item.status === 200);
  assert.deepEqual(opened().map(item => item.search), [''], 'no bookmark yet: no query');
  assert.equal(w.v2('host-a', 'hello')[0]!.resume, 3, 'C0 is the v1 cursor');
  assert.deepEqual(w.v2('host-a', 'msg').map(f => f.seq), [4, 5, 6, 7], 'the in-flight messages are replayed');
  assert.equal(cursors(dbPath).get(key(a1)), 3, 'the cursor is frozen as the start');
  assert.equal(bookmark(a.config.inboxPath).bookmark, 7);
  const receipt = await call(c.url, 'host-a', ROUTES.receipt, { subscriptionId: key(a1), messageId: sent[6]!.id });
  assert.deepEqual([receipt.status, receipt.result.message], [409, 'Host uses protocol 2']);
  const stream = await openStream(c.url, 'host-a');
  assert.deepEqual([stream.status, stream.result.message], [409, 'Host uses protocol 2']);
  sent.push(await send(c.url, 'host-b', sender, a1, 'after the refused v1 open'));
  await waitFor(() => hostAcked(dbPath, 'host-a') === 8, 'the same v2 stream delivers');
  assert.equal(opened().length, 1, 'the refused v1 open did not supersede the v2 stream');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sent.map(m => m.sequence), 'each message once');
});

test('cut-over from skewed v1 cursors: each session holds exactly its owed set, nothing at or below its cursor returns (test 41)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [s1, s2, s3, x] = [address(1), address(2), address(3), address(9)];
  for (const s of [s1, s2, s3]) await register(c.url, 'host-a', s);
  await register(c.url, 'host-b', x);
  const recipientOf = (k: number) => [s3, s1, s2][k % 3]!;
  const sent: Message[] = [];
  for (let k = 1; k <= 100; k++) {
    sent.push(k === 70 ? await send(c.url, 'host-a', s1, '*', 'broadcast 70 from s1') : await send(c.url, 'host-b', x, recipientOf(k), `m${k}`));
  }
  assert.deepEqual(sent.map(m => m.sequence), step(1, 100, 1));
  // v1 state on the coordinator: receipted cursors 10, 50 and 90; s1 has 13 offered.
  for (const [s, cursor] of [[s1, 10], [s2, 50], [s3, 90]] as const) mutate(c.dbPath, 'UPDATE sessions SET cursor = ? WHERE id = ?', cursor, key(s));
  mutate(c.dbPath, 'UPDATE sessions SET offered = 13 WHERE id = ?', key(s1));
  // v1 state on the host, written by the pre-v2 daemon's inbox (schema 3).
  const inboxPath = join(dir, 'host-a', 'inbox.sqlite');
  const old = await oldHost();
  const legacy = old.createDaemonInbox(inboxPath);
  const local = { [key(s1)]: step(1, 10), [key(s2)]: step(2, 59), [key(s3)]: step(3, 99).filter(k => k <= 10) };
  for (const [session, ids] of Object.entries(local)) {
    for (const k of ids) await legacy.receive({ type: 'message', subscriptionId: session, message: sent[k - 1]! });
  }
  await legacy.close();
  // Every stored row is acknowledged: s2's 53, 56 and 59 are the k = 3 rows whose cursor update
  // rolled back; s3's rows 11-90 were acknowledged and swept; s1's 13 was offered, not stored.
  const db = new DatabaseSync(inboxPath);
  db.prepare("UPDATE inbox_messages SET evidence_record_id = 'r-' || message_id, evidence_location = 'loc', acknowledged_at = ?").run(Date.now());
  db.close();

  const a = daemon(t, dir, 'host-a', c.url);
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 100, 'cut-over and delivery');
  assert.equal(w.v2('host-a', 'hello')[0]!.resume, 10, 'C0 = MIN(cursor)');
  const owed = (s: string, cursor: number) => sent.filter(m => m.sequence > cursor &&
    (m.recipient === s || (m.recipient === '*' && m.sender !== s))).map(m => m.sequence);
  const expected = new Map([
    [key(s1), [...local[key(s1)]!, ...owed(key(s1), 10)]],
    [key(s2), [...new Set([...local[key(s2)]!, ...owed(key(s2), 50)])].sort((p, q) => p - q)],
    [key(s3), [...local[key(s3)]!, ...owed(key(s3), 90)]],
  ]);
  assert.deepEqual(new Map([...stored(inboxPath)].sort()), new Map([...expected].sort()));
  assert.ok(!expected.get(key(s1))!.includes(70), 's1 lacks its own broadcast');
  assert.ok(expected.get(key(s2))!.includes(70));
  assert.deepEqual(owed(key(s3), 90), [93, 96, 99]);
  const frames = w.v2('host-a', 'msg');
  assert.deepEqual(frames.filter(f => f.message.recipient === key(s2) && f.seq <= 59).map(f => f.seq), [53, 56, 59], 'the k re-sent rows');
  assert.ok(!frames.some(f => f.message.recipient === key(s3) && f.seq <= 90), 'nothing at or below s3 cursor is sent');
  const rows = inboxRows(inboxPath);
  for (const k of [53, 56, 59]) {
    const row = rows.find(r => r.session_key === key(s2) && r.sequence === k)!;
    assert.equal(row.evidence_record_id, `r-fleet:msg:${k}`, 'the deduplicated row keeps its acknowledgement');
  }
  assert.equal(rows.filter(r => r.session_key === key(s2)).length, expected.get(key(s2))!.length, 'no duplicate rows');
});

test('rollback v2 -> v1 with both scripts loses nothing; the pre-v2 coordinator and daemon serve the host again (test 44)', async t => {
  reports(t);
  const dir = tempDir(t);
  const port = await freePort();
  const c = await coordinator(t, dir, {}, { port });
  const [a1, sender] = [address(1), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', c.url);
  const sent: Message[] = [];
  for (let i = 0; i < 3; i++) sent.push(await send(c.url, 'host-b', sender, a1, `v2 ${i}`));
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 3, 'v2 receipts');
  await a.stop();
  for (let i = 3; i < 5; i++) sent.push(await send(c.url, 'host-b', sender, a1, `while stopped ${i}`));
  await c.stop();
  const rolled = JSON.parse(execFileSync(process.execPath, [join(ROOT, 'deploy/rollback-host-v2.ts'), '--host', 'host-a', c.dbPath], { encoding: 'utf8' }));
  assert.deepEqual(rolled, { host: 'host-a', acked: 3 });
  execFileSync(process.execPath, [join(ROOT, 'deploy/rollback-inbox.ts'), '--stream-clients-stopped', a.config.inboxPath], { encoding: 'utf8' });
  assert.equal(query<{ user_version: number }>(a.config.inboxPath, 'PRAGMA user_version')[0]!.user_version, 3);
  assert.equal(cursors(c.dbPath).get(key(a1)), 3, 'cursor = MAX(cursor, acked)');
  // Every host is rolled back, so the older coordinator binary may run.
  const v1 = await coordinatorProcess(t, dir, { dbPath: c.dbPath, port, bin: join(preV2Tree(), 'src/bin/coordinator.ts') });
  const old = await oldHost();
  const inbox = old.createDaemonInbox(a.config.inboxPath);
  const legacy = old.createHostDaemon(a.config, inbox as never);
  const running = legacy.start(new AbortController().signal);
  t.after(async () => { await legacy.stop(); await running; await inbox.close(); });
  sent.push(await send(v1.url, 'host-b', sender, a1, 'after the rollback'));
  await waitFor(() => cursors(c.dbPath).get(key(a1)) === 6, 'v1 delivery and receipts');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sent.map(m => m.sequence), 'nothing lost, nothing duplicated');
});

test('a daemon left on v2 after a coordinator-only rollback cuts over again with C0 <= after, with no loss (test 44)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, sender] = [address(1), address(2), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-a', a2);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', c.url);
  const sent: Message[] = [await send(c.url, 'host-b', sender, a1, 'one'), await send(c.url, 'host-b', sender, a2, 'two')];
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 2, 'v2 receipts');
  await a.stop();
  sent.push(await send(c.url, 'host-b', sender, '*', 'while stopped'));
  await c.stop();
  execFileSync(process.execPath, [join(ROOT, 'deploy/rollback-host-v2.ts'), '--host', 'host-a', c.dbPath]);
  assert.equal(hostAcked(c.dbPath, 'host-a'), undefined);
  await c.start();
  w.reset();
  a.start();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 3, 're-cut-over and delivery');
  const open = w.requests(ROUTES.streamV2, 'host-a')[0]!;
  assert.equal(open.search, `?stream=${bookmark(a.config.inboxPath).stream}&after=2`);
  assert.ok(w.v2('host-a', 'hello')[0]!.resume <= 2, 'C0 = min(MIN(cursor), after)');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [1, 3]);
  assert.deepEqual(stored(a.config.inboxPath).get(key(a2)), [2, 3]);
});

test('rows a pre-hash daemon stored after --contract-clients-stopped replay at the cut-over without an identity conflict', async t => {
  const seen = reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, b1] = [address(1), address(2)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', b1);
  // Host A's inbox was at schema 4, then rolled back to schema 2, which keeps the hash column.
  const inboxPath = join(dir, 'host-a', 'inbox.sqlite');
  await createDaemonInbox(inboxPath).close();
  execFileSync(process.execPath, [join(ROOT, 'deploy/rollback-inbox.ts'), '--contract-clients-stopped', inboxPath]);
  // A schema 2 daemon from before packet hashes stores message 1 with no hash; its v1 receipt is lost.
  const m1 = await send(c.url, 'host-b', b1, a1, 'one');
  const db = new DatabaseSync(inboxPath);
  db.prepare('INSERT INTO inbox_messages (session_key, message_id, sequence, message_json) VALUES (?, ?, ?, ?)')
    .run(key(a1), m1.id, m1.sequence, JSON.stringify(m1));
  db.close();
  assert.equal(cursors(c.dbPath).get(key(a1)), 0);
  const a = daemon(t, dir, 'host-a', c.url);
  assert.equal(a.config.inboxPath, inboxPath);
  const m2 = await send(c.url, 'host-b', b1, a1, 'two');
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === m2.sequence, 'message two delivered');
  assert.deepEqual(stored(inboxPath).get(key(a1)), [m1.sequence, m2.sequence]);
  assert.ok(inboxRows(inboxPath).every(row => row.hash !== null));
  assert.ok(!seen.some(text => text.includes('identity conflict')), seen.join('\n'));
});

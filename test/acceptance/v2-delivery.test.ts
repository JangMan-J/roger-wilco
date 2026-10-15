/**
 * Host stream v2 end to end: the real coordinator and real host daemons over HTTP
 * (docs/protocol-v2.md: goals 2-4, test plan 16 and 22 with a real daemon, 42).
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { ROUTES } from '../../src/protocol/index.ts';
import {
  address, bookmark, call, coordinator, cursors, daemon, hostAcked, inboxRows, key, mutate, preV2Tree, query, register,
  send, sha256, stored, tempDir, waitFor, windowTap, wire, type Wire,
} from './v2-harness.ts';

let w: Wire;
test.beforeEach(() => { w = wire(); });
test.afterEach(() => { w.restore(); });

test('window W: exactly W msg frames before any receipt; cumulative receipts release the rest; each message stored once, in order', async t => {
  const tap = windowTap(4);
  t.after(() => tap.restore());
  const dir = tempDir(t);
  const c = await coordinator(t, dir, { window: 4 });
  const [s1, sender] = [address(1), address(9)];
  const start = await register(c.url, 'host-a', s1);
  await register(c.url, 'host-b', sender);
  const sent = [];
  for (let i = 0; i < 10; i++) sent.push(await send(c.url, 'host-b', sender, s1, `backlog ${i}`));
  const release = w.hold(item => item.path === ROUTES.receiptV2 && item.host === 'host-a');
  const a = daemon(t, dir, 'host-a', c.url);

  await waitFor(() => (stored(a.config.inboxPath).get(key(s1)) ?? []).length === 4, 'the first window');
  await delay(300);
  assert.equal(stored(a.config.inboxPath).get(key(s1))!.length, 4, 'no frame beyond W without a receipt');
  assert.equal(w.v2('host-a', 'msg').length, 4);
  assert.equal(w.requests(ROUTES.receiptV2, 'host-a').length, 1, 'one receipt in flight at most');
  assert.equal(hostAcked(c.dbPath, 'host-a'), start, 'C has not moved: the receipt is held');
  release();

  const last = sent.at(-1)!.sequence;
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === last, 'C reaches the last message');
  assert.deepEqual(stored(a.config.inboxPath).get(key(s1)), sent.map(m => m.sequence), 'every message once, in seq order');
  assert.equal(bookmark(a.config.inboxPath).bookmark, last);
  const frames = w.v2('host-a');
  assert.equal(frames[0]!.type, 'hello');
  assert.ok(frames.findIndex(f => f.type === 'member') < frames.findIndex(f => f.type === 'msg'), 'membership before any msg');
  const positions = frames.filter(f => f.type === 'msg' || f.type === 'through').map(f => f.seq);
  assert.deepEqual(positions, [...positions].sort((x, y) => x - y));
  assert.equal(new Set(positions).size, positions.length);
  // Cumulative receipts: strictly increasing, fewer than the messages they cover.
  const receipts = w.requests(ROUTES.receiptV2, 'host-a').map(item => item.body.through);
  assert.deepEqual(receipts, [...new Set(receipts)].sort((x, y) => x - y));
  assert.ok(receipts.length < sent.length, `receipts ${receipts}`);
  assert.equal(receipts.at(-1), last);
  assert.equal(tap.result.max, 4);
  assert.deepEqual(tap.result.violations, []);
});

test('a broadcast crosses the wire once per host; each host copies it to its members except the sender, with the coordinator bytes', async t => {
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, a3, b1, b2] = [address(1), address(2), address(3), address(4), address(5)];
  for (const s of [a1, a2, a3]) await register(c.url, 'host-a', s);
  for (const s of [b1, b2]) await register(c.url, 'host-b', s);
  const a = daemon(t, dir, 'host-a', c.url);
  const b = daemon(t, dir, 'host-b', c.url);
  await waitFor(() => bookmark(a.config.inboxPath).bookmark !== null && bookmark(b.config.inboxPath).bookmark !== null, 'both hellos');
  const receiptsBefore = { a: w.requests(ROUTES.receiptV2, 'host-a').length, b: w.requests(ROUTES.receiptV2, 'host-b').length };

  const broadcast = await send(c.url, 'host-a', a1, '*', 'Build is green.');
  await waitFor(() => inboxRows(a.config.inboxPath).length === 2 && inboxRows(b.config.inboxPath).length === 2, 'local fan-out');
  const [row] = query<{ hash: string }>(c.dbPath, 'SELECT hash FROM messages WHERE id = ?', broadcast.sequence);
  const json = JSON.stringify(broadcast);
  assert.equal(row!.hash, sha256(json));
  for (const [hostId, inbox, members] of [['host-a', a.config.inboxPath, [a2, a3]], ['host-b', b.config.inboxPath, [b1, b2]]] as const) {
    assert.equal(w.v2(hostId, 'msg').filter(f => f.message.id === broadcast.id).length, 1, `${hostId}: one frame`);
    const rows = inboxRows(inbox);
    assert.deepEqual(rows.map(r => r.session_key).sort(), members.map(key).sort(), `${hostId}: members except the sender`);
    for (const copy of rows) {
      assert.equal(copy.message_json, json, 'exact coordinator bytes');
      assert.equal(copy.hash, row!.hash);
      assert.equal(JSON.parse(copy.message_json).recipient, '*');
    }
  }
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === broadcast.sequence && hostAcked(c.dbPath, 'host-b') === broadcast.sequence, 'both receipts');
  assert.equal(w.requests(ROUTES.receiptV2, 'host-a').length - receiptsBefore.a, 1, 'one receipt on host A');
  assert.equal(w.requests(ROUTES.receiptV2, 'host-b').length - receiptsBefore.b, 1, 'one receipt on host B');
});

test('an expired session gets leave before later frames, and no copy of a later broadcast (test 22, real daemon)', async t => {
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, b1] = [address(1), address(2), address(3)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-a', a2);
  await register(c.url, 'host-b', b1);
  const a = daemon(t, dir, 'host-a', c.url);
  const first = await send(c.url, 'host-b', b1, '*', 'before expiry');
  await waitFor(() => inboxRows(a.config.inboxPath).length === 2, 'first broadcast to both members');
  // Expiry: last_seen_at beyond retention; the coordinator's maintenance (at open) deletes the row.
  mutate(c.dbPath, 'UPDATE sessions SET last_seen_at = 1 WHERE id = ?', key(a2));
  await c.restart();
  assert.equal(cursors(c.dbPath).has(key(a2)), false, 'maintenance expired the session');
  const second = await send(c.url, 'host-b', b1, '*', 'after expiry');
  await waitFor(() => (stored(a.config.inboxPath).get(key(a1)) ?? []).includes(second.sequence), 'second broadcast');
  await delay(100);
  assert.deepEqual(stored(a.config.inboxPath).get(key(a2)), [first.sequence], 'no copy after expiry');
  const frames = w.v2('host-a');
  const lastHello = frames.findLastIndex(f => f.type === 'hello');
  const after = frames.slice(lastHello);
  // The reconnect sends a full snapshot: a1 only. Within one connection the diff runs
  // against sentMembers, so a later expiry is a leave; here the snapshot already lacks a2.
  assert.deepEqual(after.filter(f => f.type === 'member').map(f => f.session), [key(a1)]);
  assert.ok(after.findIndex(f => f.type === 'member') < after.findIndex(f => f.type === 'msg'));

  // Expiry within one live connection: the session row goes between two batches.
  const a3 = address(4);
  await register(c.url, 'host-a', a3);
  const third = await send(c.url, 'host-b', b1, '*', 'with a3');
  await waitFor(() => (stored(a.config.inboxPath).get(key(a3)) ?? []).includes(third.sequence), 'a3 receives');
  mutate(c.dbPath, 'DELETE FROM sessions WHERE id = ?', key(a3));
  const fourth = await send(c.url, 'host-b', b1, '*', 'without a3');
  await waitFor(() => (stored(a.config.inboxPath).get(key(a1)) ?? []).includes(fourth.sequence), 'fourth broadcast');
  const tail = w.v2('host-a').slice(lastHello);
  const leave = tail.findIndex(f => f.type === 'leave' && f.session === key(a3));
  assert.ok(leave >= 0 && leave < tail.findIndex(f => f.type === 'msg' && f.seq === fourth.sequence), 'leave before the later msg');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a3)), [third.sequence]);
});

test('mixed fleet: a v1 host and a v2 host exchange directed messages and broadcasts; collection respects both (test 42)', async t => {
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const old = await import(`${preV2Tree()}/src/host/index.ts`) as typeof import('../../src/host/index.ts');
  const [a1, a2, b1, b2] = [address(1), address(2), address(3), address(4)];
  for (const s of [a1, a2]) await register(c.url, 'host-a', s);
  for (const s of [b1, b2]) await register(c.url, 'host-b', s);
  const aPath = `${dir}/host-a/inbox.sqlite`;
  const startV1 = () => {
    const inbox = old.createDaemonInbox(aPath);
    const instance = old.createHostDaemon({ coordinatorUrl: c.url, hostId: 'host-a', credential: 'token-a', inboxPath: aPath }, inbox);
    const done = instance.start(new AbortController().signal);
    return async () => { await instance.stop(); await done; await inbox.close(); };
  };
  let stopV1 = startV1();
  t.after(() => stopV1());
  const b = daemon(t, dir, 'host-b', c.url);
  const sent = [
    await send(c.url, 'host-a', a1, b1, 'v1 to v2 directed'),
    await send(c.url, 'host-b', b1, a2, 'v2 to v1 directed'),
    await send(c.url, 'host-a', a2, '*', 'v1 broadcast'),
    await send(c.url, 'host-b', b2, '*', 'v2 broadcast'),
  ];
  const owed = (session: string, rows: typeof sent) => rows.filter(m => m.recipient === session || (m.recipient === '*' && m.sender !== session))
    .map(m => m.sequence);
  const expect = (rows: typeof sent) => new Map([a1, a2, b1, b2].map(s => [key(s), owed(key(s), rows)]));
  const actual = () => new Map([...stored(aPath), ...stored(b.config.inboxPath)]);
  await waitFor(() => JSON.stringify([...actual()].sort()) === JSON.stringify([...expect(sent)].sort()), 'every owed message once');
  assert.equal(hostAcked(c.dbPath, 'host-a'), undefined, 'host A stays on v1');
  assert.ok(w.requests(ROUTES.stream, 'host-a').length >= 1 && w.requests(ROUTES.streamV2, 'host-a').length === 0);
  assert.equal(w.v2('host-b', 'msg').filter(f => f.message.recipient === '*').length, 2, 'one v2 frame per broadcast');
  await waitFor(() => cursors(c.dbPath).get(key(a1)) === sent[3]!.sequence && cursors(c.dbPath).get(key(a2)) === sent[3]!.sequence && hostAcked(c.dbPath, 'host-b') === sent[3]!.sequence, 'receipts');

  // Both hosts offline; three more messages owed to each side; everything is past retention.
  await stopV1();
  await b.stop();
  const pinned = [
    await send(c.url, 'host-b', b1, a1, 'owed to v1 host'),
    await send(c.url, 'host-a', a1, b2, 'owed to v2 host'),
    await send(c.url, 'host-a', a2, '*', 'owed to both'),
  ];
  // Eight days pass: the message hash covers its timestamp, so the clock moves, not the rows.
  // Registrations stay fresh, as presence or heartbeats would keep them.
  const days = 8 * 86_400_000;
  const realNow = Date.now;
  Date.now = () => realNow() + days;
  t.after(() => { Date.now = realNow; });
  mutate(c.dbPath, 'UPDATE sessions SET last_seen_at = last_seen_at + ?', days);
  await c.restart();
  const kept = query<{ id: number }>(c.dbPath, 'SELECT id FROM messages ORDER BY id').map(row => row.id);
  assert.deepEqual(kept, pinned.map(m => m.sequence), 'stored and receipted messages collected; owed ones kept on both hosts');
  stopV1 = startV1();
  b.start();
  const all = [...sent, ...pinned];
  await waitFor(() => JSON.stringify([...actual()].sort()) === JSON.stringify([...expect(all)].sort()), 'pinned messages delivered after restart');
  // v1 receipts and the v1 stream stay refused for the v2 host.
  const refused = await call(c.url, 'host-b', ROUTES.receipt, { subscriptionId: key(b2), messageId: pinned[1]!.id });
  assert.deepEqual([refused.status, refused.result.message], [409, 'Host uses protocol 2']);
});

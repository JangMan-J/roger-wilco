/**
 * Host stream v2 crash, reconnect and restore cases against the real coordinator and
 * real daemons (docs/protocol-v2.md §Crash and reconnect, §Coordinator restore; test
 * plan 35-40). Crash tests kill a child process with SIGKILL and reopen both SQLite files.
 */
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { copyFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { ROUTES, type Message } from '../../src/protocol/index.ts';
import {
  ROOT, TOKENS, address, bookmark, coordinator, coordinatorProcess, coordinatorTail, daemon, freePort, hostAcked, inboxRows, key,
  mutate, register, send, stored, tempDir, waitFor, wire, type Wire,
} from './v2-harness.ts';

let w: Wire;
test.beforeEach(() => { w = wire(); });
test.afterEach(() => { w.restore(); });

const crashChild = join(ROOT, 'test/host/stream-crash-child.mjs');
/** Runs the daemon in a child that kills itself with SIGKILL at `at`; resolves with its exit signal. */
async function crashDaemon(inboxPath: string, url: string, at: 'before-commit' | 'before-receipt' | 'hello', hostId = 'host-a'): Promise<string | null> {
  const child = spawn(process.execPath, [crashChild, inboxPath, url, at, hostId, TOKENS[hostId]!], { stdio: 'ignore' });
  const timer = setTimeout(() => child.kill('SIGTERM'), 15_000);
  const [, signal] = await new Promise<[number | null, string | null]>(resolve => child.once('exit', (code, sig) => resolve([code, sig])));
  clearTimeout(timer);
  return signal;
}
const sequences = (messages: Message[]) => messages.map(m => m.sequence);
/** Silences and records daemon reports. */
function reports(t: test.TestContext): string[] {
  const seen: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { seen.push(args.map(String).join(' ')); });
  return seen;
}

test('SIGKILL before the batch COMMIT: nothing stored, D and C unchanged; the restart stores every owed message once (test 35)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, sender] = [address(1), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', sender);
  const sent: Message[] = [];
  for (let i = 0; i < 5; i++) sent.push(await send(c.url, 'host-b', sender, a1, `m${i}`));
  const a = daemon(t, dir, 'host-a', c.url, { start: false });
  assert.equal(await crashDaemon(a.config.inboxPath, c.url, 'before-commit'), 'SIGKILL');
  assert.deepEqual(inboxRows(a.config.inboxPath), []);
  assert.equal(bookmark(a.config.inboxPath).bookmark, 0, 'only the hello committed');
  assert.equal(hostAcked(c.dbPath, 'host-a'), 0);
  a.start();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === sent.at(-1)!.sequence, 'C reaches the last message');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sequences(sent));
  assert.equal(w.requests(ROUTES.streamV2, 'host-a')[0]!.search, `?stream=${bookmark(a.config.inboxPath).stream}&after=0`);
});

test('SIGKILL after COMMIT, before the receipt: the reconnect sends after=N, resumes at C < N, skips the replay and C reaches N (test 36)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, sender] = [address(1), address(2), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-a', a2);
  await register(c.url, 'host-b', sender);
  const sent: Message[] = [];
  for (let i = 0; i < 4; i++) sent.push(await send(c.url, 'host-b', sender, i % 2 ? a2 : a1, `m${i}`));
  sent.push(await send(c.url, 'host-b', sender, '*', 'broadcast'));
  const a = daemon(t, dir, 'host-a', c.url, { start: false });
  assert.equal(await crashDaemon(a.config.inboxPath, c.url, 'before-receipt'), 'SIGKILL');
  const n = bookmark(a.config.inboxPath).bookmark!;
  const before = inboxRows(a.config.inboxPath);
  assert.ok(n > 0 && before.length > 0, 'the batch committed');
  assert.equal(hostAcked(c.dbPath, 'host-a'), 0, 'C < N: no receipt reached the coordinator');
  a.start();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === sent.at(-1)!.sequence, 'C reaches N and beyond');
  assert.equal(w.requests(ROUTES.streamV2, 'host-a')[0]!.search, `?stream=${bookmark(a.config.inboxPath).stream}&after=${n}`);
  const replayed = w.v2('host-a', 'msg').filter(f => f.seq <= n).map(f => f.seq);
  assert.ok(replayed.length > 0, 'the coordinator resumed at C and re-sent frames <= N');
  const after = inboxRows(a.config.inboxPath);
  for (const row of before) assert.ok(after.some(r => r.session_key === row.session_key && r.message_id === row.message_id && r.message_json === row.message_json));
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sequences(sent.filter(m => m.recipient !== key(a2))));
  assert.deepEqual(stored(a.config.inboxPath).get(key(a2)), sequences(sent.filter(m => m.recipient !== key(a1))));
});

test('SIGKILL of the coordinator after the send COMMIT, before notify: the message is delivered after restart (test 37)', async t => {
  reports(t);
  const dir = tempDir(t);
  const port = await freePort();
  const dbPath = join(dir, 'coordinator.db');
  const crashing = await coordinatorProcess(t, dir, { dbPath, port, crash: 'after-send-commit' });
  const [a1, sender] = [address(1), address(9)];
  await register(crashing.url, 'host-a', a1);
  await register(crashing.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', crashing.url);
  await waitFor(() => bookmark(a.config.inboxPath).bookmark !== null, 'hello committed');
  await assert.rejects(send(crashing.url, 'host-b', sender, a1, 'committed, never notified'));
  assert.equal((await crashing.exited).signal, 'SIGKILL');
  assert.equal(coordinatorTail(dbPath), 1, 'the send committed before the kill');
  await coordinatorProcess(t, dir, { dbPath, port });
  await waitFor(() => (stored(a.config.inboxPath).get(key(a1)) ?? []).length === 1, 'delivered after restart');
  assert.equal(JSON.parse(inboxRows(a.config.inboxPath)[0]!.message_json).body, 'committed, never notified');
  await waitFor(() => hostAcked(dbPath, 'host-a') === 1, 'receipted');
});

test('a lost NORMAL suffix lowers C: the reconnect replays and skips, with no loss and no duplicate (test 38)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, sender] = [address(1), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', c.url);
  const sent: Message[] = [];
  for (let i = 0; i < 5; i++) sent.push(await send(c.url, 'host-b', sender, a1, `m${i}`));
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 5, 'C = 5');
  await c.stop();
  mutate(c.dbPath, 'UPDATE hosts SET acked = 1');
  w.reset();
  await c.start();
  sent.push(await send(c.url, 'host-b', sender, a1, 'after the rollback'));
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 6, 'C catches up');
  assert.equal(w.requests(ROUTES.streamV2, 'host-a')[0]!.search, `?stream=${bookmark(a.config.inboxPath).stream}&after=5`);
  assert.equal(w.v2('host-a', 'hello')[0]!.resume, 1, 'resume is the coordinator copy, not the daemon claim');
  assert.deepEqual(w.v2('host-a', 'msg').map(f => f.seq), [2, 3, 4, 5, 6], 'frames 2-5 are replayed');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sequences(sent));
});

test('a coordinator restart with a receipt in flight: the late receipt is refused, the daemon reconnects with a fresh snapshot (crash table)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, sender] = [address(1), address(2), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-a', a2);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', c.url);
  await waitFor(() => bookmark(a.config.inboxPath).bookmark !== null, 'hello');
  const release = w.hold(item => item.path === ROUTES.receiptV2, 1);
  const first = await send(c.url, 'host-b', sender, a1, 'in flight');
  await waitFor(() => w.requests(ROUTES.receiptV2, 'host-a').length === 1, 'receipt posted and held');
  await c.restart();
  release();
  await waitFor(() => w.requests(ROUTES.receiptV2, 'host-a')[0]!.status !== undefined, 'held receipt answered');
  assert.equal(w.requests(ROUTES.receiptV2, 'host-a')[0]!.status, 409, 'every conn is unknown after a restart');
  const second = await send(c.url, 'host-b', sender, '*', 'after restart');
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === second.sequence, 'C reaches the broadcast');
  const hellos = w.frames.filter(f => f.host === 'host-a' && f.frame.type === 'hello').map(f => f.connection);
  const last = w.frames.filter(f => f.connection === hellos.at(-1));
  assert.deepEqual(last.filter(f => f.frame.type === 'member').map(f => f.frame.session).sort(), [key(a1), key(a2)].sort(),
    'the reconnect sends the full membership again');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [first.sequence, second.sequence]);
  assert.deepEqual(stored(a.config.inboxPath).get(key(a2)), [second.sequence]);
});

test('SIGKILL between the cut-over commit and the hello commit: the second connect gives the same final inbox (test 39)', async t => {
  reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, a2, sender] = [address(1), address(2), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', sender);
  const before = [await send(c.url, 'host-b', sender, a1, 'before a2'), await send(c.url, 'host-b', sender, '*', 'broadcast 1')];
  await register(c.url, 'host-a', a2);
  const later = [await send(c.url, 'host-b', sender, a2, 'to a2'), await send(c.url, 'host-b', sender, '*', 'broadcast 2')];
  const a = daemon(t, dir, 'host-a', c.url, { start: false });
  assert.equal(await crashDaemon(a.config.inboxPath, c.url, 'hello'), 'SIGKILL');
  assert.equal(hostAcked(c.dbPath, 'host-a'), 0, 'the cut-over committed');
  assert.deepEqual(bookmark(a.config.inboxPath), { stream: null, bookmark: null }, 'the hello commit did not');
  a.start();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === later.at(-1)!.sequence, 'delivered');
  assert.equal(w.requests(ROUTES.streamV2, 'host-a')[0]!.search, '', 'no bookmark: no query');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), sequences([...before, later[1]!]));
  assert.deepEqual(stored(a.config.inboxPath).get(key(a2)), sequences(later));
});

/** `roger-wilco-host --reset-bookmark` as the operator runs it. */
function resetBookmark(dir: string, config: object): { highWater: number } {
  const path = join(dir, `reset-${Math.random().toString(16).slice(2)}.json`);
  writeFileSync(path, JSON.stringify(config));
  return JSON.parse(execFileSync(process.execPath, [join(ROOT, 'src/bin/host.ts'), '--reset-bookmark', path], { encoding: 'utf8' }));
}
function copyDatabase(from: string, to: string): void {
  for (const suffix of ['-wal', '-shm']) rmSync(to + suffix, { force: true });
  copyFileSync(from, to);
}

test('a restored older inbox (D < C) gets 409 with no writes; --reset-bookmark then resumes at C (test 40)', async t => {
  const seen = reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, sender] = [address(1), address(9)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', c.url);
  const m1 = await send(c.url, 'host-b', sender, a1, 'm1');
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === m1.sequence, 'm1 receipted');
  await a.stop();
  const backup = join(dir, 'inbox.backup');
  copyDatabase(a.config.inboxPath, backup);
  a.start();
  const m2 = await send(c.url, 'host-b', sender, a1, 'm2');
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === m2.sequence, 'm2 receipted');
  await a.stop();
  copyDatabase(backup, a.config.inboxPath);
  w.reset();
  a.start();
  await waitFor(() => w.requests(ROUTES.streamV2, 'host-a').some(item => item.status === 409), 'refused open');
  await delay(100);
  assert.ok(seen.some(text => text.includes('Bookmark is behind the receipted position')), seen.join('\n'));
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [m1.sequence], 'no writes');
  assert.equal(bookmark(a.config.inboxPath).bookmark, m1.sequence);
  await a.stop();
  assert.deepEqual(resetBookmark(dir, a.config), { highWater: m1.sequence });
  a.start();
  const m3 = await send(c.url, 'host-b', sender, a1, 'm3');
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === m3.sequence, 'resumed at C');
  assert.equal(w.v2('host-a', 'hello').at(-1)!.resume, m2.sequence, 'the stream resumes at C');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [m1.sequence, m3.sequence]);
});

for (const floor of ['max highWater', 'too low'] as const) {
  test(`a restored coordinator file: 409 for each bookmarked daemon; reset and restore with the floor (${floor}) (test 40)`, async t => {
    const seen = reports(t);
    const dir = tempDir(t);
    const c = await coordinator(t, dir);
    const [a1, b1] = [address(1), address(2)];
    await register(c.url, 'host-a', a1);
    await register(c.url, 'host-b', b1);
    const a = daemon(t, dir, 'host-a', c.url);
    const b = daemon(t, dir, 'host-b', c.url);
    // Host A's receipt for early[1] is held, so the backup has C below what host A stored.
    const release = w.hold(item => item.path === ROUTES.receiptV2 && item.host === 'host-a', 1);
    const early = [await send(c.url, 'host-a', a1, b1, 'to b1'), await send(c.url, 'host-b', b1, a1, 'to a1')];
    await waitFor(() => (hostAcked(c.dbPath, 'host-b') ?? 0) >= early[0]!.sequence && bookmark(a.config.inboxPath).bookmark === early[1]!.sequence &&
      w.requests(ROUTES.receiptV2, 'host-a').length === 1, 'early messages stored');
    await c.stop();
    const backup = join(dir, 'coordinator.backup');
    copyDatabase(c.dbPath, backup);
    await c.start();
    release();
    const lost = [await send(c.url, 'host-a', a1, '*', 'after the backup, from a1'), await send(c.url, 'host-b', b1, '*', 'after the backup, from b1')];
    await waitFor(() => (hostAcked(c.dbPath, 'host-a') ?? 0) >= lost[1]!.sequence && (hostAcked(c.dbPath, 'host-b') ?? 0) >= lost[0]!.sequence, 'later receipts');
    // The coordinator file is restored from the backup; both daemons hold bookmarks above its tail.
    await c.stop();
    copyDatabase(backup, c.dbPath);
    w.reset();
    await c.start();
    await waitFor(() => ['host-a', 'host-b'].every(host => w.requests(ROUTES.streamV2, host).some(item => item.status === 409)), 'both refused');
    await delay(100);
    assert.ok(seen.some(text => text.includes('Bookmark is ahead of the coordinator tail')));
    assert.equal(coordinatorTail(c.dbPath), early[1]!.sequence);
    // The procedure: coordinator stopped, every daemon stopped and reset, then the floor.
    await c.stop();
    await a.stop();
    await b.stop();
    const highWater = Math.max(resetBookmark(dir, a.config).highWater, resetBookmark(dir, b.config).highWater);
    assert.equal(highWater, lost[1]!.sequence);
    // The reset unbinds the log and keeps D as a skip floor.
    const floorB = bookmark(b.config.inboxPath);
    assert.equal(floorB.stream, null);
    assert.ok(floorB.bookmark! >= lost[0]!.sequence);
    const sequenceFloor = floor === 'too low' ? early[1]!.sequence : highWater;
    execFileSync(process.execPath, [join(ROOT, 'deploy/restore-coordinator.ts'), '--sequence-floor', String(sequenceFloor), c.dbPath], { encoding: 'utf8' });
    await c.start();
    a.start();
    b.start();
    const next = await send(c.url, 'host-a', a1, '*', 'after the restore');
    // Ids at or below the backup tail come back with the same bytes and deduplicate.
    await waitFor(() => (hostAcked(c.dbPath, 'host-a') ?? 0) >= early[1]!.sequence, 'host A replayed up to its restored C');
    assert.ok(w.v2('host-a', 'msg').some(f => f.seq === early[1]!.sequence), 'replayed id');
    assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [early[1]!.sequence, lost[1]!.sequence]);
    if (floor === 'too low') {
      assert.equal(next.sequence, lost[0]!.sequence, 'a reissued id');
      await waitFor(() => seen.some(text => text.includes(`Message identity conflict: session ${key(b1)}, message ${next.id}`)), 'fail-closed conflict');
      assert.deepEqual(stored(b.config.inboxPath).get(key(b1)), [early[0]!.sequence, lost[0]!.sequence], 'the conflicting copy is not stored');
      assert.equal(bookmark(b.config.inboxPath).bookmark, floorB.bookmark, 'the conflicting batch leaves D unchanged');
      return;
    }
    assert.ok(next.sequence > highWater, 'new ids are above every id stored on any host');
    await waitFor(() => hostAcked(c.dbPath, 'host-b') === next.sequence, 'host B delivered');
    assert.deepEqual(stored(b.config.inboxPath).get(key(b1)), [early[0]!.sequence, lost[0]!.sequence, next.sequence]);
    assert.ok(!seen.some(text => text.includes('identity conflict')));
  });
}

test('a coordinator restore does not bring back a row that its session acknowledged and the host swept (test 40)', async t => {
  const seen = reports(t);
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const [a1, b1] = [address(1), address(2)];
  await register(c.url, 'host-a', a1);
  await register(c.url, 'host-b', b1);
  const a = daemon(t, dir, 'host-a', c.url);
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 0, 'cut over');
  // Host A is behind when the backup is taken: its receipts have not landed.
  const release = w.hold(item => item.path === ROUTES.receiptV2 && item.host === 'host-a');
  const sent = [await send(c.url, 'host-b', b1, a1, 'one'), await send(c.url, 'host-b', b1, a1, 'two'), await send(c.url, 'host-b', b1, a1, 'three')];
  await waitFor(() => bookmark(a.config.inboxPath).bookmark === sent[2]!.sequence, 'stored');
  const backup = join(dir, 'coordinator.backup');
  mutate(c.dbPath, `VACUUM INTO '${backup}'`);
  release();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === sent[2]!.sequence, 'receipted');
  await a.stop();
  // The session acknowledged message two more than HISTORY_MS ago; the sweep at open removes it.
  mutate(a.config.inboxPath, `UPDATE inbox_messages SET evidence_record_id = 'r2', evidence_location = 'loc', acknowledged_at = 1 WHERE sequence = ?`, sent[1]!.sequence);
  await c.stop();
  const { highWater } = resetBookmark(dir, a.config);
  assert.equal(highWater, sent[2]!.sequence);
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [sent[0]!.sequence, sent[2]!.sequence], 'swept');
  copyDatabase(backup, c.dbPath);
  execFileSync(process.execPath, [join(ROOT, 'deploy/restore-coordinator.ts'), '--sequence-floor', String(highWater), c.dbPath], { encoding: 'utf8' });
  await c.start();
  a.start();
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === sent[2]!.sequence, 'the replay is receipted');
  assert.ok(w.v2('host-a', 'msg').some(f => f.seq === sent[1]!.sequence), 'message two was replayed');
  assert.deepEqual(stored(a.config.inboxPath).get(key(a1)), [sent[0]!.sequence, sent[2]!.sequence], 'and not stored again');
  assert.ok(!seen.some(text => text.includes('identity conflict')), seen.join('\n'));
});

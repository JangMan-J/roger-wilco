/**
 * Performance gates for host stream v2 (docs/protocol-v2.md, test plan 46). The real
 * store, handler and daemon run in one process; the daemon reaches the coordinator
 * through a TCP proxy that delays every chunk by 30 ms each way. Run with
 * `npm run test:perf`; not part of `npm test`, because the gates measure time.
 */
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createHostClient } from '../../src/host/client.ts';
import { DEFAULT_SETTINGS, ROUTES, presenceIntervalMs, type HostInbox, type Message } from '../../src/protocol/index.ts';
import {
  address, bookmark, coordinator, cursors, daemon, delayProxy, hostAcked, inboxRows, key, register, send, stored, tempDir,
  waitFor, wire, type Wire,
} from '../acceptance/v2-harness.ts';

const ONE_WAY_MS = 30;
const RTT = 2 * ONE_WAY_MS;
/** Local work on top of the round trips: SQLite commits, JSON, scheduling. */
const LOCAL_ALLOWANCE_MS = 250;

let w: Wire;
test.beforeEach(() => { w = wire(); });
test.afterEach(() => { w.restore(); });

/** Counts COMMITs on connections to `path`. */
function commits(t: test.TestContext, path: string): { count: number } {
  const counter = { count: 0 };
  const exec = DatabaseSync.prototype.exec;
  DatabaseSync.prototype.exec = function (this: DatabaseSync, sql: string) {
    if (sql === 'COMMIT' && this.location() === path) counter.count++;
    return exec.call(this, sql);
  };
  t.after(() => { DatabaseSync.prototype.exec = exec; });
  return counter;
}

test(`a 100-message backlog to one session drains in at most ceil(100/W) + 1 round trips (v1: about 7 s)`, async t => {
  const W = DEFAULT_SETTINGS.window;
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const proxy = await delayProxy(t, c.port, ONE_WAY_MS);
  const [s1, sender] = [address(1), address(9)];
  await register(c.url, 'host-a', s1);
  await register(c.url, 'host-b', sender);
  const sent: Message[] = [];
  for (let i = 0; i < 100; i++) sent.push(await send(c.url, 'host-b', sender, s1, `backlog ${i}`));
  const started = Date.now();
  const a = daemon(t, dir, 'host-a', proxy.url);
  await waitFor(() => (stored(a.config.inboxPath).get(key(s1)) ?? []).length === 100, 'backlog stored');
  const storedAt = Date.now() - started;
  await waitFor(() => hostAcked(c.dbPath, 'host-a') === 100, 'backlog receipted');
  const receiptedAt = Date.now() - started;
  const roundTrips = Math.ceil(100 / W) + 1;
  t.diagnostic(`W=${W}: stored in ${storedAt} ms, receipted in ${receiptedAt} ms; gate ${roundTrips} x ${RTT} ms + ${LOCAL_ALLOWANCE_MS} ms; ` +
    `receipts ${w.requests(ROUTES.receiptV2, 'host-a').length}`);
  assert.deepEqual(stored(a.config.inboxPath).get(key(s1)), sent.map(m => m.sequence));
  assert.ok(receiptedAt <= roundTrips * RTT + LOCAL_ALLOWANCE_MS, `receipted in ${receiptedAt} ms`);
});

test('a broadcast to 100 sessions on one daemon costs 1 frame, 1 daemon commit and 1 receipt (v1: about 659 ms)', async t => {
  const dir = tempDir(t);
  const c = await coordinator(t, dir);
  const proxy = await delayProxy(t, c.port, ONE_WAY_MS);
  const sessions = Array.from({ length: 100 }, (_, i) => address(i + 1));
  for (const s of sessions) await register(c.url, 'host-a', s);
  const sender = address(999);
  await register(c.url, 'host-b', sender);
  const a = daemon(t, dir, 'host-a', proxy.url);
  await waitFor(() => bookmark(a.config.inboxPath).bookmark !== null, 'hello');
  // The first batch carries the membership snapshot; let it settle before counting.
  await waitFor(() => w.v2('host-a', 'member').length === 100, 'membership');
  await delay(RTT * 3);
  const counted = commits(t, a.config.inboxPath);
  const frames = w.v2('host-a').length;
  const receipts = w.requests(ROUTES.receiptV2, 'host-a').length;
  const started = Date.now();
  const broadcast = await send(c.url, 'host-b', sender, '*', 'to everyone');
  await waitFor(() => inboxRows(a.config.inboxPath).length === 100 && hostAcked(c.dbPath, 'host-a') === broadcast.sequence, 'fan-out and receipt');
  const elapsed = Date.now() - started;
  t.diagnostic(`broadcast to 100 sessions: stored and receipted in ${elapsed} ms`);
  assert.deepEqual(w.v2('host-a').slice(frames).map(f => f.type), ['msg'], 'one frame');
  assert.equal(counted.count, 1, 'one daemon commit');
  assert.equal(w.requests(ROUTES.receiptV2, 'host-a').length - receipts, 1, 'one receipt');
  assert.ok(elapsed <= 2 * RTT + LOCAL_ALLOWANCE_MS, `took ${elapsed} ms`);
});

test('1,000 idle sessions cost 0 per-session coordinator requests and 1 presence POST per host per interval', async t => {
  const presenceWindowMs = 15_000;
  const interval = presenceIntervalMs(presenceWindowMs);
  const dir = tempDir(t);
  const c = await coordinator(t, dir, { presenceWindowMs });
  // Counting, not latency: the daemon talks to the coordinator directly, so the
  // thousand registrations at startup do not each wait for the proxy delay.
  const a = daemon(t, dir, 'host-a', c.url);
  await waitFor(() => bookmark(a.config.inboxPath).bookmark !== null, 'hello');
  const sessions = Array.from({ length: 1000 }, (_, i) => address(i + 1));
  const clients: HostInbox[] = [];
  t.after(async () => { await Promise.all(clients.map(client => client.close())); });
  for (let i = 0; i < sessions.length; i += 50) {
    await Promise.all(sessions.slice(i, i + 50).map(async session => {
      const client = createHostClient(a.config.inboxPath);
      clients.push(client);
      assert.equal((await client.bind(session, `runtime-${session.sessionId}`)).ok, true);
      assert.equal((await client.awaitRegistration({ session })).ok, true);
    }));
  }
  assert.equal(cursors(c.dbPath).size, 1000);
  w.reset();
  const window = 2 * interval + 500;
  const started = Date.now();
  // Adapter heartbeats for every session, as their 30 s timers would send them.
  await Promise.all(clients.map((client, i) => client.enqueueRegistration({ session: sessions[i]! })));
  await delay(window - (Date.now() - started));
  const fromHost = w.exchanges.filter(item => item.host === 'host-a');
  const presence = fromHost.filter(item => item.path === ROUTES.presence);
  t.diagnostic(`${window} ms: ${fromHost.length} requests from host-a, ${presence.length} presence POSTs`);
  assert.deepEqual(fromHost.filter(item => item.path !== ROUTES.presence), [], 'no per-session request');
  assert.ok(presence.length >= 2 && presence.length <= 3, `${presence.length} presence POSTs in ${window} ms`);
  for (let i = 1; i < presence.length; i++) assert.ok(presence[i]!.at - presence[i - 1]!.at >= interval - 100, 'one POST per interval');
  for (const item of presence) assert.equal(item.body.sessions.length, 1000, 'every leased session in one POST');
});

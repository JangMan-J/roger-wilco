/**
 * Randomised model of host stream v2 (docs/protocol-v2.md, test plan 43). One process:
 * the real coordinator, its store and stream handler, and two real host daemons. A
 * seeded run interleaves sends, registrations, expiries, daemon kills at named points,
 * coordinator restarts, lost receipts and collection, and checks against an oracle:
 * each session's stored ids equal its owed set, D is monotone, C <= D at every step,
 * the window is never exceeded, and no owed, unstored message is ever collected.
 * Seeds: 1, 2, 3 and one from the clock, or RW_MODEL_SEEDS=a,b,c; each failure names its seed and step.
 */
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import type { Message, SessionAddress } from '../../src/protocol/index.ts';
import { ROUTES } from '../../src/protocol/index.ts';
import {
  address, bookmark, coordinator, daemon, hostAcked, key, mutate, query, register, send, stored, tempDir,
  windowTap, wire, type Daemon,
} from './v2-harness.ts';

const W = 4;
const STEPS = 80;
const DAY = 86_400_000;
/** Three fixed seeds and one new seed per run; the test name reports each. */
const seeds = process.env.RW_MODEL_SEEDS ? process.env.RW_MODEL_SEEDS.split(',').map(Number) : [1, 2, 3, Date.now() % 1_000_000];

/** mulberry32: small, seedable, good enough to pick operations. */
function random(seed: number) {
  let a = seed >>> 0;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let x = a;
    x = Math.imul(x ^ (x >>> 15), x | 1);
    x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
  return { next, pick: <T>(items: readonly T[]): T => items[Math.floor(next() * items.length)]! };
}

/** In-process "kill" points on a daemon's inbox file: the COMMIT throws, so the transaction rolls back. */
function killSwitch(t: test.TestContext) {
  const armed = new Map<string, 'before-commit' | 'hello'>();
  const primed = new Set<string>();
  const fired = new Set<string>();
  const prepare = DatabaseSync.prototype.prepare;
  const exec = DatabaseSync.prototype.exec;
  const where = (db: DatabaseSync) => { try { return db.location() ?? ''; } catch { return ''; } };
  DatabaseSync.prototype.prepare = function (this: DatabaseSync, sql: string) {
    const point = armed.get(where(this));
    if ((point === 'before-commit' && sql.startsWith('INSERT INTO inbox_messages')) ||
        (point === 'hello' && sql.startsWith('UPDATE inbox_stream SET stream_id'))) primed.add(where(this));
    return prepare.call(this, sql);
  } as typeof DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.exec = function (this: DatabaseSync, sql: string) {
    if (sql === 'COMMIT' && primed.size) {
      const path = where(this);
      if (primed.delete(path)) { armed.delete(path); fired.add(path); throw new Error('injected kill before COMMIT'); }
    }
    return exec.call(this, sql);
  };
  t.after(() => { DatabaseSync.prototype.prepare = prepare; DatabaseSync.prototype.exec = exec; });
  return { armed, fired, disarm(path: string) { armed.delete(path); primed.delete(path); } };
}

for (const seed of seeds) {
  test(`randomised model, seed ${seed}: stored = owed, D monotone, C <= D, window <= W, nothing owed is collected (test 43)`, async t => {
    t.mock.method(console, 'error', () => {});
    const w = wire();
    t.after(() => w.restore());
    const tap = windowTap(W);
    t.after(() => tap.restore());
    const realNow = Date.now;
    let offset = 0;
    Date.now = () => realNow() + offset;
    t.after(() => { Date.now = realNow; });
    const kills = killSwitch(t);
    const rng = random(seed);
    const dir = tempDir(t);
    const c = await coordinator(t, dir, { window: W });
    const daemons: Record<string, Daemon> = { 'host-a': daemon(t, dir, 'host-a', c.url), 'host-b': daemon(t, dir, 'host-b', c.url) };
    const pool = [1, 2, 3, 4, 5, 6].map(n => ({ address: address(n), host: n <= 3 ? 'host-a' : 'host-b' }));
    type Entry = (typeof pool)[number] & { registered: boolean; intervals: Array<{ start: number; end?: number }> };
    const sessions: Entry[] = pool.map(item => ({ ...item, registered: false, intervals: [] }));
    const sent: Message[] = [];
    const log: string[] = [];
    const where = (step: number) => `seed ${seed}, step ${step}: ${log.slice(-6).join('; ')}`;

    const owed = (s: Entry) => sent.filter(m => (m.recipient === key(s.address) || (m.recipient === '*' && m.sender !== key(s.address))) &&
      s.intervals.some(({ start, end }) => start < m.sequence && (end === undefined || m.sequence <= end))).map(m => m.sequence);
    const holdings = () => {
      const result = new Map<string, number[]>();
      for (const d of Object.values(daemons)) for (const [k, v] of stored(d.config.inboxPath)) result.set(k, v);
      return result;
    };
    const settled = () => { const have = holdings(); return sessions.every(s => JSON.stringify(have.get(key(s.address)) ?? []) === JSON.stringify(owed(s))); };
    const settle = async (step: number) => {
      const end = realNow() + 20_000;
      while (!settled()) {
        if (realNow() > end) {
          const have = holdings();
          assert.fail(`${where(step)}: not settled: ${JSON.stringify(sessions.map(s => [s.address.sessionId.slice(-2), have.get(key(s.address)) ?? [], owed(s)]))}`);
        }
        await delay(10);
      }
    };
    const lastD = new Map<string, number>();
    const check = (step: number) => {
      for (const [hostId, d] of Object.entries(daemons)) {
        const acked = hostAcked(c.dbPath, hostId);
        const { bookmark: D } = bookmark(d.config.inboxPath);
        if (D !== null) {
          assert.ok(D >= (lastD.get(hostId) ?? 0), `${where(step)}: ${hostId} D went from ${lastD.get(hostId)} to ${D}`);
          lastD.set(hostId, D);
        }
        // Before the first hello commit (a NULL bookmark) C is the cut-over position and D makes no claim.
        if (acked !== undefined && D !== null) assert.ok(acked <= D, `${where(step)}: ${hostId} C=${acked} > D=${D}`);
      }
      const extra = [...holdings()].flatMap(([k, ids]) => {
        const s = sessions.find(item => key(item.address) === k)!;
        const allowed = new Set(owed(s));
        return ids.filter(id => !allowed.has(id)).map(id => `${k}:${id}`);
      });
      assert.deepEqual(extra, [], `${where(step)}: stored but not owed`);
      assert.deepEqual(tap.result.violations, [], where(step));
    };
    const restartDaemon = async (hostId: string) => { await daemons[hostId]!.restart(); };

    for (const s of [sessions[0]!, sessions[4]!]) { s.registered = true; s.intervals.push({ start: await register(c.url, s.host, s.address) }); }
    for (let step = 0; step < STEPS; step++) {
      const registered = sessions.filter(s => s.registered);
      const roll = rng.next();
      if (roll < 0.30 && registered.length) {
        // A burst, so windows fill and receipts cover several frames.
        const burst = 1 + Math.floor(rng.next() * 8);
        log.push(`send x${burst}`);
        for (let i = 0; i < burst; i++) {
          const from = rng.pick(registered);
          sent.push(await send(c.url, from.host, from.address, rng.pick(sessions).address, `step ${step}.${i}`));
        }
      } else if (roll < 0.42 && registered.length) {
        const from = rng.pick(registered);
        log.push(`broadcast ${from.address.sessionId.slice(-2)}`);
        sent.push(await send(c.url, from.host, from.address, '*', `broadcast ${step}`));
      } else if (roll < 0.55) {
        const s = rng.pick(sessions);
        log.push(`register ${s.address.sessionId.slice(-2)}`);
        const start = await register(c.url, s.host, s.address);
        if (!s.registered) { s.registered = true; s.intervals.push({ start }); }
        else assert.equal(start, s.intervals.at(-1)!.start, `${where(step)}: a retry keeps the start`);
      } else if (roll < 0.60 && registered.length) {
        // Expiry at a quiet point, so the oracle can close the interval at the tail.
        const s = rng.pick(registered);
        log.push(`expire ${s.address.sessionId.slice(-2)}`);
        await settle(step);
        mutate(c.dbPath, 'UPDATE sessions SET last_seen_at = 1 WHERE id = ?', key(s.address));
        await c.restart();
        assert.equal(query(c.dbPath, 'SELECT 1 FROM sessions WHERE id = ?', key(s.address)).length, 0, where(step));
        s.registered = false;
        s.intervals.at(-1)!.end = sent.at(-1)?.sequence ?? 0;
      } else if (roll < 0.70) {
        const hostId = rng.pick(['host-a', 'host-b']);
        const point = rng.pick(['before-commit', 'before-receipt', 'hello', 'anywhere'] as const);
        log.push(`kill ${hostId} ${point}`);
        const path = daemons[hostId]!.config.inboxPath;
        if (point === 'before-commit') {
          kills.armed.set(path, 'before-commit');
          const to = rng.pick(sessions.filter(s => s.host === hostId));
          const from = rng.pick(registered.length ? registered : [to]);
          if (from.registered) sent.push(await send(c.url, from.host, from.address, to.address, `kill ${step}`));
          const end = realNow() + 300;
          while (!kills.fired.has(path) && realNow() < end) await delay(5);
          kills.disarm(path);
        } else if (point === 'before-receipt') {
          w.drop(item => item.path === ROUTES.receiptV2 && item.host === hostId, 'request', 1);
        } else if (point === 'hello') {
          kills.armed.set(path, 'hello');
          await restartDaemon(hostId);
          const end = realNow() + 1000;
          while (!kills.fired.has(path) && realNow() < end) await delay(5);
          kills.disarm(path);
        }
        kills.fired.delete(path);
        await restartDaemon(hostId);
      } else if (roll < 0.78) {
        log.push('coordinator restart');
        await c.restart();
      } else if (roll < 0.86) {
        const hostId = rng.pick(['host-a', 'host-b']);
        log.push(`lose receipt response ${hostId}`);
        w.drop(item => item.path === ROUTES.receiptV2 && item.host === hostId, 'response', 1);
      } else if (roll < 0.92) {
        // Collection: the clock passes retention; registrations stay fresh.
        log.push('collect');
        const have = holdings();
        const unstored = sessions.filter(s => s.registered).flatMap(s => {
          const mine = new Set(have.get(key(s.address)) ?? []);
          return owed(s).filter(id => !mine.has(id));
        });
        offset += 8 * DAY;
        mutate(c.dbPath, 'UPDATE sessions SET last_seen_at = last_seen_at + ?', 8 * DAY);
        await c.restart();
        const kept = new Set(query<{ id: number }>(c.dbPath, 'SELECT id FROM messages').map(row => row.id));
        assert.deepEqual(unstored.filter(id => !kept.has(id)), [], `${where(step)}: an owed, unstored message was collected`);
      } else {
        log.push('pause');
        await delay(Math.floor(rng.next() * 40));
      }
      check(step);
    }
    await settle(STEPS);
    check(STEPS);
    t.diagnostic(`seed ${seed}: ${sent.length} messages, ${tap.result.msgFrames} msg frames, max window ${tap.result.max}; ${log.join(', ')}`);
  });
}

/**
 * Presence end to end (docs/protocol-v2.md §Presence; test plan 30-31 with the real
 * coordinator): one presence POST per host per presenceWindowMs/3 keeps leased sessions
 * fresh on the board, a heartbeat completes locally, and a registration the coordinator
 * lost comes back through presence `unknown`. The board reads presenceWindowMs from /health.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createClaudeAdapter } from '../../src/claude/adapter.ts';
import { connectHost, requestCoordinator } from '../../src/host/http.ts';
import { ROUTES, presenceIntervalMs, type RegistrationResult } from '../../src/protocol/index.ts';
import {
  NS, address, call, coordinator, coordinatorTail, cursors, daemon, key, mutate, register, send, tempDir, waitFor, wire, type Wire,
} from './v2-harness.ts';

let w: Wire;
test.beforeEach(() => { w = wire(); });
test.afterEach(() => { w.restore(); });

test('presence keeps a leased session on the board, heartbeats complete locally, and a lost registration returns through unknown', async t => {
  const presenceWindowMs = 15_000;
  const interval = presenceIntervalMs(presenceWindowMs);
  const dir = tempDir(t);
  const c = await coordinator(t, dir, { presenceWindowMs });
  assert.equal((await call(c.url, 'host-a', ROUTES.health)).result.value.presenceWindowMs, presenceWindowMs);
  const a = daemon(t, dir, 'host-a', c.url);
  const [s1, s2, sender] = [address(1), address(2), address(9)];
  // s1 runs a real adapter (lease, registration queue, board); s2 is registered with no runtime.
  const adapter = createClaudeAdapter({ sessionId: () => s1.sessionId, transcriptPath: () => undefined, deliver: () => {} }, {
    loadConfig: async () => ({ ...a.config, namespace: NS, sessionId: s1.sessionId, projectsRoot: dir }), connectHost, request: requestCoordinator,
  });
  await adapter.start();
  t.after(() => adapter.stop());
  await register(c.url, 'host-a', s2);
  await register(c.url, 'host-b', sender);
  await waitFor(() => cursors(c.dbPath).has(key(s1)), 'adapter registration through the daemon');
  await waitFor(() => w.requests(ROUTES.presence, 'host-a').length >= 1, 'presence armed at hello');

  // Both rows age past the board window; the next presence tick refreshes only the leased one.
  mutate(c.dbPath, 'UPDATE sessions SET last_seen_at = last_seen_at - 60000');
  const ticks = w.requests(ROUTES.presence, 'host-a').length;
  await waitFor(() => w.requests(ROUTES.presence, 'host-a').length > ticks && w.requests(ROUTES.presence, 'host-a').at(-1)!.reply, 'next tick', interval + 2000);
  assert.deepEqual(w.requests(ROUTES.presence, 'host-a').at(-1)!.body, { sessions: [key(s1)] }, 'only leased sessions');
  const board = await adapter.tool('roger_board') as { ok: true; value: { sessions: RegistrationResult[] } };
  assert.equal(board.ok, true);
  assert.deepEqual(board.value.sessions.map(row => row.subscription.id), [key(s1)], 'the board window is presenceWindowMs');
  const all = await adapter.tool('roger_board', { all: true }) as { ok: true; value: { sessions: RegistrationResult[] } };
  assert.deepEqual(all.value.sessions.map(row => row.subscription.id).sort(), [key(s1), key(s2), key(sender)].sort());

  // A heartbeat that the remembered registration answers completes locally once presence runs.
  const posts = () => w.requests(ROUTES.register, 'host-a').length;
  const before = posts();
  const identity = await adapter.tool('roger_identity') as { ok: boolean };
  assert.equal(identity.ok, true);
  await delay(200);
  assert.equal(posts(), before, 'no registration POST');

  // The coordinator loses s1's registration: presence reports it unknown and the daemon registers it once.
  await send(c.url, 'host-b', sender, '*', 'moves the tail');
  mutate(c.dbPath, 'DELETE FROM sessions WHERE id = ?', key(s1));
  await waitFor(() => cursors(c.dbPath).has(key(s1)), 're-registration', interval + 2000);
  await delay(200);
  assert.equal(posts(), before + 1, 'exactly one registration POST');
  assert.ok(w.requests(ROUTES.presence, 'host-a').some(item => item.reply?.value?.unknown?.includes(key(s1))));
  assert.equal(cursors(c.dbPath).get(key(s1)), coordinatorTail(c.dbPath), 'the new start is the tail');
  // One presence POST per interval, whatever the session count.
  const times = w.requests(ROUTES.presence, 'host-a').map(item => item.at);
  for (let i = 1; i < times.length; i++) assert.ok(times[i]! - times[i - 1]! >= interval - 50 || i === 1, `presence gap ${times[i]! - times[i - 1]!}`);
  assert.equal(w.requests(ROUTES.presence, 'host-b').length, 0, 'no daemon on host B, no presence');
});

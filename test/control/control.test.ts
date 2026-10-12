import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { sessionKey } from '../../src/protocol/index.ts';

const alice = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440001' };
const bob = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440002' };

async function fixture(t: { after(fn: () => void | Promise<void>): void }, knownHosts?: ReadonlySet<string>) {
  const store = createCoordinatorStore(':memory:', 'fleet');
  const handler = createControlHandler(store, createHostAuthenticator({ 'host-a': 'secret-a', 'host-b': 'secret-b' }, knownHosts));
  const server = createServer((req, res) => { void handler(req, res); });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { await new Promise<void>(resolve => server.close(() => resolve())); store.close(); });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}`;
  async function request(path: string, body?: unknown, token = 'secret-a', init?: RequestInit) {
    const response = await fetch(base + path, {
      ...init, method: body === undefined ? 'GET' : 'POST',
      headers: { authorization: `Bearer ${token}`, ...(body !== undefined ? { 'content-type': 'application/json' } : {}), ...init?.headers },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, value: await response.json() as Record<string, any> };
  }
  return { request, base };
}

test('real HTTP authentication, strict validation, host-bound sender and claim owner', async t => {
  const { request } = await fixture(t);
  assert.equal((await request('/health', undefined, 'bad')).status, 200);
  assert.equal((await request('/v1/sessions', undefined, 'bad')).status, 401);
  assert.equal((await request('/v1/claims', undefined, 'bad')).status, 401);
  assert.deepEqual((await request('/v1/claims')).value.value, []);
  assert.equal((await request('/v1/hosts/stream')).status, 404);
  assert.equal((await request('/v1/hosts/receipts', {})).status, 404);
  assert.equal((await request('/mcp', { jsonrpc: '2.0', id: 1, method: 'tools/list' }, 'secret-a', {
    headers: { accept: 'application/json, text/event-stream' },
  })).status, 404);
  assert.equal((await request('/v1/sessions/register', { session: alice, callbackUrl: 'http://x' })).status, 400);
  const registration = await request('/v1/sessions/register', { session: alice, displayName: 'Alice' });
  assert.equal(registration.status, 200);
  assert.equal(registration.value.value.displayName, 'Alice');
  assert.equal(typeof registration.value.value.lastSeenAt, 'number');
  assert.equal((await request('/v1/sessions/register', { session: bob }, 'secret-b')).status, 200);
  assert.deepEqual((await request('/v1/sessions')).value.value.map((entry: { subscription: { id: string } }) => entry.subscription.id).sort(),
    [sessionKey(alice), sessionKey(bob)]);
  assert.equal((await request('/v1/sessions/register', { session: alice }, 'secret-b')).status, 409);
  assert.equal((await request('/v1/messages', { sender: bob, message: { recipient: '*', body: 'spoof' } })).status, 409);
  assert.equal((await request('/v1/messages', { sender: alice, message: { recipient: sessionKey(bob), body: 'hi' } })).status, 200);
  assert.equal((await request('/v1/messages', { sender: alice, message: { recipient: '*', body: 'extra', extra: true } })).status, 400);
  const query = `?namespace=fleet&piSessionId=${bob.piSessionId}&since=0&limit=10`;
  assert.equal((await request('/v1/inbox' + query)).status, 409);
  assert.equal((await request('/v1/inbox' + query, undefined, 'secret-b')).value.value[0].body, 'hi');
  assert.equal((await request('/v1/inbox' + query + '&limit=2', undefined, 'secret-b')).status, 400);
  assert.equal((await request('/v1/claims', { key: 'task', owner: bob, ttlSeconds: 30 })).status, 409);
  const claimed = await request('/v1/claims', { key: 'task', owner: alice, ttlSeconds: 30 });
  assert.equal(claimed.value.value.claim.owner, sessionKey(alice));
  assert.deepEqual((await request('/v1/claims')).value.value, [claimed.value.value.claim]);
  assert.equal((await request('/v1/claims?extra=1')).status, 400);
  assert.equal((await request('/v1/claims/release', { key: 'task', owner: bob })).status, 409);
  assert.equal((await request('/v1/claims/release', { key: 'task', owner: alice })).value.value, true);
  assert.deepEqual((await request('/v1/claims')).value.value, []);
});

test('authenticated hosts can edit another host session rank with strict HTTP validation', async t => {
  const { request, base } = await fixture(t);
  assert.equal((await request('/v1/sessions/register', { session: alice })).status, 200);
  const path = '/v1/sessions/rank';
  assert.equal((await request(path, { session: alice, rank: 3 }, 'bad')).status, 401);
  const unauthenticated = await fetch(base + path, { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ session: alice, rank: 3 }) });
  assert.equal(unauthenticated.status, 401);
  await unauthenticated.json();
  for (const rank of [1, 5]) {
    const updated = await request(path, { session: alice, rank }, 'secret-b');
    assert.equal(updated.status, 200);
    assert.equal(updated.value.value.rank, rank);
    assert.equal(updated.value.value.subscription.hostId, 'host-a');
  }
  for (const body of [
    ...[0, 6, 1.5, '3'].map(rank => ({ session: alice, rank })),
    { session: alice }, { session: alice, rank: 3, extra: true },
  ]) assert.equal((await request(path, body)).status, 400, JSON.stringify(body));
  assert.equal((await request(path, { session: bob, rank: 3 })).status, 404);
  assert.equal((await request('/v1/sessions')).value.value[0].rank, 5);
});

test('recognized credential for a deprovisioned host is distinct from invalid credentials', () => {
  const auth = createHostAuthenticator({ 'host-a': 'token-a', 'host-b': 'token-b' }, new Set(['host-a']));
  assert.equal(auth.authenticate('token-b').ok, false);
  assert.deepEqual(auth.authenticate('token-b'), { ok: false, error: 'unknown_host', message: 'Host is not provisioned' });
  assert.equal(auth.authenticate('wrong').ok, false);
  assert.throws(() => createHostAuthenticator({ a: 'duplicate', b: 'duplicate' }), TypeError);
});

test('HTTP rejects credentials for deprovisioned hosts distinctly', async t => {
  const { request } = await fixture(t, new Set(['host-a']));
  assert.equal((await request('/v1/claims', undefined, 'secret-b')).status, 403);
  assert.equal((await request('/v1/claims', undefined, 'invalid')).status, 401);
});

test('bounded JSON parsing rejects oversized, malformed, wrong media type', async t => {
  const { request, base } = await fixture(t);
  const huge = await fetch(base + '/v1/sessions/register', { method: 'POST', headers: { authorization: 'Bearer secret-a', 'content-type': 'application/json' }, body: ' '.repeat(1024 * 1024 + 1) });
  assert.equal(huge.status, 413);
  const malformed = await fetch(base + '/v1/sessions/register', { method: 'POST', headers: { authorization: 'Bearer secret-a', 'content-type': 'application/json' }, body: '{' });
  assert.equal(malformed.status, 400);
  assert.equal((await request('/v1/sessions/register', { session: alice }, 'secret-a', { headers: { 'content-type': 'text/plain' } })).status, 415);
});

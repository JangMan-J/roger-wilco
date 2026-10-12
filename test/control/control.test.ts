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
  assert.equal((await request('/v1/sessions/register', { session: alice, callbackUrl: 'http://x' })).status, 400);
  const registration = await request('/v1/sessions/register', { session: alice, displayName: 'Alice' });
  assert.equal(registration.status, 200);
  assert.equal(registration.value.value.displayName, 'Alice');
  assert.equal(typeof registration.value.value.lastSeenAt, 'number');
  assert.equal((await request('/v1/sessions/register', { session: bob }, 'secret-b')).status, 200);
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

test('MCP initialize, list, and calls expose board and host-bound session operations', async t => {
  const { request } = await fixture(t);
  const headers = { accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2025-03-26' };
  assert.equal((await request('/mcp', { jsonrpc: '2.0', id: 0, method: 'tools/list' }, 'bad', { headers })).status, 401);
  const initialized = await request('/mcp', { jsonrpc: '2.0', id: 1, method: 'initialize', params: {
    protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'control-test', version: '1.0' },
  } }, 'secret-a', { headers });
  assert.equal(initialized.status, 200);
  assert.equal(initialized.value.result.serverInfo.name, 'roger-wilco');
  const listed = await request('/mcp', { jsonrpc: '2.0', id: 2, method: 'tools/list' }, 'secret-a', { headers });
  assert.equal(listed.status, 200);
  assert.ok(listed.value.result.tools.some((tool: { name: string }) => tool.name === 'read_board'));
  const rpc = async (name: string, args: Record<string, unknown>, token = 'secret-a') => {
    const result = await request('/mcp', { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name, arguments: args } }, token, { headers });
    assert.equal(result.status, 200, JSON.stringify(result.value));
    assert.ok(result.value.result, JSON.stringify(result.value));
    return JSON.parse(result.value.result.content[0].text) as Record<string, any>;
  };
  const registered = await rpc('register_session', { session: alice });
  assert.equal(registered.value.subscription.id, sessionKey(alice));
  assert.equal((await rpc('register_session', { session: bob }, 'secret-b')).ok, true);
  assert.equal((await rpc('send_message', { session: bob, recipient: '*', body: 'spoof' })).error, 'owner_conflict');
  assert.equal((await rpc('send_message', { session: alice, recipient: sessionKey(bob), body: 'hello' })).ok, true);
  assert.equal((await rpc('inbox', { session: bob }, 'secret-b')).value[0].body, 'hello');
  const claim = (await rpc('claim_work', { session: alice, key: 'mcp', ttlSeconds: 30 })).value.claim;
  assert.equal(claim.owner, sessionKey(alice));
  const board = (await rpc('read_board', {})).value;
  assert.equal(board.sessions.length, 2);
  assert.equal(typeof board.sessions[0].lastSeenAt, 'number');
  assert.deepEqual(board.claims, [claim]);
  assert.equal((await rpc('release_work', { session: alice, key: 'mcp' })).value, true);
  assert.deepEqual((await rpc('read_board', {})).value.claims, []);
  assert.equal((await rpc('list_sessions', {})).value.length, 2);
});

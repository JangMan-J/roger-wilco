import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createPiAdapter } from '../../src/pi/adapter.ts';
import { isPiSession, piSessionAddress, requirePersistentPiSession } from '../../src/pi/identity.ts';

const ctxFor = (id, file) => ({ sessionManager: { getSessionId: () => id, getSessionFile: () => file }, hasUI: false });

test('Pi session identity maps to one lowercase session address', () => {
  const id = randomUUID();
  const upper = ctxFor(id.toUpperCase(), '/tmp/session.jsonl');
  assert.deepEqual(piSessionAddress(upper, 'fleet'), { namespace: 'fleet', sessionId: id });
  assert.equal(isPiSession(upper, { namespace: 'fleet', sessionId: id }), true);
  assert.equal(isPiSession(ctxFor(randomUUID(), undefined), { namespace: 'fleet', sessionId: id }), false);
  assert.throws(() => piSessionAddress(ctxFor('', undefined), 'fleet'), /persistent Pi UUID session/);
  assert.throws(() => requirePersistentPiSession(ctxFor(id, undefined), 'fleet'), /persistent Pi session manager/);
});

test('the Pi entry is thin registration; tools never reach delivery or identity', async () => {
  const imports = async file => [...(await readFile(new URL(`../../src/pi/${file}`, import.meta.url), 'utf8'))
    .matchAll(/from '([^']+)'/g)].map(match => match[1]);
  for (const specifier of await imports('index.ts'))
    assert.ok(['./adapter.ts', './config.ts', './host.ts', '@earendil-works/pi-coding-agent'].includes(specifier), specifier);
  for (const specifier of await imports('tools.ts'))
    assert.ok(!/delivery|identity|session-file|host/.test(specifier), specifier);
});

test('the adapter reaches the host only through the injected HostInbox and the coordinator only through request', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-adapter-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const id = randomUUID();
  const session = { namespace: 'fleet', sessionId: id };
  const config = { namespace: 'fleet', hostId: 'host-a', coordinatorUrl: 'http://127.0.0.1:1', credential: 'c', inboxPath: join(dir, 'inbox') };
  const calls = [];
  let open = false;
  const inbox = {
    onChange: () => () => {},
    authority: () => open ? 'grant' : undefined,
    async bind(address, runtimeId) { calls.push(['bind', address, typeof runtimeId]); open = true; return { ok: true, value: { leaseExpiresAt: Date.now() + 5000 } }; },
    async renew() { return { ok: true, value: { leaseExpiresAt: Date.now() + 5000 } }; },
    async release(address) { calls.push(['release', address]); },
    async pending() { return []; },
    async acknowledgeDelivery() { throw new Error('No delivery expected'); },
    async enqueueRegistration(registration) { calls.push(['enqueue', registration]); },
    async awaitRegistration(registration) { calls.push(['await', registration]);
      return { ok: true, value: { subscription: { id: `fleet:${id}`, session, hostId: 'host-a', cursor: 0 }, rank: 3 } }; },
    onRegistrationResult: () => () => {},
    async close() { calls.push(['close']); open = false; },
  };
  const requests = [];
  const handlers = new Map(), tools = new Map(), commands = new Map();
  createPiAdapter({ on: (name, handler) => handlers.set(name, handler), registerTool: tool => tools.set(tool.name, tool),
    registerCommand: (name, command) => commands.set(name, command), sendMessage() { throw new Error('No Pi delivery expected'); } }, {
    loadConfig: async () => config,
    connectHost: async received => { assert.equal(received, config); return inbox; },
    request: async (received, route, method, body) => { requests.push({ received, route, method, body }); return { ok: true, value: 'sent' }; },
  });
  assert.deepEqual([...commands.keys()], ['roger', 'roger_board']);
  assert.deepEqual([...tools.keys()], ['roger_identity', 'roger_send', 'roger_inbox', 'roger_entity', 'roger_board', 'roger_claim', 'roger_release']);
  const ctx = ctxFor(id.toUpperCase(), join(dir, 'session.jsonl'));
  await handlers.get('session_start')({ reason: 'startup' }, ctx);
  assert.deepEqual(calls.slice(0, 2), [['bind', session, 'string'], ['enqueue', { session }]]);
  const sent = await tools.get('roger_send').execute('call', { recipient: '*', body: 'hello' }, undefined, undefined, ctx);
  assert.deepEqual(sent.details, { ok: true, value: 'sent' });
  assert.deepEqual(requests, [{ received: config, route: '/v1/messages', method: 'POST',
    body: { sender: session, message: { recipient: '*', body: 'hello', data: undefined, visibility: undefined } } }]);
  const other = await tools.get('roger_send').execute('call', { recipient: '*', body: 'x' }, undefined, undefined, ctxFor(randomUUID(), join(dir, 'other.jsonl')));
  assert.deepEqual(other.details, { ok: false, error: 'temporarily_unavailable', message: 'Session changed; wait for binding' });
  await handlers.get('session_shutdown')({ reason: 'quit' }, ctx);
  assert.deepEqual(calls.slice(-2), [['release', session], ['close']]);
});

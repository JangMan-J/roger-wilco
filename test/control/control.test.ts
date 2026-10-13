import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { entityReference, sessionKey } from '../../src/protocol/index.ts';

const alice = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440001' };
const bob = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440002' };

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
  const query = `?namespace=fleet&sessionId=${bob.sessionId}&since=0&limit=10`;
  assert.equal((await request('/v1/inbox' + query)).status, 409);
  assert.equal((await request('/v1/inbox' + query, undefined, 'secret-b')).value.value[0].body, 'hi');
  assert.equal((await request('/v1/inbox' + query + '&limit=2', undefined, 'secret-b')).status, 400);
  // A pre-contract client is refused loudly, never reinterpreted.
  assert.equal((await request(`/v1/inbox?namespace=fleet&piSessionId=${bob.sessionId}&since=0&limit=10`, undefined, 'secret-b')).status, 400);
  assert.equal((await request('/v1/sessions/register', { session: { namespace: 'fleet', piSessionId: bob.sessionId } }, 'secret-b')).status, 400);
  assert.equal((await request('/v1/claims', { key: 'task', owner: bob, ttlSeconds: 30 })).status, 409);
  const claimed = await request('/v1/claims', { key: 'task', owner: alice, ttlSeconds: 30 });
  assert.equal(claimed.value.value.claim.owner, sessionKey(alice));
  assert.deepEqual((await request('/v1/claims')).value.value, [claimed.value.value.claim]);
  assert.equal((await request('/v1/claims?extra=1')).status, 400);
  assert.equal((await request('/v1/claims/release', { key: 'task', owner: bob })).status, 409);
  assert.equal((await request('/v1/claims/release', { key: 'task', owner: alice })).value.value, true);
  assert.deepEqual((await request('/v1/claims')).value.value, []);
});

test('entity HTTP lookup returns retained mentions with authentication and strict query bounds', async t => {
  const { request, base } = await fixture(t);
  assert.equal((await request('/v1/sessions/register', { session: alice })).status, 200);
  const first = await request('/v1/messages', { sender: alice, message: { recipient: '*', body: 'Start [[fix-parser]] and [[255:fix-parser]]' } });
  const second = await request('/v1/messages', { sender: alice, message: { recipient: '*', body: 'Finish [[255:fix-parser]]' } });
  assert.equal(first.status, 200);
  assert.equal(second.status, 200);
  const entity = entityReference('fleet', 'fix-parser', 255);
  const query = new URLSearchParams({ id: entity.id, since: '0', limit: '1' });
  const path = '/v1/entities?';
  assert.equal((await request(path + query, undefined, 'bad')).status, 401);
  const unauthenticated = await fetch(base + path + query);
  assert.equal(unauthenticated.status, 401);
  await unauthenticated.json();
  assert.deepEqual((await request(path + query, undefined, 'secret-b')).value,
    { ok: true, value: { entity, messages: [first.value.value] } });
  query.set('since', String(first.value.value.sequence));
  assert.deepEqual((await request(path + query)).value.value, { entity, messages: [second.value.value] });
  query.set('id', entityReference('fleet', 'fix-parser').id);
  query.set('since', '0');
  assert.deepEqual((await request(path + query)).value.value,
    { entity, messages: [first.value.value] });
  for (const suffix of ['&extra=1', '&id=x', '&since=1', '&limit=2']) {
    assert.equal((await request(path + query + suffix)).status, 400, suffix);
  }
  for (const [key, values] of Object.entries({ id: ['', 'not-an-entity', entityReference('other', 'fix-parser').id],
    since: ['', '-1', '1.5', '01', '9007199254740992'], limit: ['', '0', '501', '1.5'] })) {
    for (const value of values) {
      const invalid = new URLSearchParams(query);
      invalid.set(key, value);
      assert.equal((await request(path + invalid)).status, 400, `${key}=${value}`);
    }
    const missing = new URLSearchParams(query);
    missing.delete(key);
    assert.equal((await request(path + missing)).status, 400, `missing ${key}`);
  }
  query.set('id', entityReference('fleet', 'unmentioned').id);
  assert.equal((await request(path + query)).status, 404);
  assert.equal((await request('/v1/entities', {})).status, 404);
});

test('HTTP routes index tagged registration, nested data, and claim release without rewriting literal text', async t => {
  const { request } = await fixture(t);
  const displayName = '[[0:FLEET]]';
  const fleet = entityReference('fleet', 'FLEET');
  const shower = entityReference('fleet', 'Meteor Shower');
  const objectKey = entityReference('fleet', 'object key', 4);
  const claimKey = 'Observe [[153:Meteor Shower]]';
  const note = 'Ask [[8:FLEET]]';
  const body = 'Hey Jerry, did you see the [[153:Meteor Shower]] last night?';
  const data = { nested: ['[[7:FLEET]]', { '[[4:object key]]': '[[154:Meteor Shower]]' }], flag: true };
  const bareBody = 'still [[Meteor Shower]]';
  const laterBody = '[[200:Meteor Shower]]';
  const byId = (left: { id: string }, right: { id: string }) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
  async function lookup(id: string) {
    return request('/v1/entities?' + new URLSearchParams({ id, since: '0', limit: '10' }));
  }

  const bobRegistration = await request('/v1/sessions/register', { session: bob }, 'secret-b');
  assert.equal(bobRegistration.status, 200);
  assert.equal(bobRegistration.value.value.displayName, undefined);
  assert.equal(bobRegistration.value.value.entities, undefined);
  assert.equal(bobRegistration.value.value.subscription.id, sessionKey(bob));
  assert.deepEqual(bobRegistration.value.value.subscription.session, bob);
  assert.equal(bobRegistration.value.value.subscription.hostId, 'host-b');

  const registered = await request('/v1/sessions/register', { session: alice, displayName });
  assert.equal(registered.status, 200);
  const registration = registered.value.value;
  assert.equal(registration.displayName, displayName);
  assert.equal(registration.subscription.id, sessionKey(alice));
  assert.notEqual(registration.subscription.id, fleet.id);
  assert.deepEqual(registration.subscription.session, alice);
  assert.equal(registration.subscription.hostId, 'host-a');
  assert.equal(registration.rank, 1);
  assert.equal(typeof registration.subscription.cursor, 'number');
  assert.equal(typeof registration.lastSeenAt, 'number');
  assert.match(registration.entities[0].id, /^fleet:entity:00:[0-9a-f]{32}$/);
  assert.equal(registration.entities[0].id, entityReference('fleet', 'FLEET', 153).id);
  assert.deepEqual(registration.entities, [fleet]);

  const conflict = await request('/v1/sessions/register', { session: alice, displayName: '[[9:FLEET]]' }, 'secret-b');
  assert.equal(conflict.status, 409);
  assert.equal(conflict.value.error, 'owner_conflict');
  assert.deepEqual((await lookup(fleet.id)).value.value, { entity: fleet, messages: [] });
  const listed = await request('/v1/sessions');
  const aliceListed = listed.value.value.find((row: { subscription: { id: string } }) => row.subscription.id === sessionKey(alice));
  assert.equal(aliceListed.displayName, displayName);
  assert.deepEqual(aliceListed.subscription, registration.subscription);
  assert.deepEqual(aliceListed.entities, [fleet]);
  assert.equal(listed.value.value.find((row: { subscription: { id: string } }) => row.subscription.id === sessionKey(bob)).entities, undefined);

  assert.equal((await request('/v1/messages', {
    sender: alice, message: { recipient: sessionKey(bob), body: '[[99:FLEET]]', data: { nested: '[[99:Meteor Shower]]' } },
  }, 'secret-b')).status, 409);
  assert.equal((await lookup(shower.id)).status, 404);
  assert.equal((await lookup(fleet.id)).value.value.entity.type, 0);

  const sent = await request('/v1/messages', { sender: alice, message: { recipient: sessionKey(bob), body, data } });
  assert.equal(sent.status, 200);
  const message = sent.value.value;
  assert.equal(message.body, body);
  assert.deepEqual(message.data, data);
  assert.equal(message.sender, sessionKey(alice));
  assert.equal(message.recipient, sessionKey(bob));
  assert.equal(message.version, 1);
  assert.equal(message.visibility, 'private');
  assert.equal(message.id, `fleet:msg:${message.sequence}`);
  assert.equal(message.entities, undefined);
  assert.deepEqual((await lookup(shower.id)).value.value, { entity: entityReference('fleet', 'Meteor Shower', 154), messages: [message] });
  assert.deepEqual((await lookup(fleet.id)).value.value, { entity: entityReference('fleet', 'FLEET', 7), messages: [message] });
  assert.deepEqual((await lookup(objectKey.id)).value.value, { entity: objectKey, messages: [message] });

  const bareSent = await request('/v1/messages', { sender: alice, message: { recipient: '*', body: bareBody } });
  assert.equal(bareSent.status, 200);
  assert.equal(bareSent.value.value.body, bareBody);
  assert.equal(bareSent.value.value.data, null);
  assert.equal((await lookup(shower.id)).value.value.entity.type, 154, 'bare ref does not reset metadata');

  const heartbeat = await request('/v1/sessions/register', { session: alice });
  assert.equal(heartbeat.status, 200);
  assert.equal(heartbeat.value.value.displayName, displayName);
  assert.equal(heartbeat.value.value.rank, registration.rank);
  assert.deepEqual(heartbeat.value.value.subscription, registration.subscription);
  assert.deepEqual(heartbeat.value.value.entities, [entityReference('fleet', 'FLEET', 7)], 'heartbeat omits displayName and does not reapply its integer');

  const reasserted = await request('/v1/sessions/register', { session: alice, displayName });
  assert.equal(reasserted.status, 200);
  assert.equal(reasserted.value.value.displayName, displayName);
  assert.deepEqual(reasserted.value.value.subscription, registration.subscription);
  assert.deepEqual(reasserted.value.value.entities, [fleet], 'explicit same displayName reasserts metadata');
  const fleetAfterName = await lookup(fleet.id);
  assert.equal(fleetAfterName.value.value.entity.type, 0);
  assert.equal(fleetAfterName.value.value.messages[0].data.nested[0], '[[7:FLEET]]');

  const plain = await request('/v1/claims', { key: 'plain-task', owner: alice, ttlSeconds: 0 });
  assert.equal(plain.status, 200);
  assert.equal(plain.value.value.claim.key, 'plain-task');
  assert.equal(plain.value.value.claim.note, null);
  assert.equal(plain.value.value.claim.owner, sessionKey(alice));
  assert.equal(plain.value.value.claim.entities, undefined);
  assert.equal((await request('/v1/claims/release', { key: 'plain-task', owner: alice })).value.value, true);
  assert.deepEqual((await request('/v1/claims')).value.value, []);
  assert.equal((await lookup(fleet.id)).value.value.entity.type, 0);

  const inboxQuery = `?namespace=fleet&sessionId=${bob.sessionId}&since=0&limit=10`;
  const foreignInbox = await request('/v1/inbox' + inboxQuery);
  assert.equal(foreignInbox.status, 409);
  assert.equal(foreignInbox.value.error, 'owner_conflict');

  assert.equal((await request('/v1/claims', { key: claimKey, owner: alice, ttlSeconds: 0, note }, 'secret-b')).status, 409);
  assert.equal((await lookup(shower.id)).value.value.entity.type, 154);
  const claimed = await request('/v1/claims', { key: claimKey, owner: alice, ttlSeconds: 0, note });
  assert.equal(claimed.status, 200);
  assert.equal(claimed.value.value.acquired, true);
  const claim = claimed.value.value.claim;
  assert.equal(claim.key, claimKey);
  assert.equal(claim.note, note);
  assert.equal(claim.owner, sessionKey(alice));
  assert.equal(claim.expiresAt, null);
  assert.equal(typeof claim.claimedAt, 'number');
  assert.deepEqual(claim.entities, [entityReference('fleet', 'FLEET', 8), entityReference('fleet', 'Meteor Shower', 153)].sort(byId));
  assert.deepEqual((await request('/v1/claims')).value.value, [claim]);

  const later = await request('/v1/messages', { sender: alice, message: { recipient: '*', body: laterBody } });
  assert.equal(later.status, 200);
  assert.equal(later.value.value.body, laterBody);
  assert.equal(later.value.value.data, null);
  assert.equal((await lookup(shower.id)).value.value.entity.type, 200);
  const reread = await request('/v1/sessions');
  assert.equal(reread.value.value.find((row: { subscription: { id: string } }) => row.subscription.id === sessionKey(alice)).entities[0].type, 8);
  assert.equal(reread.value.value.find((row: { subscription: { id: string } }) => row.subscription.id === sessionKey(alice)).displayName, displayName);
  const listedClaims = await request('/v1/claims');
  assert.equal(listedClaims.value.value[0].key, claimKey);
  assert.equal(listedClaims.value.value[0].note, note);
  assert.equal(listedClaims.value.value[0].entities.find((entity: { label: string }) => entity.label === 'Meteor Shower').type, 200, 'reads do not replay claim text');
  assert.equal((await lookup(shower.id)).value.value.entity.type, 200);

  assert.equal((await request('/v1/claims/release', { key: claimKey, owner: alice }, 'secret-b')).status, 409);
  assert.equal((await request('/v1/claims/release', { key: claimKey, owner: bob })).status, 409);
  const missed = await request('/v1/claims/release', { key: claimKey, owner: bob }, 'secret-b');
  assert.equal(missed.status, 200);
  assert.equal(missed.value.value, false);
  assert.equal((await lookup(shower.id)).value.value.entity.type, 200);
  assert.equal((await request('/v1/claims')).value.value.length, 1);

  const released = await request('/v1/claims/release', { key: claimKey, owner: alice });
  assert.equal(released.status, 200);
  assert.equal(released.value.value, true);
  assert.equal(typeof released.value.value, 'boolean');
  assert.deepEqual((await request('/v1/claims')).value.value, []);
  const afterRelease = await lookup(shower.id);
  assert.equal(afterRelease.value.value.entity.type, 153, 'successful release parses its exact key');
  assert.deepEqual(afterRelease.value.value.messages.map((item: { body: string; data: unknown }) => [item.body, item.data]), [
    [body, data], [bareBody, null], [laterBody, null],
  ]);
  assert.deepEqual((await lookup(fleet.id)).value.value.entity, entityReference('fleet', 'FLEET', 8));
  assert.deepEqual((await lookup(fleet.id)).value.value.messages, [message]);
  const aliceAfter = (await request('/v1/sessions')).value.value.find((row: { subscription: { id: string } }) => row.subscription.id === sessionKey(alice));
  assert.equal(aliceAfter.displayName, displayName);
  assert.deepEqual(aliceAfter.subscription, registration.subscription);
  assert.deepEqual(aliceAfter.entities, [entityReference('fleet', 'FLEET', 8)]);
  const inbox = await request('/v1/inbox' + inboxQuery, undefined, 'secret-b');
  assert.equal(inbox.status, 200);
  assert.deepEqual(inbox.value.value.map((item: { body: string; data: unknown }) => [item.body, item.data]), [
    [body, data], [bareBody, null], [laterBody, null],
  ]);
  assert.equal((await request('/v1/claims/release', { key: claimKey, owner: alice })).value.value, false);
  assert.equal((await lookup(shower.id)).value.value.entity.type, 153);
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

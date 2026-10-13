import assert from 'node:assert/strict';
import test from 'node:test';
import { deliveryId, isJson, isMessage, isMessageInput, isRegistration, isRegistrationResult, isSessionAddress, isSessionId, isSessionKey, isStreamFrame, sessionKey, ROUTES,
  MAX_JSON_BYTES, MAX_SESSION_ID_LENGTH, MAX_STREAM_EVENT_BYTES, entityReference, extractEntities, isEntityId } from '../../src/protocol/index.ts';

const alice = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440000' };
const bob = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440001' };
const message = { version: 1, id: deliveryId('fleet', 1), sequence: 1, timestamp: 123, sender: sessionKey(alice), recipient: sessionKey(bob), body: 'Hello', data: null, visibility: 'private' };

test('entity identity includes namespace and canonical phrase but not integer metadata', () => {
  const generic = entityReference('fleet', 'fix-parser');
  assert.deepEqual(generic, { id: 'fleet:entity:00:09eec042175ea2041754a4d76bc2f603', type: 0, label: 'fix-parser' });
  assert.deepEqual(entityReference('fleet', 'fix-parser', 7), { ...generic, type: 7 });
  assert.deepEqual(entityReference('fleet', 'fix-parser', 255), { ...generic, type: 255 });
  assert.deepEqual(entityReference('fleet', ' café '), entityReference('fleet', 'cafe\u0301'));
  assert.notEqual(entityReference('other', 'fix-parser').id, generic.id);
  assert.notEqual(entityReference('fleet', 'Fix-parser').id, generic.id);
  assert.notEqual(entityReference('fleet', 'a b').id, entityReference('fleet', 'a  b').id);
  for (const type of [-1, 256, 1.5, NaN]) assert.throws(() => entityReference('fleet', 'label', type), TypeError);
  for (const label of ['', ' ', 'x'.repeat(257), 'a\nb', 'a\u2028b', 'a\u2029b', '\tname', '\uD800'])
    assert.throws(() => entityReference('fleet', label), TypeError);
  assert.equal(isEntityId(generic.id), true);
  assert.equal(isEntityId('fleet:entity:ff:82ffceb0347ff854f32d3a39d1e1625b'), true);
  for (const id of ['fleet:entity:100:09eec042175ea2041754a4d76bc2f603', `${generic.id}x`, generic.id.toUpperCase(), 42])
    assert.equal(isEntityId(id), false);
});

test('flat markers distinguish absent metadata from explicit zero and keep the last explicit value', () => {
  const body = 'Ask about [[fix-parser]], [[0:fix-parser]], [[7:fix-parser]] and [[255:wire format]]. [[ café ]] [[cafe\u0301]]';
  assert.deepEqual(extractEntities('fleet', body), [
    entityReference('fleet', 'fix-parser', 7),
    entityReference('fleet', 'wire format', 255),
    { id: entityReference('fleet', 'café').id, label: 'café' },
  ]);
  assert.deepEqual(extractEntities('fleet', '^^example^^ [[ ]] [[256:label]] [[two\nlines]] [[[nested]]] \\[[literal]] [[unclosed'), []);
  assert.deepEqual(extractEntities('fleet', '[[7:a\u2028b]] [[7:a\u2029b]] [[\tname]] [[7:\tname]]'), []);
  assert.deepEqual(extractEntities('fleet', '[[https:example]] and [[1: normal phrase ]]'), [
    { id: entityReference('fleet', 'https:example').id, label: 'https:example' }, entityReference('fleet', 'normal phrase', 1),
  ]);
  assert.deepEqual(extractEntities('fleet', '[[7:alice]] [[alice]] [[8:alice]] [[alice]]'), [entityReference('fleet', 'alice', 8)]);
  assert.deepEqual(extractEntities('fleet', '[[7:alice]] [[0:alice]] [[alice]]'), [entityReference('fleet', 'alice', 0)]);
  assert.equal(isMessageInput({ recipient: '*', body }), true, 'entity notation is still ordinary message text');
});

test('maximum normalized input fits the SSE budget with maximal v1 metadata', () => {
  const namespace = 'n'.repeat(63);
  const sender = sessionKey({ ...alice, namespace });
  const subscriptionId = sessionKey({ ...bob, namespace });
  for (const recipient of [subscriptionId, '*']) {
    const input = { recipient, body: '\n'.repeat(65536), data: '☃'.repeat(100) };
    input.data += 'x'.repeat(MAX_JSON_BYTES - Buffer.byteLength(JSON.stringify(input)));
    assert.equal(Buffer.byteLength(JSON.stringify(input)), MAX_JSON_BYTES);
    assert.equal(isMessageInput(input), true);
    assert.equal(isMessageInput({ ...input, data: `${input.data}x` }), false);
    const sequence = Number.MAX_SAFE_INTEGER;
    const frame = { type: 'message', subscriptionId, message: {
      ...input, version: 1, sender, visibility: 'private', sequence, timestamp: Number.MAX_SAFE_INTEGER,
      id: deliveryId(namespace, sequence),
    } };
    assert.equal(isStreamFrame(frame), true);
    const event = `id: ${frame.message.id}\nevent: message\ndata: ${JSON.stringify(frame)}\n\n`;
    assert.ok(Buffer.byteLength(event) > MAX_JSON_BYTES, 'metadata needs its own allowance');
    assert.ok(Buffer.byteLength(event) <= MAX_STREAM_EVENT_BYTES, 'allowance covers even longest metadata');
  }
});

test('versioned namespace+Pi UUID identity and strict envelope validation', () => {
  assert.notEqual(sessionKey(alice), sessionKey(bob));
  assert.equal(isMessage(message), true);
  assert.equal(isStreamFrame({ type: 'message', subscriptionId: sessionKey(bob), message }), true);
  assert.equal(isStreamFrame({ type: 'message', subscriptionId: sessionKey(alice), message }), false);
  assert.equal(isMessage({ ...message, id: 'old-project:1' }), false);
  assert.equal(isMessage({ ...message, version: 2 }), false);
  assert.equal(isMessage({ ...message, sequence: Number.MAX_SAFE_INTEGER + 1 }), false);
  assert.equal(isMessage({ ...message, body: '', extra: 1 }), false);
  assert.equal(isMessageInput({ recipient: '*', body: 'Broadcast' }), true);
  assert.equal(isMessageInput({ recipient: '*', body: 'Broadcast', unexpected: true }), false);
  // Documented registration example (docs/contract.md) omits optional lastSeenAt.
  assert.equal(isRegistrationResult({ subscription: { id: sessionKey(alice), session: alice, hostId: 'host-a', cursor: 0 }, rank: 1 }), true);
  assert.equal(isRegistrationResult({ subscription: { id: sessionKey(alice), session: alice, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 }), true);
  assert.equal(isRegistrationResult({ subscription: { id: sessionKey(alice), session: alice, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: -1 }), false);
  for (const lastSeenAt of [null, '1', 1.5, NaN, Number.MAX_SAFE_INTEGER + 1])
    assert.equal(isRegistrationResult({ subscription: { id: sessionKey(alice), session: alice, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt }), false);
  assert.equal(isRegistration({ session: alice }), true);
  assert.equal(isRegistration({ session: alice, callbackUrl: 'http://localhost' }), false);
  assert.equal(isRegistration({ session: { ...alice, callbackUrl: 'http://localhost' } }), false);
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  assert.equal(isJson(cyclic), false);
  assert.equal(ROUTES.stream, '/v1/hosts/stream');
});

test('one opaque session id preserves case; only a Pi UUID key is folded', () => {
  const raw = '550E8400-E29B-41D4-A716-446655440000';
  const folded = '550e8400-e29b-41d4-a716-446655440000';
  const nano = 'V1StGXR8_Z5jdHi6B-myT';
  assert.equal(isSessionAddress({ namespace: 'fleet', piSessionId: raw }), true);
  assert.equal(sessionKey({ namespace: 'fleet', piSessionId: raw }), `fleet:${folded}`);
  assert.equal(sessionKey({ namespace: 'fleet', piSessionId: folded }), `fleet:${folded}`);
  assert.equal(isSessionKey(`fleet:${raw}`), false, 'a UUID session key is the lowercase form');
  assert.equal(isSessionKey(`fleet:${folded}`), true);
  assert.equal(isSessionId(nano), true);
  assert.equal(sessionKey({ namespace: 'fleet', piSessionId: nano }), `fleet:${nano}`);
  assert.notEqual(sessionKey({ namespace: 'fleet', piSessionId: nano }), sessionKey({ namespace: 'fleet', piSessionId: nano.toLowerCase() }));
  assert.equal(isSessionKey(`fleet:${nano}`), true);
  assert.equal(isSessionKey(`fleet:${nano.toLowerCase()}`), true);
  assert.equal(isSessionId('Tracer-1'), true);
  assert.notEqual(
    sessionKey({ namespace: 'fleet', piSessionId: 'Tracer-1' }),
    sessionKey({ namespace: 'fleet', piSessionId: 'tracer-1' }),
  );
  assert.equal(isSessionKey('fleet:Tracer-1'), true);
  assert.equal(isSessionId('not-a-uuid'), true);
  assert.equal(isSessionId('a'.repeat(MAX_SESSION_ID_LENGTH)), true);
  assert.equal(isSessionId('A'.repeat(MAX_SESSION_ID_LENGTH)), true);
  assert.equal(isSessionId('a~b_c.d-1'), true);
  for (const id of ['_V1StGXR8_Z5jdHi6B-myT', '-tracer', '.tracer', '~tracer', 'A']) {
    assert.equal(isSessionId(id), true, id);
    assert.equal(sessionKey({ namespace: 'fleet', piSessionId: id }), `fleet:${id}`);
    assert.equal(isSessionKey(`fleet:${id}`), true);
  }
  assert.equal(sessionKey({ namespace: 'fleet', piSessionId: 'not-a-uuid' }), 'fleet:not-a-uuid');
  for (const id of [
    'not-a-uuid ', ' not-a-uuid', 'has:colon', 'other:tracer-1',
    '', 'a'.repeat(MAX_SESSION_ID_LENGTH + 1),
    'tracer/1', 'a&b', 'a=b', 'a?b', 'a%2f', 'a+b', 'a b', 'a\nb', 'a\u0000b',
    'tracer\n1', 'a\u2028b', 'a\u007fb', '{550e8400-e29b-41d4-a716-446655440000}', 'tracer\u0130',
  ]) {
    assert.equal(isSessionId(id), false, id);
    assert.throws(() => sessionKey({ namespace: 'fleet', piSessionId: id }), TypeError);
  }
  assert.notEqual(
    sessionKey({ namespace: 'fleet', piSessionId: '550e8400e29b41d4a716446655440000' }),
    sessionKey({ namespace: 'fleet', piSessionId: folded }),
  );
  assert.equal(isSessionKey('fleet:not-a-uuid:extra'), false);
  assert.equal(isSessionKey('fleet:Not-A-UUID'), true);
  const namespace = 'n'.repeat(63);
  const sender = sessionKey({ namespace, piSessionId: 'a'.repeat(MAX_SESSION_ID_LENGTH) });
  const recipient = sessionKey({ namespace, piSessionId: 'b'.repeat(MAX_SESSION_ID_LENGTH) });
  const input = { recipient, body: '\n'.repeat(65536), data: '\u2603'.repeat(100) };
  input.data += 'x'.repeat(MAX_JSON_BYTES - Buffer.byteLength(JSON.stringify(input)));
  const sequence = Number.MAX_SAFE_INTEGER;
  const frame = { type: 'message', subscriptionId: recipient, message: {
    ...input, version: 1, sender, visibility: 'private', sequence, timestamp: sequence,
    id: deliveryId(namespace, sequence),
  } };
  assert.equal(isStreamFrame(frame), true);
  const event = `id: ${frame.message.id}\nevent: message\ndata: ${JSON.stringify(frame)}\n\n`;
  assert.ok(Buffer.byteLength(event) <= MAX_STREAM_EVENT_BYTES);
});

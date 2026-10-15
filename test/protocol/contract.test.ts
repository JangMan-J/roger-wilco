import assert from 'node:assert/strict';
import test from 'node:test';
import { ownRegistration, ownRegistrationResult, type Registration, deliveryId, isDeliveryEvidence, isJson, isMessage, isMessageInput, isRegistration, isRegistrationResult, isSessionAddress, isSessionId, isSessionKey, isStreamFrame, sessionKey, ROUTES,
  MAX_JSON_BYTES, MAX_SESSION_ID_LENGTH, MAX_STREAM_EVENT_BYTES, entityReference, extractEntities, isEntityId,
  DEFAULT_SETTINGS, HOST_STREAM_VERSION, MAX_PRESENCE_KEYS, SETTING_BOUNDS, isHostFrame, isHostReceipt, isHostReceiptResult, isPresence, isPresenceResult,
  isSettingValue, presenceIntervalMs, streamIdleMs } from '../../src/protocol/index.ts';
import { createHash } from 'node:crypto';

const alice = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const bob = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440001' };
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
  // Version 1 fields are accepted in their old shape and dropped by the own* helpers.
  const result = { subscription: { id: sessionKey(alice), session: alice, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 };
  assert.equal(isRegistration({ session: alice, displayName: 'Alice' }), true);
  assert.equal(isRegistration({ session: alice, displayName: 'x'.repeat(201) }), false);
  assert.deepEqual(ownRegistration({ session: alice, displayName: 'Alice' } as Registration), { session: alice });
  const v1Result = { ...result, displayName: 'Alice', entities: [{ id: 'fleet:entity:00:09eec042175ea2041754a4d76bc2f603', type: 0, label: 'fix-parser' }] };
  assert.equal(isRegistrationResult(v1Result), true);
  assert.equal(isRegistrationResult({ ...result, displayName: 7 }), false);
  assert.deepEqual(ownRegistrationResult(v1Result), result);
  assert.deepEqual(ownRegistrationResult({ subscription: result.subscription, rank: 2 }), { subscription: result.subscription, rank: 2 });
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  assert.equal(isJson(cyclic), false);
  assert.equal(ROUTES.stream, '/v1/hosts/stream');
});

test('one opaque session id preserves case; only a UUID-shaped key is folded', () => {
  const raw = '550E8400-E29B-41D4-A716-446655440000';
  const folded = '550e8400-e29b-41d4-a716-446655440000';
  const nano = 'V1StGXR8_Z5jdHi6B-myT';
  assert.equal(isSessionAddress({ namespace: 'fleet', sessionId: raw }), true);
  assert.equal(sessionKey({ namespace: 'fleet', sessionId: raw }), `fleet:${folded}`);
  assert.equal(sessionKey({ namespace: 'fleet', sessionId: folded }), `fleet:${folded}`);
  assert.equal(isSessionKey(`fleet:${raw}`), false, 'a UUID session key is the lowercase form');
  assert.equal(isSessionKey(`fleet:${folded}`), true);
  assert.equal(isSessionId(nano), true);
  assert.equal(sessionKey({ namespace: 'fleet', sessionId: nano }), `fleet:${nano}`);
  assert.notEqual(sessionKey({ namespace: 'fleet', sessionId: nano }), sessionKey({ namespace: 'fleet', sessionId: nano.toLowerCase() }));
  assert.equal(isSessionKey(`fleet:${nano}`), true);
  assert.equal(isSessionKey(`fleet:${nano.toLowerCase()}`), true);
  assert.equal(isSessionId('Tracer-1'), true);
  assert.notEqual(
    sessionKey({ namespace: 'fleet', sessionId: 'Tracer-1' }),
    sessionKey({ namespace: 'fleet', sessionId: 'tracer-1' }),
  );
  assert.equal(isSessionKey('fleet:Tracer-1'), true);
  assert.equal(isSessionId('not-a-uuid'), true);
  assert.equal(isSessionId('a'.repeat(MAX_SESSION_ID_LENGTH)), true);
  assert.equal(isSessionId('A'.repeat(MAX_SESSION_ID_LENGTH)), true);
  assert.equal(isSessionId('a~b_c.d-1'), true);
  for (const id of ['_V1StGXR8_Z5jdHi6B-myT', '-tracer', '.tracer', '~tracer', 'A']) {
    assert.equal(isSessionId(id), true, id);
    assert.equal(sessionKey({ namespace: 'fleet', sessionId: id }), `fleet:${id}`);
    assert.equal(isSessionKey(`fleet:${id}`), true);
  }
  assert.equal(sessionKey({ namespace: 'fleet', sessionId: 'not-a-uuid' }), 'fleet:not-a-uuid');
  for (const id of [
    'not-a-uuid ', ' not-a-uuid', 'has:colon', 'other:tracer-1',
    '', 'a'.repeat(MAX_SESSION_ID_LENGTH + 1),
    'tracer/1', 'a&b', 'a=b', 'a?b', 'a%2f', 'a+b', 'a b', 'a\nb', 'a\u0000b',
    'tracer\n1', 'a\u2028b', 'a\u007fb', '{550e8400-e29b-41d4-a716-446655440000}', 'tracer\u0130',
  ]) {
    assert.equal(isSessionId(id), false, id);
    assert.throws(() => sessionKey({ namespace: 'fleet', sessionId: id }), TypeError);
  }
  assert.notEqual(
    sessionKey({ namespace: 'fleet', sessionId: '550e8400e29b41d4a716446655440000' }),
    sessionKey({ namespace: 'fleet', sessionId: folded }),
  );
  assert.equal(isSessionKey('fleet:not-a-uuid:extra'), false);
  assert.equal(isSessionKey('fleet:Not-A-UUID'), true);
  const namespace = 'n'.repeat(63);
  const sender = sessionKey({ namespace, sessionId: 'a'.repeat(MAX_SESSION_ID_LENGTH) });
  const recipient = sessionKey({ namespace, sessionId: 'b'.repeat(MAX_SESSION_ID_LENGTH) });
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

test('session addresses use sessionId; the pre-contract piSessionId field is rejected, not translated', () => {
  const legacy = { namespace: 'fleet', piSessionId: alice.sessionId };
  assert.equal(isSessionAddress(legacy), false);
  assert.equal(isSessionAddress({ ...alice, piSessionId: alice.sessionId }), false);
  assert.equal(isRegistration({ session: legacy }), false);
  assert.equal(isRegistration({ session: alice }), true);
});

test('delivery evidence is agent-neutral: a record inside a location, for one session id', () => {
  const evidence = { messageId: 'fleet:msg:1', sessionId: 'Tracer-1', recordId: 'rec-1', location: '/var/lib/agent/evidence.jsonl' };
  assert.equal(isDeliveryEvidence(evidence), true);
  assert.equal(isDeliveryEvidence({ ...evidence, sessionId: alice.sessionId, recordId: 'entry', location: 'session.jsonl' }), true);
  for (const bad of [
    { messageId: 'fleet:msg:1', piSessionId: alice.sessionId, entryId: 'entry', sessionFile: 'session.jsonl' },
    { ...evidence, extra: true }, { ...evidence, recordId: '' }, { ...evidence, location: '' },
    { ...evidence, sessionId: 'has:colon' }, { ...evidence, messageId: 1 }, null, [],
  ]) assert.equal(isDeliveryEvidence(bad), false, JSON.stringify(bad));
});

const STREAM = '7f3c0a1e9b2d4c6f8a0b1c2d3e4f5061';
const CONN = '6f1c2d3e-0a9b-4c8d-9e7f-102938475665';
const v2Message = { version: 1, id: 'fleet:msg:121', sequence: 121, timestamp: 1760500000000, sender: sessionKey(alice), recipient: '*', body: 'Build is green.', data: null, visibility: 'private' };
const v2Examples = {
  hello: { type: 'hello', stream: STREAM, host: 'host-a', conn: CONN, resume: 118, keepaliveMs: 15000, presenceWindowMs: 90000 },
  member: { type: 'member', session: sessionKey(alice), start: 118 },
  leave: { type: 'leave', session: sessionKey(alice) },
  msg: { type: 'msg', seq: 121, message: v2Message },
  through: { type: 'through', seq: 131 },
} as const;

test('v2 routes and constants', () => {
  assert.equal(ROUTES.streamV2, '/v2/hosts/stream');
  assert.equal(ROUTES.receiptV2, '/v2/hosts/receipts');
  assert.equal(ROUTES.presence, '/v2/hosts/presence');
  assert.equal(HOST_STREAM_VERSION, 2);
  assert.equal(MAX_PRESENCE_KEYS, 4096);
  assert.deepEqual(DEFAULT_SETTINGS, { window: 32, retentionMs: 7 * 86_400_000, keepaliveMs: 15_000, presenceWindowMs: 90_000 });
  for (const name of Object.keys(SETTING_BOUNDS) as (keyof typeof SETTING_BOUNDS)[]) {
    const { min, max } = SETTING_BOUNDS[name];
    assert.equal(isSettingValue(name, DEFAULT_SETTINGS[name]), true, name);
    assert.equal(isSettingValue(name, min), true, name);
    assert.equal(isSettingValue(name, max), true, name);
    for (const bad of [min - 1, max + 1, min + 0.5, String(min), null, NaN]) assert.equal(isSettingValue(name, bad), false, `${name} ${bad}`);
  }
  assert.equal(streamIdleMs(15_000), 45_000);
  assert.equal(presenceIntervalMs(90_000), 30_000);
  assert.equal(presenceIntervalMs(100_000), 33_333);
});

test('each HostFrame variant accepts its exact example and rejects extra or missing keys', () => {
  for (const [type, frame] of Object.entries(v2Examples)) {
    assert.equal(isHostFrame(frame), true, type);
    assert.equal(isHostFrame({ ...frame, extra: 1 }), false, `${type} extra key`);
    for (const key of Object.keys(frame)) {
      const missing: Record<string, unknown> = { ...frame };
      delete missing[key];
      assert.equal(isHostFrame(missing), false, `${type} without ${key}`);
      assert.equal(isHostFrame({ ...frame, [key]: undefined }), false, `${type} with undefined ${key}`);
    }
  }
  // The spec's wire example parses as given.
  const wire = 'data: {"type":"hello","stream":"7f3c0a1e9b2d4c6f8a0b1c2d3e4f5061","host":"host-a","conn":"6f1c2d3e-0a9b-4c8d-9e7f-102938475665","resume":118,"keepaliveMs":15000,"presenceWindowMs":90000}';
  assert.equal(isHostFrame(JSON.parse(wire.slice('data: '.length))), true);
  for (const bad of [null, [], 'hello', { type: 'message', subscriptionId: sessionKey(bob), message }, { type: 'unknown' }, { type: 'members', sessions: [] }])
    assert.equal(isHostFrame(bad), false, JSON.stringify(bad));
  assert.equal(isHostFrame(Object.assign(Object.create({ injected: true }), v2Examples.through)), false, 'non-plain object');
});

test('HostFrame integers are safe and non-negative, and position seq is positive', () => {
  for (const bad of [-1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, '1', null]) {
    assert.equal(isHostFrame({ ...v2Examples.hello, resume: bad }), false, `resume ${bad}`);
    assert.equal(isHostFrame({ ...v2Examples.member, start: bad }), false, `start ${bad}`);
    assert.equal(isHostFrame({ ...v2Examples.through, seq: bad }), false, `through ${bad}`);
    assert.equal(isHostFrame({ ...v2Examples.msg, seq: bad }), false, `msg ${bad}`);
  }
  assert.equal(isHostFrame({ ...v2Examples.through, seq: 0 }), false);
  assert.equal(isHostFrame({ ...v2Examples.msg, seq: 0 }), false);
  assert.equal(isHostFrame({ ...v2Examples.hello, resume: 0 }), true);
  assert.equal(isHostFrame({ ...v2Examples.member, start: 0 }), true);
  assert.equal(isHostFrame({ ...v2Examples.through, seq: Number.MAX_SAFE_INTEGER }), true);
});

test('msg requires a valid v1 message whose sequence equals seq', () => {
  assert.equal(isHostFrame({ ...v2Examples.msg, seq: 122 }), false);
  assert.equal(isHostFrame({ type: 'msg', seq: 1, message }), true, 'directed message');
  assert.equal(isHostFrame({ ...v2Examples.msg, message: { ...v2Message, version: 2 } }), false);
  assert.equal(isHostFrame({ ...v2Examples.msg, message: { ...v2Message, id: 'fleet:msg:120' } }), false);
  assert.equal(isHostFrame({ ...v2Examples.msg, message: { ...v2Message, extra: true } }), false);
  assert.equal(isHostFrame({ ...v2Examples.msg, message: { ...v2Message, body: '' } }), false);
});

test('member and leave require canonical session keys', () => {
  for (const session of ['fleet:550E8400-E29B-41D4-A716-446655440000', 'fleet', 'Fleet:a', 'fleet:a:b', '*', '', 7]) {
    assert.equal(isHostFrame({ ...v2Examples.member, session }), false, String(session));
    assert.equal(isHostFrame({ ...v2Examples.leave, session }), false, String(session));
  }
});

test('hello checks the stream id, conn, host and SETTING_BOUNDS', () => {
  for (const stream of [STREAM.toUpperCase(), STREAM.slice(1), `${STREAM}0`, STREAM.replace('7', 'g'), 7])
    assert.equal(isHostFrame({ ...v2Examples.hello, stream }), false, String(stream));
  for (const conn of [CONN.toUpperCase(), CONN.replaceAll('-', ''), `{${CONN}}`, 'conn-1', ''])
    assert.equal(isHostFrame({ ...v2Examples.hello, conn }), false, conn);
  assert.equal(isHostFrame({ ...v2Examples.hello, host: '' }), false);
  assert.equal(isHostFrame({ ...v2Examples.hello, host: 7 }), false);
  const keepalive = SETTING_BOUNDS.keepaliveMs, presence = SETTING_BOUNDS.presenceWindowMs;
  assert.equal(isHostFrame({ ...v2Examples.hello, keepaliveMs: keepalive.min, presenceWindowMs: presence.min }), true);
  assert.equal(isHostFrame({ ...v2Examples.hello, keepaliveMs: keepalive.max, presenceWindowMs: presence.max }), true);
  for (const keepaliveMs of [keepalive.min - 1, keepalive.max + 1, 15000.5, 0])
    assert.equal(isHostFrame({ ...v2Examples.hello, keepaliveMs }), false, `keepaliveMs ${keepaliveMs}`);
  for (const presenceWindowMs of [presence.min - 1, presence.max + 1, 90000.5, 0])
    assert.equal(isHostFrame({ ...v2Examples.hello, presenceWindowMs }), false, `presenceWindowMs ${presenceWindowMs}`);
});

test('the largest v2 msg event fits MAX_STREAM_EVENT_BYTES', () => {
  const namespace = 'n'.repeat(63);
  const sender = sessionKey({ namespace, sessionId: 'a'.repeat(MAX_SESSION_ID_LENGTH) });
  const input = { recipient: sessionKey({ namespace, sessionId: 'b'.repeat(MAX_SESSION_ID_LENGTH) }), body: '\n'.repeat(65536), data: '☃'.repeat(100) };
  input.data += 'x'.repeat(MAX_JSON_BYTES - Buffer.byteLength(JSON.stringify(input)));
  const seq = Number.MAX_SAFE_INTEGER;
  const frame = { type: 'msg', seq, message: { ...input, version: 1, sender, visibility: 'private', sequence: seq, timestamp: seq, id: deliveryId(namespace, seq) } };
  assert.equal(isHostFrame(frame), true);
  const v2 = `event: v2\ndata: ${JSON.stringify(frame)}\n\n`;
  const v1 = `id: ${frame.message.id}\nevent: message\ndata: ${JSON.stringify({ type: 'message', subscriptionId: input.recipient, message: frame.message })}\n\n`;
  assert.ok(Buffer.byteLength(v2) <= MAX_STREAM_EVENT_BYTES);
  assert.ok(Buffer.byteLength(v2) < Buffer.byteLength(v1), 'a v2 msg frame has less overhead than a v1 frame');
});

test('HostReceipt and HostReceiptResult accept exact shapes only', () => {
  assert.equal(isHostReceipt({ conn: CONN, through: 131 }), true);
  assert.equal(isHostReceipt({ conn: CONN, through: 0 }), true);
  for (const bad of [
    { conn: 'not-a-uuid', through: 1 }, { conn: CONN.toUpperCase(), through: 1 }, { conn: CONN, through: -1 },
    { conn: CONN, through: 1.5 }, { conn: CONN, through: Number.MAX_SAFE_INTEGER + 1 }, { conn: CONN, through: '1' },
    { conn: CONN }, { through: 1 }, { conn: CONN, through: 1, extra: true },
    { subscriptionId: sessionKey(bob), messageId: 'fleet:msg:1' }, null, [],
  ]) assert.equal(isHostReceipt(bad), false, JSON.stringify(bad));
  assert.equal(isHostReceiptResult({ through: 131 }), true);
  assert.equal(isHostReceiptResult({ through: 0 }), true);
  for (const bad of [{ through: -1 }, { through: 1.5 }, {}, { through: 1, conn: CONN }, null])
    assert.equal(isHostReceiptResult(bad), false, JSON.stringify(bad));
});

test('Presence and PresenceResult take canonical, unique keys, at most MAX_PRESENCE_KEYS', () => {
  const keys = Array.from({ length: MAX_PRESENCE_KEYS }, (_, index) => `fleet:s${index}`);
  for (const [accept, field] of [[isPresence, 'sessions'], [isPresenceResult, 'unknown']] as const) {
    assert.equal(accept({ [field]: [] }), true, field);
    assert.equal(accept({ [field]: [sessionKey(alice), sessionKey(bob)] }), true, field);
    assert.equal(accept({ [field]: keys }), true, `${field} at the cap`);
    assert.equal(accept({ [field]: [...keys, 'fleet:extra'] }), false, `${field} above the cap`);
    assert.equal(accept({ [field]: [sessionKey(alice), sessionKey(alice)] }), false, `${field} duplicate`);
    for (const key of ['fleet:550E8400-E29B-41D4-A716-446655440000', 'fleet', '*', 'fleet:a:b', 7, null])
      assert.equal(accept({ [field]: [key] }), false, `${field} key ${key}`);
    assert.equal(accept({ [field]: 'fleet:a' }), false, `${field} not an array`);
    assert.equal(accept({}), false, `${field} missing`);
    assert.equal(accept(null), false, `${field} null`);
  }
  assert.equal(isPresence({ sessions: [], extra: true }), false);
  assert.equal(isPresence({ unknown: [] }), false);
  assert.equal(isPresenceResult({ unknown: [], foreign: [] }), false, 'the removed foreign result is rejected');
  assert.equal(isPresenceResult({ sessions: [] }), false);
});

test('a v2 msg carries the v1 message bytes and hash unchanged; v1 golden frames still validate', () => {
  // Golden v1 SSE frame from docs/contract.md.
  const golden = '{"type":"message","subscriptionId":"fleet:550e8400-e29b-41d4-a716-446655440000","message":{"version":1,"id":"fleet:msg:1","sequence":1,"timestamp":123,"sender":"fleet:550e8400-e29b-41d4-a716-446655440001","recipient":"fleet:550e8400-e29b-41d4-a716-446655440000","body":"Please read the update.","data":null,"visibility":"private"}}';
  const v1 = JSON.parse(golden);
  assert.equal(isStreamFrame(v1), true);
  assert.equal(isHostFrame(v1), false, 'a v1 frame is not a v2 frame');
  const v2 = JSON.parse(JSON.stringify({ type: 'msg', seq: v1.message.sequence, message: v1.message }));
  assert.equal(isHostFrame(v2), true);
  assert.equal(isStreamFrame(v2), false, 'a v2 frame is not a v1 frame');
  const bytes = (value: unknown) => JSON.stringify(value);
  const hash = (value: unknown) => createHash('sha256').update(bytes(value)).digest('hex');
  assert.equal(bytes(v2.message), bytes(v1.message));
  assert.equal(hash(v2.message), hash(v1.message));
  assert.equal(golden.includes(bytes(v2.message)), true, 'the message bytes are the golden bytes');
});

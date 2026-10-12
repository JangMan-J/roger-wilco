import assert from 'node:assert/strict';
import test from 'node:test';
import { deliveryId, isJson, isMessage, isMessageInput, isRegistration, isStreamFrame, sessionKey, ROUTES,
  MAX_JSON_BYTES, MAX_STREAM_EVENT_BYTES } from '../../src/protocol/index.ts';

const alice = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440000' };
const bob = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440001' };
const message = { version: 1, id: deliveryId('fleet', 1), sequence: 1, timestamp: 123, sender: sessionKey(alice), recipient: sessionKey(bob), body: 'Hello', data: null, visibility: 'private' };

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
  assert.equal(isRegistration({ session: alice }), true);
  assert.equal(isRegistration({ session: alice, callbackUrl: 'http://localhost' }), false);
  assert.equal(isRegistration({ session: { ...alice, callbackUrl: 'http://localhost' } }), false);
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  assert.equal(isJson(cyclic), false);
  assert.equal(ROUTES.stream, '/v1/hosts/stream');
});

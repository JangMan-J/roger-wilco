import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { sessionKey, type CoordinatorStore, type SessionAddress, type StreamFrame } from '../../src/protocol/index.ts';

const a: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440001' };
const b: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440002' };
const c: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440003' };
const hostA = { hostId: 'host-a' };
const hostB = { hostId: 'host-b' };
const auth = { authenticate(token: string | null) {
  if (token === 'a') return { ok: true as const, value: hostA };
  if (token === 'b') return { ok: true as const, value: hostB };
  return { ok: false as const, error: 'unauthorized' as const, message: 'Invalid credential' };
} };
function ok<T>(value: { ok: true; value: T } | { ok: false; error: string }): T {
  assert.equal(value.ok, true, !value.ok ? value.error : 'expected success');
  return (value as { ok: true; value: T }).value;
}
async function fixture(t: { after(fn: () => Promise<void>): void }) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-delivery-'));
  const path = join(dir, 'coordinator.sqlite');
  let store = createCoordinatorStore(path, 'fleet');
  let server: Server | undefined;
  t.after(async () => {
    if (server) { server.closeAllConnections(); await new Promise<void>(resolve => server!.close(() => resolve())); }
    store.close();
    await rm(dir, { recursive: true, force: true });
  });
  async function listen() {
    server = createServer((req, res) => { void createHandler(req, res); });
    const createHandler = createStreamHandler(store, auth);
    await new Promise<void>(resolve => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    assert.ok(address && typeof address !== 'string');
    return `http://127.0.0.1:${address.port}`;
  }
  return { path, get store() { return store; }, async reopen() {
    if (server) { server.closeAllConnections(); await new Promise<void>(resolve => server!.close(() => resolve())); server = undefined; }
    store.close(); store = createCoordinatorStore(path, 'fleet');
  }, listen };
}
type Stream = { response: Response; next(): Promise<StreamFrame>; cancel(): void; closed(): Promise<boolean> };
async function stream(base: string, token = 'a'): Promise<Stream> {
  const controller = new AbortController();
  const response = await fetch(`${base}/v1/hosts/stream`, { headers: { authorization: `Bearer ${token}`, accept: 'text/event-stream' }, signal: controller.signal });
  if (response.status !== 200) { controller.abort(); throw new Error(`stream HTTP ${response.status}`); }
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const read = async () => {
    const timer = setTimeout(() => controller.abort(), 4000);
    try { return await reader.read(); } finally { clearTimeout(timer); }
  };
  return {
    response,
    cancel: () => controller.abort(),
    async closed() { try { return (await read()).done ?? false; } catch { return true; } },
    async next() {
      while (true) {
        const boundary = buffer.indexOf('\n\n');
        if (boundary >= 0) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const data = block.split('\n').find(line => line.startsWith('data: '));
          if (data) return JSON.parse(data.slice(6)) as StreamFrame;
          continue;
        }
        const item = await read();
        if (item.done) throw new Error('stream closed before message');
        buffer += decoder.decode(item.value, { stream: true });
      }
    },
  };
}
async function receipt(base: string, token: string, body: unknown) {
  const response = await fetch(`${base}/v1/hosts/receipts`, { method: 'POST', headers: {
    authorization: `Bearer ${token}`, 'content-type': 'application/json',
  }, body: JSON.stringify(body) });
  return { status: response.status, body: await response.json() as { ok: boolean; value?: { cursor: number }; error?: string } };
}

test('HTTP SSE catches up from SQLite, gates one per subscription, accepts durable ACK and replays on reopen', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  const first = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'first' }));
  const second = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'second' }));
  let base = await f.listen();
  let s = await stream(base);
  t.after(() => s.cancel());
  assert.equal((await s.next()).message.id, first.id);
  // Superseding an unacknowledged connection replays the exact durable ID, not an event-buffer tail.
  const replacement = await stream(base);
  assert.equal(await s.closed(), true);
  s = replacement;
  assert.equal((await s.next()).message.id, first.id);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: second.id })).status, 400);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: first.id })).body.value?.cursor, first.sequence);
  assert.equal((await s.next()).message.id, second.id);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: second.id })).body.value?.cursor, second.sequence);
  s.cancel();
  await f.reopen();
  assert.deepEqual(f.store.pending(hostA, 10), []);
  const third = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'after restart' }));
  base = await f.listen();
  s = await stream(base);
  assert.equal((await s.next()).message.id, third.id);
});

test('authentication, malformed and cross-host receipts fail without advancing; concurrent host streams stay independent', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  ok(f.store.register(hostB, { session: c }));
  const base = await f.listen();
  const denied = await fetch(`${base}/v1/hosts/stream`, { headers: { accept: 'text/event-stream' } });
  assert.equal(denied.status, 401);
  assert.equal((await receipt(base, 'wrong', { subscriptionId: sessionKey(b), messageId: 'fleet:msg:1' })).status, 401);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: 'fleet:msg:0' })).status, 400);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: 'fleet:msg:1', extra: true })).status, 400);
  const malformed = await fetch(`${base}/v1/hosts/receipts`, { method: 'POST', headers: {
    authorization: 'Bearer a', 'content-type': 'application/json',
  }, body: '{' });
  assert.equal(malformed.status, 400);
  const slow = await stream(base, 'a');
  const other = await stream(base, 'b');
  t.after(() => { slow.cancel(); other.cancel(); });
  const toA = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'no ACK from host A' }));
  assert.equal((await slow.next()).message.id, toA.id);
  assert.equal((await receipt(base, 'b', { subscriptionId: sessionKey(b), messageId: toA.id })).status, 409);
  const toB = ok(f.store.send(hostA, a, { recipient: sessionKey(c), body: 'host B unaffected' }));
  assert.equal((await other.next()).message.id, toB.id);
  assert.equal((await receipt(base, 'b', { subscriptionId: sessionKey(c), messageId: toB.id })).body.value?.cursor, toB.sequence);
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: toA.id })).body.value?.cursor, toA.sequence);
});

test('one stalled subscription does not advance past its first item; handler close cancels streams promptly', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  ok(f.store.register(hostA, { session: c }));
  const first = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'first' }));
  ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'must wait' }));
  const other = ok(f.store.send(hostA, a, { recipient: sessionKey(c), body: 'independent' }));
  const handler = createStreamHandler(f.store, auth);
  const server = createServer((req, res) => { void handler(req, res); });
  t.after(async () => { await handler.close(); server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}`;
  const s = await stream(base);
  assert.equal((await s.next()).message.id, first.id);
  assert.equal((await s.next()).message.id, other.id);
  await handler.close();
  assert.equal(await s.closed(), true);
  assert.equal((await fetch(`${base}/v1/hosts/stream`, { headers: { authorization: 'Bearer a', accept: 'text/event-stream' } })).status, 503);
});

test('message arriving after initial empty poll reaches the stream without reconnect', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  const base = await f.listen();
  const s = await stream(base);
  t.after(() => s.cancel());
  const message = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'live' }));
  assert.equal((await s.next()).message.id, message.id);
});

test('quiet idle does not reread pending; send and receipt wake the stream', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  let reads = 0;
  const store = new Proxy(f.store, {
    get(target, property, receiver) {
      if (property === 'pending') return (host: typeof hostA, limit: number) => { reads += 1; return target.pending(host, limit); };
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }) as CoordinatorStore;
  const handler = createStreamHandler(store, auth);
  const server = createServer((req, res) => { void handler(req, res); });
  t.after(async () => { await handler.close(); server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}`;
  const s = await stream(base);
  t.after(() => s.cancel());
  const until = async (ready: () => boolean, label: string) => {
    const deadline = Date.now() + 200;
    while (!ready()) {
      assert.ok(Date.now() < deadline, label);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  };
  const idle = async () => {
    const mark = reads;
    await new Promise(resolve => setTimeout(resolve, 600));
    assert.equal(reads, mark);
  };
  await until(() => reads >= 1, 'expected initial replay read');
  await idle();
  const first = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'wake' }));
  assert.equal((await s.next()).message.id, first.id);
  await idle();
  const beforeQueued = reads;
  const second = ok(f.store.send(hostA, a, { recipient: sessionKey(b), body: 'queued' }));
  await until(() => reads > beforeQueued, 'send did not wake replay');
  let early: string | undefined;
  const queued = s.next().then(frame => { early = frame.message.id; return frame; });
  await new Promise(resolve => setTimeout(resolve, 100));
  assert.equal(early, undefined);
  const beforeAck = reads;
  assert.equal((await receipt(base, 'a', { subscriptionId: sessionKey(b), messageId: first.id })).body.value?.cursor, first.sequence);
  await until(() => reads > beforeAck, 'receipt did not wake replay');
  assert.equal((await queued).message.id, second.id);
});

test('a throwing listener cannot hide a committed wake', async t => {
  const f = await fixture(t);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  const base = await f.listen();
  const s = await stream(base);
  t.after(() => s.cancel());
  f.store.subscribe(() => { throw new Error('notifier fault'); });
  const waiting = s.next();
  assert.throws(() => f.store.send(hostA, a, { recipient: sessionKey(b), body: 'fault' }), /notifier fault/);
  assert.equal((await waiting).message.body, 'fault');
});

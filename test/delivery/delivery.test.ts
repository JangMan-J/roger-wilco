import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { connect as connectSocket } from 'node:net';
import { once } from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createStreamHandler, INFLIGHT_BYTE_CAP } from '../../src/server/delivery/index.ts';
import { createControlHandler } from '../../src/server/control/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';
import {
  DEFAULT_SETTINGS, ROUTES, isHostFrame, sessionKey,
  type CoordinatorStore, type HostFrame, type SessionAddress, type Settings, type StreamFrame,
} from '../../src/protocol/index.ts';

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

// ---- Host stream v2 (docs/protocol-v2.md, test plan 15-23) ----
// The daemon side is a minimal fake written here: it parses `event: v2` frames, checks the
// order rules a v2 daemon enforces, and posts receipts. It is not the real daemon.

const x: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440010' };
const y: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440011' };
type Event = HostFrame | { type: 'keepalive' };

async function v2Fixture(t: test.TestContext, settings: Partial<Settings> = {}, wrap = (store: CoordinatorStore) => store) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-delivery-v2-'));
  const path = join(dir, 'coordinator.sqlite');
  const store = createCoordinatorStore(path, 'fleet');
  const full: Settings = { ...DEFAULT_SETTINGS, ...settings };
  let server: Server | undefined;
  let handler: ReturnType<typeof createStreamHandler> | undefined;
  let base = '';
  async function start() {
    const wrapped = wrap(store);
    const control = createControlHandler(wrapped, auth, full);
    const stream = handler = createStreamHandler(wrapped, auth, full);
    const routes: string[] = [ROUTES.stream, ROUTES.receipt, ROUTES.streamV2, ROUTES.receiptV2];
    server = createServer((req, res) => {
      const route = new URL(req.url ?? '/', 'http://localhost').pathname;
      void (routes.includes(route) ? stream : control)(req, res);
    });
    await new Promise<void>(resolve => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    assert.ok(address && typeof address !== 'string');
    base = `http://127.0.0.1:${address.port}`;
  }
  async function stop() {
    await handler?.close();
    if (server) { server.closeAllConnections(); await new Promise<void>(resolve => server!.close(() => resolve())); server = undefined; }
  }
  await start();
  t.after(async () => { await stop(); store.close(); await rm(dir, { recursive: true, force: true }); });
  return { store, path, get base() { return base; }, get handler() { return handler!; },
    async restart() { await stop(); await start(); } };
}

/** A fake v2 daemon connection: frames in arrival order, with the daemon's order checks applied. */
async function openV2(base: string, token = 'a', query = '') {
  const controller = new AbortController();
  const response = await fetch(`${base}${ROUTES.streamV2}${query}`, {
    headers: { authorization: `Bearer ${token}`, accept: 'text/event-stream' }, signal: controller.signal });
  assert.equal(response.status, 200, `v2 stream HTTP ${response.status}`);
  assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream/);
  const events: Event[] = [];
  const members = new Map<string, number>();
  let resume = -1;
  let last = -1;
  let ended = false;
  let notify = () => {};
  const accept = (event: Event) => {
    if (event.type === 'keepalive') { events.push(event); return; }
    assert.ok(isHostFrame(event), `invalid frame ${JSON.stringify(event).slice(0, 200)}`);
    if (!events.length) assert.equal(event.type, 'hello', 'hello comes first');
    else assert.notEqual(event.type, 'hello', 'hello comes once');
    if (event.type === 'hello') { resume = event.resume; last = event.resume; }
    if (event.type === 'member') members.set(event.session, event.start);
    if (event.type === 'leave') members.delete(event.session);
    if (event.type === 'msg' || event.type === 'through') {
      assert.ok(event.seq > last, `position ${event.seq} not above ${last}`);
      last = event.seq;
    }
    if (event.type === 'msg' && event.message.recipient !== '*') {
      const start = members.get(event.message.recipient);
      assert.ok(start !== undefined && start < event.seq, 'a directed msg names a current member below its start');
    }
    events.push(event);
  };
  const pump = (async () => {
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let end: number;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          if (block === ': keepalive') accept({ type: 'keepalive' });
          else {
            const lines = block.split('\n');
            assert.equal(lines[0], 'event: v2');
            assert.equal(lines.length, 2, 'one data line, no id line');
            accept(JSON.parse(lines[1]!.slice('data: '.length)));
          }
          notify();
        }
      }
    } catch (error) { if (!controller.signal.aborted) throw error; }
    finally { ended = true; notify(); }
  })();
  void pump.catch(() => {});
  let read = 0;
  const connection = {
    events, members,
    get ended() { return ended; },
    get hello() { return events[0] as Extract<HostFrame, { type: 'hello' }>; },
    /** The next unread event. */
    async next(timeout = 4000): Promise<Event> {
      const deadline = Date.now() + timeout;
      while (read >= events.length) {
        assert.ok(!ended, 'stream ended');
        assert.ok(Date.now() < deadline, 'no event before the deadline');
        await new Promise<void>(resolve => { notify = resolve; setTimeout(resolve, 20); });
      }
      return events[read++]!;
    },
    /** Read until a position frame, returning the frames read. */
    async untilPosition(timeout = 4000): Promise<Event[]> {
      const seen: Event[] = [];
      while (true) {
        const event = await connection.next(timeout);
        seen.push(event);
        if (event.type === 'msg' || event.type === 'through') return seen;
      }
    },
    unread: () => events.slice(read),
    async closed(timeout = 4000) {
      const deadline = Date.now() + timeout;
      while (!ended && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
      return ended;
    },
    close() { controller.abort(); },
    resume: () => resume,
  };
  await connection.next();
  return connection;
}
async function postV2(base: string, route: string, token: string, body: unknown) {
  const response = await fetch(`${base}${route}`, { method: 'POST', headers: {
    authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify(body) });
  return { status: response.status, body: await response.json() as { ok: boolean; value?: any; error?: string; message?: string } };
}
const receiptV2 = (base: string, token: string, body: unknown) => postV2(base, ROUTES.receiptV2, token, body);
async function quiet(connection: { unread(): Event[] }, ms: number) {
  await new Promise(resolve => setTimeout(resolve, ms));
  return connection.unread();
}
const positions = (events: Event[]) => events.filter(event => event.type === 'msg' || event.type === 'through')
  .map(event => `${event.type}:${(event as { seq: number }).seq}`);

test('v2 open: hello first, membership before msg, positions above resume; a refused open writes no SSE header', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  const first = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'directed' }));
  ok(f.store.send(hostB, x, { recipient: sessionKey(x), body: 'other host' }));
  const broadcast = ok(f.store.send(hostB, x, { recipient: '*', body: 'broadcast' }));
  const tail = ok(f.store.send(hostB, x, { recipient: sessionKey(x), body: 'other host again' }));
  // Bad queries and a refused open: JSON outcomes, no event stream.
  for (const query of ['?stream=' + '0'.repeat(32), '?after=1', '?stream=XYZ&after=1', '?stream=' + '0'.repeat(32) + '&after=01',
    '?stream=' + '0'.repeat(32) + '&after=1&after=2', '?stream=' + '0'.repeat(32) + '&after=1&x=1']) {
    const response = await fetch(`${f.base}${ROUTES.streamV2}${query}`, { headers: { authorization: 'Bearer a', accept: 'text/event-stream' } });
    assert.equal(response.status, 400, query);
    assert.match(response.headers.get('content-type') ?? '', /^application\/json/);
    await response.body?.cancel();
  }
  const s = await openV2(f.base);
  t.after(() => s.close());
  assert.deepEqual({ ...s.hello, conn: 'c' }, { type: 'hello', stream: s.hello.stream, host: 'host-a', conn: 'c', resume: 0,
    keepaliveMs: DEFAULT_SETTINGS.keepaliveMs, presenceWindowMs: DEFAULT_SETTINGS.presenceWindowMs });
  const events = [s.hello, ...await s.untilPosition()];
  events.push(...await s.untilPosition(), ...await s.untilPosition());
  assert.deepEqual(events.map(event => event.type), ['hello', 'member', 'member', 'msg', 'msg', 'through']);
  assert.deepEqual(positions(events), [`msg:${first.sequence}`, `msg:${broadcast.sequence}`, `through:${tail.sequence}`]);
  s.close();
  const ahead = await fetch(`${f.base}${ROUTES.streamV2}?stream=${s.hello.stream}&after=${tail.sequence + 1}`,
    { headers: { authorization: 'Bearer a', accept: 'text/event-stream' } });
  assert.equal(ahead.status, 409);
  assert.match(ahead.headers.get('content-type') ?? '', /^application\/json/);
  assert.equal(((await ahead.json()) as { message: string }).message, 'Bookmark is ahead of the coordinator tail');
});

test('v2 window: W frames before any receipt, the process stays responsive, a receipt releases exactly its frames', async t => {
  const f = await v2Fixture(t, { window: 4 });
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const sent = Array.from({ length: 10 }, (_, i) => ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: `m${i}` })));
  const s = await openV2(f.base);
  t.after(() => s.close());
  const other = await openV2(f.base, 'b');
  t.after(() => other.close());
  for (let i = 0; i < 4; i++) await s.untilPosition();
  assert.deepEqual(positions(s.events), sent.slice(0, 4).map(m => `msg:${m.sequence}`));
  assert.deepEqual(await quiet(s, 300), [], 'no fifth frame before a receipt');
  // Another host's receipt and /health are answered while this host's window is full.
  assert.deepEqual((await receiptV2(f.base, 'b', { conn: other.hello.conn, through: 0 })).body, { ok: true, value: { through: other.hello.resume } });
  assert.equal((await fetch(`${f.base}/health`)).status, 200);
  assert.deepEqual((await receiptV2(f.base, 'a', { conn: s.hello.conn, through: sent[1]!.sequence })).body,
    { ok: true, value: { through: sent[1]!.sequence } });
  await s.untilPosition(); await s.untilPosition();
  assert.deepEqual(await quiet(s, 300), [], 'a receipt at the 2nd frame releases exactly 2');
  assert.deepEqual(positions(s.events).slice(4), sent.slice(4, 6).map(m => `msg:${m.sequence}`));
  assert.equal(f.store.hostAcked(hostA), sent[1]!.sequence);
});

test('v2 byte cap: unreceipted bytes stop a batch early, but a batch always sends at least one frame', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const data = 'x'.repeat(1_000_000);
  const sent = Array.from({ length: 20 }, (_, i) => ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: `big ${i}`, data })));
  const s = await openV2(f.base);
  t.after(() => s.close());
  const frameBytes = Buffer.byteLength(`event: v2\ndata: ${JSON.stringify({ type: 'msg', seq: sent[0]!.sequence, message: sent[0] })}\n\n`);
  const expected = Math.ceil(INFLIGHT_BYTE_CAP / frameBytes);
  assert.ok(expected < 20 && expected < DEFAULT_SETTINGS.window);
  for (let i = 0; i < expected; i++) await s.untilPosition(8000);
  assert.deepEqual(await quiet(s, 300), [], 'the frame that crosses the cap is the last one');
  ok((await receiptV2(f.base, 'a', { conn: s.hello.conn, through: sent[0]!.sequence })).body as never);
  await s.untilPosition(8000);
  assert.deepEqual(await quiet(s, 300), [], 'one frame after releasing one');
  assert.deepEqual(positions(s.events), sent.slice(0, expected + 1).map(m => `msg:${m.sequence}`));
});

test('v2 receipts: <= C is idempotent from any conn; (C, sentThrough] advances C; everything else is refused unchanged', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const m1 = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'one' }));
  ok(f.store.send(hostB, x, { recipient: sessionKey(x), body: 'gap' }));
  ok(f.store.send(hostB, x, { recipient: sessionKey(x), body: 'gap' }));
  const m2 = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'two' }));
  const s = await openV2(f.base);
  t.after(() => s.close());
  await s.untilPosition(); await s.untilPosition();
  const { conn } = s.hello;
  const stranger = '00000000-0000-4000-8000-000000000000';
  assert.deepEqual((await receiptV2(f.base, 'a', { conn: stranger, through: 0 })).body, { ok: true, value: { through: 0 } });
  assert.equal((await receiptV2(f.base, 'a', { conn: stranger, through: m1.sequence })).status, 409);
  assert.equal((await receiptV2(f.base, 'a', { conn, through: m2.sequence + 1 })).status, 400);
  assert.equal((await receiptV2(f.base, 'a', { subscriptionId: sessionKey(a), messageId: m1.id })).status, 400);
  assert.equal((await receiptV2(f.base, 'a', { conn: conn.toUpperCase(), through: 1 })).status, 400);
  assert.equal((await receiptV2(f.base, 'b', { conn, through: m1.sequence })).status, 409, 'another host cannot move this row');
  assert.equal(f.store.hostAcked(hostA), 0);
  // A value between two sent frames is sound: nothing is owed between them.
  const between = m1.sequence + 1;
  assert.deepEqual((await receiptV2(f.base, 'a', { conn, through: between })).body, { ok: true, value: { through: between } });
  assert.equal(f.store.hostAcked(hostA), between);
  assert.deepEqual((await receiptV2(f.base, 'a', { conn: stranger, through: m1.sequence })).body, { ok: true, value: { through: between } });
  // After a coordinator restart every conn is unknown.
  await f.restart();
  assert.equal((await receiptV2(f.base, 'a', { conn, through: m2.sequence })).status, 409);
  assert.equal(f.store.hostAcked(hostA), between);
  // A v1 host has no hosts row.
  assert.equal((await receiptV2(f.base, 'b', { conn, through: 0 })).body.message, 'Host is not on protocol 2');
});

test('v2 through: a trailing through follows a msg batch', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const m = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'owed' }));
  const after = ok(f.store.send(hostB, x, { recipient: sessionKey(x), body: 'irrelevant' }));
  const s = await openV2(f.base);
  t.after(() => s.close());
  await s.untilPosition();
  await s.untilPosition();
  assert.deepEqual(positions(s.events), [`msg:${m.sequence}`, `through:${after.sequence}`]);
  assert.deepEqual(await quiet(s, 200), []);
});

test('v2 supersede: v2 replaces v1; a v1 open or receipt for a v2 host is refused without ending the v2 stream', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const v1 = await stream(f.base);
  const s = await openV2(f.base);
  t.after(() => s.close());
  assert.equal(await v1.closed(), true, 'the v2 open aborts the live v1 stream');
  const refused = await fetch(`${f.base}${ROUTES.stream}`, { headers: { authorization: 'Bearer a', accept: 'text/event-stream' } });
  assert.equal(refused.status, 409);
  assert.equal(((await refused.json()) as { message: string }).message, 'Host uses protocol 2');
  const m = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'still live' }));
  assert.equal((await receipt(f.base, 'a', { subscriptionId: sessionKey(a), messageId: m.id })).status, 409);
  await s.next(); // member
  assert.deepEqual(positions(await s.untilPosition()), [`msg:${m.sequence}`]);
  assert.equal(s.ended, false);
  const second = await openV2(f.base);
  t.after(() => second.close());
  assert.equal(await s.closed(), true, 'a second v2 open aborts the first');
  assert.notEqual(second.hello.conn, s.hello.conn);
  assert.equal((await receiptV2(f.base, 'a', { conn: s.hello.conn, through: m.sequence })).status, 409);
});

test('v2 wakes: a broadcast wakes every stream, a directed send only its host; registration and presence wake none', async t => {
  const reads = new Map<string, number>();
  const count = (store: CoordinatorStore) => new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'hostBatch') return (host: { hostId: string }, from: number, limit: number) => {
        reads.set(host.hostId, (reads.get(host.hostId) ?? 0) + 1);
        return target.hostBatch(host, from, limit);
      };
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }) as CoordinatorStore;
  const f = await v2Fixture(t, {}, count);
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostB, { session: x }));
  const sa = await openV2(f.base, 'a');
  const sb = await openV2(f.base, 'b');
  t.after(() => { sa.close(); sb.close(); });
  await sa.next(); await sb.next();
  const settle = async () => { await new Promise(resolve => setTimeout(resolve, 150)); return { a: reads.get('host-a')!, b: reads.get('host-b')! }; };
  const idle = await settle();
  ok(f.store.register(hostA, { session: b }));
  assert.equal((await postV2(f.base, ROUTES.presence, 'a', { sessions: [sessionKey(a)] })).status, 200);
  assert.deepEqual(await settle(), idle, 'registration and presence wake no stream');
  ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'directed' }));
  const directed = await settle();
  assert.ok(directed.a > idle.a);
  assert.equal(directed.b, idle.b, 'a directed send wakes only its host');
  ok(f.store.send(hostA, a, { recipient: '*', body: 'broadcast' }));
  const broadcast = await settle();
  assert.ok(broadcast.a > directed.a && broadcast.b > directed.b, 'a broadcast wakes every v2 stream');
});

test('v2 expiry: a session gone between batches is sent as leave before any later frame', async t => {
  const f = await v2Fixture(t);
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostA, { session: b }));
  const s = await openV2(f.base);
  t.after(() => s.close());
  await s.next(); await s.next();
  assert.deepEqual([...s.members.keys()], [sessionKey(a), sessionKey(b)]);
  // Expiry deletes the row; the next batch diffs against the membership sent on this connection.
  const db = new DatabaseSync(f.path);
  try { db.prepare('DELETE FROM sessions WHERE id = ?').run(sessionKey(b)); } finally { db.close(); }
  const m = ok(f.store.send(hostB, x, { recipient: '*', body: 'after expiry' }));
  const events = await s.untilPosition();
  assert.deepEqual(events, [{ type: 'leave', session: sessionKey(b) }, { type: 'msg', seq: m.sequence, message: m }]);
  assert.deepEqual([...s.members.keys()], [sessionKey(a)]);
});

test('v2 gap: a host far behind crosses a million irrelevant ids within one wake while other hosts are served', async t => {
  let batches = 0;
  let answeredAt: number | undefined;
  let base = '';
  const slow = (store: CoordinatorStore) => new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'hostBatch') return (host: { hostId: string }, from: number, limit: number) => {
        if (host.hostId === 'host-a') {
          batches++;
          if (batches === 5) void fetch(`${base}/v1/sessions`, { headers: { authorization: 'Bearer b' } })
            .then(response => { assert.equal(response.status, 200); answeredAt = batches; });
          const until = Date.now() + 2;
          while (Date.now() < until) { /* stand-in for a slower disk */ }
        }
        return target.hostBatch(host, from, limit);
      };
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }) as CoordinatorStore;
  const f = await v2Fixture(t, {}, slow);
  base = f.base;
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostA, { session: a }));
  const s = await openV2(f.base);
  t.after(() => s.close());
  await s.next();
  await new Promise(resolve => setTimeout(resolve, 50));
  const db = new DatabaseSync(f.path);
  try {
    const tail = (db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'messages'").get() as { seq: number } | undefined)?.seq ?? 0;
    db.prepare("INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('messages', ?)").run(tail + 1_000_000);
  } finally { db.close(); }
  batches = 0;
  const started = Date.now();
  const m = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'beyond the gap' }));
  const events = await s.untilPosition(DEFAULT_SETTINGS.keepaliveMs);
  assert.deepEqual(events, [{ type: 'msg', seq: m.sequence, message: m }]);
  assert.ok(Date.now() - started < DEFAULT_SETTINGS.keepaliveMs, 'delivered before any keepalive tick');
  assert.ok(batches >= Math.floor(1_000_000 / 4096), `crossed in spans (${batches} batches)`);
  assert.ok(answeredAt !== undefined && answeredAt < batches, `another host was answered during the crossing (${answeredAt}/${batches})`);
});

test('v2 isolation and keepalive: a stalled host closes alone; an idle host gets a through on the tick, then a keepalive', { timeout: 30_000 }, async t => {
  const f = await v2Fixture(t, { keepaliveMs: 5_000 });
  ok(f.store.register(hostA, { session: a }));
  ok(f.store.register(hostB, { session: x }));
  ok(f.store.register(hostB, { session: y }));
  // Host B: a real TCP peer that reads the hello and then stops reading.
  const socket = connectSocket({ host: '127.0.0.1', port: Number(new URL(f.base).port) });
  socket.on('error', () => {});
  t.after(() => socket.destroy());
  await once(socket, 'connect');
  let received = '';
  socket.on('data', chunk => { received += chunk; });
  socket.write(`GET ${ROUTES.streamV2} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer b\r\nAccept: text/event-stream\r\n\r\n`);
  const deadline = Date.now() + 4000;
  while (!received.includes('"type":"member"')) { assert.ok(Date.now() < deadline); await new Promise(resolve => setTimeout(resolve, 10)); }
  socket.pause();
  let ended = false;
  socket.once('close', () => { ended = true; });
  const s = await openV2(f.base, 'a');
  t.after(() => s.close());
  await s.next(); // member
  const data = 'x'.repeat(1_000_000);
  for (let i = 0; i < 32; i++) ok(f.store.send(hostB, x, { recipient: sessionKey(y), body: `stall ${i}`, data }));
  const stalledAt = Date.now();
  // Host A is served while host B is backpressured.
  const live = ok(f.store.send(hostB, x, { recipient: sessionKey(a), body: 'not delayed' }));
  assert.deepEqual(positions(await s.untilPosition(1000)), [`msg:${live.sequence}`]);
  assert.deepEqual((await receiptV2(f.base, 'a', { conn: s.hello.conn, through: live.sequence })).body, { ok: true, value: { through: live.sequence } });
  // Only host B's traffic follows, which never wakes host A: the keepalive tick sends through(tail).
  const noise = ok(f.store.send(hostB, x, { recipient: sessionKey(y), body: 'other host only' }));
  const tick = await s.untilPosition(7000);
  assert.deepEqual(tick.map(event => event.type), ['through']);
  assert.equal((tick[0] as { seq: number }).seq, noise.sequence);
  // The idle host's C advances through its receipt.
  ok((await receiptV2(f.base, 'a', { conn: s.hello.conn, through: noise.sequence })).body as never);
  assert.equal(f.store.hostAcked(hostA), noise.sequence);
  // Past host B's write deadline (5 s) its stream is closed; reading again finds the end.
  await new Promise(resolve => setTimeout(resolve, Math.max(0, stalledAt + 6_000 - Date.now())));
  socket.resume();
  const closeBy = Date.now() + 4000;
  while (!ended && Date.now() < closeBy) await new Promise(resolve => setTimeout(resolve, 20));
  assert.ok(ended, 'the stalled stream was closed');
  assert.ok(Buffer.byteLength(received) < 32 * 1_000_000, 'it was closed before all its frames were written');
  // Nothing moved for another K: a keepalive comment, and no position frame.
  assert.deepEqual(await s.next(7000), { type: 'keepalive' });
  assert.equal(s.ended, false);
  const started = Date.now();
  await f.handler.close();
  assert.ok(Date.now() - started < 2000, 'close() returns promptly');
  assert.equal(await s.closed(), true);
});

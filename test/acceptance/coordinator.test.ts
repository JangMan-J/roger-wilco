import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer, type Server } from 'node:http';
import { connect as connectSocket } from 'node:net';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createHostDaemon, createDaemonInbox as createHostInbox } from '../../src/host/index.ts';
import { ROUTES, sessionKey, type SessionAddress, type StreamFrame } from '../../src/protocol/index.ts';

const namespace = 'fleet';
const a = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const b = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440001' };
const c = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440002' };
const tokens: Record<string, string> = { 'host-a': 'token-a', 'host-b': 'token-b' };

async function fixture(t: test.TestContext) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-accept-'));
  const path = join(dir, 'coordinator.db');
  let store = createCoordinatorStore(path, namespace);
  let server: Server | undefined;
  let url = '';
  let dropNextRegistration = false;
  let streamHandler: ReturnType<typeof createStreamHandler>;
  let blockedWrites = 0;
  let drains = 0;
  async function start() {
    const auth = createHostAuthenticator(tokens);
    const control = createControlHandler(store, auth);
    streamHandler = createStreamHandler(store, auth);
    server = createServer((req, res) => {
      const path = new URL(req.url ?? '/', 'http://localhost').pathname;
      if (path === ROUTES.stream && req.headers.authorization === 'Bearer token-b') {
        const write = res.write.bind(res);
        res.write = ((...args: Parameters<typeof res.write>) => {
          const accepted = write(...args);
          if (!accepted) blockedWrites++;
          return accepted;
        }) as typeof res.write;
        res.on('drain', () => { drains++; });
      }
      if (path === ROUTES.register && dropNextRegistration) {
        dropNextRegistration = false;
        // Keep the real handler and its durable store transaction; lose only its response.
        res.end = ((..._args: unknown[]) => { req.socket.destroy(); return res; }) as typeof res.end;
      }
      void (path === ROUTES.stream || path === ROUTES.receipt ? streamHandler : control)(req, res);
    });
    await new Promise<void>(resolve => server!.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    assert.ok(address && typeof address !== 'string');
    url = `http://127.0.0.1:${address.port}`;
  }
  async function stop() {
    if (server) { server.closeAllConnections(); await new Promise<void>((resolve, reject) => server!.close(error => error ? reject(error) : resolve())); server = undefined; }
    store.close();
  }
  async function restart() { await stop(); store = createCoordinatorStore(path, namespace); await start(); }
  async function request(host: string, route: string, body?: unknown, method = body === undefined ? 'GET' : 'POST') {
    const response = await fetch(`${url}${route}`, { method, headers: {
      Authorization: `Bearer ${tokens[host] ?? host}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
    }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    return { status: response.status, result: await response.json() as { ok: boolean; value?: any; error?: string } };
  }
  const register = (host: string, session: SessionAddress) => request(host, ROUTES.register, { session });
  const send = (host: string, sender: SessionAddress, recipient: string, body: string) => request(host, ROUTES.messages, { sender, message: { recipient, body } });
  await start();
  t.after(async () => { await stop(); await rm(dir, { recursive: true, force: true }); });
  return { get url() { return url; }, get store() { return store; }, path, request, register, send, restart,
    closeStreams: () => streamHandler.close(), backpressure: () => ({ blockedWrites, drains }),
    dropNextRegistrationResponse: () => { dropNextRegistration = true; } };
}

/** Real HTTP SSE, including headers, server routing and parsing; no event-source stub. */
async function connect(url: string, host: string) {
  const abort = new AbortController();
  const response = await fetch(`${url}${ROUTES.stream}`, { headers: { Authorization: `Bearer ${tokens[host] ?? host}`, Accept: 'text/event-stream' }, signal: abort.signal });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream/);
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const frames: StreamFrame[] = [];
  const pump = (async () => {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
        let end: number;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, end); buffer = buffer.slice(end + 2);
          const data = block.split('\n').filter(line => line.startsWith('data: ')).map(line => line.slice(6)).join('\n');
          if (block.includes('event: message') && data) frames.push(JSON.parse(data));
        }
      }
    } catch (error) { if (!abort.signal.aborted) throw error; }
  })();
  // A coordinator restart may close the socket before the fixture explicitly cancels it.
  void pump.catch(() => {});
  return { frames, close: async () => { abort.abort(); await pump.catch(() => {}); }, pump };
}
async function until(predicate: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 5000;
  while (!await predicate()) { assert.ok(Date.now() < deadline, reason); await delay(20); }
}

test('real HTTP registration, SSE routing, receipt replay and SQLite restart preserve independent session cursors', async t => {
  const h = await fixture(t);
  // An old directed message remains available only via explicit historical inbox after initial registration.
  assert.equal((await h.register('host-a', b)).status, 200);
  const historical = await h.send('host-a', b, sessionKey(a), 'before registration');
  assert.equal(historical.status, 200);
  const first = await h.register('host-a', a);
  assert.equal(first.result.value.subscription.cursor, historical.result.value.sequence);
  assert.equal((await h.register('host-b', c)).status, 200);
  const hostA = await connect(h.url, 'host-a');
  const hostB = await connect(h.url, 'host-b');
  t.after(async () => { await hostA.close(); await hostB.close(); });
  const one = await h.send('host-b', c, sessionKey(a), 'only a');
  const two = await h.send('host-b', c, sessionKey(b), 'only b');
  const three = await h.send('host-a', a, sessionKey(c), 'from host a');
  assert.equal(one.status, 200); assert.equal(two.status, 200); assert.equal(three.status, 200);
  await until(() => hostA.frames.length === 2 && hostB.frames.length === 1, 'three routed frames');
  assert.deepEqual(hostA.frames.map(f => [f.subscriptionId, f.message.id]).sort((x, y) => x[0]!.localeCompare(y[0]!)),
    [[sessionKey(a), one.result.value.id], [sessionKey(b), two.result.value.id]]);
  assert.equal(hostB.frames[0]?.message.id, three.result.value.id);
  assert.equal((await h.request('host-b', ROUTES.receipt, { subscriptionId: sessionKey(a), messageId: one.result.value.id })).status, 409);
  assert.equal((await h.request('host-a', ROUTES.receipt, { subscriptionId: sessionKey(a), messageId: two.result.value.id })).status, 400);
  // Crash after host persistence, before coordinator receipt: stream replay is the identical durable ID.
  await hostA.close(); await hostB.close();
  await h.restart();
  const retried = await h.register('host-a', a);
  assert.equal(retried.result.value.subscription.cursor, historical.result.value.sequence, 'lost registration response never resets cursor');
  const replay = await connect(h.url, 'host-a');
  const replayB = await connect(h.url, 'host-b');
  t.after(async () => { await replay.close(); await replayB.close(); });
  await until(() => replay.frames.length === 2 && replayB.frames.length === 1, 'SQLite replay after restart');
  assert.deepEqual(replay.frames.map(f => f.message.id).sort(), [one.result.value.id, two.result.value.id].sort());
  for (const frame of replay.frames) assert.equal((await h.request('host-a', ROUTES.receipt, { subscriptionId: frame.subscriptionId, messageId: frame.message.id })).status, 200);
  const ack = { subscriptionId: sessionKey(c), messageId: three.result.value.id };
  assert.equal((await h.request('host-b', ROUTES.receipt, ack)).status, 200);
  assert.equal((await h.request('host-b', ROUTES.receipt, ack)).status, 200, 'duplicate receipt is idempotent');
  await replay.close(); await replayB.close(); await h.restart();
  const quiet = await connect(h.url, 'host-a');
  t.after(() => quiet.close());
  const next = await h.send('host-b', c, sessionKey(a), 'after catch-up');
  assert.equal(next.status, 200);
  await until(() => quiet.frames.length > 0, 'live message after catch-up');
  assert.deepEqual(quiet.frames.map(f => f.message.id), [next.result.value.id]);
  const inbox = await h.request('host-a', `${ROUTES.inbox}?namespace=fleet&sessionId=${a.sessionId}&since=0&limit=100`);
  assert.deepEqual(inbox.result.value.map((m: { id: string }) => m.id), [historical.result.value.id, one.result.value.id, next.result.value.id]);
});

test('1 MiB HTTP messages persist through the real host SSE parser, ACK, and unblock the next delivery', async t => {
  const h = await fixture(t);
  await h.register('host-a', a); await h.register('host-b', c);
  const inboxPath = `${h.path}.host`;
  let inbox = createHostInbox(inboxPath);
  const daemon = createHostDaemon({ coordinatorUrl: h.url, hostId: 'host-b', credential: tokens['host-b']!, inboxPath }, inbox);
  const running = daemon.start(new AbortController().signal);
  const received: { id: string; data: string }[] = [];
  const limit = 1024 * 1024;
  const post = async (route: string, body: string) => {
    const response = await fetch(`${h.url}${route}`, { method: 'POST', body, headers: {
      Authorization: `Bearer ${tokens['host-a']}`, 'Content-Type': 'application/json',
      Accept: 'application/json',
    } });
    return { status: response.status, result: await response.json() };
  };
  try {
    assert.equal((await inbox.bind(c, 'boundary-check', Date.now())).ok, true);
    {
      const message = { recipient: sessionKey(c), body: 'boundary', data: '☃'.repeat(100) };
      const envelope = { sender: a, message };
      message.data += 'x'.repeat(limit - Buffer.byteLength(JSON.stringify(envelope)));
      const body = JSON.stringify(envelope);
      assert.equal(Buffer.byteLength(body), limit, 'exact HTTP byte boundary, including multibyte data');
      const response = await post(ROUTES.messages, body);
      assert.equal(response.status, 200);
      const result = response.result;
      assert.equal(result.ok, true);
      received.push({ id: result.value.id, data: message.data });
      await until(() => h.store.listSessions().find(row => row.subscription.id === sessionKey(c))?.subscription.cursor === result.value.sequence,
        'boundary message reaches durable host receipt and coordinator ACK');
      assert.equal((await inbox.pending(c, 'boundary-check')).find(item => item.id === result.value.id)?.data, message.data);
      assert.equal((await post(ROUTES.messages, `${body} `)).status, 413, 'one extra request byte is rejected');
    }

    // JSON number normalization can expand a small wire request. Reject before
    // inserting, through HTTP and the direct store interface alike.
    const expanded = { recipient: sessionKey(c), body: 'oversize', data: Array(50_000).fill(1e20) };
    const httpBody = JSON.stringify({ sender: a, message: expanded }).replaceAll('100000000000000000000', '1e20');
    assert.ok(Buffer.byteLength(httpBody) < limit);
    assert.ok(Buffer.byteLength(JSON.stringify(expanded)) > limit);
    assert.equal((await post(ROUTES.messages, httpBody)).status, 400);
    assert.equal(h.store.send({ hostId: 'host-a' }, a, expanded).ok, false);
    const stored = h.store.inbox({ hostId: 'host-b' }, c, 0, 100);
    assert.equal(stored.ok, true);
    if (stored.ok) assert.deepEqual(stored.value.map(item => item.id), received.map(item => item.id), 'no poison records');

    const next = await h.send('host-a', a, sessionKey(c), 'after boundary and rejected messages');
    assert.equal(next.status, 200);
    assert.equal(next.result.value.sequence, 2, 'rejections never insert a message');
    await until(() => h.store.listSessions().find(row => row.subscription.id === sessionKey(c))?.subscription.cursor === 2,
      'next message is not stranded behind a boundary message');
    await daemon.stop(); await running;
    await inbox.release(c, 'boundary-check'); await inbox.close();
    inbox = createHostInbox(inboxPath);
    assert.equal((await inbox.bind(c, 'reopened-check', Date.now())).ok, true);
    const durable = await inbox.pending(c, 'reopened-check');
    assert.deepEqual(durable.map(item => item.id), [...received.map(item => item.id), next.result.value.id]);
    for (const item of received) assert.equal(durable.find(message => message.id === item.id)?.data, item.data);
    await h.restart();
    assert.equal(h.store.listSessions().find(row => row.subscription.id === sessionKey(c))?.subscription.cursor, 2,
      'coordinator ACKs survive SQLite reopen');
  } finally { await daemon.stop(); await running; await inbox.close(); }
});

test('registration response loss retries the same atomic starting cursor instead of skipping a new message', async t => {
  const h = await fixture(t);
  await h.register('host-a', b);
  const old = await h.send('host-a', b, sessionKey(a), 'historical only');
  h.dropNextRegistrationResponse();
  await assert.rejects(h.register('host-a', a), 'client lost the reply after SQLite committed');
  const newMessage = await h.send('host-a', b, sessionKey(a), 'must replay after lost reply');
  const retry = await h.register('host-a', a);
  assert.equal(retry.result.value.subscription.cursor, old.result.value.sequence);
  const stream = await connect(h.url, 'host-a');
  t.after(() => stream.close());
  await until(() => stream.frames.length === 1, 'message since first registration is replayed');
  assert.equal(stream.frames[0]?.message.id, newMessage.result.value.id);
});

test('disconnected host cannot hold up another; catch-up/live transition retains an in-flight message', async t => {
  const h = await fixture(t);
  await h.register('host-a', a); await h.register('host-b', c);
  const disconnected = await connect(h.url, 'host-b');
  await disconnected.close();
  const first = await h.send('host-a', a, sessionKey(c), 'catch-up first');
  assert.equal(first.status, 200);
  const live = await connect(h.url, 'host-a');
  t.after(() => live.close());
  const next = await h.send('host-b', c, sessionKey(a), 'host A continues while B is offline');
  await until(() => live.frames.length === 1, 'unrelated host receives despite B disconnect');
  assert.equal(live.frames[0]?.message.id, next.result.value.id);
  const recovering = await connect(h.url, 'host-b');
  t.after(() => recovering.close());
  await until(() => recovering.frames.length === 1, 'B catches up from SQLite');
  assert.equal(recovering.frames[0]?.message.id, first.result.value.id);
  const during = await h.send('host-a', a, sessionKey(c), 'arrived during replay');
  // The unacknowledged earlier item must not be bypassed by a newer global sequence.
  assert.equal((await h.request('host-b', ROUTES.receipt, { subscriptionId: sessionKey(c), messageId: during.result.value.id })).status, 400);
  assert.equal((await h.request('host-b', ROUTES.receipt, { subscriptionId: sessionKey(c), messageId: first.result.value.id })).status, 200);
  await until(() => recovering.frames.some(frame => frame.message.id === during.result.value.id), 'live arrival follows caught-up item');
  assert.equal((await h.request('host-b', ROUTES.receipt, { subscriptionId: sessionKey(c), messageId: during.result.value.id })).status, 200);
});

test('backpressured real TCP host stream cannot stall another host; close is bounded and reconnect replays', async t => {
  const h = await fixture(t);
  await h.register('host-a', a);
  const recipients: SessionAddress[] = [];
  for (let i = 0; i < 96; i++) {
    const session = { namespace, sessionId: `550e8400-e29b-41d4-a716-${String(i + 100).padStart(12, '0')}` };
    assert.equal((await h.register('host-b', session)).status, 200);
    recipients.push(session);
  }
  const socket = connectSocket({ host: '127.0.0.1', port: Number(new URL(h.url).port) });
  socket.on('error', () => {}); // Expected when stream shutdown destroys a paused TCP peer.
  t.after(() => { socket.destroy(); });
  await once(socket, 'connect');
  socket.pause(); // An actual peer that never consumes SSE bytes, not an instrumented write stub.
  socket.write(`GET ${ROUTES.stream} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer token-b\r\nAccept: text/event-stream\r\nConnection: keep-alive\r\n\r\n`);
  const body = 'x'.repeat(60_000);
  // Hosts have different kernel TCP buffers. Fill with valid, bounded envelopes
  // until backpressure is observed rather than assuming 6 MiB fills every host.
  const data = 'x'.repeat(800_000);
  for (const session of recipients) {
    assert.equal((await h.request('host-a', ROUTES.messages, {
      sender: a, message: { recipient: sessionKey(session), body, data },
    })).status, 200);
    const pressure = h.backpressure();
    if (pressure.blockedWrites > pressure.drains) {
      await delay(60);
      if (h.backpressure().drains === pressure.drains) break;
    }
  }
  await until(() => h.backpressure().blockedWrites > h.backpressure().drains, 'paused TCP receive buffer actually backpressures SSE writes');
  const { blockedWrites, drains } = h.backpressure();
  await delay(60);
  assert.equal(h.backpressure().drains, drains, 'backpressure persists while the client is paused');
  assert.ok(blockedWrites > drains);
  const healthy = await connect(h.url, 'host-a');
  t.after(() => healthy.close());
  const delivered = await h.send('host-b', recipients[0]!, sessionKey(a), 'healthy host keeps receiving');
  await until(() => healthy.frames.some(frame => frame.message.id === delivered.result.value.id), 'unrelated host receives during B backpressure');
  assert.equal((await h.request('host-a', ROUTES.receipt, {
    subscriptionId: sessionKey(a), messageId: delivered.result.value.id,
  })).status, 200);
  const started = Date.now();
  await Promise.race([h.closeStreams(), delay(2000).then(() => assert.fail('backpressured stream close exceeded two seconds'))]);
  assert.ok(Date.now() - started < 2000, 'stream shutdown remains bounded');
  socket.destroy();
  await h.restart();
  const replay = await connect(h.url, 'host-b');
  t.after(() => replay.close());
  await until(() => replay.frames.length > 0, 'SQLite replay resumes after backpressured peer reconnects');
  assert.equal(replay.frames[0]?.subscriptionId, sessionKey(recipients[0]!));
  assert.equal(replay.frames[0]?.message.body, body);
});

test('HTTP auth/invalid envelopes, cross-host ownership, broadcast and competing claims', async t => {
  const h = await fixture(t);
  await h.register('host-a', a); await h.register('host-a', b); await h.register('host-b', c);
  assert.equal((await h.register('host-b', a)).status, 409);
  assert.equal((await h.request('wrong', ROUTES.sessions)).status, 401);
  const deniedStream = await fetch(`${h.url}${ROUTES.stream}`, { headers: { Authorization: 'Bearer wrong', Accept: 'text/event-stream' } });
  assert.equal(deniedStream.status, 401);
  assert.equal((await h.request('host-a', ROUTES.receipt, { subscriptionId: sessionKey(a), messageId: 'fleet:msg:999999' })).status, 400);
  assert.equal((await h.request('host-a', ROUTES.messages, { sender: a, message: { recipient: sessionKey(b), body: '', extra: 1 } })).status, 400);
  assert.equal((await h.request('host-b', ROUTES.messages, { sender: a, message: { recipient: sessionKey(b), body: 'forged host' } })).status, 409);
  const broadcast = await h.send('host-a', a, '*', 'all other sessions');
  assert.equal(broadcast.status, 200);
  const inbox = (session: SessionAddress, host: string) => h.request(host, `${ROUTES.inbox}?namespace=fleet&sessionId=${session.sessionId}&since=0&limit=100`);
  assert.deepEqual((await inbox(a, 'host-a')).result.value, []);
  assert.deepEqual((await inbox(b, 'host-a')).result.value.map((m: { id: string }) => m.id), [broadcast.result.value.id]);
  assert.deepEqual((await inbox(c, 'host-b')).result.value.map((m: { id: string }) => m.id), [broadcast.result.value.id]);
  const claim = (host: string, owner: SessionAddress, ttlSeconds: number) => h.request(host, ROUTES.claims, { key: 'shared-work', owner, ttlSeconds });
  assert.equal((await claim('host-a', a, 1)).result.value.acquired, true);
  assert.equal((await claim('host-b', c, 1)).result.value.acquired, false);
  assert.equal((await claim('host-a', a, 1)).result.value.acquired, true, 'owner renews');
  assert.equal((await h.request('host-b', ROUTES.claimRelease, { key: 'shared-work', owner: c })).result.value, false);
  await until(async () => (await claim('host-b', c, 1)).result.value.acquired, 'expired claim takeover');
  assert.equal((await h.request('host-a', ROUTES.claimRelease, { key: 'shared-work', owner: a })).result.value, false);
  assert.equal((await h.request('host-b', ROUTES.claimRelease, { key: 'shared-work', owner: c })).result.value, true);
});

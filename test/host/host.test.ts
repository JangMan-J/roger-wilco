import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { createHostDaemon, createDaemonInbox as createHostInbox, type LocalHostInbox } from '../../src/host/index.ts';
import { ROUTES, MAX_STREAM_EVENT_BYTES, sessionKey, type Message, type SessionAddress, type StreamFrame } from '../../src/protocol/index.ts';

const a: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const b: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440001' };
const c: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440002' };
function message(sequence: number, recipient: string = sessionKey(a)): Message {
  return { version: 1, id: `fleet:msg:${sequence}`, sequence, timestamp: 123, sender: sessionKey(c), recipient,
    body: `message ${sequence}`, data: null, visibility: 'private' };
}
function frame(sequence: number, session: SessionAddress = a, recipient = sessionKey(session)): StreamFrame {
  return { type: 'message', subscriptionId: sessionKey(session), message: message(sequence, recipient) };
}
function fixture(t: { after(fn: () => void | Promise<void>): void }): { inbox: LocalHostInbox; path: string } {
  const dir = mkdtempSync(join(tmpdir(), 'roger-host-'));
  const path = join(dir, 'inbox.sqlite');
  const inbox = createHostInbox(path);
  t.after(async () => { await inbox.close(); rmSync(dir, { recursive: true, force: true }); });
  return { inbox, path };
}
async function waitFor(check: () => boolean | Promise<boolean>, timeout = 4000): Promise<void> {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.fail('Timed out waiting for observable state');
}

test('durable per-recipient dedup, ownership, expiration and delivery evidence guards across reopen', async t => {
  const { inbox, path } = fixture(t);
  assert.deepEqual(await inbox.receive(frame(1, a, '*')), { inserted: true });
  assert.deepEqual(await inbox.receive(frame(1, b, '*')), { inserted: true });
  assert.deepEqual(await inbox.receive(frame(1, a, '*')), { inserted: false });
  await assert.rejects(() => inbox.receive({ ...frame(1), subscriptionId: sessionKey(b) }), /Invalid stream frame/);
  const now = Date.now();
  assert.equal((await inbox.bind(a, 'first', now)).ok, true);
  assert.equal((await inbox.bind(a, 'second', now)).ok, false);
  assert.deepEqual(await inbox.pending(b, 'first'), []);
  assert.deepEqual((await inbox.pending(a, 'first')).map(m => m.id), ['fleet:msg:1']);
  assert.equal((await inbox.acknowledgeDelivery(a, 'first', {
    messageId: 'fleet:msg:1', sessionId: b.sessionId, recordId: 'entry-1', location: 'session.jsonl',
  })).ok, false);
  assert.deepEqual((await inbox.pending(a, 'first')).map(m => m.id), ['fleet:msg:1']);
  // A pre-contract Pi receipt shape is not delivery evidence.
  assert.deepEqual(await inbox.acknowledgeDelivery(a, 'first', {
    messageId: 'fleet:msg:1', piSessionId: a.sessionId, entryId: 'entry-1', sessionFile: 'session.jsonl',
  } as never), { ok: false, error: 'invalid_request', message: 'Invalid delivery evidence' });
  assert.equal((await inbox.acknowledgeDelivery(a, 'first', {
    messageId: 'fleet:msg:1', sessionId: a.sessionId, recordId: 'entry-1', location: 'session.jsonl',
  })).ok, true);
  assert.deepEqual(await inbox.pending(a, 'first'), []);
  assert.equal((await inbox.renew(a, 'second', now + 1)).ok, false);
  assert.equal((await inbox.bind(a, 'second', now + 30_001)).ok, true);
  assert.equal((await inbox.renew(a, 'first', now + 30_002)).ok, false);
  await inbox.release(a, 'first');
  await inbox.close();
  const reopened = createHostInbox(path);
  t.after(() => reopened.close());
  assert.equal((await reopened.bind(b, 'other', Date.now())).ok, true);
  assert.deepEqual((await reopened.pending(b, 'other')).map(m => m.id), ['fleet:msg:1']);
  assert.deepEqual(await reopened.pending(a, 'second'), []);
  assert.equal((await reopened.acknowledgeDelivery(a, 'second', {
    messageId: 'fleet:msg:1', sessionId: a.sessionId, recordId: 'different', location: 'session.jsonl',
  })).ok, false);
});

test('host commits before receipt, reconnect replay is idempotent, invalid and oversized events never ACK', async t => {
  const { inbox, path } = fixture(t);
  let connections = 0;
  let receipts = 0;
  let firstCommitted = false;
  let server: Server;
  server = createServer(async (req, res) => {
    if (req.headers.authorization !== 'Bearer secret') { res.writeHead(401).end(); return; }
    if (req.url === ROUTES.stream) {
      connections++;
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      if (connections === 1) {
        // Simulate a coordinator receipt request failing after the local commit.
        res.write(`: heartbeat\r\nevent: message\r\ndata: ${JSON.stringify(frame(1))}\r\n\r\n`);
      } else if (connections === 2) {
        res.write(`event: message\ndata: ${JSON.stringify(frame(1))}\n\n`);
        res.write('event: message\ndata: {"invalid":true}\n\n');
        res.end(); // Reconnect on EOF, not because an individual event was rejected.
      } else if (connections === 3) {
        const oversized = frame(2);
        oversized.message.data = 'x'.repeat(MAX_STREAM_EVENT_BYTES);
        res.write(`event: message\ndata: ${JSON.stringify(oversized)}\n\n`);
      } else {
        res.write('event: message\ndata: {"invalid":true}\n\n');
      }
      return;
    }
    if (req.url === ROUTES.receipt) {
      const body = await new Promise<string>(resolve => { let value = ''; req.on('data', d => value += d); req.on('end', () => resolve(value)); });
      assert.deepEqual(JSON.parse(body), { subscriptionId: sessionKey(a), messageId: 'fleet:msg:1' });
      receipts++;
      if (receipts === 1) {
        const opened = new DatabaseSync(path, { readOnly: true });
        firstCommitted = opened.prepare('SELECT count(*) AS n FROM inbox_messages WHERE message_id = ?').get('fleet:msg:1')!.n === 1;
        opened.close();
        res.writeHead(503).end();
      } else {
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ ok: true, value: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 } }));
      }
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const config = { coordinatorUrl: `http://127.0.0.1:${address.port}`, hostId: 'host-a', credential: 'secret', inboxPath: path };
  const daemon = createHostDaemon(config, inbox);
  const run = daemon.start(new AbortController().signal);
  try { await waitFor(() => receipts === 1); }
  finally { await daemon.stop(); await run; }
  assert.equal(firstCommitted, true);
  await inbox.close(); // Simulate process death after local commit, before coordinator receipt ACK.
  const reopened = createHostInbox(path);
  t.after(() => reopened.close());
  const resumed = createHostDaemon(config, reopened);
  const resumedRun = resumed.start(new AbortController().signal);
  try {
    await waitFor(() => connections >= 4 && receipts >= 2);
    assert.equal(receipts, 2);
    assert.equal((await reopened.bind(a, 'owner', Date.now())).ok, true);
    assert.deepEqual((await reopened.pending(a, 'owner')).map(m => m.id), ['fleet:msg:1']);
  } finally { await resumed.stop(); await resumedRun; }
});

test('unsupported and malformed events do not block another subscription on the same connection', async t => {
  const { inbox, path } = fixture(t);
  let connections = 0;
  const receipts: unknown[] = [];
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.stream) {
      connections++;
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      const unsupported = { ...frame(1), message: { ...message(1), version: 2 } };
      res.write(`event: message\ndata: ${JSON.stringify(unsupported)}\n\n`);
      res.write('event: message\ndata: {broken json}\n\n');
      res.write(`event: message\ndata: ${JSON.stringify(frame(2, b))}\n\n`);
      return;
    }
    if (req.url === ROUTES.receipt) {
      let body = '';
      for await (const chunk of req) body += chunk;
      const receipt = JSON.parse(body);
      receipts.push(receipt);
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ok: true, value: { id: receipt.subscriptionId, session: b, hostId: 'host-a', cursor: 0 } }));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const daemon = createHostDaemon({ coordinatorUrl: `http://127.0.0.1:${address.port}`,
    hostId: 'host-a', credential: 'secret', inboxPath: path }, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await waitFor(() => receipts.length > 0);
    assert.equal(connections, 1);
    assert.deepEqual(receipts, [{ subscriptionId: sessionKey(b), messageId: 'fleet:msg:2' }]);
    assert.equal((await inbox.bind(a, 'a', Date.now())).ok, true);
    assert.equal((await inbox.bind(b, 'b', Date.now())).ok, true);
    assert.deepEqual(await inbox.pending(a, 'a'), [], 'unsupported event is neither stored nor acknowledged');
    assert.deepEqual((await inbox.pending(b, 'b')).map(item => item.id), ['fleet:msg:2']);
  } finally { await daemon.stop(); await run; }
});

test('acknowledged inbox history expires after seven days, never pending deliveries or recent receipts', async t => {
  const { inbox, path } = fixture(t);
  const now = Date.now();
  t.mock.method(Date, 'now', () => now);
  await inbox.bind(a, 'owner', now);
  for (const sequence of [1, 2, 3]) await inbox.receive(frame(sequence));
  const proof = (sequence: number) => ({ messageId: `fleet:msg:${sequence}` as const,
    sessionId: a.sessionId, recordId: `entry-${sequence}`, location: 'session.jsonl' });
  assert.equal((await inbox.acknowledgeDelivery(a, 'owner', proof(1))).ok, true);
  t.mock.method(Date, 'now', () => now + 7 * 86400_000);
  await inbox.bind(a, 'owner', Date.now());
  assert.equal((await inbox.acknowledgeDelivery(a, 'owner', proof(3))).ok, true);
  await inbox.close();
  t.mock.method(Date, 'now', () => now + 7 * 86400_000 + 1);
  const reopened = createHostInbox(path);
  t.after(() => reopened.close());
  const db = new DatabaseSync(path);
  try {
    assert.deepEqual(db.prepare('SELECT message_id FROM inbox_messages ORDER BY sequence').all().map(row => row.message_id),
      ['fleet:msg:2', 'fleet:msg:3']);
  } finally { db.close(); }
  assert.equal((await reopened.bind(a, 'owner', Date.now())).ok, true);
  assert.deepEqual((await reopened.pending(a, 'owner')).map(item => item.id), ['fleet:msg:2']);
  assert.deepEqual(await reopened.receive(frame(3)), { inserted: false }, 'recent dedup survives cleanup');
});

test('silent open stream reconnects and commits the frame offered only on the next connection', async t => {
  const { inbox, path } = fixture(t);
  let connections = 0;
  let receipts = 0;
  const server = createServer(async (req, res) => {
    if (req.headers.authorization !== 'Bearer secret') { res.writeHead(401).end(); return; }
    if (req.url === ROUTES.stream) {
      connections++;
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      if (connections === 1) res.write(': heartbeat\n\n'); // Leave this socket open, but silent.
      else res.write(`event: message\ndata: ${JSON.stringify(frame(2))}\n\n`);
      return;
    }
    if (req.url === ROUTES.receipt) {
      let body = '';
      for await (const chunk of req) body += chunk;
      assert.deepEqual(JSON.parse(body), { subscriptionId: sessionKey(a), messageId: 'fleet:msg:2' });
      receipts++;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ok: true, value: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 } }));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const daemon = createHostDaemon({ coordinatorUrl: `http://127.0.0.1:${address.port}`,
    hostId: 'host-a', credential: 'secret', inboxPath: path }, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await waitFor(() => receipts === 1, 52_000);
    assert.equal(connections, 2);
    assert.equal((await inbox.bind(a, 'inspect', Date.now())).ok, true);
    assert.deepEqual((await inbox.pending(a, 'inspect')).map(m => m.id), ['fleet:msg:2']);
  } finally { await daemon.stop(); await run; }
});

test('stream fetch without response headers times out and reconnects', async t => {
  const { inbox, path } = fixture(t);
  let connections = 0;
  let receipts = 0;
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.stream) {
      connections++;
      if (connections === 1) return; // Keep the TCP connection open without HTTP headers.
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(`event: message\ndata: ${JSON.stringify(frame(3))}\n\n`);
      return;
    }
    if (req.url === ROUTES.receipt) {
      let body = '';
      for await (const chunk of req) body += chunk;
      assert.deepEqual(JSON.parse(body), { subscriptionId: sessionKey(a), messageId: 'fleet:msg:3' });
      receipts++;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ok: true, value: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 } }));
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const daemon = createHostDaemon({ coordinatorUrl: `http://127.0.0.1:${address.port}`,
    hostId: 'host-a', credential: 'secret', inboxPath: path }, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await waitFor(() => receipts === 1, 16_000);
    assert.equal(connections, 2);
    assert.equal((await inbox.bind(a, 'inspect', Date.now())).ok, true);
    assert.deepEqual((await inbox.pending(a, 'inspect')).map(m => m.id), ['fleet:msg:3']);
  } finally { await daemon.stop(); await run; }
});

test('a failing first registration does not starve a later session in the same pass', async t => {
  const { inbox, path } = fixture(t);
  await inbox.enqueueRegistration({ session: a, displayName: 'Queued A' });
  await inbox.enqueueRegistration({ session: a }); // bare heartbeat must keep the explicit name
  assert.match(inbox.registrations()[0]!.registration_json, /Queued A/);
  await inbox.enqueueRegistration({ session: b });
  const posts: string[] = [];
  const results: Array<{ key: string; ok: boolean; error?: string }> = [];
  inbox.onRegistrationResult((key, _json, _generation, result) => results.push({ key, ok: result.ok, ...(result.ok ? {} : { error: result.error }) }));
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.register) {
      let payload = '';
      for await (const chunk of req) payload += chunk;
      const session = (JSON.parse(payload) as { session: SessionAddress }).session;
      posts.push(session.sessionId);
      res.setHeader('content-type', 'application/json');
      if (session.sessionId === a.sessionId) {
        res.writeHead(409);
        res.end(JSON.stringify({ ok: false, error: 'owner_conflict', message: 'Session belongs to another host' }));
      } else {
        res.end(JSON.stringify({ ok: true, value: { subscription: { id: sessionKey(session), session, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 } }));
      }
      return;
    }
    if (req.url === ROUTES.stream) { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(': idle\n\n'); return; }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const daemon = createHostDaemon({ coordinatorUrl: `http://127.0.0.1:${address.port}`, hostId: 'host-a', credential: 'secret', inboxPath: path }, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await waitFor(() => posts.length >= 2);
    assert.deepEqual(posts.slice(0, 2), [a.sessionId, b.sessionId], 'second session posts in the same pass, before any retry of the first');
    assert.deepEqual(inbox.registrations().map(row => row.session_key), [sessionKey(a)], 'failed row remains queued, completed row is gone');
    assert.deepEqual(results.find(row => row.key === sessionKey(b)), { key: sessionKey(b), ok: true });
    assert.deepEqual(results.find(row => row.key === sessionKey(a)), { key: sessionKey(a), ok: false, error: 'owner_conflict' }, 'coordinator code is preserved');
  } finally { await daemon.stop(); await run; }
});

test('registration queue survives reopen, retries lost response and stops promptly offline', async t => {
  const { inbox, path } = fixture(t);
  await inbox.enqueueRegistration({ session: a });
  await inbox.close();
  const reopened = createHostInbox(path);
  t.after(() => reopened.close());
  let posts = 0;
  let registered: string | undefined;
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.register) {
      let payload = '';
      for await (const chunk of req) payload += chunk;
      assert.deepEqual(JSON.parse(payload), { session: a });
      posts++;
      registered ??= payload; // Server applied registration, then lost its first response.
      assert.equal(payload, registered);
      if (posts === 1) { res.destroy(); return; }
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ ok: true, value: { subscription: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 } }));
      return;
    }
    if (req.url === ROUTES.stream) {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write(': idle\n\n');
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const config = { coordinatorUrl: `http://127.0.0.1:${address.port}`, hostId: 'host-a', credential: 'secret', inboxPath: path };
  const daemon = createHostDaemon(config, reopened);
  const run = daemon.start(new AbortController().signal);
  try { await waitFor(() => posts >= 2 && reopened.registrations().length === 0); }
  finally { await daemon.stop(); await run; }
  // Confirm successful response removed the durable queue: no new registration after restart.
  const again = createHostDaemon(config, reopened);
  const againRun = again.start(new AbortController().signal);
  await new Promise(resolve => setTimeout(resolve, 650));
  await again.stop(); await againRun;
  assert.equal(posts, 2);
  assert.ok(registered);
  await new Promise<void>(resolve => server.close(() => resolve()));
  const offline = createHostDaemon(config, reopened);
  const offlineRun = offline.start(new AbortController().signal);
  const before = Date.now();
  await offline.stop(); await offlineRun;
  assert.ok(Date.now() - before < 1000);
});

test('a non-UUID session has its own lease and inbox; a Pi UUID lease stays on the folded key', async t => {
  const { inbox } = fixture(t);
  const agent: SessionAddress = { namespace: 'fleet', sessionId: 'V1StGXR8_Z5jdHi6B-myT' };
  const lower: SessionAddress = { namespace: 'fleet', sessionId: agent.sessionId.toLowerCase() };
  const upper = { ...a, sessionId: a.sessionId.toUpperCase() };
  const now = Date.now();
  assert.equal(sessionKey(upper), sessionKey(a));
  assert.notEqual(sessionKey(agent), sessionKey(lower));
  assert.equal((await inbox.bind(agent, 'agent-runtime', now)).ok, true);
  assert.equal((await inbox.bind(lower, 'lower-runtime', now)).ok, true);
  assert.equal((await inbox.bind(upper, 'pi-runtime', now)).ok, true);
  assert.equal((await inbox.bind(a, 'other-pi', now)).ok, false);
  assert.equal((await inbox.bind(agent, 'other-agent', now)).ok, false);
  assert.deepEqual(await inbox.receive(frame(1, agent)), { inserted: true });
  assert.deepEqual(await inbox.receive(frame(2, a)), { inserted: true });
  assert.deepEqual(await inbox.receive(frame(3, lower)), { inserted: true });
  assert.deepEqual((await inbox.pending(agent, 'agent-runtime')).map(message => message.id), ['fleet:msg:1']);
  assert.deepEqual((await inbox.pending(a, 'pi-runtime')).map(message => message.id), ['fleet:msg:2']);
  assert.deepEqual((await inbox.pending(lower, 'lower-runtime')).map(message => message.id), ['fleet:msg:3']);
  assert.equal((await inbox.bind({ namespace: 'fleet', sessionId: 'other:tracer-1' }, 'x', now)).ok, false);
  assert.equal((await inbox.bind({ namespace: 'fleet', sessionId: 'a'.repeat(129) }, 'x', now)).ok, false);
  assert.equal((await inbox.bind({ namespace: 'fleet', sessionId: 'bad\nid' }, 'x', now)).ok, false);
});

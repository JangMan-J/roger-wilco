import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { lstat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createConnection, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { createHostClient } from '../../src/host/client.ts';
import { createHostDaemon, createDaemonInbox as createHostInbox, LocalHostInbox } from '../../src/host/index.ts';
import { listenLocal } from '../../src/host/ipc.ts';
import {
  MAX_JSON_BYTES, ROUTES, sessionKey,
  type HostConfig, type HostInbox, type Message, type Outcome, type DeliveryEvidence, type SessionAddress, type StreamFrame,
} from '../../src/protocol/index.ts';

const a: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const b: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440001' };
const c: SessionAddress = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440002' };
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const registrationSql = 'SELECT count(*) AS n FROM inbox_registrations';
const leaseSql = 'SELECT count(*) AS n FROM inbox_leases';

function message(sequence: number, recipient: string, body = `m${sequence}`): Message {
  return { version: 1, id: `fleet:msg:${sequence}`, sequence, timestamp: 1_700_000_000_000, sender: sessionKey(c), recipient, body, data: null, visibility: 'private' };
}
function frame(sequence: number, session: SessionAddress, body?: string): StreamFrame {
  return { type: 'message', subscriptionId: sessionKey(session), message: message(sequence, sessionKey(session), body) };
}
function proof(sequence: number, session: SessionAddress): DeliveryEvidence {
  return { messageId: `fleet:msg:${sequence}`, sessionId: session.sessionId, recordId: `e${sequence}`, location: 's.jsonl' };
}
async function waitFor(check: () => boolean | Promise<boolean>, timeout = 4000): Promise<void> {
  const end = Date.now() + timeout;
  while (Date.now() < end) { if (await check()) return; await delay(15); }
  assert.fail('timed out');
}
function rig(t: { after(fn: () => void | Promise<void>): void }) {
  const dir = mkdtempSync(join(tmpdir(), 'rw-'));
  const path = join(dir, 'inbox.sqlite');
  const inbox = createHostInbox(path) as LocalHostInbox;
  const config: HostConfig = { coordinatorUrl: 'http://127.0.0.1:9', hostId: 'host-a', credential: 'secret', inboxPath: path };
  t.after(async () => { await inbox.close().catch(() => {}); rmSync(dir, { recursive: true, force: true }); });
  return { path, inbox, config };
}
function clientFor(t: { after(fn: () => void | Promise<void>): void }, path: string): HostInbox {
  const client = createHostClient(path);
  t.after(() => client.close().catch(() => {}));
  return client;
}
async function sock(path: string): Promise<Socket> {
  const end = Date.now() + 4_000;
  while (Date.now() < end) {
    const socket = createConnection(`${path}.sock`);
    socket.on('error', () => {});
    const up = await Promise.race([once(socket, 'connect').then(() => true, () => false), delay(40).then(() => false)]);
    if (up) return socket;
    socket.destroy();
  }
  assert.fail('socket unavailable');
}
async function modeReady(path: string): Promise<void> {
  await waitFor(async () => { try { return ((await lstat(`${path}.sock`)).mode & 0o777) === 0o600; } catch { return false; } });
}
function writeFrame(socket: Socket, id: number, method: string, args: object): void {
  socket.write(`${JSON.stringify({ v: 1, id, method, args })}\n`);
}
function readResult(socket: Socket, id: number): Promise<Outcome<unknown>> {
  let buffer = Buffer.alloc(0);
  return new Promise((resolve, reject) => {
    const onData = (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      let nl: number;
      while ((nl = buffer.indexOf(10)) >= 0) {
        const line = buffer.subarray(0, nl);
        buffer = buffer.subarray(nl + 1);
        const body = JSON.parse(line.toString('utf8')) as { id?: number; result?: Outcome<unknown> };
        if (body.id === id) { socket.off('data', onData); resolve(body.result!); return; }
      }
    };
    socket.on('data', onData);
    socket.once('close', () => reject(new Error('socket closed')));
  });
}
async function count(path: string, sql: string): Promise<number> {
  const db = new DatabaseSync(path, { readOnly: true });
  try { return db.prepare(sql).get()!.n as number; }
  finally { db.close(); }
}
function watchReads(): { sql(): string[]; stop(): void } {
  const seen: string[] = [];
  const original = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function (this: DatabaseSync, sql: string) {
    if (typeof sql === 'string' && /^\s*select\b/i.test(sql) && /inbox_(messages|registrations)\b/i.test(sql)) seen.push(sql);
    return original.call(this, sql);
  } as typeof DatabaseSync.prototype.prepare;
  return { sql: () => seen, stop: () => { DatabaseSync.prototype.prepare = original; } };
}
function daemonClock(at: number): void {
  assert.ok(Math.abs(at - Date.now() - 30_000) < 2_000);
}
function conflict(result: Outcome<unknown>): void {
  assert.equal(result.ok, false);
  if (!result.ok) assert.equal(result.error, 'owner_conflict');
}

test('catch-up finds a pre-bind commit and a hint during reconciliation, isolated by session', async t => {
  const { path, inbox, config } = rig(t);
  await inbox.receive(frame(1, a));
  await inbox.receive(frame(2, b));
  let injected = false;
  const pendingPage = inbox.pendingPage.bind(inbox);
  inbox.pendingPage = (session, owner, after) => {
    const page = pendingPage(session, owner, after);
    if (!injected && page.ok && page.value.messages.some(item => item.id === 'fleet:msg:1')) {
      injected = true;
      void inbox.receive(frame(9, a));
    }
    return page;
  };
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const left = clientFor(t, path);
  const right = clientFor(t, path);
  assert.equal((await right.bind(b, 'rt-b')).ok, true);
  let rightWakes = 0;
  right.onChange!(() => { rightWakes++; });
  let wakes = 0;
  left.onChange!(() => { wakes++; });
  const bound = await left.bind(a, 'rt-a');
  assert.equal(bound.ok, true);
  if (bound.ok) daemonClock(bound.value.leaseExpiresAt);
  const baseline = wakes;
  const ids = new Set<string>();
  let seen = -1;
  do {
    seen = wakes;
    for (const item of await left.pending(a, 'rt-a')) ids.add(item.id);
  } while (wakes !== seen);
  assert.equal(injected, true);
  assert.ok(wakes > baseline);
  assert.deepEqual([...ids].sort(), ['fleet:msg:1', 'fleet:msg:9']);
  assert.equal(rightWakes, 0);
  assert.deepEqual((await right.pending(b, 'rt-b')).map(item => item.id), ['fleet:msg:2']);
  assert.equal((await left.acknowledgeDelivery(b, 'rt-a', proof(2, b))).ok, false);
  assert.equal((await right.acknowledgeDelivery(b, 'rt-b', proof(2, b))).ok, true);
  assert.deepEqual(await right.pending(b, 'rt-b'), []);
  assert.ok((await left.pending(a, 'rt-a')).every(item => item.id !== 'fleet:msg:2'));
});

test('stale grants fail after disconnect and restart; client time does not set the lease', async t => {
  const { path, inbox, config } = rig(t);
  let ipc = await listenLocal(config, inbox);
  let reopened: LocalHostInbox | undefined;
  t.after(async () => { await ipc.close(); await reopened?.close(); });
  const first = clientFor(t, path);
  const bound = await first.bind(a, 'rt-a');
  assert.equal(bound.ok, true);
  if (bound.ok) daemonClock(bound.value.leaseExpiresAt);
  const token = first.authority!();
  assert.ok(token);
  assert.equal((await clientFor(t, path).bind(a, 'rt-a')).ok, false);
  const closing = first.close();
  assert.equal(first.authority!(), undefined);
  await closing;
  await waitFor(async () => await count(path, leaseSql) === 0);
  const raw = await sock(path);
  const calls: Array<[number, string, object]> = [
    [1, 'renew', { token }], [2, 'pending', { token }], [3, 'ack', { token, evidence: proof(1, a) }],
  ];
  for (const [id, method, args] of calls) {
    const waiting = readResult(raw, id);
    writeFrame(raw, id, method, args);
    conflict(await waiting);
  }
  raw.destroy();
  const second = clientFor(t, path);
  assert.equal((await second.bind(a, 'rt-a')).ok, true);
  const retired = second.authority!();
  assert.ok(retired && retired !== token);
  await ipc.close();
  await waitFor(() => second.authority!() === undefined);
  await waitFor(async () => await count(path, leaseSql) === 0);
  await inbox.close();
  reopened = new LocalHostInbox(path);
  ipc = await listenLocal(config, reopened);
  const again = await sock(path);
  const waiting = readResult(again, 1);
  writeFrame(again, 1, 'ack', { token: retired, evidence: proof(1, a) });
  conflict(await waiting);
  again.destroy();
  const rebound = await clientFor(t, path).bind(a, 'rt-a');
  assert.equal(rebound.ok, true);
  if (rebound.ok) daemonClock(rebound.value.leaseExpiresAt);
});

test('quiet idle does not select pending inbox or registration rows', async t => {
  const { path, inbox, config } = rig(t);
  let streams = 0;
  const server = createServer((req, res) => {
    if (req.url === ROUTES.stream) { streams++; res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(': idle\n\n'); return; }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  config.coordinatorUrl = `http://127.0.0.1:${address.port}`;
  const daemon = createHostDaemon(config, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await waitFor(() => streams >= 1);
    await delay(150);
    await modeReady(path);
    const client = clientFor(t, path);
    const bound = await client.bind(a, 'rt-a');
    assert.equal(bound.ok, true);
    if (bound.ok) daemonClock(bound.value.leaseExpiresAt);
    assert.deepEqual(await client.pending(a, 'rt-a'), []);
    const watch = watchReads();
    try {
      const renewed = await client.renew(a, 'rt-a');
      assert.equal(renewed.ok, true);
      if (renewed.ok) daemonClock(renewed.value.leaseExpiresAt);
      await delay(650);
      assert.deepEqual(watch.sql(), []);
    } finally { watch.stop(); }
  } finally { await daemon.stop(); await run; }
});

test('awaitRegistration does not block renew, pending, or ack while the daemon attempt starts past 10s', { timeout: 30_000 }, async t => {
  const { path, inbox, config } = rig(t);
  // Two blockers ordered ahead of the target delay its attempt start past
  // 10s through the real daemon; each blocker responds within the POST
  // deadline, so no production timeout changes are needed.
  const blockers: SessionAddress[] = [
    { namespace: 'fleet', sessionId: '00000000-0000-4000-8000-000000000000' },
    { namespace: 'fleet', sessionId: '11111111-1111-4111-8111-111111111111' },
  ];
  await inbox.enqueueRegistration({ session: blockers[0]! });
  await inbox.enqueueRegistration({ session: blockers[1]! });
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.register) {
      let body = '';
      for await (const chunk of req) body += chunk;
      const registration = JSON.parse(body) as { session: SessionAddress; displayName?: string };
      if (blockers.some(item => item.sessionId === registration.session.sessionId)) await delay(6_000);
      res.end(JSON.stringify({ ok: true, value: { subscription: { id: sessionKey(registration.session), session: registration.session, hostId: 'host-a', cursor: 0 }, rank: 1, ...(registration.displayName ? { displayName: registration.displayName } : {}) } }));
      return;
    }
    if (req.url === ROUTES.stream) { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(': idle\n\n'); return; }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  config.coordinatorUrl = `http://127.0.0.1:${address.port}`;
  const daemon = createHostDaemon(config, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await modeReady(path);
    const client = clientFor(t, path);
    assert.equal((await client.bind(b, 'rt-b')).ok, true);
    const token = client.authority!();
    await inbox.receive(frame(1, b));
    const started = Date.now();
    let settled = false;
    const waiting = client.awaitRegistration({ session: b, displayName: 'Target' });
    void waiting.then(() => { settled = true; }, () => { settled = true; });
    await waitFor(async () => await count(path, registrationSql) === 3);
    assert.equal((await client.renew(b, 'rt-b')).ok, true);
    assert.deepEqual((await client.pending(b, 'rt-b')).map(item => item.id), ['fleet:msg:1']);
    assert.equal((await client.acknowledgeDelivery(b, 'rt-b', proof(1, b))).ok, true);
    assert.equal(client.authority!(), token, 'same-socket work completes without retiring the lease');
    await delay(Math.max(0, 10_500 - (Date.now() - started)));
    assert.equal(settled, false, 'registration still waits for its own attempt');
    assert.equal((await client.renew(b, 'rt-b')).ok, true);
    assert.equal(client.authority!(), token);
    const registered = await waiting;
    assert.equal(registered.ok, true);
    if (registered.ok) assert.equal(registered.value.displayName, 'Target');
    assert.equal((await client.renew(b, 'rt-b')).ok, true, 'lease survives the long wait');
  } finally { await daemon.stop(); await run; }
});

test('a stale snapshotted attempt cannot complete a newer identity request', { timeout: 20_000 }, async t => {
  const { path, inbox, config } = rig(t);
  await inbox.enqueueRegistration({ session: a });
  await inbox.enqueueRegistration({ session: b, displayName: 'Old name' });
  let releaseA = () => {};
  const gateA = new Promise<void>(resolve => { releaseA = resolve; });
  let aStarted = () => {};
  const aAttempt = new Promise<void>(resolve => { aStarted = resolve; });
  let releaseFresh = () => {};
  const gateFresh = new Promise<void>(resolve => { releaseFresh = resolve; });
  let freshStarted = () => {};
  const freshAttempt = new Promise<void>(resolve => { freshStarted = resolve; });
  const applied = new Map<string, string | undefined>();
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.register) {
      let body = '';
      for await (const chunk of req) body += chunk;
      const registration = JSON.parse(body) as { session: SessionAddress; displayName?: string };
      if (registration.session.sessionId === a.sessionId) { aStarted(); await gateA; }
      if (registration.displayName === 'New name') { freshStarted(); await gateFresh; }
      applied.set(sessionKey(registration.session), registration.displayName);
      res.end(JSON.stringify({ ok: true, value: { subscription: { id: sessionKey(registration.session), session: registration.session, hostId: 'host-a', cursor: 0 }, rank: 1, ...(registration.displayName ? { displayName: registration.displayName } : {}) } }));
      return;
    }
    if (req.url === ROUTES.stream) { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(': idle\n\n'); return; }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  config.coordinatorUrl = `http://127.0.0.1:${address.port}`;
  const daemon = createHostDaemon(config, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    await aAttempt; // daemon has snapshotted both rows and is posting A
    await modeReady(path);
    const client = clientFor(t, path);
    assert.equal((await client.bind(b, 'rt-b')).ok, true);
    let settled = false;
    const waiting = client.awaitRegistration({ session: b, displayName: 'New name' });
    void waiting.then(() => { settled = true; }, () => { settled = true; });
    await waitFor(() => inbox.registrations().some(row => row.session_key === sessionKey(b) && row.registration_json.includes('New name')));
    releaseA();
    await freshAttempt; // the stale Old snapshot was skipped, never posted; the New attempt is held
    assert.equal((await client.renew(b, 'rt-b')).ok, true, 'socket round trip while the New attempt is held');
    assert.equal(settled, false, 'no stale result may settle the New name waiter');
    assert.equal(applied.get(sessionKey(b)), undefined, 'stale Old name is never applied');
    releaseFresh();
    const registered = await waiting;
    assert.equal(registered.ok, true);
    if (registered.ok) assert.equal(registered.value.displayName, 'New name');
    assert.equal(applied.get(sessionKey(b)), 'New name');
    assert.equal(inbox.registrations().some(row => row.session_key === sessionKey(b)), false);
  } finally { releaseA(); releaseFresh(); await daemon.stop(); await run; }
});

test('a waiter replaced by a newer registration settles as superseded, not with the newer result', async t => {
  const { path, inbox, config } = rig(t);
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const client = clientFor(t, path);
  assert.equal((await client.bind(a, 'rt-a')).ok, true);
  const waiting = client.awaitRegistration({ session: a, displayName: 'Old name' });
  void waiting.catch(() => {});
  await waitFor(async () => await count(path, registrationSql) === 1);
  await client.enqueueRegistration({ session: a, displayName: 'New name' });
  assert.deepEqual(await waiting, { ok: false, error: 'temporarily_unavailable', message: 'Registration superseded by a newer registration' });
});

test('a bare waiter follows a newer explicit registration instead of settling superseded', async t => {
  const { path, inbox, config } = rig(t);
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const client = clientFor(t, path);
  assert.equal((await client.bind(a, 'rt-a')).ok, true);
  const waiting = client.awaitRegistration({ session: a });
  void waiting.catch(() => {});
  await waitFor(async () => await count(path, registrationSql) === 1);
  await client.enqueueRegistration({ session: a, displayName: 'New name' });
  const row = inbox.registrations()[0]!;
  inbox.reportRegistration(sessionKey(a), row.registration_json, row.generation,
    { ok: true, value: { subscription: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 }, rank: 1, displayName: 'New name' } });
  const registered = await waiting;
  assert.equal(registered.ok, true);
  if (registered.ok) assert.equal(registered.value.displayName, 'New name');
});

test('registered event preserves the coordinator failure code to the client', async t => {
  const { path, inbox, config } = rig(t);
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const client = clientFor(t, path);
  assert.equal((await client.bind(a, 'rt-a')).ok, true);
  const seen = new Promise<unknown>(resolve => client.onRegistrationResult!(result => resolve(result)));
  inbox.reportRegistration(sessionKey(a), JSON.stringify({ session: a }), 1,
    { ok: false, error: 'owner_conflict', message: 'Session belongs to another host' });
  assert.deepEqual(await seen, { ok: false, error: 'owner_conflict', message: 'Session belongs to another host' });
});

test('lost registration reply leaves a committed row, then the same enqueue drains again', async t => {
  const { path, inbox, config } = rig(t);
  let enqueues = 0;
  inbox.onRegistration(() => { enqueues++; });
  const gates: Array<() => void> = [];
  const arm = () => new Promise<void>(resolve => { gates.push(resolve); });
  let gate = arm();
  let posts = 0;
  const server = createServer(async (req, res) => {
    if (req.url === ROUTES.register) {
      let body = '';
      for await (const chunk of req) body += chunk;
      assert.deepEqual(JSON.parse(body), { session: a });
      const release = gate;
      gate = arm();
      await release;
      posts++;
      res.end(JSON.stringify({ ok: true, value: { subscription: { id: sessionKey(a), session: a, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 } }));
      return;
    }
    if (req.url === ROUTES.stream) { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.write(': idle\n\n'); return; }
    res.writeHead(404).end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  config.coordinatorUrl = `http://127.0.0.1:${address.port}`;
  const daemon = createHostDaemon(config, inbox);
  const run = daemon.start(new AbortController().signal);
  try {
    const socket = await sock(path);
    writeFrame(socket, 1, 'enqueueRegistration', { registration: { session: a } });
    await waitFor(async () => await count(path, registrationSql) === 1);
    socket.destroy();
    await modeReady(path);
    await clientFor(t, path).enqueueRegistration({ session: a });
    assert.equal(enqueues, 1, 'an identical queued registration does not write or notify again');
    gates.shift()?.();
    await waitFor(async () => posts === 1 && await count(path, registrationSql) === 0);
    const again = await sock(path);
    writeFrame(again, 1, 'enqueueRegistration', { registration: { session: a } });
    await waitFor(async () => await count(path, registrationSql) === 1);
    again.destroy();
    gates.shift()?.();
    await waitFor(async () => posts === 2 && await count(path, registrationSql) === 0);
  } finally {
    for (const release of gates) release();
    await daemon.stop();
    await run;
  }
});

test('invalid frames close only their socket, and a slow reader stays bounded', { timeout: 15_000 }, async t => {
  const { path, inbox, config } = rig(t);
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const healthy = clientFor(t, path);
  assert.equal((await healthy.bind(b, 'rt-b')).ok, true);
  const oversized = Buffer.alloc(MAX_JSON_BYTES + 8192 + 1, 0x61);
  oversized[oversized.length - 1] = 10;
  for (const chunk of [Buffer.from([0xff, 0x0a]), oversized]) {
    const bad = await sock(path);
    const closed = new Promise<void>(resolve => bad.once('close', () => resolve()));
    bad.write(chunk);
    await closed;
  }
  assert.equal((await healthy.renew(b, 'rt-b')).ok, true);
  const large = frame(3, a);
  large.message.data = 'x'.repeat(900_000);
  await inbox.receive(large);
  const slow = await sock(path);
  let read = 0;
  slow.on('data', (chunk: Buffer) => { read += chunk.length; });
  const granting = readResult(slow, 1);
  writeFrame(slow, 1, 'bind', { session: a, runtimeId: 'slow' });
  const grant = await granting;
  assert.equal(grant.ok, true);
  if (!grant.ok) return;
  slow.pause();
  const drained = read;
  const started = Date.now();
  for (const id of [2, 3, 4]) writeFrame(slow, id, 'pending', { token: (grant.value as { token: string }).token });
  await inbox.receive(frame(4, b));
  assert.deepEqual((await healthy.pending(b, 'rt-b')).map(item => item.id), ['fleet:msg:4']);
  assert.equal((await healthy.acknowledgeDelivery(b, 'rt-b', proof(4, b))).ok, true);
  await waitFor(async () => await count(path, `SELECT count(*) AS n FROM inbox_leases WHERE session_key = '${sessionKey(a)}'`) === 0, 8_000);
  slow.destroy();
  const elapsed = Date.now() - started;
  assert.ok(elapsed < 8_000);
  assert.ok(read - drained < 8_192);
  assert.equal((await healthy.renew(b, 'rt-b')).ok, true);
});

test('post-commit notifier failure disconnects readers and reconnect catches up', async t => {
  const { path, inbox, config } = rig(t);
  const ipc = await listenLocal(config, inbox);
  t.after(() => ipc.close());
  const client = clientFor(t, path);
  assert.equal((await client.bind(a, 'owner')).ok, true);
  const previous = client.authority!();
  const removeFault = inbox.subscribe(() => { throw new Error('injected notifier fault'); });
  await assert.rejects(inbox.receive(frame(1, a)), /injected notifier fault/);
  removeFault();
  await waitFor(() => client.authority!() === undefined);
  assert.equal((await client.bind(a, 'owner')).ok, true);
  assert.notEqual(client.authority!(), previous);
  assert.deepEqual((await client.pending(a, 'owner')).map(item => item.id), ['fleet:msg:1']);
});

test('SIGKILL after inbox commit before hint survives stale-socket recovery without redelivery', { timeout: 10000 }, async t => {
  const dir = mkdtempSync(join(tmpdir(), 'rw-crash-host-'));
  const path = join(dir, 'inbox.sqlite');
  const child = fork(fileURLToPath(new URL('./crash-child.mjs', import.meta.url)), [path],
    { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  const exited = new Promise<{ code: number | null; signal: string | null }>(resolve =>
    child.once('exit', (code, signal) => resolve({ code, signal })));
  let stderr = '';
  child.stderr!.on('data', chunk => { stderr += chunk; });
  let inbox: LocalHostInbox | undefined;
  let ipc: Awaited<ReturnType<typeof listenLocal>> | undefined;
  let reader: Socket | undefined;
  let client: HostInbox | undefined;
  t.after(async () => {
    child.kill('SIGKILL');
    await exited;
    reader?.destroy();
    await client?.close();
    await ipc?.close();
    await inbox?.close();
    rmSync(dir, { recursive: true, force: true });
  });
  await Promise.race([
    once(child, 'message'),
    exited.then(result => { throw new Error(`Daemon exited before ready: ${JSON.stringify(result)} ${stderr}`); }),
  ]);
  reader = await sock(path);
  let wire = '';
  reader.on('data', chunk => { wire += chunk.toString(); });
  const binding = readResult(reader, 1);
  writeFrame(reader, 1, 'bind', { session: a, runtimeId: 'before-crash' });
  assert.equal((await binding).ok, true);
  child.send(frame(1, a));
  assert.equal((await exited).signal, 'SIGKILL', stderr);
  assert.equal(wire.includes('"event":"changed"'), false, 'daemon died before sending the commit hint');
  assert.ok((await lstat(`${path}.sock`)).isSocket(), 'SIGKILL left the stale socket');
  inbox = new LocalHostInbox(path);
  ipc = await listenLocal({ inboxPath: path, coordinatorUrl: 'http://127.0.0.1:1',
    hostId: 'host-a', credential: 'offline' }, inbox);
  client = createHostClient(path);
  assert.equal((await client.bind(a, 'after-crash')).ok, true);
  assert.deepEqual((await client.pending(a, 'after-crash')).map(item => item.id), ['fleet:msg:1']);
});

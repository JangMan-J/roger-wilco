import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { appendFile, lstat, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test, { type TestContext } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { runMuseCli } from '../../src/muse/cli.ts';
import { createMuseAdapter, type MuseHarness } from '../../src/muse/adapter.ts';
import { museSessionAddress } from '../../src/muse/identity.ts';
import { museTranscriptEvidence } from '../../src/muse/transcript.ts';
import { createHostClient } from '../../src/host/client.ts';
import { connectHost, requestCoordinator } from '../../src/host/http.ts';
import { createDaemonInbox, createHostDaemon } from '../../src/host/index.ts';
import { ROUTES, sessionKey, type Message, type SessionAddress } from '../../src/protocol/index.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';

const namespace = 'fleet';
const hostId = 'host-a';
const credential = 'token-a';
const peer: SessionAddress = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const museSessionId = '01a13d1b-8a0a-7ee0-8d8e-52d312288314';

async function until(predicate: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 5000;
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, reason);
    await delay(20);
  }
}

async function stack(t: TestContext) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-muse-'));
  const inboxPath = join(dir, 'inbox.sqlite');
  const transcript = join(dir, 'session.jsonl');
  await writeFile(transcript, '{"retained_frame":"session_start"}\n');
  const store = createCoordinatorStore(join(dir, 'coordinator.db'), namespace);
  const auth = createHostAuthenticator({ [hostId]: credential });
  const control = createControlHandler(store, auth);
  const stream = createStreamHandler(store, auth);
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? '/', 'http://localhost').pathname;
    const handler = path === ROUTES.stream || path === ROUTES.receipt ? stream : control;
    void handler(request, response).catch(() => {
      if (!response.headersSent) response.writeHead(500);
      response.end();
    });
  });
  await new Promise<void>(resolveListen => server.listen(0, '127.0.0.1', () => resolveListen()));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}`;
  const inbox = createDaemonInbox(inboxPath);
  const abort = new AbortController();
  const daemon = createHostDaemon({ coordinatorUrl: url, hostId, credential, inboxPath }, inbox);
  const running = daemon.start(abort.signal).catch(error => {
    if (!abort.signal.aborted) throw error;
  });
  const config = {
    coordinatorUrl: url, hostId, credential, inboxPath, namespace,
    sessionId: museSessionId, transcriptPath: transcript, sessionsRoot: join(dir, 'sessions'),
  };
  t.after(async () => {
    abort.abort();
    await daemon.stop();
    await running;
    await stream.close();
    await new Promise<void>(done => { server.close(() => done()); server.closeAllConnections(); });
    await inbox.close();
    store.close();
    await rm(dir, { recursive: true, force: true });
  });
  await until(async () => {
    try {
      const info = await lstat(`${inboxPath}.sock`);
      return info.isSocket() && (info.mode & 0o777) === 0o600;
    } catch { return false; }
  }, 'host socket');
  assert.equal(store.register({ hostId }, { session: peer }).ok, true);
  return { dir, store, config, transcript, inboxPath };
}

/** A fake Muse harness: delivering appends the delivery to the transcript, as the real runtime records tool output. */
function fakeHarness(transcript: string): MuseHarness & { delivered: Message[] } {
  const delivered: Message[] = [];
  return {
    delivered,
    sessionId: () => museSessionId,
    transcriptPath: () => transcript,
    deliver: async (message: Message) => {
      delivered.push(message);
      // As the Muse runtime records the adapter's delivered line as tool output.
      await appendFile(transcript, `${JSON.stringify({ retained_frame: 'tool_result', output: JSON.stringify({ event: 'delivered', message }) })}\n`);
    },
  };
}

function receipt(inboxPath: string, messageId: string): { evidence_record_id: string | null; evidence_location: string | null } {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare('SELECT evidence_record_id, evidence_location FROM inbox_messages WHERE message_id = ?').get(messageId) as
      { evidence_record_id: string | null; evidence_location: string | null };
  } finally { db.close(); }
}

test('Muse session identity maps to one core session address', () => {
  assert.deepEqual(museSessionAddress(museSessionId, namespace), { namespace, sessionId: museSessionId });
  assert.equal(sessionKey(museSessionAddress(museSessionId, namespace)), `fleet:${museSessionId}`);
  assert.throws(() => museSessionAddress('bad id!', namespace), /Muse session id/);
  assert.throws(() => museSessionAddress('', namespace), /Muse session id/);
});

test('transcript evidence finds an exact delivery only in the current transcript', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-muse-evidence-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const transcript = join(dir, 'session.jsonl');
  const session = museSessionAddress(museSessionId, namespace);
  const inbox = { acknowledgeDelivery: async () => ({ ok: true as const, value: null }) } as never;
  const source = museTranscriptEvidence(() => transcript, inbox, session, 'rt');
  assert.deepEqual(await source.lookup(transcript, 'fleet:msg:1'), { state: 'store_missing' });
  const delivered = { event: 'delivered', message: { id: 'fleet:msg:1', sender: 'fleet:peer', body: 'hi' } };
  await writeFile(transcript, `{"note":"discussing fleet:msg:1 and fleet:msg:99"}\n${JSON.stringify({ output: JSON.stringify(delivered) })}\n`);
  // A bare mention of the ID is not evidence.
  assert.deepEqual(await source.lookup(transcript, 'fleet:msg:99'), { state: 'absent' });
  // A forged record naming the ID with different content is not this delivery.
  const real = { version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: 7, sender: 'fleet:peer', recipient: 'fleet:me', body: 'real', data: null, visibility: 'private' };
  assert.deepEqual(await source.lookup(transcript, 'fleet:msg:1', real), { state: 'absent' });
  const found = await source.lookup(transcript, 'fleet:msg:1');
  assert.equal(found.state, 'persisted');
  if (found.state === 'persisted') {
    assert.equal(found.evidence.recordId, 'line-2');
    assert.equal(source.locatedAt(found.evidence, transcript), true);
    assert.equal(source.locatedAt(found.evidence, join(dir, 'other.jsonl')), false);
  }
  assert.deepEqual(await source.lookup(transcript, 'fleet:msg:99'), { state: 'absent' });
});

test('Muse adapter exchanges messages with a Pi-shaped peer through the host daemon', async t => {
  const h = await stack(t);
  const harness = fakeHarness(h.transcript);
  const adapter = createMuseAdapter(harness, {
    loadConfig: async () => h.config,
    connectHost,
    request: requestCoordinator,
  });
  t.after(() => adapter.stop());
  await adapter.start();
  const identity = await adapter.bound();
  assert.ok(!('ok' in identity));
  if ('ok' in identity) return;
  assert.equal(sessionKey(identity.actor), `fleet:${museSessionId}`);

  const sent = await adapter.tool('roger_send', { recipient: sessionKey(peer), body: 'from claude' });
  assert.equal(sent.ok, true);

  const peerClient = createHostClient(h.inboxPath);
  t.after(() => peerClient.close());
  assert.equal((await peerClient.bind(peer, 'peer-rt')).ok, true);
  await until(async () => (await peerClient.pending(peer, 'peer-rt')).some(item => item.body === 'from claude'), 'peer receives through the host daemon');

  const inbound = h.store.send({ hostId }, peer, { recipient: sessionKey(identity.actor), body: 'from peer' });
  assert.equal(inbound.ok, true);
  await until(async () => {
    await adapter.receive();
    return harness.delivered.some(message => message.id === inbound.value.id);
  }, 'Muse adapter delivers from pending');
  assert.deepEqual(harness.delivered.map(message => message.body), ['from peer']);
  const probe = createHostClient(h.inboxPath);
  t.after(() => probe.close());
  await adapter.stop();
  assert.equal((await probe.bind(identity.actor, 'probe')).ok, true);
  assert.deepEqual(await probe.pending(identity.actor, 'probe'), [], 'transcript-proved delivery is acknowledged');
  const stored = receipt(h.inboxPath, inbound.value.id);
  assert.match(stored.evidence_record_id ?? '', /^line-\d+$/);
  assert.equal(stored.evidence_location, resolve(h.transcript));

  // Restart with a fresh runtime: the acknowledged message is not redelivered.
  const harness2 = fakeHarness(h.transcript);
  const adapter2 = createMuseAdapter(harness2, {
    loadConfig: async () => h.config,
    connectHost,
    request: requestCoordinator,
  });
  t.after(() => adapter2.stop());
  await adapter2.start();
  assert.deepEqual(await adapter2.receive(), []);
  assert.deepEqual(harness2.delivered, []);
});

test('a delivery is not acknowledged until the transcript records it', async t => {
  const h = await stack(t);
  const session = museSessionAddress(museSessionId, namespace);
  let transcript: string | undefined;
  const silent: MuseHarness = {
    sessionId: () => museSessionId,
    transcriptPath: () => transcript,
    deliver: () => {},
  };
  const adapter = createMuseAdapter(silent, {
    loadConfig: async () => h.config,
    connectHost,
    request: requestCoordinator,
  });
  t.after(() => adapter.stop());
  await adapter.start();
  // First registration sets the subscription cursor at the tail; register before the peer sends.
  assert.equal((await adapter.tool('roger_identity', {})).ok, true);
  // A stray version 1 name argument is ignored; nothing returns a name.
  const named = await adapter.tool('roger_identity', { displayName: 'Ignored' });
  assert.equal(named.ok, true);
  if (named.ok) assert.equal('displayName' in (named.value as object), false);
  const inbound = h.store.send({ hostId }, peer, { recipient: sessionKey(session), body: 'needs evidence' });
  assert.equal(inbound.ok, true);
  await until(async () => (await adapter.receive()).length === 1, 'handed to the harness once');
  assert.equal((await adapter.receive()).length, 0, 'not handed over twice in one runtime');
  assert.equal(receipt(h.inboxPath, inbound.value.id).evidence_record_id, null, 'no acknowledgement without transcript evidence');
  // The harness now records the earlier delivery in its transcript; the next pass acknowledges it.
  transcript = h.transcript;
  await appendFile(h.transcript, `${JSON.stringify({ output: JSON.stringify({ event: 'delivered', message: inbound.value }) })}\n`);
  await adapter.receive();
  assert.match(receipt(h.inboxPath, inbound.value.id).evidence_record_id ?? '', /^line-\d+$/);
});

test('Muse entry imports no Pi module', async () => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const bin = fileURLToPath(new URL('../../src/bin/muse.ts', import.meta.url));
  const pending = [bin];
  const seen = new Set<string>();
  while (pending.length) {
    const file = pending.pop()!;
    if (seen.has(file) || !file.startsWith(root) || file.includes('node_modules')) continue;
    seen.add(file);
    const text = await readFile(file, 'utf8');
    assert.doesNotMatch(text, /@earendil-works|from '\.\.\/pi|from '\.\/pi/);
    for (const match of text.matchAll(/(?:from|import\()\s*'(\.[^']+)'/g)) {
      let target = fileURLToPath(new URL(match[1]!, pathToFileURL(file)));
      if (!target.endsWith('.ts')) target += '.ts';
      pending.push(target);
    }
  }
  assert.ok([...seen].some(file => file.endsWith(join('src', 'host', 'client.ts'))));
  assert.ok([...seen].some(file => file.endsWith(join('src', 'protocol', 'index.ts'))));
  assert.ok([...seen].some(file => file.endsWith(join('src', 'evidence.ts'))));
  assert.equal([...seen].some(file => file.includes(join('src', 'pi'))), false);
});

test('a harness session change fails closed: no call, no delivery, no acknowledgement', async t => {
  const h = await stack(t);
  const session = museSessionAddress(museSessionId, namespace);
  const otherSessionId = '01a13d1b-8a0a-7ee0-8d8e-52d312288315';
  let current = museSessionId;
  let switchAfterDeliver = false;
  const delivered: Message[] = [];
  const harness: MuseHarness = {
    sessionId: () => current,
    transcriptPath: () => h.transcript,
    deliver: async message => {
      delivered.push(message);
      await appendFile(h.transcript, `${JSON.stringify({ output: JSON.stringify({ event: 'delivered', message }) })}\n`);
      if (switchAfterDeliver) current = otherSessionId;
    },
  };
  const requests: string[] = [];
  let switchDuringRegistration = false;
  let registrations = 0;
  const adapter = createMuseAdapter(harness, {
    loadConfig: async () => h.config,
    connectHost: async config => {
      const inbox = await connectHost(config);
      const awaitRegistration = inbox.awaitRegistration.bind(inbox);
      inbox.awaitRegistration = async registration => {
        registrations += 1;
        const result = await awaitRegistration(registration);
        if (switchDuringRegistration) current = otherSessionId;
        return result;
      };
      return inbox;
    },
    request: (config, route, method, body) => { requests.push(route); return requestCoordinator(config, route, method, body); },
  });
  t.after(() => adapter.stop());
  await adapter.start();
  assert.equal((await adapter.tool('roger_identity', {})).ok, true);
  const first = h.store.send({ hostId }, peer, { recipient: sessionKey(session), body: 'one' });
  const second = h.store.send({ hostId }, peer, { recipient: sessionKey(session), body: 'two' });
  assert.ok(first.ok && second.ok);
  await until(() => receipt(h.inboxPath, second.value.id) !== undefined, 'both messages reach the local inbox');

  // The harness now runs another session; the runtime still holds the old lease.
  const before = registrations;
  current = otherSessionId;
  const reason = new RegExp(`Session changed: harness session fleet:${otherSessionId} is not bound session fleet:${museSessionId}`);
  assert.equal(adapter.isBound(), false);
  const identity = await adapter.bound();
  assert.ok('ok' in identity && !identity.ok);
  assert.match(identity.message, reason);
  const sent = await adapter.tool('roger_send', { recipient: sessionKey(peer), body: 'stale' });
  assert.equal(sent.ok, false);
  assert.match(!sent.ok ? sent.message : '', reason);
  const board = await adapter.tool('roger_board', {});
  assert.equal(board.ok, false);
  assert.match(!board.ok ? board.message : '', reason);
  const direct = await adapter.call(ROUTES.sessions, 'GET');
  assert.equal(direct.ok, false);
  assert.match(!direct.ok ? direct.message : '', reason);
  assert.deepEqual(requests, [], 'no coordinator call as the stale session');
  assert.equal(registrations, before, 'no registration wait as the stale session');

  // The session changes while call() waits for registration: the request is still refused.
  current = museSessionId;
  switchDuringRegistration = true;
  const raced = await adapter.call(ROUTES.sessions, 'GET');
  switchDuringRegistration = false;
  assert.equal(raced.ok, false);
  assert.match(!raced.ok ? raced.message : '', reason);
  assert.deepEqual(requests, [], 'no coordinator call after a change during registration');
  await assert.rejects(adapter.receive(), reason);
  assert.equal(delivered.length, 0, 'nothing delivered to the other session');

  // The session changes during a delivery: the delivered message is not acknowledged, the next is not delivered.
  current = museSessionId;
  switchAfterDeliver = true;
  await assert.rejects(adapter.receive(), reason);
  assert.deepEqual(delivered.map(message => message.body), ['one']);
  assert.equal(receipt(h.inboxPath, first.value.id).evidence_record_id, null, 'transcript evidence is not acknowledged after the change');
  assert.equal(receipt(h.inboxPath, second.value.id).evidence_record_id, null);

  // Back on the bound session, the next pass acknowledges the first and delivers the second.
  current = museSessionId;
  switchAfterDeliver = false;
  await adapter.receive();
  assert.deepEqual(delivered.map(message => message.body), ['one', 'two']);
  assert.match(receipt(h.inboxPath, first.value.id).evidence_record_id ?? '', /^line-\d+$/);
  assert.match(receipt(h.inboxPath, second.value.id).evidence_record_id ?? '', /^line-\d+$/);
});

test('the Muse CLI register and identity commands take no name', async () => {
  for (const command of ['register', 'identity'])
    await assert.rejects(runMuseCli([command, 'Alice']), /takes no name; a call sign is an entity/);
});

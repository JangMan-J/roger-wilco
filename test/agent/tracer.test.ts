import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { lstat, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import test, { type TestContext } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  agentSession, parseAgentConfig, persistAndAcknowledge, persistEvidence, readAgentConfig, readEvidence, registerSession, sendMessage,
  type AgentConfig,
} from '../../src/agent/tracer.ts';
import { createHostClient } from '../../src/host/client.ts';
import { createDaemonInbox, createHostDaemon } from '../../src/host/index.ts';
import { ROUTES, isMessage, sessionKey, type Message, type SessionAddress } from '../../src/protocol/index.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';

const namespace = 'fleet';
const hostId = 'host-a';
const credential = 'token-a';
const peer: SessionAddress = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const agentBin = fileURLToPath(new URL('../../src/bin/agent.ts', import.meta.url));

function message(body: string): Message {
  return {
    version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: 1, sender: 'fleet:tracer-1', recipient: sessionKey(peer),
    body, data: null, visibility: 'private',
  };
}

async function until(predicate: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 5000;
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, reason);
    await delay(20);
  }
}

async function stack(t: TestContext) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-agent-'));
  const inboxPath = join(dir, 'inbox.sqlite');
  const evidencePath = join(dir, 'evidence.jsonl');
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
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}`;
  const inbox = createDaemonInbox(inboxPath);
  const abort = new AbortController();
  const daemon = createHostDaemon({ coordinatorUrl: url, hostId, credential, inboxPath }, inbox);
  const running = daemon.start(abort.signal).catch(error => {
    if (!abort.signal.aborted) throw error;
  });
  const config: AgentConfig = {
    coordinatorUrl: url, credential, namespace, sessionId: 'tracer-1', inboxPath, evidencePath, runtimeId: 'tracer-rt', displayName: 'tracer',
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
  return { dir, store, config, evidencePath, inboxPath };
}

function receipt(inboxPath: string, messageId: string): { evidence_record_id: string | null; evidence_location: string | null } {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare('SELECT evidence_record_id, evidence_location FROM inbox_messages WHERE message_id = ?').get(messageId) as
      { evidence_record_id: string | null; evidence_location: string | null };
  } finally { db.close(); }
}

function spawnAgent(t: TestContext, configPath: string, args: string[]) {
  const child = spawn(process.execPath, [agentBin, configPath, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  const lines: string[] = [];
  child.stderr.on('data', chunk => { stderr += chunk; });
  createInterface({ input: child.stdout }).on('line', line => { lines.push(line); });
  let stopped = false;
  async function stop(): Promise<number | null> {
    if (stopped) return child.exitCode;
    stopped = true;
    if (child.exitCode !== null || child.signalCode !== null) return child.exitCode;
    const exited = new Promise<number | null>(resolve => child.once('exit', resolve));
    child.kill('SIGTERM');
    const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
    try { return await exited; }
    finally { clearTimeout(timer); }
  }
  t.after(() => stop());
  return {
    lines,
    stderr: () => stderr,
    async line(predicate: (value: Record<string, unknown>) => boolean, reason: string) {
      const deadline = Date.now() + 5000;
      for (;;) {
        const found = lines.map(line => JSON.parse(line) as Record<string, unknown>).find(predicate);
        if (found) return found;
        if (child.exitCode !== null || child.signalCode !== null) {
          await delay(50);
          const late = lines.map(line => JSON.parse(line) as Record<string, unknown>).find(predicate);
          if (late) return late;
          assert.fail(`${reason}: exited ${child.exitCode ?? child.signalCode}: ${stderr}`);
        }
        assert.ok(Date.now() < deadline, `${reason}: ${stderr}`);
        await delay(20);
      }
    },
    stop,
  };
}

test('evidence reuses the record id for a delivery that was persisted and not yet acknowledged', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-evidence-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'evidence.jsonl');
  const first = await persistEvidence(path, message('kept'));
  const second = await persistEvidence(path, message('kept'));
  assert.equal(second.id, first.id);
  assert.equal((await readEvidence(path)).length, 1);
  assert.equal(await readFile(path, 'utf8'), `${JSON.stringify(first)}\n`);
});

test('tracer and a Pi-shaped peer exchange messages through the host daemon', async t => {
  const h = await stack(t);
  const tracer = agentSession(h.config);
  const client = createHostClient(h.inboxPath);
  const peerClient = createHostClient(h.inboxPath);
  t.after(() => client.close());
  t.after(() => peerClient.close());
  const registered = await registerSession(client, tracer, 'tracer');
  assert.equal(registered.subscription.id, 'fleet:tracer-1');
  assert.equal(registered.subscription.session.sessionId, 'tracer-1');
  assert.equal((await client.bind(tracer, h.config.runtimeId)).ok, true);
  assert.equal((await peerClient.bind(peer, 'peer-rt')).ok, true);

  const outbound = await sendMessage(h.config, sessionKey(peer), 'from tracer');
  assert.equal(outbound.sender, 'fleet:tracer-1');
  await until(async () => (await peerClient.pending(peer, 'peer-rt')).some(item => item.id === outbound.id), 'peer receives through the host daemon');
  const seen = await peerClient.pending(peer, 'peer-rt');
  assert.equal(seen.find(item => item.id === outbound.id)?.body, 'from tracer');

  const inbound = h.store.send({ hostId }, peer, { recipient: sessionKey(tracer), body: 'from peer' });
  assert.equal(inbound.ok, true);
  await until(async () => (await persistAndAcknowledge(client, tracer, h.config.runtimeId, h.evidencePath))
    .some(record => record.messageId === inbound.value.id), 'tracer persists then acknowledges');
  assert.deepEqual(await client.pending(tracer, h.config.runtimeId), []);
  const evidence = await readEvidence(h.evidencePath);
  assert.deepEqual(evidence.map(record => record.message.body), ['from peer']);
  assert.equal(evidence[0]!.id, 'rec-1');
  const stored = receipt(h.inboxPath, inbound.value.id);
  assert.equal(stored.evidence_record_id, evidence[0]!.id);
  assert.equal(stored.evidence_location, resolve(h.evidencePath));
});

test('agent process restart does not redeliver an acknowledged message', async t => {
  const h = await stack(t);
  const configPath = join(h.dir, 'agent.json');
  await writeFile(configPath, JSON.stringify(h.config));
  const first = spawnAgent(t, configPath, ['serve']);
  await first.line(value => value.event === 'ready' && value.session === 'fleet:tracer-1', 'first ready');
  const acked = h.store.send({ hostId }, peer, { recipient: 'fleet:tracer-1', body: 'while-up' });
  assert.equal(acked.ok, true);
  const delivered = await first.line(value => value.event === 'delivered' && value.messageId === acked.value.id, 'acked delivery');
  assert.equal(delivered.recordId, 'rec-1');
  assert.equal(await first.stop(), 0);
  const snapshot = await readFile(h.evidencePath, 'utf8');

  const probe = createHostClient(h.inboxPath);
  t.after(() => probe.close());
  assert.equal((await probe.bind(agentSession(h.config), 'probe')).ok, true);
  assert.deepEqual(await probe.pending(agentSession(h.config), 'probe'), [], 'acknowledged message is not pending for a new runtime');
  await probe.close();

  const waiting = h.store.send({ hostId }, peer, { recipient: 'fleet:tracer-1', body: 'while-down' });
  assert.equal(waiting.ok, true);
  const second = spawnAgent(t, configPath, ['serve']);
  await second.line(value => value.event === 'ready', 'second ready');
  const redelivered = await second.line(value => value.event === 'delivered', 'unacknowledged redelivery');
  assert.equal(redelivered.messageId, waiting.value.id);
  assert.equal(redelivered.recordId, 'rec-2');
  assert.equal(second.lines.filter(line => line.includes('"event":"delivered"')).length, 1);
  const evidence = await readEvidence(h.evidencePath);
  assert.deepEqual(evidence.map(record => [record.id, record.message.body]), [['rec-1', 'while-up'], ['rec-2', 'while-down']]);
  assert.equal((await readFile(h.evidencePath, 'utf8')).startsWith(snapshot), true, 'restart appends; it does not rewrite acknowledged evidence');
  assert.equal(receipt(h.inboxPath, acked.value.id).evidence_record_id, 'rec-1');
  assert.equal(receipt(h.inboxPath, waiting.value.id).evidence_record_id, 'rec-2');

  const sent = spawnAgent(t, configPath, ['send', sessionKey(peer), 'after restart']);
  const outbound = await sent.line(value => value.event === 'sent', 'send after restart');
  assert.equal(isMessage(outbound.message) && outbound.message.body, 'after restart');
  const peerClient = createHostClient(h.inboxPath);
  t.after(() => peerClient.close());
  assert.equal((await peerClient.bind(peer, 'peer-rt')).ok, true);
  await until(async () => (await peerClient.pending(peer, 'peer-rt')).some(item => isMessage(outbound.message) && item.id === outbound.message.id),
    'restarted agent send is delivered');
  await sent.stop();
  await second.stop();

  const beforeQuiet = await readFile(h.evidencePath, 'utf8');
  const third = spawnAgent(t, configPath, ['serve']);
  await third.line(value => value.event === 'ready', 'third ready');
  await delay(400);
  assert.equal(third.lines.some(line => line.includes('"event":"delivered"')), false);
  assert.equal(await readFile(h.evidencePath, 'utf8'), beforeQuiet);
});

test('agent entry imports no Pi module', async () => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const pending = [agentBin];
  const seen = new Set<string>();
  while (pending.length) {
    const file = pending.pop()!;
    if (seen.has(file) || !file.startsWith(root) || file.includes('node_modules')) continue;
    seen.add(file);
    const text = await readFile(file, 'utf8');
    assert.doesNotMatch(text, /@earendil-works|src\/pi|\/pi\/index|from '\.\.\/pi|from '\.\/pi/);
    for (const match of text.matchAll(/from '(\.[^']+)'/g)) {
      let target = fileURLToPath(new URL(match[1]!, pathToFileURL(file)));
      if (!target.endsWith('.ts')) target += '.ts';
      pending.push(target);
    }
  }
  assert.ok([...seen].some(file => file.endsWith(`${join('src', 'host', 'client.ts')}`)));
  assert.ok([...seen].some(file => file.endsWith(`${join('src', 'protocol', 'index.ts')}`)));
  assert.equal([...seen].some(file => file.includes(`${join('src', 'pi')}`)), false);
});

test('agent config accepts an opaque session id and rejects a foreign field', async () => {
  const root = join(tmpdir(), 'rw-agent-config');
  const config = parseAgentConfig({
    coordinatorUrl: 'http://127.0.0.1:9', credential: 'token', namespace: 'fleet', sessionId: 'tracer-1',
    inboxPath: join(root, 'inbox.sqlite'), evidencePath: join(root, 'evidence.jsonl'),
  });
  assert.equal(config.sessionId, 'tracer-1');
  assert.equal(agentSession(config).sessionId, 'tracer-1');
  assert.throws(() => parseAgentConfig({ ...config, callbackUrl: 'http://127.0.0.1:9' }), /Unknown agent/);
  const dir = await mkdtemp(join(tmpdir(), 'rw-agent-config-'));
  const path = join(dir, 'agent.json');
  await writeFile(path, JSON.stringify(config));
  assert.equal((await readAgentConfig(path)).sessionId, 'tracer-1');
  await rm(dir, { recursive: true, force: true });
});

import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { appendFile, lstat, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test, { type TestContext } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createClaudeAdapter, type ClaudeHarness } from '../../src/claude/adapter.ts';
import { claudeSessionAddress } from '../../src/claude/identity.ts';
import { claudeTranscriptEvidence } from '../../src/claude/transcript.ts';
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
const claudeSessionId = '191ef82e-07c3-4d1a-a11a-38d707efd124';

async function until(predicate: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 5000;
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, reason);
    await delay(20);
  }
}

async function stack(t: TestContext) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-claude-'));
  const inboxPath = join(dir, 'inbox.sqlite');
  const transcript = join(dir, 'session.jsonl');
  await writeFile(transcript, `${JSON.stringify({ type: 'mode', sessionId: claudeSessionId })}\n`);
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
    sessionId: claudeSessionId, transcriptPath: transcript, projectsRoot: join(dir, 'projects'),
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

let uuid = 0;
/** One transcript line as Claude Code writes it: a main-chain line stamped with the session ID. */
function line(type: string, content: unknown, extra: Record<string, unknown> = {}): string {
  return `${JSON.stringify({ parentUuid: null, isSidechain: false, type, message: { role: type, content },
    uuid: `00000000-0000-4000-8000-${String(++uuid).padStart(12, '0')}`, sessionId: claudeSessionId, ...extra })}\n`;
}
/** The Bash tool result Claude Code records for a `receive` that printed `stdout`. */
const toolResult = (stdout: string, extra: Record<string, unknown> = {}) =>
  line('user', [{ tool_use_id: 'toolu_x', type: 'tool_result', content: stdout, is_error: false }], extra);
const deliveredLine = (message: unknown) => JSON.stringify({ event: 'delivered', message });

/** A fake Claude Code harness: delivering appends the Bash tool result to the transcript, as Claude Code records it. */
function fakeHarness(transcript: string): ClaudeHarness & { delivered: Message[] } {
  const delivered: Message[] = [];
  return {
    delivered,
    sessionId: () => claudeSessionId,
    transcriptPath: () => transcript,
    deliver: async (message: Message) => {
      delivered.push(message);
      await appendFile(transcript, toolResult(`${deliveredLine(message)}\n`));
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

test('Claude Code session identity maps to one core session address', () => {
  assert.deepEqual(claudeSessionAddress(claudeSessionId, namespace), { namespace, sessionId: claudeSessionId });
  assert.equal(sessionKey(claudeSessionAddress(claudeSessionId, namespace)), `fleet:${claudeSessionId}`);
  assert.throws(() => claudeSessionAddress('bad id!', namespace), /Claude Code session id/);
  assert.throws(() => claudeSessionAddress('', namespace), /Claude Code session id/);
});

test('only a tool result the model received in this session proves delivery', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-claude-evidence-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const transcript = join(dir, `${claudeSessionId}.jsonl`);
  const session = claudeSessionAddress(claudeSessionId, namespace);
  const inbox = { acknowledgeDelivery: async () => ({ ok: true as const, value: null }) } as never;
  const source = claudeTranscriptEvidence(() => transcript, inbox, session, 'rt');
  const id = 'fleet:msg:1';
  const message = { version: 1, id, sequence: 1, timestamp: 7, sender: 'fleet:peer', recipient: `fleet:${claudeSessionId}`, body: 'hi', data: null, visibility: 'private' };
  const record = deliveredLine(message);
  assert.deepEqual(await source.lookup(transcript, id, message), { state: 'store_missing' });
  await writeFile(transcript, [
    // A human pasting the record into the prompt is not the agent receiving it.
    line('user', record),
    line('user', [{ type: 'text', text: record }]),
    // The assistant quoting it is not a receipt either.
    line('assistant', [{ type: 'text', text: record }]),
    // A subagent's tool result is not the main session's.
    toolResult(record, { isSidechain: true }),
    // Another session's tool result is not this session's.
    toolResult(record, { sessionId: '550e8400-e29b-41d4-a716-446655440000' }),
    // A bare mention and a forged body are not this delivery.
    toolResult(`discussing ${id}`),
    toolResult(deliveredLine({ ...message, body: 'forged' })),
    // A wrapped or truncated line proves nothing.
    toolResult(`prefix ${record}`),
  ].join(''));
  assert.deepEqual(await source.lookup(transcript, id, message), { state: 'absent' });
  // Multi-line Bash output with the record on its own line, as `receive` prints it.
  await appendFile(transcript, toolResult(`${deliveredLine({ ...message, id: 'fleet:msg:2' })}\n${record}\n`));
  // A torn final line is ignored until it completes.
  await appendFile(transcript, toolResult(record).slice(0, 40));
  const found = await source.lookup(transcript, id, message);
  assert.equal(found.state, 'persisted');
  if (found.state === 'persisted') {
    assert.match(found.evidence.recordId, /^00000000-0000-4000-8000-\d{12}$/);
    assert.equal(source.locatedAt(found.evidence, transcript), true);
    assert.equal(source.locatedAt(found.evidence, join(dir, 'other.jsonl')), false);
  }
  // Text-block tool results count the same as string ones.
  const blocks = join(dir, 'blocks.jsonl');
  await writeFile(blocks, line('user', [{ tool_use_id: 'toolu_y', type: 'tool_result', content: [{ type: 'text', text: record }] }]));
  assert.equal((await source.lookup(blocks, id, message)).state, 'persisted');
});

test('Claude Code adapter exchanges messages with a Pi-shaped peer through the host daemon', async t => {
  const h = await stack(t);
  const harness = fakeHarness(h.transcript);
  const adapter = createClaudeAdapter(harness, { loadConfig: async () => h.config, connectHost, request: requestCoordinator });
  t.after(() => adapter.stop());
  await adapter.start();
  const identity = await adapter.bound();
  assert.ok(!('ok' in identity));
  if ('ok' in identity) return;
  assert.equal(sessionKey(identity.actor), `fleet:${claudeSessionId}`);

  assert.equal((await adapter.tool('roger_send', { recipient: sessionKey(peer), body: 'from claude' })).ok, true);
  const peerClient = createHostClient(h.inboxPath);
  t.after(() => peerClient.close());
  assert.equal((await peerClient.bind(peer, 'peer-rt')).ok, true);
  await until(async () => (await peerClient.pending(peer, 'peer-rt')).some(item => item.body === 'from claude'), 'peer receives through the host daemon');

  const inbound = h.store.send({ hostId }, peer, { recipient: sessionKey(identity.actor), body: 'from peer' });
  assert.equal(inbound.ok, true);
  await until(async () => {
    await adapter.receive();
    return harness.delivered.some(message => message.id === inbound.value.id);
  }, 'Claude Code adapter delivers from pending');
  assert.deepEqual(harness.delivered.map(message => message.body), ['from peer']);
  await adapter.stop();
  const probe = createHostClient(h.inboxPath);
  t.after(() => probe.close());
  assert.equal((await probe.bind(identity.actor, 'probe')).ok, true);
  assert.deepEqual(await probe.pending(identity.actor, 'probe'), [], 'transcript-proved delivery is acknowledged');
  await probe.release(identity.actor, 'probe');
  const stored = receipt(h.inboxPath, inbound.value.id);
  assert.match(stored.evidence_record_id ?? '', /^00000000-0000-4000-8000-\d{12}$/);
  assert.equal(stored.evidence_location, resolve(h.transcript));

  const harness2 = fakeHarness(h.transcript);
  const adapter2 = createClaudeAdapter(harness2, { loadConfig: async () => h.config, connectHost, request: requestCoordinator });
  t.after(() => adapter2.stop());
  await adapter2.start();
  assert.deepEqual(await adapter2.receive(), []);
  assert.deepEqual(harness2.delivered, []);
});

test('a delivery a human pastes is never acknowledged; the tool result is', async t => {
  const h = await stack(t);
  const session = claudeSessionAddress(claudeSessionId, namespace);
  const silent: ClaudeHarness = { sessionId: () => claudeSessionId, transcriptPath: () => h.transcript, deliver: () => {} };
  const adapter = createClaudeAdapter(silent, { loadConfig: async () => h.config, connectHost, request: requestCoordinator });
  t.after(() => adapter.stop());
  await adapter.start();
  assert.equal((await adapter.tool('roger_identity', {})).ok, true);
  const inbound = h.store.send({ hostId }, peer, { recipient: sessionKey(session), body: 'prove it' });
  assert.equal(inbound.ok, true);
  await until(async () => (await adapter.receive()).length === 1, 'handed to the harness once');
  await appendFile(h.transcript, line('user', deliveredLine(inbound.value)));
  await adapter.receive();
  assert.equal(receipt(h.inboxPath, inbound.value.id).evidence_record_id, null, 'a pasted prompt is not evidence');
  await appendFile(h.transcript, toolResult(`${deliveredLine(inbound.value)}\n`));
  await adapter.receive();
  assert.match(receipt(h.inboxPath, inbound.value.id).evidence_record_id ?? '', /^00000000-0000-4000-8000-\d{12}$/);
});

test('Claude Code entry imports no other adapter', async () => {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const pending = [fileURLToPath(new URL('../../src/bin/claude.ts', import.meta.url))];
  const seen = new Set<string>();
  while (pending.length) {
    const file = pending.pop()!;
    if (seen.has(file) || !file.startsWith(root) || file.includes('node_modules')) continue;
    seen.add(file);
    const text = await readFile(file, 'utf8');
    assert.doesNotMatch(text, /@earendil-works|from '\.\.\/(pi|muse)\/|from '\.\/(pi|muse)\//);
    for (const match of text.matchAll(/(?:from|import\()\s*'(\.[^']+)'/g)) {
      let target = fileURLToPath(new URL(match[1]!, pathToFileURL(file)));
      if (!target.endsWith('.ts')) target += '.ts';
      pending.push(target);
    }
  }
  assert.ok([...seen].some(file => file.endsWith(join('src', 'evidence.ts'))));
  assert.equal([...seen].some(file => file.includes(join('src', 'pi')) || file.includes(join('src', 'muse'))), false);
});

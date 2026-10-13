import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir, homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createDaemonInbox as createHostInbox } from '../../src/host/index.ts';
import { listenLocal } from '../../src/host/ipc.ts';
import rogerWilco, { persistedReceipt } from '../../src/pi/index.ts';

const childFile = fileURLToPath(new URL('./crash-child.mjs', import.meta.url));
const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const stages = ['before-write', 'after-write-before-fsync', 'after-fsync-before-ack', 'after-ack'];

function receiptState(inboxPath, address, id) {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare(`SELECT receipt_entry_id, receipt_session_file, acknowledged_at
      FROM inbox_messages WHERE session_key = ? AND message_id = ?`)
      .get(`${address.namespace}:${address.piSessionId}`, id);
  } finally { db.close(); }
}
function receiptEntry(inboxPath, address, id) {
  return receiptState(inboxPath, address, id)?.receipt_entry_id;
}
function leaseOwner(inboxPath, address) {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare('SELECT runtime_id FROM inbox_leases WHERE session_key = ?')
      .get(`${address.namespace}:${address.piSessionId}`)?.runtime_id;
  } finally { db.close(); }
}
async function until(check, attempts = 300) {
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.fail('Expected native Pi delivery/ACK before deadline');
}

for (const stage of stages) test(`SIGKILL ${stage}: actual Pi extension resumes its persistent inbox`, { timeout: 20_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-crash-'));
  const sessionId = randomUUID();
  const address = { namespace: 'fleet', piSessionId: sessionId };
  const id = 'fleet:msg:1';
  const inboxPath = join(dir, 'inbox.sqlite');
  const config = { namespace: 'fleet', hostId: 'host-a', credential: 'offline',
    coordinatorUrl: 'http://127.0.0.1:1', inboxPath };
  await writeFile(join(dir, 'host.json'), JSON.stringify(config));
  const inbox = createHostInbox(inboxPath);
  let local;
  let child;
  let exited;
  let resumeAck;
  t.after(async () => {
    child?.kill('SIGKILL');
    await exited;
    resumeAck?.();
    await local?.close();
    await inbox.close();
    await rm(dir, { recursive: true, force: true });
  });
  local = await listenLocal(config, inbox);
  let checkpoint;
  let failCheckpoint;
  const reached = new Promise((resolve, reject) => {
    checkpoint = resolve;
    failCheckpoint = reject;
  });
  const originalAck = inbox.acknowledgePi;
  let readyFile;
  if (stage === 'after-ack') {
    const gate = new Promise(resolve => { resumeAck = resolve; });
    inbox.acknowledgePi = async function(session, owner, proof) {
      try {
        assert.deepEqual(session, address);
        assert.equal(proof.messageId, id);
        assert.equal(proof.piSessionId, sessionId);
        assert.equal(proof.sessionFile, readyFile);
        const result = await originalAck.call(this, session, owner, proof);
        assert.deepEqual(result, { ok: true, value: null });
        assert.equal(receiptEntry(inboxPath, address, id), proof.entryId);
        checkpoint({ stage, file: readyFile, proof });
        // The transaction committed, but no ACK response reaches Pi before death.
        await gate;
        return result;
      } catch (error) {
        failCheckpoint(error);
        throw error;
      }
    };
  }
  child = fork(childFile, [dir, sessionId, stage], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  let stderr = '';
  child.stderr.on('data', data => { stderr += data; });
  child.once('error', failCheckpoint);
  child.once('exit', (code, signal) => {
    failCheckpoint(new Error(`Child exited ${code}/${signal}: ${stderr}`));
  });
  child.on('message', state => {
    if (state.stage !== 'ready') {
      checkpoint(state);
      return;
    }
    void (async () => {
      assert.equal(readyFile, undefined, 'one ready barrier');
      readyFile = state.file;
      assert.ok(leaseOwner(inboxPath, address), 'Pi bound through the daemon before receive');
      assert.deepEqual(await inbox.receive({
        type: 'message', subscriptionId: `fleet:${sessionId}`,
        message: { version: 1, id, sequence: 1, timestamp: Date.now(),
          sender: 'fleet:550e8400-e29b-41d4-a716-446655440001',
          recipient: `fleet:${sessionId}`, body: 'offline message',
          data: null, visibility: 'private' },
      }), { inserted: true });
    })().catch(failCheckpoint);
  });
  let deadline;
  const state = await Promise.race([
    reached,
    new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error(`Timed out waiting for ${stage}: ${stderr}`)), 10_000); }),
  ]).finally(() => clearTimeout(deadline));
  assert.equal(state.stage, stage);
  assert.equal(state.file, readyFile);
  const killedOwner = leaseOwner(inboxPath, address);
  assert.ok(killedOwner);
  child.kill('SIGKILL');
  assert.equal((await exited).signal, 'SIGKILL');
  resumeAck?.();
  inbox.acknowledgePi = originalAck;
  await until(() => leaseOwner(inboxPath, address) === undefined);

  const file = state.file;
  const initial = pi.SessionManager.open(file);
  assert.equal(initial.getSessionId(), sessionId);
  const priorEntries = initial.getBranch().filter(e => e.type === 'custom_message' && e.customType === 'roger-wilco' && e.details?.deliveryId === id);
  assert.equal(priorEntries.length, stage === 'before-write' ? 0 : 1);
  assert.equal(receiptEntry(inboxPath, address, id), stage === 'after-ack' ? priorEntries[0].id : null,
    'SIGKILL checkpoint is on the real host ACK boundary');
  const committed = receiptState(inboxPath, address, id);
  assert.equal(committed.receipt_session_file, stage === 'after-ack' ? file : null);
  if (stage === 'after-ack') assert.equal(typeof committed.acknowledged_at, 'number');
  else assert.equal(committed.acknowledged_at, null);
  if (state.proof) {
    assert.equal(state.proof.messageId, id);
    assert.equal(state.proof.entryId, priorEntries[0].id);
    assert.equal(state.proof.sessionFile, file);
  }

  // The same daemon survives Pi's death. Disconnect retires the old grant;
  // recovery must not wait for its former 30-second expiry.
  const savedConfig = process.env.ROGER_WILCO_CONFIG;
  const savedFetch = globalThis.fetch;
  globalThis.fetch = async () => { throw new Error('Network disabled in crash fixture'); };
  process.env.ROGER_WILCO_CONFIG = join(dir, 'host.json');
  try {
    const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
    await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
    const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
    const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
    const loader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager,
      extensionFactories: [rogerWilco], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, []);
    const { session } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
      settingsManager, resourceLoader: loader, sessionManager: pi.SessionManager.open(file) });
    let turns = 0;
    session.agent.streamFunction = () => {
      turns++;
      const stream = ai.createAssistantMessageEventStream();
      queueMicrotask(() => { stream.push({ type: 'done', reason: 'stop', message: {
        role: 'assistant', content: [{ type: 'text', text: 'offline' }], api: model.api, provider: model.provider,
        model: model.id, stopReason: 'stop', timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      } }); stream.end(); });
      return stream;
    };
    try {
      await session.bindExtensions({});
      await until(() => {
        const owner = leaseOwner(inboxPath, address);
        return owner !== undefined && owner !== killedOwner;
      });
      await until(() => typeof receiptEntry(inboxPath, address, id) === 'string');
      // Exercise another lifecycle reconciliation, not an assumed idle poll.
      await session.extensionRunner.emit({ type: 'turn_end' });
      await session.extensionRunner.emit({ type: 'agent_settled', aborted: false });
      assert.equal(turns, stage === 'before-write' ? 1 : 0,
        'only an ID absent from native Pi JSONL is submitted on recovery');
      const recovered = pi.SessionManager.open(file).getBranch()
        .filter(e => e.type === 'custom_message' && e.customType === 'roger-wilco' && e.details?.deliveryId === id);
      assert.equal(recovered.length, 1, 'recovery does not create duplicate Pi entries');
      assert.equal(receiptEntry(inboxPath, address, id), recovered[0].id);
      assert.equal(receiptState(inboxPath, address, id).receipt_session_file, file);
      if (priorEntries.length) assert.equal(recovered[0].id, priorEntries[0].id);
    } finally {
      await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
      session.dispose();
    }
    await until(() => leaseOwner(inboxPath, address) === undefined);
    await local.close();
    await inbox.close();
    const reopened = createHostInbox(inboxPath);
    try {
      assert.equal((await reopened.bind(address, 'verified-reopen', Date.now())).ok, true);
      assert.deepEqual(await reopened.pending(address, 'verified-reopen'), []);
      await reopened.release(address, 'verified-reopen');
    } finally { await reopened.close(); }
  } finally {
    globalThis.fetch = savedFetch;
    if (savedConfig === undefined) delete process.env.ROGER_WILCO_CONFIG;
    else process.env.ROGER_WILCO_CONFIG = savedConfig;
  }
});

test('only complete JSONL entries count; a malformed committed line never ACKs', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-jsonl-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const file = join(dir, 'session.jsonl');
  const header = JSON.stringify({ type: 'session', version: 3, id: session.piSessionId, timestamp: new Date().toISOString(), cwd: dir });
  const entry = JSON.stringify({ type: 'custom_message', id: 'a0b1c2d3', details: { deliveryId: 'fleet:msg:1' }, customType: 'roger-wilco' });
  await writeFile(file, `${header}\n${entry}\n{"type":"custom_message"`);
  assert.equal((await persistedReceipt(file, session, 'fleet:msg:1')).entryId, 'a0b1c2d3');
  await writeFile(file, `${header}\n{"type":"custom_message"\n${entry}\n`);
  await assert.rejects(persistedReceipt(file, session, 'fleet:msg:1'), SyntaxError);
});

test('a receipt from another persistent Pi session is not accepted for this session', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-isolation-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const own = { namespace: 'fleet', piSessionId: randomUUID() };
  const other = { namespace: 'fleet', piSessionId: randomUUID() };
  const file = join(dir, 'other.jsonl');
  await writeFile(file, `${JSON.stringify({ type: 'session', version: 3, id: other.piSessionId, timestamp: new Date().toISOString(), cwd: dir })}\n${JSON.stringify({ type: 'custom_message', id: 'a0b1c2d3', parentId: null, timestamp: new Date().toISOString(), customType: 'roger-wilco', content: 'wrong session', display: true, details: { deliveryId: 'fleet:msg:1' } })}\n`);
  assert.equal(await persistedReceipt(file, own, 'fleet:msg:1'), undefined);
});

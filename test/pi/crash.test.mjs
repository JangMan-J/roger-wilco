import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir, homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { createHostInbox } from '../../src/host/index.ts';
import rogerWilco, { persistedReceipt } from '../../src/pi/index.ts';

const childFile = fileURLToPath(new URL('./crash-child.mjs', import.meta.url));
const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const stages = ['before-write', 'after-write-before-fsync', 'after-fsync-before-ack', 'after-ack'];

function receiptEntry(inboxPath, address, id) {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare('SELECT receipt_entry_id FROM messages WHERE session_key = ? AND message_id = ?')
      .get(`${address.namespace}:${address.piSessionId}`, id)?.receipt_entry_id;
  } finally { db.close(); }
}
function leaseOwner(inboxPath, address) {
  const db = new DatabaseSync(inboxPath, { readOnly: true });
  try {
    return db.prepare('SELECT runtime_id FROM leases WHERE session_key = ?')
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

for (const stage of stages) test(`SIGKILL ${stage}: actual Pi extension resumes its persistent inbox`, { timeout: 45_000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-crash-'));
  const sessionId = randomUUID();
  const address = { namespace: 'fleet', piSessionId: sessionId };
  const id = 'fleet:msg:1';
  const inboxPath = join(dir, 'inbox.sqlite');
  const child = fork(childFile, [dir, sessionId, stage], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let stderr = '';
  child.stderr.on('data', data => { stderr += data; });
  t.after(async () => { child.kill('SIGKILL'); await rm(dir, { recursive: true, force: true }); });
  let deadline;
  const state = await Promise.race([
    new Promise((resolve, reject) => {
      child.once('message', resolve);
      child.once('exit', (code, signal) => reject(new Error(`Child exited ${code}/${signal}: ${stderr}`)));
    }),
    new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error(`Timed out waiting for ${stage}: ${stderr}`)), 10_000); }),
  ]).finally(() => clearTimeout(deadline));
  assert.equal(state.stage, stage);
  child.kill('SIGKILL');
  await new Promise(resolve => child.once('exit', resolve));

  const file = state.file;
  const killedOwner = leaseOwner(inboxPath, address);
  assert.ok(killedOwner);
  const initial = pi.SessionManager.open(file);
  assert.equal(initial.getSessionId(), sessionId);
  const priorEntries = initial.getBranch().filter(e => e.type === 'custom_message' && e.customType === 'roger-wilco' && e.details?.deliveryId === id);
  assert.equal(priorEntries.length, stage === 'before-write' ? 0 : 1);
  assert.equal(receiptEntry(inboxPath, address, id), stage === 'after-ack' ? priorEntries[0].id : null,
    'SIGKILL checkpoint is on the real host ACK boundary');
  if (state.proof) {
    assert.equal(state.proof.messageId, id);
    assert.equal(state.proof.entryId, priorEntries[0].id);
    assert.equal(state.proof.sessionFile, file);
  }

  // Resume immediately, inside the dead owner's real lease window. The extension
  // must stay alive and reclaim it when the original 30-second expiry arrives.
  const savedConfig = process.env.ROGER_WILCO_CONFIG;
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
      assert.equal(receiptEntry(inboxPath, address, id), stage === 'after-ack' ? priorEntries[0].id : null,
        'immediate resume must not steal the unexpired lease');
      await until(() => leaseOwner(inboxPath, address) !== killedOwner, 3500);
      await until(() => typeof receiptEntry(inboxPath, address, id) === 'string');
      // Even a message already ACKed before SIGKILL must not cause a late retry on the next poll.
      await new Promise(resolve => setTimeout(resolve, 550));
      assert.equal(turns, stage === 'before-write' ? 1 : 0,
        'only an ID absent from native Pi JSONL is submitted on recovery');
      const recovered = pi.SessionManager.open(file).getBranch()
        .filter(e => e.type === 'custom_message' && e.customType === 'roger-wilco' && e.details?.deliveryId === id);
      assert.equal(recovered.length, 1, 'recovery does not create duplicate Pi entries');
      assert.equal(receiptEntry(inboxPath, address, id), recovered[0].id);
      if (priorEntries.length) assert.equal(recovered[0].id, priorEntries[0].id);
    } finally {
      await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
      session.dispose();
    }
    const reopened = createHostInbox(inboxPath);
    try {
      assert.equal((await reopened.bind(address, 'verified-reopen', Date.now())).ok, true);
      assert.deepEqual(await reopened.pending(address, 'verified-reopen'), []);
      await reopened.release(address, 'verified-reopen');
    } finally { await reopened.close(); }
  } finally {
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

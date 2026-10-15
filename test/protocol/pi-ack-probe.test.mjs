import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, open } from 'node:fs/promises';
import { tmpdir, homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

// Test the selected installed/build Pi, never silently substitute an npm version.
const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi/packages/coding-agent'));
const piVersion = JSON.parse(await readFile(join(root, 'package.json'), 'utf8')).version;
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const message = { customType: 'roger-wilco', content: 'probe message', display: true, details: { deliveryId: 'rw:test:1' } };

async function until(check) {
  for (let i = 0; i < 200; i++) { if (await check()) return; await delay(10); }
  assert.fail('Pi did not reach expected state in 2 seconds');
}
async function entries(path) {
  const jsonl = await readFile(path, 'utf8').catch(e => { if (e.code === 'ENOENT') return ''; throw e; });
  return jsonl.split('\n').filter(Boolean).map(JSON.parse);
}
function matches(entries) { return entries.filter(e => e.type === 'custom_message' && e.customType === message.customType && e.details?.deliveryId === message.details.deliveryId); }
async function setup(t) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-pi-ack-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const manager = pi.SessionManager.create(dir, join(dir, 'sessions'));
  let send;
  const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
  await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
  const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
  const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
  const loader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager, extensionFactories: [extension => { send = () => extension.sendMessage(message, { deliverAs: 'followUp', triggerTurn: true }); }], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await loader.reload();
  assert.deepEqual(loader.getExtensions().errors, []);
  const { session } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime, settingsManager, resourceLoader: loader, sessionManager: manager });
  let calls = 0;
  let release;
  session.agent.streamFunction = () => {
    calls++;
    const stream = ai.createAssistantMessageEventStream();
    const finish = () => {
      const assistant = { role: 'assistant', content: [{ type: 'text', text: 'offline' }], api: model.api, provider: model.provider, model: model.id, stopReason: 'stop', timestamp: Date.now(), usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      stream.push({ type: 'done', reason: 'stop', message: assistant }); stream.end();
    };
    if (calls === 1) release = finish;
    else queueMicrotask(finish);
    return stream;
  };
  await session.bindExtensions({});
  t.after(async () => { release?.(); await session.abort(); session.dispose(); });
  return { session, manager, send: () => send(), file: manager.getSessionFile(), calls: () => calls, release: () => release?.() };
}

test('idle send returns before persistence; exact JSONL identity then fsync is observable and survives reopen', async t => {
  const h = await setup(t);
  assert.equal(h.send(), undefined);
  assert.equal(matches(await entries(h.file)).length, 0);
  assert.equal(matches(h.manager.getBranch()).length, 1, 'volatile branch is not a receipt');
  h.release();
  await until(async () => matches(await entries(h.file)).length === 1);
  const handle = await open(h.file, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
  assert.equal(matches(pi.SessionManager.open(h.file).getBranch()).length, 1);
  console.log(JSON.stringify({ piVersion, idle: 'send returns undefined before file exists', observedAndSynced: true, reopen: true }));
});

test('busy followUp is not ACK-able until queued turn persists; restart scan finds persisted identity', async t => {
  const h = await setup(t);
  const work = h.session.prompt('active user work');
  await until(() => h.calls() === 1);
  assert.equal(h.send(), undefined);
  assert.equal(matches(await entries(h.file)).length, 0);
  assert.equal(matches(h.manager.getBranch()).length, 0, 'busy message is queued, not persisted');
  h.release();
  await work;
  await until(async () => matches(await entries(h.file)).length === 1);
  assert.equal(matches(pi.SessionManager.open(h.file).getBranch()).length, 1);
  assert.equal(h.calls(), 2, 'queued follow-up runs after the original turn');
  console.log(JSON.stringify({ busy: 'queued before original completes', persistedAfter: true, resumedIdentity: true }));
});

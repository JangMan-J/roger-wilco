import assert from 'node:assert/strict';
import { promises as fsPromises } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { pathToFileURL } from 'node:url';
import { createHostInbox } from '../../src/host/index.ts';

const [dir, sessionId, stage] = process.argv.slice(2);
const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const address = { namespace: 'fleet', piSessionId: sessionId };
const id = 'fleet:msg:1';
const manager = pi.SessionManager.create(dir, join(dir, 'sessions'), { id: sessionId });
const file = manager.getSessionFile();
const inboxPath = join(dir, 'inbox.sqlite');
const configPath = join(dir, 'host.json');
await writeFile(configPath, JSON.stringify({ namespace: 'fleet', hostId: 'host-a', credential: 'offline',
  coordinatorUrl: 'http://127.0.0.1:1', inboxPath }));
process.env.ROGER_WILCO_CONFIG = configPath;

// Only the actual Pi delivery ACK boundary is wrapped; the underlying SQLite implementation stays real.
const prototype = Object.getPrototypeOf(createHostInbox(inboxPath));
const originalAck = prototype.acknowledgePi;
if (stage === 'after-fsync-before-ack' || stage === 'after-ack') {
  prototype.acknowledgePi = async function(session, runtimeId, proof) {
    assert.deepEqual(session, address);
    assert.equal(proof.messageId, id);
    assert.equal(proof.sessionFile, file);
    if (stage === 'after-fsync-before-ack') {
      process.send({ stage, file, proof });
      await new Promise(() => {});
    }
    const result = await originalAck.call(this, session, runtimeId, proof);
    assert.deepEqual(result, { ok: true, value: null });
    process.send({ stage, file, proof });
    await new Promise(() => {});
  };
}
if (stage === 'after-write-before-fsync') {
  const originalOpen = fsPromises.open;
  fsPromises.open = async (...args) => {
    const handle = await originalOpen(...args);
    if (args[0] === file) {
      handle.sync = async () => {
        // persistedReceipt found the actual native Pi entry; the fsync has not run yet.
        process.send({ stage, file });
        await new Promise(() => {});
      };
    }
    return handle;
  };
  syncBuiltinESMExports();
}
// Import after the filesystem wrapper is installed, so the extension receives the wrapped named export.
const { default: rogerWilco } = await import('../../src/pi/index.ts');
const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
const loader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager,
  extensionFactories: [rogerWilco], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
await loader.reload();
assert.deepEqual(loader.getExtensions().errors, []);
const { session } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
  settingsManager, resourceLoader: loader, sessionManager: manager });
let release;
session.agent.streamFunction = () => {
  const stream = ai.createAssistantMessageEventStream();
  const finish = () => { stream.push({ type: 'done', reason: 'stop', message: {
    role: 'assistant', content: [{ type: 'text', text: 'offline' }], api: model.api, provider: model.provider,
    model: model.id, stopReason: 'stop', timestamp: Date.now(),
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  } }); stream.end(); };
  if (stage === 'before-write') release = finish;
  else queueMicrotask(finish);
  return stream;
};
const inbox = createHostInbox(inboxPath);
const message = { version: 1, id, sequence: 1, timestamp: Date.now(),
  sender: 'fleet:550e8400-e29b-41d4-a716-446655440001', recipient: `fleet:${sessionId}`,
  body: 'offline message', data: null, visibility: 'private' };
if (stage === 'before-write') {
  await session.bindExtensions({});
  void session.prompt('busy turn');
  while (!release) await new Promise(r => setTimeout(r, 5));
}
// The real extension reads the durable host inbox on startup or while Pi is busy.
await inbox.receive({ type: 'message', subscriptionId: `fleet:${sessionId}`, message });
await inbox.close();
if (stage !== 'before-write') await session.bindExtensions({});
if (stage === 'before-write') {
  for (let attempt = 0; attempt < 400 && !session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === id); attempt++) {
    await new Promise(r => setTimeout(r, 5));
  }
  assert.ok(session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === id), 'actual extension queued the Pi follow-up');
  const content = await readFile(file, 'utf8').catch(error => error.code === 'ENOENT' ? '' : Promise.reject(error));
  assert.ok(!content.includes(`"deliveryId":"${id}"`), 'Pi has not written the queued message');
  process.send({ stage, file });
}
// The extension's poll/lease timers are unref'ed; keep this process alive for the parent's SIGKILL.
setInterval(() => {}, 1000);
await new Promise(() => {});

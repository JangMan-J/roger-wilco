import assert from 'node:assert/strict';
import { promises as fsPromises } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { join, resolve } from 'node:path';
import { homedir } from 'node:os';
import { pathToFileURL } from 'node:url';

const [dir, sessionId, stage] = process.argv.slice(2);
const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi/packages/coding-agent'));
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const id = 'fleet:msg:1';
const manager = pi.SessionManager.create(dir, join(dir, 'sessions'), { id: sessionId });
const file = manager.getSessionFile();
const configPath = join(dir, 'host.json');
process.env.ROGER_WILCO_CONFIG = configPath;
globalThis.fetch = async () => { throw new Error('Network disabled in crash fixture'); };
// Keep Pi alive even while its unref'ed timers or receipt hook are waiting.
setInterval(() => {}, 1000);

// Pi never opens the inbox. Pause only its actual JSONL receipt sync.
if (stage === 'after-write-before-fsync' || stage === 'after-fsync-before-ack') {
  const originalOpen = fsPromises.open;
  fsPromises.open = async (...args) => {
    const handle = await originalOpen(...args);
    if (args[0] === file) {
      const originalSync = handle.sync.bind(handle);
      handle.sync = async () => {
        // Returning from sync would allow persistedReceipt to construct an ACK.
        // Pause before that return, optionally after the real sync has completed.
        if (stage === 'after-fsync-before-ack') await originalSync();
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
await session.bindExtensions({});
if (stage === 'before-write') {
  void session.prompt('busy turn');
  while (!release) await new Promise(r => setTimeout(r, 5));
}
// Parent injects only after binding (and, for before-write, the busy turn).
process.send({ stage: 'ready', file });
if (stage === 'before-write') {
  for (let attempt = 0; attempt < 400 && !session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === id); attempt++) {
    await new Promise(r => setTimeout(r, 5));
  }
  assert.ok(session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === id), 'actual extension queued the Pi follow-up');
  const content = await readFile(file, 'utf8').catch(error => error.code === 'ENOENT' ? '' : Promise.reject(error));
  assert.ok(!content.includes(`"deliveryId":"${id}"`), 'Pi has not written the queued message');
  process.send({ stage, file });
}
await new Promise(() => {});

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { TestContext } from 'node:test';

/** Real persistent Pi runtime. Only provider streaming is synthetic; no network model calls. */
export async function nativePi(t: TestContext, dir: string, options: { sessionFile?: string; extensions?: string[] } = {}) {
  const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
  const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
  const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
  assert.equal(typeof pi.SessionManager.create, 'function', `Installed Pi at ${root} must support persistent sessions`);
  const manager = options.sessionFile ? pi.SessionManager.open(options.sessionFile) : pi.SessionManager.create(dir, join(dir, 'sessions'));
  const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
  await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
  const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
  const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
  const loader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager,
    additionalExtensionPaths: options.extensions ?? [resolve('src/pi/index.ts')],
    noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await loader.reload();
  assert.deepEqual(loader.getExtensions().errors, [], 'native Pi extension loader');
  const { session } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
    settingsManager, resourceLoader: loader, sessionManager: manager });
  const calls: unknown[] = [];
  const synthetic = installSyntheticModel(session, ai, model, calls);
  await session.bindExtensions({});
  let stopped = false;
  async function stop() {
    if (stopped) return;
    stopped = true;
    synthetic.release();
    await session.abort();
    await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'exit' });
    session.dispose();
  }
  t.after(stop);
  return { session, manager, calls, stop, ...synthetic,
    file: manager.getSessionFile() as string, id: manager.getSessionId() as string };
}

function installSyntheticModel(session: any, ai: any, model: any, calls: unknown[]) {
  let hold: (() => void) | undefined;
  let holdNext = false;
  session.agent.streamFunction = (_model: unknown, context: unknown) => {
    calls.push(structuredClone(context));
    const stream = ai.createAssistantMessageEventStream();
    const finish = () => {
      const message = { role: 'assistant', content: [{ type: 'text', text: 'offline' }], api: model.api,
        provider: model.provider, model: model.id, stopReason: 'stop', timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      stream.push({ type: 'done', reason: 'stop', message }); stream.end();
    };
    if (holdNext) { holdNext = false; hold = finish; } else queueMicrotask(finish);
    return stream;
  };
  return { holdNext: () => { holdNext = true; }, release: () => hold?.() };
}

/** Real Pi AgentSessionRuntime performs fork/switch teardown and session_start; no manual event simulation. */
export async function nativePiRuntime(t: TestContext, dir: string) {
  const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
  const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
  const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
  const agentDir = join(dir, 'agent');
  const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
  await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
  const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
  const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
  const calls = new Map<string, unknown[]>();
  const create = async ({ cwd, sessionManager, sessionStartEvent }: { cwd: string; sessionManager: any; sessionStartEvent?: any }) => {
    const resourceLoader = new pi.DefaultResourceLoader({ cwd, agentDir, settingsManager,
      additionalExtensionPaths: [resolve('src/pi/index.ts')],
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
    await resourceLoader.reload();
    assert.deepEqual(resourceLoader.getExtensions().errors, [], 'native Pi extension loader on replacement');
    const services = { cwd, agentDir, modelRuntime, settingsManager, resourceLoader, diagnostics: [] };
    const created = await pi.createAgentSession({ cwd, agentDir, model, modelRuntime, settingsManager,
      resourceLoader, sessionManager, sessionStartEvent });
    return { ...created, services, diagnostics: [] };
  };
  const runtime = await pi.createAgentSessionRuntime(create, {
    cwd: dir, agentDir, sessionManager: pi.SessionManager.create(dir, join(dir, 'sessions')),
  });
  const bind = async (session: any) => {
    const current: unknown[] = [];
    calls.set(session.sessionId, current);
    installSyntheticModel(session, ai, model, current);
    await session.bindExtensions({});
  };
  runtime.setRebindSession(bind);
  await bind(runtime.session);
  t.after(async () => { await runtime.dispose(); });
  return { runtime, calls };
}

export async function persistedMessages(file: string, customType = 'roger-wilco') {
  const contents = await readFile(file, 'utf8').catch(error => {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return '';
    throw error;
  });
  return contents.split('\n').filter(Boolean).map(line => JSON.parse(line) as {
    type?: string; customType?: string; details?: { deliveryId?: string }; content?: string;
  }).filter(entry => entry.type === 'custom_message' && entry.customType === customType);
}

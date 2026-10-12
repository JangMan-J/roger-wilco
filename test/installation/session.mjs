#!/usr/bin/env node
/**
 * Native Pi subprocess fixture for a provisioned host. Start with:
 *   RW_SESSION_DIR=<directory> ROGER_WILCO_CONFIG=<host.json> node test/installation/session.mjs
 * Optional: PI_PACKAGE_DIR (default ~/pi-main/packages/coding-agent), RW_PACKAGE_DIR (default repo root).
 * One JSON command per stdin line: send {recipient,body,id?}, entity {label,type?,id?}, busy, release, stop.
 * Stdout is exclusively NDJSON: ready, send, entity, received, turn, busy, released.
 * `received` is emitted only for a complete custom_message in this session's JSONL.
 */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath, pathToFileURL } from 'node:url';

const output = value => process.stdout.write(`${JSON.stringify(value)}\n`);
const withTimeout = (promise, ms) => Promise.race([
  promise,
  new Promise((_, reject) => { const timer = setTimeout(() => reject(new Error(`Timed out after ${ms} ms`)), ms); promise.finally(() => clearTimeout(timer)).catch(() => {}); }),
]);

async function main() {
  // Pi and extensions must not put diagnostic chatter on the machine-readable channel.
  for (const method of ['log', 'info', 'warn']) console[method] = (...args) => console.error(...args);
  const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
  const packageDir = resolve(process.env.RW_PACKAGE_DIR ?? fileURLToPath(new URL('../..', import.meta.url)));
  const dir = process.env.RW_SESSION_DIR;
  assert.ok(dir, 'RW_SESSION_DIR is required');
  assert.ok(process.env.ROGER_WILCO_CONFIG, 'ROGER_WILCO_CONFIG is required');
  const sessionDir = resolve(dir);
  const config = JSON.parse(await readFile(process.env.ROGER_WILCO_CONFIG, 'utf8'));
  assert.match(config.namespace, /^[a-z][a-z0-9-]{0,62}$/, 'Host config must contain a valid namespace');
  const extension = join(packageDir, 'src/pi/index.ts');
  await readFile(extension); // fail before ready if the packaged extension is missing
  await mkdir(sessionDir, { recursive: true });
  const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
  const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
  const agentDir = join(sessionDir, 'agent');
  const modelRuntime = await pi.ModelRuntime.create({ authPath: join(sessionDir, 'auth.json'), modelsPath: join(sessionDir, 'models.json') });
  await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
  const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
  const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
  const loader = new pi.DefaultResourceLoader({ cwd: sessionDir, agentDir, settingsManager,
    additionalExtensionPaths: [extension], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await loader.reload();
  assert.deepEqual(loader.getExtensions().errors, [], 'Native Pi extension loader');
  const manager = pi.SessionManager.create(sessionDir, join(sessionDir, 'sessions'));
  const { session } = await pi.createAgentSession({ cwd: sessionDir, agentDir, model, modelRuntime,
    settingsManager, resourceLoader: loader, sessionManager: manager });
  let turns = 0;
  let holdNext = false;
  let release;
  let started;
  let busyWork;
  let stopped = false;
  let receiptTimer;
  const seen = new Set();
  const file = manager.getSessionFile();
  const sessionId = manager.getSessionId();
  assert.ok(file && sessionId, 'Native Pi requires a persistent session');
  const address = `${config.namespace}:${sessionId}`;
  session.agent.streamFunction = () => {
    turns++;
    output({ type: 'turn', count: turns });
    const stream = ai.createAssistantMessageEventStream();
    const finish = () => {
      stream.push({ type: 'done', reason: 'stop', message: { role: 'assistant', content: [{ type: 'text', text: 'offline' }],
        api: model.api, provider: model.provider, model: model.id, stopReason: 'stop', timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } } });
      stream.end();
    };
    if (holdNext) {
      holdNext = false;
      release = () => { release = undefined; finish(); };
      started?.(); started = undefined;
    }
    else queueMicrotask(finish);
    return stream;
  };
  async function scan() {
    const raw = await readFile(file, 'utf8').catch(error => {
      if (error.code === 'ENOENT') return ''; // First Pi turn creates the file.
      throw error;
    });
    const lines = raw.split('\n');
    lines.pop(); // Never treat an incomplete JSONL tail as a receipt.
    if (!lines.length) return;
    const header = JSON.parse(lines.shift());
    assert.equal(header.type, 'session');
    assert.equal(header.id, sessionId, 'Receipt file belongs to another Pi session');
    for (const line of lines) {
      const entry = JSON.parse(line);
      if (entry.type !== 'custom_message' || entry.customType !== 'roger-wilco' ||
          typeof entry.id !== 'string' || typeof entry.details?.deliveryId !== 'string') continue;
      const id = entry.details.deliveryId;
      if (seen.has(id)) continue;
      const envelope = JSON.parse(entry.content.slice(entry.content.indexOf('\n') + 1));
      assert.equal(envelope.id, id, 'Receipt must match the immutable message ID');
      seen.add(id);
      output({ type: 'received', id, body: envelope.body, turns });
    }
  }
  async function stop() {
    if (stopped) return;
    stopped = true;
    clearInterval(receiptTimer);
    release?.();
    await withTimeout((async () => {
      try { await session.abort(); }
      finally {
        try { await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'exit' }); }
        finally { session.dispose(); }
      }
    })(), 5000);
  }
  process.once('SIGTERM', () => { void stop().then(() => process.exit(0), error => { console.error(error); process.exit(1); }); });
  process.once('SIGINT', () => { void stop().then(() => process.exit(0), error => { console.error(error); process.exit(1); }); });
  try {
    await session.bindExtensions({});
    assert.ok(session.extensionRunner.getToolDefinition('roger_send'), 'Packaged Roger-Wilco tool not registered');
    receiptTimer = setInterval(() => { void scan().catch(error => {
      console.error(error);
      void stop().then(() => process.exit(1), shutdownError => { console.error(shutdownError); process.exit(1); });
    }); }, 50);
    output({ type: 'ready', sessionId, address, file });
    for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
      try {
        const command = JSON.parse(line);
        if (command.command === 'stop') {
          await stop().catch(error => { console.error(error); process.exit(1); });
          break;
        }
        if (command.command === 'send') {
          assert.equal(typeof command.recipient, 'string');
          assert.equal(typeof command.body, 'string');
          const tool = session.extensionRunner.getToolDefinition('roger_send');
          const callId = randomUUID();
          const result = await tool.execute(callId, { recipient: command.recipient, body: command.body },
            undefined, undefined, session.extensionRunner.createToolContext(callId, undefined));
          output({ type: 'send', id: command.id, result: result.details });
        } else if (command.command === 'entity') {
          const tool = session.extensionRunner.getToolDefinition('roger_entity');
          assert.ok(tool, 'Packaged entity lookup tool is registered');
          const callId = randomUUID();
          const result = await tool.execute(callId, { label: command.label, type: command.type },
            undefined, undefined, session.extensionRunner.createToolContext(callId, undefined));
          output({ type: 'entity', id: command.id, result: result.details });
        } else if (command.command === 'busy') {
          assert.ok(!busyWork, 'Already busy');
          await session.waitForIdle();
          holdNext = true;
          const began = new Promise(resolve => { started = resolve; });
          busyWork = session.prompt('Offline busy turn');
          await withTimeout(began, 5000);
          output({ type: 'busy', turns });
        } else if (command.command === 'release') {
          assert.ok(busyWork && release, 'No busy model turn');
          release();
          await withTimeout(busyWork, 5000);
          busyWork = undefined;
          output({ type: 'released', turns });
        } else throw new Error(`Unknown command: ${command.command}`);
      } catch (error) {
        console.error('Session command failed:', error);
        output({ type: 'error', message: String(error) });
      }
    }
  } finally { await stop(); }
}

main().catch(error => { console.error(error); process.exitCode = 1; });

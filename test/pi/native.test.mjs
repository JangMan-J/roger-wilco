import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir, homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { createDaemonInbox as createHostInbox, createHostDaemon } from '../../src/host/index.ts';
import { createHostClient } from '../../src/host/client.ts';

const root = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi-main/packages/coding-agent'));
const pi = await import(pathToFileURL(join(root, 'dist/index.js')).href);
const ai = await import(pathToFileURL(resolve(root, '../ai/dist/compat.js')).href);
const extension = fileURLToPath(new URL('../../src/pi/index.ts', import.meta.url));
async function until(check) {
  for (let i = 0; i < 300; i++) { if (await check()) return; await delay(10); }
  assert.fail('Timed out waiting for native Pi delivery');
}
async function entries(file) {
  return (await readFile(file, 'utf8').catch(e => e.code === 'ENOENT' ? '' : Promise.reject(e)))
    .split('\n').filter(Boolean).map(JSON.parse);
}

test('native Pi extension binds automatically, routes only its session, wakes idle and queues busy follow-up; resume does not resend', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-native-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const a = randomUUID(), b = randomUUID();
  const inboxPath = join(dir, 'host.sqlite');
  const configPath = join(dir, 'host.json');
  const previous = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = configPath;
  t.after(() => { if (previous === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = previous; });
  const calls = [];
  const server = createServer(async (req, res) => {
    const body = req.method === 'POST' ? JSON.parse(await new Promise(resolve => {
      let value = ''; req.on('data', chunk => { value += chunk; }); req.on('end', () => resolve(value));
    })) : undefined;
    if (req.url === '/v1/hosts/stream') {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.end(': idle\n\n');
      return;
    }
    calls.push({ path: req.url, body, authorization: req.headers.authorization });
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ ok: true, value: { subscription: { id: `fleet:${body?.session?.sessionId}`, session: body?.session, hostId: 'host-a', cursor: 0 }, rank: 1, lastSeenAt: 1 } }));
  });
  server.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const hostConfig = { namespace: 'fleet', hostId: 'host-a',
    coordinatorUrl: `http://127.0.0.1:${server.address().port}`, credential: 'offline-token', inboxPath };
  await writeFile(configPath, JSON.stringify(hostConfig));
  const modelRuntime = await pi.ModelRuntime.create({ authPath: join(dir, 'auth.json'), modelsPath: join(dir, 'models.json') });
  await modelRuntime.setRuntimeApiKey('anthropic', 'offline-test-key');
  const model = ai.getModel('anthropic', 'claude-sonnet-4-5');
  const settingsManager = pi.SettingsManager.inMemory({ retry: { enabled: false } });
  const loader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager,
    additionalExtensionPaths: [extension], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await loader.reload();
  assert.deepEqual(loader.getExtensions().errors, []);
  const manager = pi.SessionManager.create(dir, join(dir, 'sessions'), { id: a });
  const { session } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
    settingsManager, resourceLoader: loader, sessionManager: manager });
  let hold;
  let release;
  const requests = [];
  session.agent.streamFunction = (_model, context, options) => {
    requests.push(JSON.stringify(context));
    const stream = ai.createAssistantMessageEventStream();
    const finish = () => {
      const stopReason = options?.signal?.aborted ? 'aborted' : 'stop';
      const message = { role: 'assistant', content: [{ type: 'text', text: 'offline' }],
        api: model.api, provider: model.provider, model: model.id, stopReason, timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      stream.push(stopReason === 'aborted' ? { type: 'error', reason: 'aborted', error: message } :
        { type: 'done', reason: 'stop', message });
      stream.end();
    };
    if (hold) release = finish;
    else queueMicrotask(finish);
    return stream;
  };
  const external = createHostInbox(inboxPath);
  const daemon = createHostDaemon(hostConfig, external);
  const running = daemon.start(new AbortController().signal);
  t.after(async () => { release?.(); await session.abort(); await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' }); session.dispose(); await daemon.stop(); await running; await external.close(); });
  async function openClient() {
    const client = await createHostClient(inboxPath);
    t.after(() => client.close());
    return client;
  }
  async function retireLiveLease() {
    const db = new DatabaseSync(inboxPath, { readOnly: true });
    try {
      db.exec('PRAGMA busy_timeout = 5000');
      const token = db.prepare('SELECT runtime_id FROM inbox_leases WHERE session_key = ?').get(`fleet:${a}`)?.runtime_id;
      assert.equal(typeof token, 'string');
      await external.release({ namespace: 'fleet', sessionId: a }, token);
    } finally { db.close(); }
  }
  await session.bindExtensions({});
  await until(() => calls.some(call => call.path === '/v1/sessions/register'));
  assert.ok(calls.every(call => call.authorization === 'Bearer offline-token'));
  const message = (sequence, recipient, body) => ({ type: 'message', subscriptionId: `fleet:${recipient}`, message: {
    version: 1, id: `fleet:msg:${sequence}`, sequence, timestamp: Date.now(), sender: `fleet:${b}`,
    recipient: `fleet:${recipient}`, body, data: null, visibility: 'private',
  } });
  const idle = message(1, a, 'Idle update');
  await external.receive(idle);
  await until(() => requests.length === 1 && !session.isStreaming);
  assert.match(requests[0], /Idle update/);
  const file = manager.getSessionFile();
  await until(async () => (await entries(file)).some(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:1'));
  assert.equal((await entries(file)).find(e => e.details?.deliveryId === 'fleet:msg:1').content, JSON.stringify(idle.message),
    'native custom messages preserve the envelope without Roger warning boilerplate');
  hold = true;
  const work = session.prompt('Original user work');
  await until(() => requests.length === 2);
  await external.receive(message(2, a, 'Busy update'));
  await external.receive(message(3, b, 'Not for this session'));
  await until(() => session.agent.peekQueuedMessages().some(message => message.details?.deliveryId === 'fleet:msg:2'));
  assert.equal(requests.length, 2);
  hold = false;
  release();
  await work;
  await until(() => requests.length === 3 && !session.isStreaming);
  await until(async () => (await entries(file)).some(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:2'));
  assert.match(requests[2], /Busy update/);
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:3').length, 0);
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:1').length, 1);

  hold = true;
  const abortedWork = session.prompt('Work cancelled while delivery waits');
  await until(() => requests.length === 4);
  await external.receive(message(4, a, 'Follow-up after abort'));
  await until(() => session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === 'fleet:msg:4'));
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:4').length, 0);
  const aborting = session.abort();
  hold = false;
  release();
  await Promise.all([aborting, abortedWork]);
  await until(async () => (await entries(file)).some(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:4'));
  await until(async () => {
    const db = new DatabaseSync(inboxPath, { readOnly: true });
    try { return !!db.prepare('SELECT evidence_record_id FROM inbox_messages WHERE message_id = ?').get('fleet:msg:4')?.evidence_record_id; }
    finally { db.close(); }
  });
  await delay(600);
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:4').length, 1);
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.customType === 'roger-wilco-wake').length, 1,
    'a genuinely aborted provider response requires one explicit control wake');
  assert.ok(requests.slice(4).some(request => request.includes('Follow-up after abort')));
  assert.equal(session.agent.peekQueuedMessages().filter(m => m.details?.deliveryId === 'fleet:msg:4').length, 0);

  // Simulate a paused live runtime missing the expiry; no test bind repairs it.
  await retireLiveLease();
  await external.receive(message(5, a, 'Recovered live lease'));
  await until(async () => (await entries(file)).some(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:5'));
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:5').length, 1);

  // Abort the recovery turn itself: repeated settled notifications must not
  // create a wake loop or resubmit the still-queued original ID.
  await until(() => !session.isStreaming);
  const beforeInterrupted = requests.length;
  hold = true;
  const interrupted = session.prompt('Interrupt recovery');
  await until(() => requests.length > beforeInterrupted && session.isStreaming);
  await external.receive(message(6, a, 'Original after interrupted wake'));
  await until(() => session.agent.peekQueuedMessages().some(m => m.details?.deliveryId === 'fleet:msg:6'));
  const interruptedAbort = session.abort();
  release();
  await until(() => requests.length > beforeInterrupted + 1);
  await session.extensionRunner.emit({ type: 'agent_settled', aborted: true });
  await session.extensionRunner.emit({ type: 'agent_settled', aborted: true });
  assert.equal(session.agent.peekQueuedMessages().filter(m => m.customType === 'roger-wilco-wake').length, 0);
  assert.equal(session.agent.peekQueuedMessages().filter(m => m.details?.deliveryId === 'fleet:msg:6').length, 1);
  const wakesBefore = (await entries(file)).filter(e => e.customType === 'roger-wilco-wake').length;
  assert.equal(wakesBefore, 2);
  const recoveryAbort = session.abort();
  hold = false;
  release();
  await Promise.all([interruptedAbort, interrupted, recoveryAbort]);
  await delay(600);
  assert.equal((await entries(file)).filter(e => e.customType === 'roger-wilco-wake').length, wakesBefore);
  assert.equal((await entries(file)).filter(e => e.details?.deliveryId === 'fleet:msg:6').length, 0);
  assert.equal(session.agent.peekQueuedMessages().filter(m => m.details?.deliveryId === 'fleet:msg:6').length, 1);
  await session.prompt('Continue after interrupted recovery');
  await until(async () => (await entries(file)).some(e => e.details?.deliveryId === 'fleet:msg:6'));
  assert.equal((await entries(file)).filter(e => e.details?.deliveryId === 'fleet:msg:6').length, 1);
  await until(async () => {
    const db = new DatabaseSync(inboxPath, { readOnly: true });
    try { return !!db.prepare('SELECT evidence_record_id FROM inbox_messages WHERE message_id = ?').get('fleet:msg:6')?.evidence_record_id; }
    finally { db.close(); }
  });

  let probe = await openClient();
  await until(async () => {
    const attempt = await probe.bind({ namespace: 'fleet', sessionId: a }, 'competing-runtime', Date.now());
    return !attempt.ok && attempt.error === 'owner_conflict';
  });
  await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
  session.dispose();
  probe = await openClient();
  assert.deepEqual((await probe.bind({ namespace: 'fleet', sessionId: a }, 'probe-runtime', Date.now())).ok, true);
  const pendingAtStop = await probe.pending({ namespace: 'fleet', sessionId: a }, 'probe-runtime');
  assert.ok(pendingAtStop.every(message => ['fleet:msg:2', 'fleet:msg:5'].includes(message.id)),
    'only a just-persisted message may still need its ACK');
  await probe.release({ namespace: 'fleet', sessionId: a }, 'probe-runtime');
  const resumedLoader = new pi.DefaultResourceLoader({ cwd: dir, agentDir: join(dir, 'agent'), settingsManager,
    additionalExtensionPaths: [extension], noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  await resumedLoader.reload();
  assert.deepEqual(resumedLoader.getExtensions().errors, []);
  const { session: resumed } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
    settingsManager, resourceLoader: resumedLoader, sessionManager: pi.SessionManager.open(file) });
  let resumedCalls = 0;
  resumed.agent.streamFunction = () => { resumedCalls++; throw new Error('Acknowledged message replayed on resume'); };
  t.after(async () => { await resumed.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' }); resumed.dispose(); });
  const registrations = calls.length;
  await resumed.bindExtensions({});
  await until(() => calls.length > registrations);
  await delay(600); // Two inbox poll cycles after binding: persisted IDs are ACKed, not resubmitted.
  assert.equal(resumedCalls, 0);
  await resumed.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
  probe = await openClient();
  assert.equal((await probe.bind({ namespace: 'fleet', sessionId: a }, 'probe-runtime', Date.now())).ok, true);
  assert.deepEqual(await probe.pending({ namespace: 'fleet', sessionId: a }, 'probe-runtime'), []);
  await probe.release({ namespace: 'fleet', sessionId: a }, 'probe-runtime');
  assert.equal((await entries(file)).filter(e => e.type === 'custom_message' && e.details?.deliveryId === 'fleet:msg:1').length, 1);

  // The old runtime can still have a cached bound flag between takeover and
  // its renewal tick. Its aborted turn must not start recovery on the loser.
  const { session: loser } = await pi.createAgentSession({ cwd: dir, agentDir: join(dir, 'agent'), model, modelRuntime,
    settingsManager, resourceLoader: resumedLoader, sessionManager: pi.SessionManager.open(file) });
  loser.agent.streamFunction = session.agent.streamFunction;
  t.after(async () => { release?.(); await loser.abort(); await loser.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' }); loser.dispose(); });
  await loser.bindExtensions({});
  await until(() => requests.length > 0 && !loser.isStreaming);
  const beforeTakeover = requests.length;
  hold = true;
  const losingWork = loser.prompt('Work before lease takeover');
  await until(() => requests.length > beforeTakeover && loser.isStreaming);
  await external.receive(message(7, a, 'Delivery belongs to new owner'));
  await until(() => loser.agent.peekQueuedMessages().some(m => m.details?.deliveryId === 'fleet:msg:7'));
  await loser.followUp('User work already queued');
  assert.equal((await entries(file)).filter(e => e.details?.deliveryId === 'fleet:msg:7').length, 0);
  await retireLiveLease();
  const owner = { namespace: 'fleet', sessionId: a };
  probe = await openClient();
  assert.equal((await probe.bind(owner, 'takeover-owner', Date.now())).ok, true);
  const wakeCount = (await entries(file)).filter(e => e.customType === 'roger-wilco-wake').length;
  const losingAbort = loser.abort();
  hold = false;
  release();
  await Promise.all([losingAbort, losingWork]);
  await delay(600);
  assert.equal(requests.length, beforeTakeover + 1, 'loser must not initiate a recovery model turn');
  assert.equal((await entries(file)).filter(e => e.customType === 'roger-wilco-wake').length, wakeCount);
  assert.equal((await entries(file)).filter(e => e.details?.deliveryId === 'fleet:msg:7').length, 0);
  assert.equal((await probe.pending(owner, 'takeover-owner')).filter(m => m.id === 'fleet:msg:7').length, 1);
  await loser.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' });
  loser.dispose();
  await probe.release(owner, 'takeover-owner');
});

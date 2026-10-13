import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { promises as fsPromises } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir, homedir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { readHostConfig } from '../../src/bin/config.ts';
import { createDaemonInbox as createHostInbox } from '../../src/host/index.ts';
import { listenLocal } from '../../src/host/ipc.ts';
import { createHostClient } from '../../src/host/client.ts';
import rogerWilco, { createPiDelivery, loadPiConfig, persistedReceipt } from '../../src/pi/index.ts';

async function serve(inboxPath) {
  const inbox = createHostInbox(inboxPath);
  const server = await listenLocal({ coordinatorUrl: 'http://127.0.0.1:1', hostId: 'host-a', credential: 'offline', inboxPath }, inbox);
  return { inbox, server };
}

test('a stalled startup bind has at most one scheduled retry', { timeout: 8000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-bind-retry-'));
  const inboxPath = join(dir, 'inbox.sqlite');
  const { inbox, server } = await serve(inboxPath);
  const configPath = join(dir, 'host.json');
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = configPath;
  await writeFile(configPath, JSON.stringify({ namespace: 'fleet', hostId: 'host-a',
    coordinatorUrl: 'http://127.0.0.1:1', credential: 'offline', inboxPath }));
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  await inbox.bind(session, 'existing-owner', Date.now());
  let enqueues = 0;
  const enqueue = inbox.enqueueRegistration.bind(inbox);
  inbox.enqueueRegistration = async registration => { enqueues++; await enqueue(registration); };
  const handlers = new Map();
  rogerWilco({ on: (name, handler) => handlers.set(name, handler),
    registerTool() {}, registerCommand() {}, sendMessage() {} });
  const ctx = { sessionManager: { getSessionId: () => session.piSessionId,
    getSessionFile: () => join(dir, 'session.jsonl') }, hasUI: false };
  let resume;
  const gate = new Promise(resolve => { resume = resolve; });
  try {
    await handlers.get('session_start')({}, ctx);
    assert.equal(enqueues, 1);
    await inbox.release(session, 'existing-owner');
    const bind = inbox.bind.bind(inbox);
    let entered;
    const waiting = new Promise(resolve => { entered = resolve; });
    inbox.bind = async (...args) => { entered(); await gate; return bind(...args); };
    await waiting;
    // Two more timer ticks occur while the real IPC request remains outstanding.
    await delay(2200);
    resume();
    await handlers.get('session_shutdown')({}, ctx);
    assert.equal(enqueues, 2, 'startup and one completed retry, without queued duplicate retries');
  } finally {
    resume();
    await handlers.get('session_shutdown')({}, ctx);
    await server.close();
    await inbox.close();
    if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG;
    else process.env.ROGER_WILCO_CONFIG = saved;
    await rm(dir, { recursive: true, force: true });
  }
});

function expireLease(path, key) {
  const db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA busy_timeout = 5000');
    assert.equal(db.prepare('UPDATE inbox_leases SET expires_at = ? WHERE session_key = ?').run(Date.now() - 1, key).changes, 1);
  } finally { db.close(); }
}

test('~/ host configuration gives Pi and host daemon the same durable inbox', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-config-'));
  const savedHome = process.env.HOME;
  const savedConfig = process.env.ROGER_WILCO_CONFIG;
  t.after(async () => {
    if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
    if (savedConfig === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = savedConfig;
    await rm(dir, { recursive: true, force: true });
  });
  process.env.HOME = dir;
  assert.equal(homedir(), dir);
  process.env.ROGER_WILCO_CONFIG = '~/.config/roger-wilco/host.json';
  const configFile = join(dir, '.config/roger-wilco/host.json');
  await mkdir(join(dir, '.config/roger-wilco'), { recursive: true });
  const config = { namespace: 'fleet', coordinatorUrl: 'http://127.0.0.1:1', hostId: 'host-a',
    credential: 'offline', inboxPath: '~/shared/host.sqlite' };
  await writeFile(configFile, JSON.stringify(config));
  const piConfig = await loadPiConfig();
  const daemonConfig = await readHostConfig();
  assert.equal(piConfig.inboxPath, join(dir, 'shared/host.sqlite'));
  assert.deepEqual(piConfig, daemonConfig);
  assert.deepEqual(await loadPiConfig(configFile), piConfig);
  delete process.env.ROGER_WILCO_CONFIG;
  assert.deepEqual(await loadPiConfig(), piConfig, 'default config path is resolved from HOME');

  const owner = randomUUID();
  const sender = randomUUID();
  const sent = [];
  const handlers = new Map();
  rogerWilco({ on: (name, handler) => handlers.set(name, handler), registerTool() {}, registerCommand() {},
    sendMessage: (message, options) => sent.push({ message, options }) });
  const ctx = { sessionManager: { getSessionId: () => owner, getSessionFile: () => join(dir, 'session.jsonl') }, hasUI: false };
  const inbox = createHostInbox(daemonConfig.inboxPath);
  const server = await listenLocal(daemonConfig, inbox);
  try {
    await handlers.get('session_start')({ reason: 'startup' }, ctx);
    const message = { version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${sender}`, recipient: `fleet:${owner}`, body: 'shared inbox', data: null, visibility: 'private' };
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${owner}`, message });
    for (let i = 0; i < 200 && !sent.length; i++) await delay(10);
    assert.equal(sent.length, 1, 'Pi received the event stored through the daemon inbox path');
    assert.equal(sent[0].message.details.deliveryId, message.id);
    assert.equal(sent[0].message.content, JSON.stringify(message), 'Roger sends metadata without a repeated warning prefix');
    assert.deepEqual(sent[0].options, { deliverAs: 'followUp', triggerTurn: true });
  } finally {
    await handlers.get('session_shutdown')({ reason: 'quit' }, ctx);
    await server.close();
    await inbox.close();
  }
});

test('a delayed JSONL read cannot submit after another runtime takes the lease', { timeout: 5000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-takeover-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const file = join(dir, 'session.jsonl');
  await writeFile(file, `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n`);
  const inboxPath = join(dir, 'inbox.sqlite');
  const { inbox, server } = await serve(inboxPath);
  const old = createHostClient(inboxPath);
  const fresh = createHostClient(inboxPath);
  const sent = [];
  const ctx = { sessionManager: { getSessionId: () => session.piSessionId, getSessionFile: () => file }, hasPendingMessages: () => false };
  const originalOpen = fsPromises.open;
  let enteredRead;
  const reading = new Promise(resolve => { enteredRead = resolve; });
  let releaseRead;
  fsPromises.open = async (...args) => {
    const handle = await originalOpen(...args);
    if (args[0] === file) {
      const originalRead = handle.readFile.bind(handle);
      handle.readFile = async (...readArgs) => {
        enteredRead();
        await new Promise(resolve => { releaseRead = resolve; });
        return originalRead(...readArgs);
      };
    }
    return handle;
  };
  syncBuiltinESMExports();
  const delivery = createPiDelivery({ sendMessage: message => sent.push(message) }, ctx, old, session, 'old-runtime');
  try {
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    const grant = old.authority();
    assert.equal(typeof grant, 'string');
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${session.piSessionId}`, message: {
      version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${randomUUID()}`, recipient: `fleet:${session.piSessionId}`,
      body: 'takeover', data: null, visibility: 'private',
    } });
    await reading;
    expireLease(inboxPath, `fleet:${session.piSessionId}`);
    assert.equal(old.authority(), grant);
    assert.equal((await fresh.bind(session, 'new-runtime', Date.now())).ok, true);
    assert.equal(old.authority(), grant);
    releaseRead();
    await delay(550);
    assert.deepEqual(sent, [], 'old runtime must not submit after the async file read');
    assert.equal(old.authority(), undefined);
    assert.equal((await fresh.pending(session, 'new-runtime')).length, 1);
  } finally {
    releaseRead?.();
    fsPromises.open = originalOpen;
    syncBuiltinESMExports();
    await delivery.stop();
    await old.close();
    await fresh.close();
    await server.close();
    await inbox.close();
  }
});

test('a rebound runtime does not wake an ID acknowledged by the takeover owner', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-stale-wake-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const file = join(dir, 'old.jsonl');
  const newFile = join(dir, 'new.jsonl');
  await writeFile(file, `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n`);
  const inboxPath = join(dir, 'inbox.sqlite');
  const { inbox, server } = await serve(inboxPath);
  const old = createHostClient(inboxPath);
  const fresh = createHostClient(inboxPath);
  const sent = [];
  const ctx = { sessionManager: { getSessionId: () => session.piSessionId, getSessionFile: () => file } };
  const delivery = createPiDelivery({ sendMessage: message => sent.push(message) }, ctx, old, session, 'old-runtime');
  const key = `fleet:${session.piSessionId}`;
  try {
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    const grant = old.authority();
    assert.equal(typeof grant, 'string');
    const message = { version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${randomUUID()}`, recipient: `fleet:${session.piSessionId}`,
      body: 'acknowledged during takeover', data: null, visibility: 'private' };
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${session.piSessionId}`, message });
    for (let i = 0; i < 100 && !sent.length; i++) await delay(10);
    assert.equal(sent.filter(m => m.details?.deliveryId === message.id).length, 1);
    expireLease(inboxPath, key);
    assert.equal(old.authority(), grant);
    assert.equal((await fresh.bind(session, 'new-runtime', Date.now())).ok, true);
    assert.equal(old.authority(), grant);
    const lost = await old.renew(session, 'old-runtime', Date.now());
    assert.equal(lost.ok, false);
    for (let i = 0; i < 50 && old.authority() === grant; i++) await delay(10);
    assert.equal(old.authority(), undefined);
    await delivery.resumeAfterAbort();
    const entry = { type: 'custom_message', customType: 'roger-wilco', id: randomUUID(), details: { deliveryId: message.id } };
    await writeFile(newFile, `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n${JSON.stringify(entry)}\n`);
    const proof = await persistedReceipt(newFile, session, message.id);
    assert.ok(proof);
    assert.equal((await fresh.acknowledgePi(session, 'new-runtime', proof)).ok, true);
    await fresh.release(session, 'new-runtime');
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    const rebound = old.authority();
    assert.notEqual(rebound, grant);
    const db = new DatabaseSync(inboxPath);
    try {
      db.exec('PRAGMA busy_timeout = 5000');
      assert.equal(db.prepare('SELECT runtime_id FROM inbox_leases WHERE session_key = ?').get(key).runtime_id, rebound);
    } finally { db.close(); }
    await delivery.resumeAfterAbort();
    assert.equal(sent.filter(m => m.customType === 'roger-wilco-wake').length, 0);
    assert.equal(sent.filter(m => m.details?.deliveryId === message.id).length, 1);
  } finally {
    await delivery.stop();
    await old.close();
    await fresh.close();
    await server.close();
    await inbox.close();
  }
});

test('Pi and host share trust validation; Pi additionally requires a namespace', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-config-invalid-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configFile = join(dir, 'host.json');
  const base = { namespace: 'fleet', hostId: 'host-a', credential: 'offline', inboxPath: join(dir, 'host.sqlite') };
  for (const change of [
    { coordinatorUrl: 'http://example.com' }, { coordinatorUrl: 'http://user:password@localhost' },
    { coordinatorUrl: 'https://example.com/?query=1' }, { coordinatorUrl: 'https://example.com/#fragment' },
    { inboxPath: './host.sqlite' }, { namespace: 'Invalid' }, { namespace: null },
    { hostId: '' }, { credential: ' ' }, { callbackUrl: 'https://callback.example' },
  ]) {
    await writeFile(configFile, JSON.stringify({ ...base, coordinatorUrl: 'https://example.com', ...change }));
    await assert.rejects(loadPiConfig(configFile));
    await assert.rejects(readHostConfig(configFile));
  }
  await writeFile(configFile, JSON.stringify({ ...base, coordinatorUrl: 'https://example.com', namespace: undefined }));
  assert.equal((await readHostConfig(configFile)).hostId, 'host-a', 'daemon does not require a namespace');
  await assert.rejects(loadPiConfig(configFile), /namespace/);
});

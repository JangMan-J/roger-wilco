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
import { createHostInbox } from '../../src/host/index.ts';
import rogerWilco, { createPiDelivery, loadPiConfig, persistedReceipt } from '../../src/pi/index.ts';

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
  assert.equal(piConfig.inboxPath, daemonConfig.inboxPath);

  const owner = randomUUID();
  const sender = randomUUID();
  const sent = [];
  const handlers = new Map();
  rogerWilco({ on: (name, handler) => handlers.set(name, handler), registerTool() {}, registerCommand() {},
    sendMessage: (message, options) => sent.push({ message, options }) });
  const ctx = { sessionManager: { getSessionId: () => owner, getSessionFile: () => join(dir, 'session.jsonl') }, hasUI: false };
  await handlers.get('session_start')({ reason: 'startup' }, ctx);
  const inbox = createHostInbox(daemonConfig.inboxPath);
  try {
    const message = { version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${sender}`, recipient: `fleet:${owner}`, body: 'shared inbox', data: null, visibility: 'private' };
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${owner}`, message });
    for (let i = 0; i < 200 && !sent.length; i++) await delay(10);
    assert.equal(sent.length, 1, 'Pi received the event stored through the daemon inbox path');
    assert.equal(sent[0].message.details.deliveryId, message.id);
    assert.deepEqual(sent[0].options, { deliverAs: 'followUp', triggerTurn: true });
  } finally {
    await handlers.get('session_shutdown')({ reason: 'quit' }, ctx);
    await inbox.close();
  }
});

test('a delayed JSONL read cannot submit after another runtime takes the lease', { timeout: 5000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-takeover-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const file = join(dir, 'session.jsonl');
  await writeFile(file, `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n`);
  const inbox = createHostInbox(join(dir, 'inbox.sqlite'));
  const other = createHostInbox(join(dir, 'inbox.sqlite'));
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
  const delivery = createPiDelivery({ sendMessage: message => sent.push(message) }, ctx, inbox, session, 'old-runtime');
  try {
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${session.piSessionId}`, message: {
      version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${randomUUID()}`, recipient: `fleet:${session.piSessionId}`,
      body: 'takeover', data: null, visibility: 'private',
    } });
    await reading;
    const db = new DatabaseSync(join(dir, 'inbox.sqlite'));
    try { db.prepare('UPDATE leases SET expires_at = ? WHERE session_key = ?').run(Date.now() - 1, `fleet:${session.piSessionId}`); }
    finally { db.close(); }
    assert.equal((await other.bind(session, 'new-runtime', Date.now())).ok, true);
    releaseRead();
    await delay(550);
    assert.deepEqual(sent, [], 'old runtime must not submit after the async file read');
    assert.equal((await other.pending(session, 'new-runtime')).length, 1);
  } finally {
    releaseRead?.();
    fsPromises.open = originalOpen;
    syncBuiltinESMExports();
    await delivery.stop();
    await inbox.close();
    await other.close();
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
  const inbox = createHostInbox(inboxPath);
  const other = createHostInbox(inboxPath);
  const sent = [];
  const ctx = { sessionManager: { getSessionId: () => session.piSessionId, getSessionFile: () => file } };
  const delivery = createPiDelivery({ sendMessage: message => sent.push(message) }, ctx, inbox, session, 'old-runtime');
  try {
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    const message = { version: 1, id: 'fleet:msg:1', sequence: 1, timestamp: Date.now(),
      sender: `fleet:${randomUUID()}`, recipient: `fleet:${session.piSessionId}`,
      body: 'acknowledged during takeover', data: null, visibility: 'private' };
    await inbox.receive({ type: 'message', subscriptionId: `fleet:${session.piSessionId}`, message });
    for (let i = 0; i < 100 && !sent.length; i++) await delay(10);
    assert.equal(sent.filter(m => m.details?.deliveryId === message.id).length, 1);
    const db = new DatabaseSync(inboxPath);
    try { db.prepare('UPDATE leases SET expires_at = ? WHERE session_key = ?')
      .run(Date.now() - 1, `fleet:${session.piSessionId}`); }
    finally { db.close(); }
    assert.equal((await other.bind(session, 'new-runtime', Date.now())).ok, true);
    await delivery.resumeAfterAbort(); // This runtime notices its lost lease.
    const entry = { type: 'custom_message', customType: 'roger-wilco', id: randomUUID(), details: { deliveryId: message.id } };
    await writeFile(newFile, `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n${JSON.stringify(entry)}\n`);
    const proof = await persistedReceipt(newFile, session, message.id);
    assert.ok(proof);
    assert.equal((await other.acknowledgePi(session, 'new-runtime', proof)).ok, true);
    await other.release(session, 'new-runtime');
    assert.equal((await delivery.bind(session, 'old-runtime')).ok, true);
    await delivery.resumeAfterAbort();
    assert.equal(sent.filter(m => m.customType === 'roger-wilco-wake').length, 0);
    assert.equal(sent.filter(m => m.details?.deliveryId === message.id).length, 1);
  } finally {
    await delivery.stop();
    await inbox.close();
    await other.close();
  }
});

test('Pi applies the host HTTPS and absolute/home-relative inbox requirements', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-config-invalid-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configFile = join(dir, 'host.json');
  const base = { namespace: 'fleet', hostId: 'host-a', credential: 'offline', inboxPath: join(dir, 'host.sqlite') };
  for (const coordinatorUrl of ['http://example.com', 'http://user:password@localhost', 'https://example.com/?query=1']) {
    await writeFile(configFile, JSON.stringify({ ...base, coordinatorUrl }));
    await assert.rejects(loadPiConfig(configFile), /HTTPS|coordinator/);
  }
  await writeFile(configFile, JSON.stringify({ ...base, coordinatorUrl: 'https://example.com', inboxPath: './host.sqlite' }));
  await assert.rejects(loadPiConfig(configFile), /inboxPath/);
});

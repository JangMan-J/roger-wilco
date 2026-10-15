import assert from 'node:assert/strict';
import test from 'node:test';
import { createServer } from 'node:http';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createDaemonInbox as createHostInbox, createHostDaemon, type LocalHostInbox } from '../../src/host/index.ts';
import { ROUTES, sessionKey, type SessionAddress } from '../../src/protocol/index.ts';
import type { PiConfig } from '../../src/pi/config.ts';
import { nativePi, nativePiRuntime, persistedMessages } from './native-pi.ts';

async function until(check: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 7000;
  while (!await check()) { assert.ok(Date.now() < deadline, reason); await delay(20); }
}

/** Actual coordinator, HTTP and two independently configured host daemons using separate SQLite files. */
async function rig(t: test.TestContext, fault: { dropHostAReceipts?: boolean } = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-native-'));
  const store = createCoordinatorStore(join(dir, 'server.db'), 'fleet');
  const auth = createHostAuthenticator({ 'host-a': 'token-a', 'host-b': 'token-b' });
  const control = createControlHandler(store, auth);
  const stream = createStreamHandler(store, auth);
  let receiptAttempts = 0;
  const server = createServer((req, res) => {
    const path = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (path === ROUTES.receipt && req.headers.authorization === 'Bearer token-a' && fault.dropHostAReceipts) {
      receiptAttempts++;
      req.socket.destroy(); // transport loss after host inbox commit, before coordinator receipt
      return;
    }
    void (path === ROUTES.stream || path === ROUTES.receipt ? stream : control)(req, res);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}`;
  const hosts: { config: PiConfig; inbox: LocalHostInbox; daemon: ReturnType<typeof createHostDaemon>; running: Promise<void> }[] = [];
  for (const [hostId, credential] of [['host-a', 'token-a'], ['host-b', 'token-b']]) {
    const path = join(dir, hostId, 'inbox.db');
    const config = { coordinatorUrl: url, hostId, credential, inboxPath: path, namespace: 'fleet' };
    const inbox = createHostInbox(path);
    const daemon = createHostDaemon(config, inbox);
    const running = daemon.start(new AbortController().signal);
    void running.catch(() => {});
    hosts.push({ config, inbox, daemon, running });
  }
  const previousConfig = process.env.ROGER_WILCO_CONFIG;
  const configFile = join(dir, 'host-config.json');
  await writeFile(configFile, JSON.stringify(hosts[0]!.config));
  process.env.ROGER_WILCO_CONFIG = configFile;
  t.after(async () => {
    if (previousConfig === undefined) delete process.env.ROGER_WILCO_CONFIG;
    else process.env.ROGER_WILCO_CONFIG = previousConfig;
    for (const host of hosts) { await host.daemon.stop(); await host.running; await host.inbox.close(); }
    server.closeAllConnections();
    await new Promise<void>((done, reject) => server.close(error => error ? reject(error) : done()));
    store.close();
    await rm(dir, { recursive: true, force: true });
  });
  const send = async (sender: SessionAddress, recipient: string, body: string, host = 'host-b') => {
    const response = await fetch(`${url}${ROUTES.messages}`, {
      method: 'POST', headers: { Authorization: `Bearer token-${host.slice(-1)}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sender, message: { recipient, body } }),
    });
    assert.equal(response.status, 200);
    const result = await response.json();
    assert.equal(result.ok, true);
    return result.value as { id: string };
  };
  return { dir, hosts, send, store, receiptAttempts: () => receiptAttempts };
}

test('native Pi idle and busy delivery isolates two sessions on one host, host B sends, closed owner resumes', async t => {
  const h = await rig(t);
  const alice = await nativePi(t, h.dir);
  const bob = await nativePi(t, h.dir);
  const remote = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440099' };
  await h.hosts[1]!.inbox.enqueueRegistration({ session: remote });
  const address = (id: string) => ({ namespace: 'fleet', sessionId: id });
  await until(() => h.store.listSessions().length === 3, 'automatic startup and second-host registration');
  assert.notEqual(alice.id, bob.id);
  const first = await h.send(remote, sessionKey(address(alice.id)), 'Alice only');
  await until(async () => (await persistedMessages(alice.file)).some(m => m.details?.deliveryId === first.id), 'idle Pi JSONL persistence');
  await until(() => alice.calls.length > 0, 'idle Pi model turn');
  assert.equal((await persistedMessages(bob.file)).length, 0, 'other session is not woken');
  bob.holdNext();
  const active = bob.session.prompt('Original user work');
  await until(() => bob.calls.length === 1, 'Pi active user turn');
  const busy = await h.send(remote, sessionKey(address(bob.id)), 'Queued for Bob');
  await until(async () => (await h.hosts[0]!.inbox.bind(address(bob.id), 'intruder', Date.now())).ok === false, 'live owner protected');
  assert.equal(bob.calls.length, 1, 'new message did not interrupt active model');
  bob.release(); await active;
  await until(async () => (await persistedMessages(bob.file)).some(m => m.details?.deliveryId === busy.id) && bob.calls.length === 2, 'busy follow-up persisted and runs');
  await bob.stop();
  const closed = await h.send(remote, sessionKey(address(bob.id)), 'Retained while Pi closed');
  await until(async () => {
    const lease = await h.hosts[0]!.inbox.bind(address(bob.id), 'read-only-check', Date.now());
    if (!lease.ok) return false;
    const pending = await h.hosts[0]!.inbox.pending(address(bob.id), 'read-only-check');
    await h.hosts[0]!.inbox.release(address(bob.id), 'read-only-check');
    return pending.some(message => message.id === closed.id);
  }, 'closed Pi durable host inbox');
  const resumed = await nativePi(t, h.dir, { sessionFile: bob.file });
  assert.equal(resumed.id, bob.id);
  await until(async () => (await persistedMessages(resumed.file)).some(m => m.details?.deliveryId === closed.id), 'resume drains pending inbox');
  assert.equal((await persistedMessages(resumed.file)).filter(m => m.details?.deliveryId === busy.id).length, 1, 'acknowledged message not replayed');
  assert.equal((await persistedMessages(alice.file)).some(m => m.details?.deliveryId === closed.id), false);
});

test('Pi starts despite unavailable coordinator URL; queued registration is retried by the host daemon', async t => {
  const h = await rig(t);
  await writeFile(process.env.ROGER_WILCO_CONFIG!, JSON.stringify({
    ...h.hosts[0]!.config, coordinatorUrl: 'http://127.0.0.1:1',
  }));
  const session = await nativePi(t, h.dir);
  assert.match(session.id, /^[0-9a-f-]{36}$/);
  await until(() => h.store.listSessions().some(row => row.subscription.id === sessionKey({ namespace: 'fleet', sessionId: session.id })),
    'durable registration queued by Pi is retried by connected host daemon');
});

test('host restart after local commit but lost receipt replays one durable inbox event', async t => {
  const fault = { dropHostAReceipts: true };
  const h = await rig(t, fault);
  const recipient = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440088' };
  const sender = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440099' };
  await h.hosts[0]!.inbox.enqueueRegistration({ session: recipient });
  await h.hosts[1]!.inbox.enqueueRegistration({ session: sender });
  await until(() => h.store.listSessions().length === 2, 'both hosts registered');
  const message = await h.send(sender, sessionKey(recipient), 'replayed after receipt failure');
  await until(() => h.receiptAttempts() > 0, 'receipt failed after local persistence');
  assert.equal(h.store.listSessions().find(item => item.subscription.id === sessionKey(recipient))?.subscription.cursor, 0);
  const host = h.hosts[0]!;
  await host.daemon.stop(); await host.running; await host.inbox.close();
  host.inbox = createHostInbox(host.config.inboxPath);
  const lease = await host.inbox.bind(recipient, 'inspector', Date.now());
  assert.equal(lease.ok, true);
  assert.deepEqual((await host.inbox.pending(recipient, 'inspector')).map(item => item.id), [message.id]);
  await host.inbox.release(recipient, 'inspector');
  fault.dropHostAReceipts = false;
  host.daemon = createHostDaemon(host.config, host.inbox);
  host.running = host.daemon.start(new AbortController().signal);
  void host.running.catch(() => {});
  await until(() => h.store.listSessions().find(item => item.subscription.id === sessionKey(recipient))?.subscription.cursor === 1,
    'coordinator advances after replayed receipt');
  const owner = await host.inbox.bind(recipient, 'inspector', Date.now());
  assert.equal(owner.ok, true);
  assert.deepEqual((await host.inbox.pending(recipient, 'inspector')).map(item => item.id), [message.id], 'one event after replay');
  await host.inbox.release(recipient, 'inspector');
});

test('native Pi loads pi-webtasks beside Roger-Wilco: research dispatch remains, new messages have one consumer', async t => {
  const h = await rig(t);
  const sibling = resolve('..', 'pi-webtasks');
  const researchPackage = resolve(process.env.WEBTASKS_PACKAGE_DIR ??
    (existsSync(sibling) ? sibling : join(homedir(), '.pi/agent/git/github.com/JangMan-J/pi-webtasks')));
  const installed = resolve(researchPackage, 'src/index.ts');
  const previousWebtasks = process.env.WEBTASKS_DIR;
  const previousKey = process.env._PARALLEL_API_KEY;
  const realFetch = globalThis.fetch;
  const researchDir = join(h.dir, 'research');
  process.env.WEBTASKS_DIR = researchDir;
  process.env._PARALLEL_API_KEY = 'offline-test-key';
  const providerCalls: { path: string; body: any }[] = [];
  globalThis.fetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    if (url.origin === 'https://api.parallel.ai') {
      providerCalls.push({ path: url.pathname, body: JSON.parse(await request.text()) });
      assert.equal(url.pathname, '/v1/search', 'only offline synthetic search uses the provider boundary');
      return new Response(JSON.stringify({ search_id: 'synthetic-search-1', results: [
        { url: 'https://example.org/evidence', title: 'Offline source', excerpts: ['Offline synthetic research answer'], full_content: null },
      ] }), { headers: { 'Content-Type': 'application/json' } });
    }
    assert.equal(url.hostname, '127.0.0.1', 'no external provider network calls');
    return realFetch(input, init);
  };
  t.after(() => {
    globalThis.fetch = realFetch;
    if (previousWebtasks === undefined) delete process.env.WEBTASKS_DIR; else process.env.WEBTASKS_DIR = previousWebtasks;
    if (previousKey === undefined) delete process.env._PARALLEL_API_KEY; else process.env._PARALLEL_API_KEY = previousKey;
  });
  const { WebhookInbox } = await import(pathToFileURL(resolve(researchPackage, 'src/webhook-inbox.ts')).href);
  const researchStore = new WebhookInbox(researchDir);
  const referenceId = 'research_550e8400-e29b-41d4-a716-446655440099';
  const current = await nativePi(t, h.dir, { extensions: [resolve('src/pi/index.ts'), installed] });
  const now = Date.now();
  await researchStore.createResearch({ reference: referenceId, session_id: current.id, group_id: 'offline',
    query: 'Previously recorded research', effort: 'low', status: 'failed', artifact_dir: join(h.dir, 'artifacts'),
    created_at: now, updated_at: now });
  t.after(() => researchStore.close());
  assert.ok(current.session.extensionRunner.getToolDefinition('deep_search'), 'real research extension registered');
  assert.ok(current.session.extensionRunner.getToolDefinition('roger_send'), 'Roger-Wilco registered');
  const researchTool = (current.session as any)._toolRegistry.get('deep_search');
  assert.ok(researchTool, 'research tool wired into native Pi');
  const lookup = await researchTool.execute('read-saved', { research: referenceId });
  assert.match(JSON.stringify(lookup), /Previously recorded research/);
  assert.match(JSON.stringify(lookup), /failed/, 'native research tool reads actual pi-webtasks storage');
  const searchTool = (current.session as any)._toolRegistry.get('web_search');
  assert.ok(searchTool, 'real research search tool wired into native Pi');
  const searched = await searchTool.execute('offline-search', { query: 'synthetic evidence' });
  assert.match(JSON.stringify(searched), /Offline synthetic research answer/);
  assert.deepEqual(providerCalls.map(call => [call.path, call.body.search_queries]), [['/v1/search', ['synthetic evidence']]],
    'research submission reached the synthetic HTTP provider exactly once');
  assert.match(await readFile(searched.details.artifact, 'utf8'), /Offline synthetic research answer/);
  const remote = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440099' };
  await h.hosts[1]!.inbox.enqueueRegistration({ session: remote });
  await until(() => h.store.listSessions().length === 2, 'both hosts registered');
  const message = await h.send(remote, sessionKey({ namespace: 'fleet', sessionId: current.id }), 'New path only');
  await until(async () => (await persistedMessages(current.file)).some(m => m.details?.deliveryId === message.id), 'Roger-Wilco persistent delivery');
  assert.equal((await persistedMessages(current.file, 'agent-bus')).length, 0, 'legacy bus does not duplicate Roger-Wilco message');
});

test('real Pi fork and switch release the old binding and deliver only each session’s pending inbox', async t => {
  const h = await rig(t);
  const { runtime, calls } = await nativePiRuntime(t, h.dir);
  const originalId = runtime.session.sessionId as string;
  const originalFile = runtime.session.sessionFile as string;
  await runtime.session.prompt('Establish an entry to fork from');
  const forkPoint = runtime.session.sessionManager.getEntries().find((entry: any) => entry.type === 'message' && entry.message.role === 'user');
  assert.ok(forkPoint?.id, 'fork uses an actual persisted user entry');
  const remote = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440099' };
  await h.hosts[1]!.inbox.enqueueRegistration({ session: remote });
  await until(() => h.store.listSessions().length === 2, 'source and remote registered');
  assert.deepEqual(await runtime.fork(forkPoint.id, { position: 'at' }), { cancelled: false, selectedText: undefined });
  const forkId = runtime.session.sessionId as string;
  const forkFile = runtime.session.sessionFile as string;
  assert.notEqual(forkId, originalId);
  assert.notEqual(forkFile, originalFile);
  await until(() => h.store.listSessions().length === 3, 'fork registered automatically');
  const oldAddress = { namespace: 'fleet', sessionId: originalId };
  const forkAddress = { namespace: 'fleet', sessionId: forkId };
  const forOld = await h.send(remote, sessionKey(oldAddress), 'pending for switched-out original');
  const forFork = await h.send(remote, sessionKey(forkAddress), 'fork only');
  await until(async () => (await persistedMessages(forkFile)).some(m => m.details?.deliveryId === forFork.id), 'fork wakes with its own message');
  assert.equal((await persistedMessages(forkFile)).some(m => m.details?.deliveryId === forOld.id), false);
  assert.equal((await persistedMessages(originalFile)).some(m => m.details?.deliveryId === forOld.id), false);
  const oldLease = await h.hosts[0]!.inbox.bind(oldAddress, 'inspector', Date.now());
  assert.equal(oldLease.ok, true, 'actual fork released the original Pi runtime lease');
  await until(async () => (await h.hosts[0]!.inbox.pending(oldAddress, 'inspector')).some(m => m.id === forOld.id), 'old message stored while fork active');
  await h.hosts[0]!.inbox.release(oldAddress, 'inspector');
  assert.deepEqual(await runtime.switchSession(originalFile), { cancelled: false });
  assert.equal(runtime.session.sessionId, originalId);
  await until(async () => (await persistedMessages(originalFile)).some(m => m.details?.deliveryId === forOld.id), 'switch resumes original pending message');
  const laterFork = await h.send(remote, sessionKey(forkAddress), 'pending for switched-out fork');
  const callsBefore = calls.get(originalId)!.length;
  await until(async () => {
    const lease = await h.hosts[0]!.inbox.bind(forkAddress, 'inspector', Date.now());
    if (!lease.ok) return false;
    try { return (await h.hosts[0]!.inbox.pending(forkAddress, 'inspector')).some(m => m.id === laterFork.id); }
    finally { await h.hosts[0]!.inbox.release(forkAddress, 'inspector'); }
  }, 'actual switch releases fork and keeps its pending event');
  assert.equal(calls.get(originalId)!.length, callsBefore, 'fork-only message did not wake the original');
  await runtime.switchSession(forkFile);
  assert.equal(runtime.session.sessionId, forkId);
  await until(async () => (await persistedMessages(forkFile)).some(m => m.details?.deliveryId === laterFork.id), 'switch resumes fork pending message');
  assert.equal((await persistedMessages(originalFile)).some(m => m.details?.deliveryId === laterFork.id), false);
});

test('native Pi duplicate live opening conflicts; distinct new session never consumes old pending inbox', async t => {
  const h = await rig(t);
  const original = await nativePi(t, h.dir);
  await original.session.prompt('Persist session before reopening it');
  await until(async () => (await persistedMessages(original.file)).length >= 0 && h.store.listSessions().length === 1, 'persistent original session registered');
  const duplicate = await nativePi(t, h.dir, { sessionFile: original.file });
  assert.equal(duplicate.id, original.id);
  const address = { namespace: 'fleet', sessionId: original.id };
  await until(() => h.store.listSessions().length === 1, 'original registered');
  const pending = await h.hosts[0]!.inbox.bind(address, 'intruder', Date.now());
  assert.deepEqual(pending.ok ? pending : { error: pending.error }, { error: 'owner_conflict' });
  const remote = { namespace: 'fleet', sessionId: '550e8400-e29b-41d4-a716-446655440099' };
  await h.hosts[1]!.inbox.enqueueRegistration({ session: remote });
  await until(() => h.store.listSessions().length === 2, 'remote registered');
  const owned = await h.send(remote, sessionKey(address), 'one active owner');
  await until(async () => (await persistedMessages(original.file)).some(m => m.details?.deliveryId === owned.id), 'original runtime delivered');
  assert.equal(duplicate.calls.length, 0, 'conflicting runtime never wakes');
  await duplicate.stop(); await original.stop();
  const closed = await h.send(remote, sessionKey(address), 'old address only');
  const fresh = await nativePi(t, h.dir);
  assert.notEqual(fresh.id, original.id, 'new/forked session must have a new address');
  await until(() => h.store.listSessions().length === 3, 'new session registered');
  assert.equal((await persistedMessages(fresh.file)).length, 0, 'new session never consumes old pending ID');
  const resumed = await nativePi(t, h.dir, { sessionFile: original.file });
  assert.equal(resumed.id, original.id);
  await until(async () => (await persistedMessages(resumed.file)).some(m => m.details?.deliveryId === closed.id), 'old pending ID follows only the resumed owner');
  assert.equal((await persistedMessages(fresh.file)).length, 0);
});

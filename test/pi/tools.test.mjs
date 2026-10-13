import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import rogerWilco from '../../src/pi/index.ts';
import { createHostInbox } from '../../src/host/index.ts';
import { listenLocal } from '../../src/host/ipc.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { nativePi } from '../acceptance/native-pi.ts';
import { entityReference } from '../../src/protocol/index.ts';

function extension() {
  const handlers = new Map(), tools = new Map();
  rogerWilco({ on: (event, handler) => handlers.set(event, handler), registerTool: tool => tools.set(tool.name, tool),
    registerCommand() {},
    sendMessage() { throw new Error('No Pi delivery expected'); } });
  return { handlers, tools };
}
function context(sessionId, dir) {
  const state = { sessionId, status: undefined };
  return { state, ctx: { sessionManager: { getSessionId: () => state.sessionId, getSessionFile: () => join(dir, `${state.sessionId}.jsonl`) },
    hasUI: true, ui: { setStatus: (_key, value) => { state.status = value; } } } };
}
async function hostSocket(t, config) {
  const inbox = createHostInbox(config.inboxPath);
  const ipc = await listenLocal(config, inbox);
  t.after(async () => { await ipc.close(); await inbox.close(); });
}

test('session-bound tools use the invoking UUID; board includes sessions and claims; duplicate runtime conflicts', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-tools-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = join(dir, 'host.json');
  t.after(() => { if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = saved; });
  const id = randomUUID();
  const now = Date.now();
  t.mock.method(Date, 'now', () => now);
  const recent = { subscription: { id: `fleet:${id}` }, rank: 1, lastSeenAt: now - 90_000 };
  const stale = { subscription: { id: `fleet:${randomUUID()}` }, rank: 3, lastSeenAt: now - 90_001 };
  const unknown = { subscription: { id: `fleet:${randomUUID()}` }, rank: 1 };
  const typed = entityReference('fleet', 'fix-parser', 255);
  const mention = { version: 1, id: 'fleet:msg:3', sequence: 3, timestamp: now, sender: `fleet:${id}`,
    recipient: '*', body: 'Review [[fix-parser]] and [[255:fix-parser]]', data: null, visibility: 'private' };
  const requests = [];
  const server = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : undefined;
    requests.push({ path: req.url, body, authorization: req.headers.authorization });
    res.setHeader('Content-Type', 'application/json');
    const url = new URL(req.url, 'http://localhost');
    const value = req.url === '/v1/sessions' ? [recent, stale, unknown] :
      req.url === '/v1/claims' ? [{ key: 'work', owner: `fleet:${id}` }] :
      url.pathname === '/v1/entities' ? { entity: typed, messages: [mention] } : { accepted: true };
    res.end(JSON.stringify({ ok: true, value }));
  });
  server.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const hostConfig = { namespace: 'fleet', hostId: 'host-a',
    credential: 'offline-secret', coordinatorUrl: `http://127.0.0.1:${server.address().port}`, inboxPath: join(dir, 'host.sqlite') };
  await writeFile(process.env.ROGER_WILCO_CONFIG, JSON.stringify(hostConfig));
  await hostSocket(t, hostConfig);
  const first = extension();
  const { state, ctx } = context(id, dir);
  await first.handlers.get('session_start')({}, ctx);
  const second = extension();
  const other = context(id, dir);
  await second.handlers.get('session_start')({}, other.ctx);
  assert.match(other.state.status, /owner_conflict/);
  assert.deepEqual((await first.tools.get('roger_board').execute('', {}, undefined, undefined, ctx)).details,
    { ok: true, value: { sessions: [recent], claims: [{ key: 'work', owner: `fleet:${id}` }] } });
  assert.deepEqual((await first.tools.get('roger_board').execute('', { all: true }, undefined, undefined, ctx)).details,
    { ok: true, value: { sessions: [recent, stale, unknown], claims: [{ key: 'work', owner: `fleet:${id}` }] } });
  const tool = async (name, params) => (await first.tools.get(name).execute('', params, undefined, undefined, ctx)).details;
  await tool('roger_identity', { displayName: '[[0:FLEET]]' });
  assert.deepEqual(requests.at(-1).body, { session: { namespace: 'fleet', piSessionId: id }, displayName: '[[0:FLEET]]' });
  assert.equal(requests.at(-1).path, '/v1/sessions/register');
  await tool('roger_identity', {});
  assert.deepEqual(requests.at(-1).body, { session: { namespace: 'fleet', piSessionId: id } });
  await tool('roger_send', { recipient: '*', body: 'Hello', data: { priority: 2 }, visibility: 'public' });
  await tool('roger_inbox', { since: 2, limit: 20 });
  await tool('roger_claim', { key: 'work', ttlSeconds: 90 });
  await tool('roger_release', { key: 'work' });
  for (const [params, entity, since, limit] of [
    [{ label: ' fix-parser ' }, typed, 0, 100],
    [{ label: 'fix-parser', type: 255, since: 2, limit: 1 }, typed, 2, 1],
    [{ id: typed.id }, typed, 0, 100],
  ]) {
    assert.deepEqual(await tool('roger_entity', params), { ok: true, value: { entity, messages: [mention] } });
    assert.equal(requests.at(-1).path, '/v1/entities?' + new URLSearchParams({ id: entity.id, since: String(since), limit: String(limit) }));
    assert.deepEqual(requests.at(-2).body, { session: { namespace: 'fleet', piSessionId: id } }, 'entity lookup registers the bound session first');
    assert.equal(requests.at(-2).path, '/v1/sessions/register');
  }
  const beforeInvalid = requests.length;
  for (const params of [{}, { label: 'fix-parser', id: typed.id }, { id: typed.id, type: 0 }, { id: 'invalid' },
    { label: '' }, { label: 'fix-parser', type: 256 }, { label: 'fix-parser', type: -1 }, { label: 'fix-parser', type: 1.5 },
    { id: typed.id, since: -1 }, { id: typed.id, since: Number.MAX_SAFE_INTEGER + 1 }, { id: typed.id, limit: 0 }, { id: typed.id, limit: 501 }]) {
    assert.equal((await tool('roger_entity', params)).error, 'invalid_request', JSON.stringify(params));
  }
  assert.equal(requests.length, beforeInvalid, 'invalid entity tool arguments never reach the coordinator');
  assert.deepEqual(requests.find(r => r.path === '/v1/messages').body.sender, { namespace: 'fleet', piSessionId: id });
  assert.deepEqual(requests.find(r => r.path === '/v1/messages').body.message.data, { priority: 2 });
  assert.equal(requests.find(r => r.path === '/v1/inbox?namespace=fleet&piSessionId=' + id + '&since=2&limit=20').path.includes(id), true);
  assert.deepEqual(requests.find(r => r.path === '/v1/claims' && r.body).body.owner, { namespace: 'fleet', piSessionId: id });
  assert.deepEqual(requests.find(r => r.path === '/v1/claims/release').body.owner, { namespace: 'fleet', piSessionId: id });
  assert.ok(requests.every(r => r.authorization === 'Bearer offline-secret'));
  const before = requests.length;
  const newId = randomUUID();
  state.sessionId = newId;
  assert.equal((await tool('roger_send', { recipient: '*', body: 'Stale runtime' })).ok, false);
  assert.equal((await tool('roger_entity', { label: 'fix-parser' })).ok, false);
  assert.equal((await tool('roger_identity', { displayName: '[[99:FLEET]]' })).ok, false);
  assert.equal(requests.length, before, 'old binding cannot act for a new session');
  await first.handlers.get('session_shutdown')({}, ctx);
  await first.handlers.get('session_start')({ reason: 'fork' }, ctx);
  assert.equal((await tool('roger_send', { recipient: '*', body: 'New session' })).ok, true);
  assert.deepEqual(requests.find(r => r.path === '/v1/messages' && r.body.message.body === 'New session').body.sender,
    { namespace: 'fleet', piSessionId: newId });
  await second.handlers.get('session_start')({ reason: 'resume' }, other.ctx);
  assert.equal(other.state.status, undefined, 'switch released the old lease; its original session may resume');
  await first.handlers.get('session_shutdown')({}, ctx);
  await second.handlers.get('session_shutdown')({}, other.ctx);
  const ephemeral = extension();
  const noFile = context(randomUUID(), dir);
  noFile.ctx.sessionManager.getSessionFile = () => undefined;
  await ephemeral.handlers.get('session_start')({}, noFile.ctx);
  assert.match(noFile.state.status, /persistent Pi session manager/);
  assert.equal((await ephemeral.tools.get('roger_board').execute('', {}, undefined, undefined, noFile.ctx)).details.ok, false);
});

test('native slash commands show the board without a model turn and report unavailable coordination', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-command-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = join(dir, 'host.json');
  t.after(() => { if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = saved; });
  let unavailable = false;
  let actor;
  const now = Date.now();
  const staleSession = { namespace: 'fleet', piSessionId: randomUUID() };
  const staleId = `fleet:${staleSession.piSessionId}`;
  const unknownId = `fleet:${randomUUID()}`;
  const rankRequests = [];
  let savedRank = 1;
  let rankUnavailable = false;
  const server = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : undefined;
    if (req.url === '/v1/sessions/register') actor = body.session;
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/v1/sessions/rank') {
      rankRequests.push({ body, authorization: req.headers.authorization });
      if (rankUnavailable) {
        res.writeHead(503);
        res.end(JSON.stringify({ ok: false, error: 'temporarily_unavailable', message: 'Rank update unavailable' }));
      } else {
        savedRank = body.rank;
        res.end(JSON.stringify({ ok: true, value: { subscription: { id: staleId, session: staleSession, hostId: 'host-b' }, rank: savedRank } }));
      }
      return;
    }
    if (unavailable) {
      res.writeHead(503);
      res.end(JSON.stringify({ ok: false, error: 'temporarily_unavailable', message: 'Coordinator is offline' }));
      return;
    }
    const address = actor ? `${actor.namespace}:${actor.piSessionId}` : '';
    const value = req.url === '/v1/sessions' ? [
      { subscription: { id: address, session: actor, hostId: 'host-a' }, rank: 1, displayName: 'Alice', lastSeenAt: now },
      { subscription: { id: staleId, session: staleSession, hostId: 'host-b' }, rank: savedRank, lastSeenAt: now - 120_000 },
      { subscription: { id: unknownId, hostId: 'host-b' }, rank: 1 },
    ] :
      req.url === '/v1/claims' ? [{ key: 'build', owner: address }] : {};
    res.end(JSON.stringify({ ok: true, value }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const hostConfig = { namespace: 'fleet', hostId: 'host-a',
    credential: 'offline-secret', coordinatorUrl: `http://127.0.0.1:${server.address().port}`, inboxPath: join(dir, 'host.sqlite') };
  await writeFile(process.env.ROGER_WILCO_CONFIG, JSON.stringify(hostConfig));
  await hostSocket(t, hostConfig);
  const native = await nativePi(t, dir, {
    extensions: [resolve(process.env.RW_PACKAGE_DIR ?? '.', 'src/pi/index.ts')],
  });
  const notices = [], pickers = [];
  let choose = async (_title, _options) => undefined;
  native.session.extensionRunner.setUIContext({
    setStatus() {},
    notify: (message, type) => notices.push({ message, type }),
    select: async (title, options) => { pickers.push({ title, options }); return choose(title, options); },
  });
  for (const command of ['/roger', '/roger_board']) {
    await native.session.prompt(command);
    const notice = notices.at(-1);
    assert.equal(notice.type, 'info');
    assert.ok(notice.message.includes(`Your address: fleet:${native.id}`));
    assert.ok(notice.message.includes(`fleet:${native.id} (you) — host "host-a" — "Alice"`));
    assert.ok(notice.message.includes(`"build" — fleet:${native.id}`));
    assert.ok(!notice.message.includes('offline-secret'));
    assert.ok(!notice.message.includes(staleId));
    assert.ok(!notice.message.includes(unknownId));
    assert.ok(notice.message.includes(`last seen ${new Date(now).toISOString()}`));
    assert.match(notice.message, /\(\d+s ago\)/);
    assert.match(notice.message, /not proof that Pi is open or owns a lease/);
    assert.match(notice.message, /fleet \(rank 1\)/);
    assert.ok(pickers.at(-1).options.some(option => option.includes(native.id) && option.includes('fleet (rank 1)')));
    assert.ok(!pickers.at(-1).options.some(option => option.includes(staleId)));
  }
  assert.equal(notices.length, 2);
  await native.session.prompt('/roger all');
  assert.ok(notices.at(-1).message.includes(staleId));
  assert.ok(notices.at(-1).message.includes(`${unknownId} — host "host-b" — fleet (rank 1) — last seen unknown`));
  assert.ok(notices.at(-1).message.includes(`last seen ${new Date(now - 120_000).toISOString()}`));
  assert.deepEqual(rankRequests, [], 'Escape at the session picker does not mutate ranks');
  choose = async () => 'View only (no changes)';
  await native.session.prompt('/roger');
  choose = async (_title, options) => options.find(option => option.includes(staleId));
  await native.session.prompt('/roger all');
  assert.deepEqual(pickers.at(-1).options, ['fleet (rank 1)', 'Rank 2', 'Rank 3', 'Rank 4', 'Rank 5']);
  assert.deepEqual(rankRequests, [], 'view-only and cancelling the rank picker do not mutate ranks');
  choose = async (_title, options) => options.find(option => option.includes(staleId)) ?? 'Rank 4';
  await native.session.prompt('/roger all');
  assert.deepEqual(rankRequests, [{ body: { session: staleSession, rank: 4 }, authorization: 'Bearer offline-secret' }]);
  assert.deepEqual(notices.at(-1), { message: `${staleId}: Rank 4`, type: 'info' });
  choose = async () => undefined;
  await native.session.prompt('/roger all');
  assert.ok(notices.at(-1).message.includes(`${staleId} — host "host-b" — Rank 4`));
  rankUnavailable = true;
  choose = async (_title, options) => options.find(option => option.includes(staleId)) ?? 'Rank 5';
  await native.session.prompt('/roger all');
  assert.deepEqual(notices.at(-1), { message: 'Rank update unavailable', type: 'error' });
  assert.equal(savedRank, 4);
  unavailable = true;
  await native.session.prompt('/roger');
  assert.deepEqual(notices.at(-1), { message: 'Coordinator is offline', type: 'error' });
  await native.session.extensionRunner.emit({ type: 'session_shutdown', reason: 'exit' });
  await native.session.prompt('/roger');
  assert.deepEqual(notices.at(-1), { message: 'Roger-Wilco session is not bound', type: 'error' });
  assert.equal(native.calls.length, 0, 'slash commands never invoke the model');
});

test('native rank picker persists another host’s session metadata through the real coordinator', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-rank-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = join(dir, 'host.json');
  t.after(() => { if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = saved; });
  const database = join(dir, 'coordinator.sqlite');
  let store = createCoordinatorStore(database, 'fleet');
  t.after(() => store.close());
  const target = { namespace: 'fleet', piSessionId: randomUUID() };
  const registration = store.register({ hostId: 'host-b' }, { session: target, displayName: 'Other host' });
  assert.equal(registration.ok, true);
  assert.equal(registration.value.rank, 1);
  const handler = createControlHandler(store, createHostAuthenticator({ 'host-a': 'offline-secret', 'host-b': 'other-secret' }));
  const server = createServer((req, res) => { void handler(req, res); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const hostConfig = { namespace: 'fleet', hostId: 'host-a',
    credential: 'offline-secret', coordinatorUrl: `http://127.0.0.1:${server.address().port}`, inboxPath: join(dir, 'host.sqlite') };
  await writeFile(process.env.ROGER_WILCO_CONFIG, JSON.stringify(hostConfig));
  await hostSocket(t, hostConfig);
  const native = await nativePi(t, dir, { extensions: [resolve(process.env.RW_PACKAGE_DIR ?? '.', 'src/pi/index.ts')] });
  const notices = [];
  native.session.extensionRunner.setUIContext({
    setStatus() {}, notify: (message, type) => notices.push({ message, type }),
    select: async (_title, options) => options.find(option => option.includes(target.piSessionId)) ?? 'Rank 5',
  });
  await native.session.prompt('/roger');
  assert.deepEqual(notices.at(-1), { message: `fleet:${target.piSessionId}: Rank 5`, type: 'info' });
  assert.equal(native.calls.length, 0);
  await native.stop();
  store.close();
  store = createCoordinatorStore(database, 'fleet');
  const sessions = store.listSessions();
  const updated = sessions.find(row => row.subscription.id === `fleet:${target.piSessionId}`);
  assert.equal(updated.rank, 5);
  assert.equal(updated.subscription.hostId, 'host-b', 'rank does not migrate ownership');
  assert.equal(updated.subscription.cursor, registration.value.subscription.cursor, 'rank does not alter delivery routing/cursor');
  assert.equal(updated.lastSeenAt, registration.value.lastSeenAt, 'rank does not fabricate presence');
  assert.equal(sessions.find(row => row.subscription.id === `fleet:${native.id}`).rank, 1, 'selection targets the chosen session, not the invoker');
});

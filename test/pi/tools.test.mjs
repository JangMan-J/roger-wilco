import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import rogerWilco from '../../src/pi/index.ts';
import { nativePi } from '../acceptance/native-pi.ts';

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

test('session-bound tools use the invoking UUID; board includes sessions and claims; duplicate runtime conflicts', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-tools-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const saved = process.env.ROGER_WILCO_CONFIG;
  process.env.ROGER_WILCO_CONFIG = join(dir, 'host.json');
  t.after(() => { if (saved === undefined) delete process.env.ROGER_WILCO_CONFIG; else process.env.ROGER_WILCO_CONFIG = saved; });
  const id = randomUUID();
  const requests = [];
  const server = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : undefined;
    requests.push({ path: req.url, body, authorization: req.headers.authorization });
    res.setHeader('Content-Type', 'application/json');
    const value = req.url === '/v1/sessions' ? [{ subscription: { id: `fleet:${id}` }, lastSeenAt: 123 }] :
      req.url === '/v1/claims' ? [{ key: 'work', owner: `fleet:${id}` }] : { accepted: true };
    res.end(JSON.stringify({ ok: true, value }));
  });
  server.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  await writeFile(process.env.ROGER_WILCO_CONFIG, JSON.stringify({ namespace: 'fleet', hostId: 'host-a',
    credential: 'offline-secret', coordinatorUrl: `http://127.0.0.1:${server.address().port}`, inboxPath: join(dir, 'host.sqlite') }));
  const first = extension();
  const { state, ctx } = context(id, dir);
  await first.handlers.get('session_start')({}, ctx);
  const second = extension();
  const other = context(id, dir);
  await second.handlers.get('session_start')({}, other.ctx);
  assert.match(other.state.status, /owner_conflict/);
  assert.deepEqual((await first.tools.get('roger_board').execute('', {}, undefined, undefined, ctx)).details,
    { ok: true, value: { sessions: [{ subscription: { id: `fleet:${id}` }, lastSeenAt: 123 }], claims: [{ key: 'work', owner: `fleet:${id}` }] } });
  const tool = async (name, params) => (await first.tools.get(name).execute('', params, undefined, undefined, ctx)).details;
  await tool('roger_send', { recipient: '*', body: 'Hello', data: { priority: 2 }, visibility: 'public' });
  await tool('roger_inbox', { since: 2, limit: 20 });
  await tool('roger_claim', { key: 'work', ttlSeconds: 90 });
  await tool('roger_release', { key: 'work' });
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
  const server = createServer(async (req, res) => {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    if (req.url === '/v1/sessions/register') actor = JSON.parse(raw).session;
    res.setHeader('Content-Type', 'application/json');
    if (unavailable) {
      res.writeHead(503);
      res.end(JSON.stringify({ ok: false, error: 'temporarily_unavailable', message: 'Coordinator is offline' }));
      return;
    }
    const address = actor ? `${actor.namespace}:${actor.piSessionId}` : '';
    const value = req.url === '/v1/sessions' ? [{ subscription: { id: address, hostId: 'host-a' }, displayName: 'Alice' }] :
      req.url === '/v1/claims' ? [{ key: 'build', owner: address }] : {};
    res.end(JSON.stringify({ ok: true, value }));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  await writeFile(process.env.ROGER_WILCO_CONFIG, JSON.stringify({ namespace: 'fleet', hostId: 'host-a',
    credential: 'offline-secret', coordinatorUrl: `http://127.0.0.1:${server.address().port}`, inboxPath: join(dir, 'host.sqlite') }));
  const native = await nativePi(t, dir, {
    extensions: [resolve(process.env.RW_PACKAGE_DIR ?? '.', 'src/pi/index.ts')],
  });
  const notices = [];
  native.session.extensionRunner.setUIContext({
    setStatus() {},
    notify: (message, type) => notices.push({ message, type }),
  });
  for (const command of ['/roger', '/roger_board']) {
    await native.session.prompt(command);
    const notice = notices.at(-1);
    assert.equal(notice.type, 'info');
    assert.ok(notice.message.includes(`Your address: fleet:${native.id}`));
    assert.ok(notice.message.includes(`fleet:${native.id} (you) — host "host-a" — "Alice"`));
    assert.ok(notice.message.includes(`"build" — fleet:${native.id}`));
    assert.ok(!notice.message.includes('offline-secret'));
  }
  assert.equal(notices.length, 2);
  unavailable = true;
  await native.session.prompt('/roger');
  assert.deepEqual(notices.at(-1), { message: 'Coordinator is offline', type: 'error' });
  await native.session.extensionRunner.emit({ type: 'session_shutdown', reason: 'exit' });
  await native.session.prompt('/roger');
  assert.deepEqual(notices.at(-1), { message: 'Roger-Wilco session is not bound', type: 'error' });
  assert.equal(native.calls.length, 0, 'slash commands never invoke the model');
});

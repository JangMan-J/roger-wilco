import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createServer } from 'node:http';
import { lstat, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { before, type TestContext } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createHostClient } from '../../src/host/client.ts';
import { createDaemonInbox, createHostDaemon } from '../../src/host/index.ts';
import { ROUTES, sessionKey, type SessionAddress } from '../../src/protocol/index.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import { createStreamHandler } from '../../src/server/delivery/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';

const namespace = 'fleet';
const hostId = 'host-a';
const credential = 'token-a';
const peer: SessionAddress = { namespace, sessionId: '550e8400-e29b-41d4-a716-446655440000' };
const rustSessionId = 'rust-client-1';
const binary = join(process.cwd(), 'rust/target/debug/roger-wilco-rs');

before(async () => {
  // The second-language client is built from its own toolchain; the gate
  // builds it if this checkout has not.
  await new Promise<void>((resolveBuild, rejectBuild) => {
    execFile('cargo', ['build', '--offline', '--manifest-path', join(process.cwd(), 'rust/Cargo.toml')], { timeout: 300_000 },
      error => error ? rejectBuild(error) : resolveBuild());
  });
});

async function until(predicate: () => boolean | Promise<boolean>, reason: string) {
  const deadline = Date.now() + 8000;
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, reason);
    await delay(25);
  }
}

function run(args: string[], env: Record<string, string>): Promise<string> {
  return new Promise((resolveRun, rejectRun) => {
    execFile(binary, args, { env: { ...process.env, ...env }, timeout: 30_000 }, (error, stdout, stderr) => {
      if (error) rejectRun(new Error(`${args[0]} failed: ${stderr || error.message}`));
      else resolveRun(stdout.trim());
    });
  });
}

test('the Rust client interoperates with the TypeScript coordinator and daemon', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-rust-'));
  const inboxPath = join(dir, 'inbox.sqlite');
  const store = createCoordinatorStore(join(dir, 'coordinator.db'), namespace);
  const auth = createHostAuthenticator({ [hostId]: credential });
  const control = createControlHandler(store, auth);
  const stream = createStreamHandler(store, auth);
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? '/', 'http://localhost').pathname;
    const handler = path === ROUTES.stream || path === ROUTES.receipt ? stream : control;
    void handler(request, response).catch(() => {
      if (!response.headersSent) response.writeHead(500);
      response.end();
    });
  });
  await new Promise<void>(resolveListen => server.listen(0, '127.0.0.1', () => resolveListen()));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}`;
  const inbox = createDaemonInbox(inboxPath);
  const abort = new AbortController();
  const daemon = createHostDaemon({ coordinatorUrl: url, hostId, credential, inboxPath }, inbox);
  const running = daemon.start(abort.signal).catch(error => {
    if (!abort.signal.aborted) throw error;
  });
  t.after(async () => {
    abort.abort();
    await daemon.stop();
    await running;
    await stream.close();
    await new Promise<void>(done => { server.close(() => done()); server.closeAllConnections(); });
    await inbox.close();
    store.close();
    await rm(dir, { recursive: true, force: true });
  });
  await until(async () => {
    try { return (await lstat(`${inboxPath}.sock`)).isSocket(); }
    catch { return false; }
  }, 'host socket');

  const env = {
    RW_COORDINATOR_URL: url, RW_CREDENTIAL: credential,
    RW_INBOX_SOCK: `${inboxPath}.sock`, RW_NAMESPACE: namespace, RW_SESSION_ID: rustSessionId,
  };
  // Registration through the daemon socket, from the contract alone.
  const registered = JSON.parse(await run(['register'], env)) as { ok: boolean };
  assert.equal(registered.ok, true);

  assert.equal(store.register({ hostId }, { session: peer }).ok, true);
  // Send through the coordinator HTTP API, as the bound Rust session.
  const sent = JSON.parse(await run(['send', sessionKey(peer), 'from rust'], env)) as { ok: boolean };
  assert.equal(sent.ok, true);

  // The TypeScript socket client sees it in the peer's mailbox.
  const peerClient = createHostClient(inboxPath);
  t.after(() => peerClient.close());
  assert.equal((await peerClient.bind(peer, 'peer-rt')).ok, true);
  await until(async () => (await peerClient.pending(peer, 'peer-rt')).some(m => m.body === 'from rust'),
    'peer receives the Rust-sent message through the daemon');

  // The board, through the Rust client, lists both sessions.
  const board = JSON.parse(await run(['board'], env)) as { sessions: { ok: boolean; value: unknown[] } };
  assert.equal(board.sessions.ok, true);
  assert.equal(board.sessions.value.length >= 2, true);
});

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';

const remote = process.env.RW_REMOTE ?? 'agent-jail';
const home = homedir();
const configPath = join(home, '.config/roger-wilco/host.json');
const config = JSON.parse(await readFile(configPath, 'utf8'));
const packageDir = join(home, '.local/share/roger-wilco/package');
const evidenceDir = join(home, '.local/share/roger-wilco/installation-check');
await mkdir(evidenceDir, { recursive: true, mode: 0o700 });
const children = [];
function launch(name, command, args, env = process.env) {
  const child = spawn(command, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
  const events = [];
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  createInterface({ input: child.stdout }).on('line', line => {
    try { events.push(JSON.parse(line)); } catch { stderr += `Unexpected stdout: ${line}\n`; }
  });
  const exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  const instance = { name, child, events, exited,
    send(value) { child.stdin.write(JSON.stringify(value) + '\n'); },
    async wait(predicate) {
      const deadline = Date.now() + 20_000;
      for (;;) {
        const error = events.find(event => event.type === 'error');
        assert.equal(error, undefined, `${name} command error: ${JSON.stringify(error)}`);
        const found = events.find(predicate);
        if (found) return found;
        assert.equal(child.exitCode, null, `${name} exited: ${stderr}`);
        assert.ok(Date.now() < deadline, `${name} timed out: ${stderr}\n${JSON.stringify(events)}`);
        await delay(20);
      }
    } };
  children.push(instance);
  return instance;
}
const fixture = fileURLToPath(new URL('./session.mjs', import.meta.url));
const localEnv = name => ({ ...process.env, RW_PACKAGE_DIR: packageDir, RW_SESSION_DIR: join(evidenceDir, name), ROGER_WILCO_CONFIG: configPath });
let report;
try {
  const alice = launch('alice', process.execPath, [fixture], localEnv('alice'));
  const spare = launch('spare', process.execPath, [fixture], localEnv('spare'));
  const bob = launch('bob', 'ssh', ['-T', remote,
    'env RW_PACKAGE_DIR=$HOME/.local/share/roger-wilco/package RW_SESSION_DIR=$HOME/.local/share/roger-wilco/installation-check/bob ROGER_WILCO_CONFIG=$HOME/.config/roger-wilco/host.json node $HOME/.local/share/roger-wilco/session.mjs']);
  const [a, b, s] = await Promise.all([alice.wait(e => e.type === 'ready'), bob.wait(e => e.type === 'ready'), spare.wait(e => e.type === 'ready')]);
  const deadline = Date.now() + 20_000;
  for (;;) {
    const response = await fetch(new URL('/v1/sessions', config.coordinatorUrl), { headers: { authorization: `Bearer ${config.credential}` } });
    const board = await response.json();
    if ([a, b, s].every(ready => board.value.some(row => row.subscription.id === ready.address))) break;
    assert.ok(Date.now() < deadline, 'Automatic cross-host registration timed out');
    await delay(25);
  }
  const setRank = await fetch(new URL('/v1/sessions/rank', config.coordinatorUrl), {
    method: 'POST', headers: { authorization: `Bearer ${config.credential}`, 'content-type': 'application/json' },
    body: JSON.stringify({ session: { namespace: config.namespace, piSessionId: b.sessionId }, rank: 3 }),
  });
  assert.equal(setRank.status, 200, 'local authenticated host can rank a remote session');
  const ranked = await setRank.json();
  assert.equal(ranked.value.rank, 3);
  assert.equal(ranked.value.subscription.id, b.address);
  bob.send({ command: 'send', recipient: a.address, body: 'Physical-host directed ping', id: 'ping' });
  const ping = await bob.wait(e => e.type === 'send' && e.id === 'ping');
  assert.equal(ping.result.ok, true);
  const gotPing = await alice.wait(e => e.type === 'received' && e.id === ping.result.value.id);
  assert.equal(gotPing.body, 'Physical-host directed ping');
  await alice.wait(e => e.type === 'turn');
  alice.send({ command: 'send', recipient: b.address, body: 'Physical-host directed reply', id: 'reply' });
  const reply = await alice.wait(e => e.type === 'send' && e.id === 'reply');
  assert.equal(reply.result.ok, true);
  const gotReply = await bob.wait(e => e.type === 'received' && e.id === reply.result.value.id);
  assert.equal(gotReply.body, 'Physical-host directed reply');
  await bob.wait(e => e.type === 'turn');
  bob.send({ command: 'busy' });
  await bob.wait(e => e.type === 'busy');
  alice.send({ command: 'send', recipient: b.address, body: 'Physical-host busy follow-up', id: 'busy' });
  const sentBusy = await alice.wait(e => e.type === 'send' && e.id === 'busy');
  assert.equal(sentBusy.result.ok, true);
  bob.send({ command: 'release' });
  await bob.wait(e => e.type === 'received' && e.id === sentBusy.result.value.id);
  await bob.wait(e => e.type === 'turn' && e.count >= 3);
  assert.equal(spare.events.some(e => e.type === 'received' || e.type === 'turn'), false, 'Other local session must remain idle');
  report = { checkedAt: new Date().toISOString(), hosts: [config.hostId, remote],
    package: 'roger-wilco@0.1.0', provider: 'synthetic offline model stream',
    sessions: { local: a.address, remote: b.address, isolated: s.address },
    rankedSession: { address: b.address, rank: 3 },
    messageIds: [ping.result.value.id, reply.result.value.id, sentBusy.result.value.id],
    result: 'PASS: auto-registration, cross-host rank metadata, directed round trip, idle wake, busy follow-up, same-host isolation' };
} finally {
  for (const instance of children) if (instance.child.exitCode === null) {
    instance.send({ command: 'stop' });
    instance.child.stdin.end();
  }
  await Promise.all(children.map(async instance => {
    const timer = setTimeout(() => instance.child.kill('SIGKILL'), 8000);
    try {
      const result = await instance.exited;
      assert.equal(result.code, 0, `${instance.name} bounded shutdown failed: ${JSON.stringify(result)}`);
    } finally { clearTimeout(timer); }
  }));
}
await writeFile(join(evidenceDir, 'result.json'), JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify(report));

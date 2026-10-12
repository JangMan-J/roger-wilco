import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { readHostConfig } from '../../src/bin/config.ts';

test('packaged entry-point composition starts, serves health, and exits on SIGTERM', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-bin-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'coordinator.json');
  await writeFile(path, JSON.stringify({ namespace: 'fleet', databasePath: join(dir, 'coordinator.sqlite'),
    listenHost: '127.0.0.1', port: 0, hostCredentials: { host: 'test-token' } }));
  const coordinator = spawn(process.execPath, [fileURLToPath(new URL('../../src/bin/coordinator.ts', import.meta.url)), path],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(() => coordinator.kill('SIGKILL'));
  let stderr = '';
  coordinator.stderr.on('data', chunk => { stderr += chunk; });
  const done = new Promise<number | null>(resolve => coordinator.once('exit', resolve));
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Coordinator startup timeout: ${stderr}`)), 5000);
    createInterface({ input: coordinator.stdout }).once('line', line => {
      clearTimeout(timer);
      try { resolve(JSON.parse(line).url); } catch (error) { reject(error); }
    });
    coordinator.once('error', reject);
  });
  assert.equal((await fetch(`${url}/health`)).status, 200);
  const hostConfig = join(dir, 'host.json');
  await writeFile(hostConfig, JSON.stringify({ namespace: 'fleet', coordinatorUrl: url, hostId: 'host', credential: 'test-token', inboxPath: join(dir, 'inbox.sqlite') }));
  const host = spawn(process.execPath, [fileURLToPath(new URL('../../src/bin/host.ts', import.meta.url)), hostConfig],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(() => host.kill('SIGKILL'));
  const hostDone = new Promise<number | null>(resolve => host.once('exit', resolve));
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Host startup timeout')), 5000);
    createInterface({ input: host.stdout }).once('line', () => { clearTimeout(timer); resolve(); });
    host.once('error', reject);
  });
  host.kill('SIGTERM');
  coordinator.kill('SIGTERM');
  const deadline = setTimeout(() => { host.kill('SIGKILL'); coordinator.kill('SIGKILL'); }, 5000);
  try {
    assert.equal(await hostDone, 0);
    assert.equal(await done, 0);
  } finally { clearTimeout(deadline); }
});

test('host configuration rejects callback fields and off-host plaintext HTTP', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-config-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'host.json');
  const base = { namespace: 'fleet', coordinatorUrl: 'https://coordinator.example', hostId: 'host', credential: 'token', inboxPath: '~/inbox.sqlite' };
  await writeFile(path, JSON.stringify({ ...base, callbackUrl: 'https://callback.example' }));
  await assert.rejects(readHostConfig(path), /Unknown host/);
  await writeFile(path, JSON.stringify({ ...base, coordinatorUrl: 'http://remote.example' }));
  await assert.rejects(readHostConfig(path), /HTTPS/);
});

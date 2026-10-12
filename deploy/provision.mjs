#!/usr/bin/env node
// One-time provisioning. Existing files are deliberately not overwritten.
import { randomBytes } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';

const [url, namespace, ...hosts] = process.argv.slice(2);
if (!url || !namespace || !hosts.length || !/^[a-z][a-z0-9-]{0,62}$/.test(namespace) ||
    hosts.some(host => !/^[a-z0-9][a-z0-9-]*$/.test(host)) || new Set(hosts).size !== hosts.length) {
  throw new Error('Usage: node deploy/provision.mjs https://coordinator:8445 namespace host-a [host-b]');
}
if (new URL(url).protocol !== 'https:') throw new Error('Provisioned coordinator URL must use HTTPS');
const directory = join(homedir(), '.config/roger-wilco');
await mkdir(directory, { recursive: true, mode: 0o700 });
const credentials = Object.fromEntries(hosts.map(host => [host, randomBytes(32).toString('base64url')]));
const save = (name, value) => writeFile(join(directory, name), JSON.stringify(value, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
await save('coordinator.json', { namespace, databasePath: '~/.local/share/roger-wilco/coordinator.sqlite',
  listenHost: '127.0.0.1', port: 7088, hostCredentials: credentials });
for (const host of hosts) {
  await save(`${host}.json`, { namespace, coordinatorUrl: url, hostId: host, credential: credentials[host],
    inboxPath: '~/.local/share/roger-wilco/inbox.sqlite' });
}
console.log(`Provisioned ${hosts.length} host configurations. Credentials remain in private files.`);

import { DatabaseSync } from 'node:sqlite';
import { access, cp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

const name = process.argv[2];
if (!name || !/^[a-z0-9-]+$/.test(name)) throw new Error('Usage: node deploy/snapshot-current.mjs snapshot-name');
const home = homedir();
const root = join(home, '.local/share/roger-wilco');
const output = join(root, name);
await mkdir(output, { mode: 0o700 });
const exists = path => access(path).then(() => true, () => false);
const pathOf = path => path.startsWith('~/') ? join(home, path.slice(2)) : resolve(path);
for (const [from, to] of [
  [join(root, 'package'), 'package'],
  [join(home, '.config/roger-wilco'), 'config'],
  [join(home, '.pi/agent/settings.json'), 'pi-settings.json'],
  [join(home, '.config/systemd/user/roger-wilco-host.service'), 'roger-wilco-host.service'],
  [join(home, '.config/systemd/user/roger-wilco-coordinator.service'), 'roger-wilco-coordinator.service'],
]) {
  if (await exists(from)) await cp(from, join(output, to), { recursive: true, errorOnExist: true, force: false });
}
const summary = { createdAt: new Date().toISOString(), databases: {} };
for (const [service, key, tables] of [
  ['host', 'inboxPath', ['messages', 'leases', 'registrations', 'inbox_messages', 'inbox_leases', 'inbox_registrations']],
  ['coordinator', 'databasePath', ['messages', 'sessions', 'claims', 'entities']],
]) {
  const configPath = join(home, '.config/roger-wilco', `${service}.json`);
  if (!await exists(configPath)) continue;
  const config = JSON.parse(await readFile(configPath, 'utf8'));
  const db = new DatabaseSync(pathOf(config[key]), { readOnly: true });
  try {
    db.prepare('VACUUM INTO ?').run(join(output, `${service}.sqlite`));
  } finally { db.close(); }
  const backup = new DatabaseSync(join(output, `${service}.sqlite`), { readOnly: true });
  try {
    const counts = {};
    for (const table of tables) {
      if (backup.prepare("SELECT 1 FROM sqlite_master WHERE name = ? AND type = 'table'").get(table))
        counts[table] = backup.prepare(`SELECT count(*) AS n FROM ${table}`).get().n;
    }
    summary.databases[service] = { integrity: backup.prepare('PRAGMA integrity_check').get().integrity_check, counts };
    if (summary.databases[service].integrity !== 'ok') throw new Error(`Invalid ${service} backup`);
  } finally { backup.close(); }
}
await writeFile(join(output, 'inventory.json'), `${JSON.stringify(summary, null, 2)}\n`, { mode: 0o600 });
console.log(JSON.stringify({ backup: name, ...summary }));

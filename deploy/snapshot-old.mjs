import { DatabaseSync } from 'node:sqlite';
import { cp, mkdir, writeFile, access } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';

const home = homedir();
const name = process.argv[2];
if (!name || !/^[a-z0-9-]+$/.test(name)) throw new Error('Usage: node deploy/snapshot-old.mjs snapshot-name');
const output = join(home, '.local/share/roger-wilco', name);
await mkdir(output, { mode: 0o700 });
const exists = path => access(path).then(() => true, () => false);
for (const [from, to] of [
  ['.config/agent-bus', 'agent-bus-config'],
  ['.pi/agent/settings.json', 'settings.json'],
  ['.pi/agent/mcp.json', 'mcp.json'],
  ['.config/systemd/user/agent-bus-receiver.service', 'agent-bus-receiver.service'],
]) {
  if (await exists(join(home, from))) await cp(join(home, from), join(output, to), { recursive: true, errorOnExist: true, force: false });
}
const summary = { createdAt: new Date().toISOString(), inboxes: {}, sources: {} };
for (const [label, relative] of [
  ['research', '.pi/agent/webtasks/webtasks.sqlite'],
  ['agent', '.pi/agent/webtasks/agent-bus/webtasks.sqlite'],
]) {
  const path = join(home, relative);
  if (!await exists(path)) continue;
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    db.prepare('VACUUM INTO ?').run(join(output, `${label}.sqlite`));
    const rows = db.prepare('SELECT webhook_id, resource_id, kind, handled, notified FROM events ORDER BY received_at').all();
    summary.inboxes[label] = rows;
  } finally { db.close(); }
}
for (const relative of ['Projects/pi-webtasks', '.pi/agent/git/github.com/JangMan-J/pi-webtasks']) {
  const path = join(home, relative);
  if (!await exists(join(path, '.git'))) continue;
  const id = relative.startsWith('Projects') ? 'maintained' : 'installed';
  summary.sources[id] = execFileSync('git', ['-C', path, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  execFileSync('git', ['-C', path, 'bundle', 'create', join(output, `${id}.bundle`), 'HEAD']);
}
await writeFile(join(output, 'inventory.json'), JSON.stringify(summary, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify({ backup: name, sources: summary.sources,
  inboxes: Object.fromEntries(Object.entries(summary.inboxes).map(([kind, rows]) => [kind, { events: rows.length, unresolved: rows.filter(row => !row.notified).length }])) }));

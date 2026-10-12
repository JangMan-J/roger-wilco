import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';

const name = process.argv[2];
if (!name || !/^[a-z0-9-]+$/.test(name)) throw new Error('Supply the private snapshot name');
const directory = join(homedir(), '.local/share/roger-wilco', name);
const targets = JSON.parse(await readFile(join(directory, 'agent-bus-config/webhooks/targets.json'), 'utf8'));
const code = `import {Database} from "bun:sqlite"; const db=new Database("/data/agent-bus.db",{readonly:true});
console.log(JSON.stringify({claims:db.query("SELECT * FROM claims").all(),cursors:db.query("SELECT * FROM webhook_cursors").all(),messages:db.query("SELECT * FROM messages ORDER BY id").all()}));`;
const old = JSON.parse(execFileSync('docker', ['exec', 'agent-bus', 'bun', '-e', code], { encoding: 'utf8' }));
const delivery = targets.map(target => {
  const key = createHash('sha256').update(JSON.stringify([target.agent, target.url])).digest('hex');
  const cursor = old.cursors.find(row => row.target === key)?.cursor;
  if (cursor === undefined) throw new Error('Missing destination cursor');
  return { recipient: target.agent, cursor,
    pending: old.messages.filter(message => message.id > cursor &&
      (message.recipient === target.agent || (message.recipient === '*' && message.sender !== target.agent))).map(message => message.id) };
});
const inventory = { checkedAt: new Date().toISOString(), delivery, claims: old.claims, messages: old.messages };
await writeFile(join(directory, 'coordinator-inventory.json'), JSON.stringify(inventory, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify({ delivery, claims: old.claims, messageCount: old.messages.length }));

import { readFile, writeFile, access } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';

const home = homedir();
const snapshot = process.argv[2];
if (!snapshot || !/^[a-z0-9-]+$/.test(snapshot)) throw new Error('Supply a verified private rollback snapshot name');
await access(join(home, '.local/share/roger-wilco', snapshot, 'inventory.json'));
const environment = join(home, '.config/agent-bus/receiver.env');
const source = await readFile(environment, 'utf8');
let retained = source.split('\n').filter(line => !line.startsWith('WEBTASKS_AGENT_BUS_KEY='));
const current = retained.find(line => line.startsWith('PARALLEL_WEBHOOK_KEY='))?.slice('PARALLEL_WEBHOOK_KEY='.length).trim();
if (!current) {
  // A bus-only receiver was allowed to leave the research key empty.
  retained = retained.filter(line => !line.startsWith('PARALLEL_WEBHOOK_KEY='));
  retained.push(`PARALLEL_WEBHOOK_KEY=whsec_${randomBytes(32).toString('base64')}`, '');
  console.log('Provisioned the previously unset research signing key in the private environment file.');
}
await writeFile(environment, retained.join('\n'), { mode: 0o600 });
const targets = join(home, '.config/agent-bus/webhooks/targets.json');
try {
  const value = JSON.parse(await readFile(targets, 'utf8'));
  if (!Array.isArray(value)) throw new Error('Unexpected old destination configuration');
  await writeFile(targets, '[]\n', { mode: 0o600 });
} catch (error) { if (error.code !== 'ENOENT') throw error; }
console.log('Retired agent-message credentials and destinations; research credentials retained.');

import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const home = homedir();
const remote = process.env.RW_REMOTE ?? 'agent-jail';
const state = () => {
  const db = new DatabaseSync(join(home, '.local/share/roger-wilco/coordinator.sqlite'), { readOnly: true });
  try { return db.prepare('SELECT * FROM messages ORDER BY id').all(); } finally { db.close(); }
};
const before = state();
const run = (command, args) => execFileSync(command, args, { stdio: 'pipe' });
const settings = JSON.parse(await readFile(join(home, '.pi/agent/mcp.json'), 'utf8'));
const old = settings.mcpServers['agent-bus'];
assert.ok(old?.url && old?.headers, 'Run rollback rehearsal before removing the old MCP declaration');
const serverEnvironment = await readFile(join(home, '.config/agent-bus/server.env'), 'utf8');
const token = /^AGENT_BUS_TOKEN=(.+)$/m.exec(serverEnvironment)?.[1]?.trim();
assert.ok(token, 'Old coordinator credential is configured');
let evidence;
try {
  run('systemctl', ['--user', 'stop', 'roger-wilco-host', 'roger-wilco-coordinator']);
  run('ssh', [remote, 'systemctl --user stop roger-wilco-host']);
  run('docker', ['stop', 'agent-bus']);
  run('docker', ['start', 'agent-bus']);
  const client = new Client({ name: 'roger-wilco-rollback-check', version: '1.0.0' });
  const transport = new StreamableHTTPClientTransport(new URL(old.url), {
    requestInit: { headers: { Authorization: `Bearer ${token}`, 'X-Agent-Name': 'roger-wilco-rollback-check' } },
  });
  try {
    // Docker reports started before HTTP listen; retry only transport startup.
    const deadline = Date.now() + 10_000;
    while (true) {
      try {
        const response = await fetch(new URL('/health', old.url), { signal: AbortSignal.timeout(1000) });
        if (!response.ok) throw new Error(`Old health: ${response.status}`);
        break;
      }
      catch (error) { if (Date.now() >= deadline) throw error; await new Promise(resolve => setTimeout(resolve, 100)); }
    }
    await client.connect(transport);
    const call = async (name, args) => {
      const result = await client.callTool({ name, arguments: args });
      assert.ok(!result.isError, `Old MCP ${name} failed`);
      return JSON.parse(result.content.find(item => item.type === 'text').text);
    };
    const actor = 'roger-wilco-rollback-check';
    await call('register', { agent: actor, host: 'isolated-rollback-rehearsal' });
    const sent = await call('send_message', { from: actor, to: actor, body: 'Isolated rollback directed delivery verification.', visibility: 'private' });
    const inbox = await call('inbox', { agent: actor, since: sent.message.id - 1 });
    assert.equal(inbox.messages[0].id, sent.message.id);
    assert.equal(inbox.messages[0].recipient, actor);
    evidence = { checkedAt: new Date().toISOString(), oldMessageId: sent.message.id,
      oldDirectedMcp: true, newServicesStoppedDuringOldVerification: true };
  } finally { await client.close(); }
} finally {
  run('systemctl', ['--user', 'start', 'roger-wilco-coordinator', 'roger-wilco-host']);
  run('ssh', [remote, 'systemctl --user start roger-wilco-host']);
}
assert.deepEqual(state(), before, 'Rollback must retain all new pending messages and identities');
evidence.newMessagesPreserved = before.length;
await writeFile(join(home, '.local/share/roger-wilco/installation-check/rollback.json'), JSON.stringify(evidence, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify(evidence));

import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { homedir } from 'node:os';
import { join } from 'node:path';

const home = homedir();
const environment = await readFile(join(home, '.config/agent-bus/receiver.env'), 'utf8');
const secret = /^PARALLEL_WEBHOOK_KEY=(.+)$/m.exec(environment)?.[1]?.trim();
assert.ok(secret, 'Research signing key is configured');
assert.ok(!/^WEBTASKS_AGENT_BUS_KEY=/m.test(environment), 'Old agent-message key is retired');
const db = new DatabaseSync(join(home, '.pi/agent/webtasks/webtasks.sqlite'), { readOnly: true });
const before = db.prepare('SELECT webhook_id, event_json, handled, notified FROM events ORDER BY webhook_id').all();
assert.ok(before.length, 'Use an existing research event to verify replay without adding a new task');
const post = async (path, id, body) => {
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = createHmac('sha256', Buffer.from(secret.replace(/^whsec_/, ''), 'base64')).update(`${id}.${timestamp}.${body}`).digest('base64');
  const response = await fetch(`http://127.0.0.1:8787${path}`, { method: 'POST', body,
    headers: { 'webhook-id': id, 'webhook-timestamp': timestamp, 'webhook-signature': `v1,${signature}` },
    signal: AbortSignal.timeout(5000) });
  await response.text();
  return response.status;
};
try {
  assert.equal(await post('/webtasks/webhook', before[0].webhook_id, before[0].event_json), 200, 'Existing signed research callback remains accepted');
  const bus = JSON.stringify({ type: 'agent_bus.message', data: {} });
  assert.equal(await post('/agent-bus/webhook', 'retired-route-check', bus), 404);
  assert.equal(await post('/webtasks/webhook', 'wrong-type-check', bus), 400);
  assert.deepEqual(db.prepare('SELECT webhook_id, event_json, handled, notified FROM events ORDER BY webhook_id').all(), before, 'Replay and rejected agent events must preserve research state');
  const result = { checkedAt: new Date().toISOString(), researchReplayStatus: 200,
    oldAgentRouteStatus: 404, agentEventOnResearchRouteStatus: 400, unchangedResearchEvents: before.length };
  const output = join(home, '.local/share/roger-wilco/installation-check');
  await mkdir(output, { recursive: true, mode: 0o700 });
  await writeFile(join(output, 'research.json'), JSON.stringify(result, null, 2) + '\n', { mode: 0o600 });
  console.log(JSON.stringify(result));
} finally { db.close(); }

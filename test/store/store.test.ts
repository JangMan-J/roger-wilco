import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { sessionKey, type CoordinatorStore, type Receipt, type SessionAddress } from '../../src/protocol/index.ts';

const a: SessionAddress = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440001' };
const b: SessionAddress = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440002' };
const c: SessionAddress = { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440003' };
const hostA = { hostId: 'host-a' };
const hostB = { hostId: 'host-b' };

async function fileStore(t: { after(fn: () => Promise<void>): void }) {
  const dir = await mkdtemp(join(tmpdir(), 'rw-store-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return join(dir, 'store.sqlite');
}
function ok<T>(outcome: { ok: true; value: T } | { ok: false; error: string }): T {
  assert.equal(outcome.ok, true, !outcome.ok ? outcome.error : 'expected success');
  return (outcome as { ok: true; value: T }).value;
}
function register(store: CoordinatorStore) {
  ok(store.register(hostA, { session: a }));
  ok(store.register(hostA, { session: b }));
  ok(store.register(hostB, { session: c }));
}

test('registration snapshots the current tail atomically; retry and reopen preserve progress and host assignment', async t => {
  const path = await fileStore(t);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    const old = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'before registration' }));
    const registered = ok(store.register(hostA, { session: b, displayName: 'Bee' }));
    assert.equal(registered.subscription.cursor, old.sequence);
    assert.equal(registered.displayName, 'Bee');
    assert.equal(typeof registered.lastSeenAt, 'number');
    assert.deepEqual(store.pending(hostA, 100), []);
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)).map(message => message.id), [old.id]);
    assert.equal(store.register(hostB, { session: b }).ok, false);
    const current = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'after registration' }));
    assert.deepEqual(store.pending(hostA, 1).map(frame => frame.message.id), [current.id]);
    const db = new DatabaseSync(path);
    try { db.prepare('UPDATE sessions SET last_seen_at = 1 WHERE id = ?').run(sessionKey(b)); }
    finally { db.close(); }
    const retried = ok(store.register(hostA, { session: b, displayName: 'Bee again' }));
    assert.equal(retried.subscription.cursor, old.sequence);
    assert.equal(retried.displayName, 'Bee again');
    assert.ok(retried.lastSeenAt! > 1);
    assert.deepEqual(store.pending(hostA, 1).map(frame => frame.message.id), [current.id]);
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    const resumed = ok(store.register(hostA, { session: b }));
    assert.equal(resumed.subscription.cursor, old.sequence);
    assert.equal(resumed.displayName, 'Bee again');
    assert.deepEqual(store.listSessions().find(result => result.subscription.id === sessionKey(b)), resumed);
    assert.deepEqual(store.pending(hostA, 1).map(frame => frame.message.id), [current.id]);
    assert.equal(ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: current.id })).cursor, current.sequence);
    assert.deepEqual(store.pending(hostA, 10), []);
    assert.throws(() => createCoordinatorStore(path, 'other'), /namespace mismatch/);
  } finally { store.close(); }
});

test('opening a pre-presence sessions table retains bindings and upgrades metadata', async t => {
  const path = await fileStore(t);
  const db = new DatabaseSync(path);
  try {
    db.exec(`CREATE TABLE sessions (
      id TEXT PRIMARY KEY, host_id TEXT NOT NULL, display_name TEXT,
      initial_cursor INTEGER NOT NULL, cursor INTEGER NOT NULL, offered INTEGER
    )`);
    db.prepare('INSERT INTO sessions (id, host_id, display_name, initial_cursor, cursor) VALUES (?, ?, ?, ?, ?)')
      .run(sessionKey(a), hostA.hostId, 'Original', 0, 0);
    db.exec(`CREATE TABLE messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL,
      sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL,
      data TEXT NOT NULL, visibility TEXT NOT NULL
    )`);
    db.prepare('INSERT INTO messages (timestamp, sender, recipient, body, data, visibility) VALUES (?, ?, ?, ?, ?, ?)')
      .run(Date.now(), sessionKey(a), sessionKey(a), '[[legacy]]', 'null', 'private');
  } finally { db.close(); }
  const store = createCoordinatorStore(path, 'fleet');
  try {
    assert.equal(store.listSessions()[0]?.displayName, 'Original');
    assert.equal(store.listSessions()[0]?.rank, 1);
    const retry = ok(store.register(hostA, { session: a }));
    assert.equal(retry.subscription.cursor, 0);
    assert.equal(retry.displayName, 'Original');
    assert.ok(retry.lastSeenAt! > 0);
    const ref = entityRef('legacy');
    assert.equal((store.entity(ref.id, 0, 10) as { error: string }).error, 'not_found');
    const current = ok(store.send(hostA, a, { recipient: sessionKey(a), body: 'Now [[legacy]]' }));
    assert.equal(ok(store.inbox(hostA, a, 0, 10)).length, 2);
    assert.deepEqual(ok(store.entity(ref.id, 0, 10)), { entity: ref, messages: [current] });
  } finally { store.close(); }
});

test('rank is persistent cross-host metadata without presence, cursor, or delivery effects; expired registrations reset it', async t => {
  const path = await fileStore(t);
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    assert.equal(ok(store.register(hostA, { session: b, displayName: 'Bee' })).rank, 1);
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'acknowledged' }));
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: first.id }));
    const next = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'offered' }));
    const offered = store.pending(hostA, 10);
    const before = store.listSessions().find(row => row.subscription.id === sessionKey(b))!;
    now += 1000;
    const ranked = ok(store.setRank(hostB, b, 5));
    assert.deepEqual(ranked, { ...before, rank: 5 });
    assert.deepEqual(store.pending(hostA, 10), offered);
    assert.equal(store.send(hostB, b, { recipient: '*', body: 'rank grants no ownership' }).ok, false);
    const refreshed = ok(store.register(hostA, { session: b }));
    assert.deepEqual(refreshed, { ...ranked, lastSeenAt: now });
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.deepEqual(store.listSessions().find(row => row.subscription.id === sessionKey(b)), refreshed);
    assert.equal(ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: next.id })).cursor, next.sequence);
    now += 7 * 24 * 60 * 60 * 1000 + 1;
    const renewed = ok(store.register(hostA, { session: b }));
    assert.equal(renewed.rank, 1);
    assert.equal(renewed.subscription.cursor, next.sequence);
  } finally { store.close(); }
});

test('rank updates validate principals, addresses, and integer range without mutating on failure', () => {
  const store = createCoordinatorStore(':memory:', 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    for (const rank of [1, 2, 3, 4, 5]) assert.equal(ok(store.setRank(hostB, a, rank)).rank, rank);
    const before = store.listSessions();
    for (const rank of [0, 6, -1, 1.5, NaN, Infinity, '2', null, undefined]) {
      assert.deepEqual(store.setRank(hostA, a, rank as number), {
        ok: false, error: 'invalid_request', message: 'Invalid rank update',
      });
    }
    for (const [host, address] of [
      [{ hostId: '' }, a], [null, a], [hostA, null], [hostA, { ...a, namespace: 'other' }],
      [hostA, { ...a, piSessionId: 'not-a-uuid' }],
    ] as const) {
      assert.deepEqual(store.setRank(host as typeof hostA, address as SessionAddress, 2), {
        ok: false, error: 'invalid_request', message: 'Invalid rank update',
      });
    }
    assert.deepEqual(store.setRank(hostA, b, 2), {
      ok: false, error: 'not_found', message: 'Session is not registered',
    });
    assert.deepEqual(store.listSessions(), before);
  } finally { store.close(); }
});

test('broadcast excludes sender, private/public metadata does not alter routing, hosts are isolated', () => {
  const store = createCoordinatorStore(':memory:', 'fleet');
  try {
    register(store);
    const direct = ok(store.send(hostA, a, { recipient: sessionKey(c), body: 'direct' }));
    const broadcast = ok(store.send(hostA, a, { recipient: '*', body: 'all', visibility: 'public', data: { x: 1 } }));
    assert.equal(direct.visibility, 'private');
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.subscriptionId), [sessionKey(b)]);
    assert.deepEqual(store.pending(hostB, 10).map(frame => frame.message.id), [direct.id]);
    assert.deepEqual(ok(store.inbox(hostB, c, 0, 10)).map(message => message.id), [direct.id, broadcast.id]);
    assert.deepEqual(ok(store.inbox(hostA, a, 0, 10)), []);
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)).map(message => message.id), [broadcast.id]);
    assert.deepEqual(store.listSessions().map(result => result.subscription.id), [sessionKey(a), sessionKey(b), sessionKey(c)]);
    assert.equal(store.inbox(hostB, b, 0, 10).ok, false);
  } finally { store.close(); }
});

test('offered receipt only advances the next deliverable item; replay, forged future, old pre-registration, cross-host, and reopen', async t => {
  const path = await fileStore(t);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    const before = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'historical' }));
    ok(store.register(hostA, { session: b }));
    ok(store.register(hostB, { session: c }));
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'first' }));
    const unrelated = ok(store.send(hostA, a, { recipient: sessionKey(c), body: 'other' }));
    const last = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'last' }));
    const receipt = (id: string) => ({ subscriptionId: sessionKey(b), messageId: id });
    assert.equal(store.acknowledge(hostA, receipt(before.id)).ok, false);
    assert.equal(store.acknowledge(hostA, receipt(last.id)).ok, false);
    assert.equal(store.acknowledge(hostB, receipt(first.id)).ok, false);
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [first.id]);
    assert.equal(store.acknowledge(hostA, receipt(last.id)).ok, false);
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.equal(ok(store.acknowledge(hostA, receipt(first.id))).cursor, first.sequence);
    assert.equal(ok(store.acknowledge(hostA, receipt(first.id))).cursor, first.sequence);
    assert.equal(store.acknowledge(hostA, receipt(unrelated.id)).ok, false);
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [last.id]);
    assert.equal(ok(store.acknowledge(hostA, receipt(last.id))).cursor, last.sequence);
    assert.equal(ok(store.acknowledge(hostA, receipt(first.id))).cursor, last.sequence);
    assert.deepEqual(store.pending(hostA, 10), []);
  } finally { store.close(); }
});

test('pending visits independent subscriptions fairly even when one remains unacknowledged', () => {
  const store = createCoordinatorStore(':memory:', 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    ok(store.register(hostA, { session: b }));
    ok(store.register(hostA, { session: c }));
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'b' }));
    const second = ok(store.send(hostA, a, { recipient: sessionKey(c), body: 'c' }));
    const offered = [store.pending(hostA, 1)[0]?.message.id, store.pending(hostA, 1)[0]?.message.id,
      store.pending(hostA, 1)[0]?.message.id];
    // Polling another host must not reset this host's round-robin position.
    ok(store.register(hostB, { session: { namespace: 'fleet', piSessionId: '550e8400-e29b-41d4-a716-446655440004' } }));
    const interleaved = Array.from({ length: 3 }, () => {
      const frame = store.pending(hostA, 1)[0];
      store.pending(hostB, 1);
      return frame?.message.id;
    });
    assert.deepEqual(new Set(offered), new Set([first.id, second.id]));
    assert.deepEqual(new Set(interleaved), new Set([first.id, second.id]));
  } finally { store.close(); }
});

test('claim is atomic for competing owners, renewal, zero TTL, expiry takeover, owner release, and reopen', async t => {
  const path = await fileStore(t);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    register(store);
    assert.equal(ok(store.claim(hostA, 'work', sessionKey(a), 30, 'run')).acquired, true);
    assert.equal(ok(store.claim(hostA, 'work', sessionKey(b), 30)).acquired, false);
    assert.equal(ok(store.claim(hostA, 'work', sessionKey(a), 0)).claim.expiresAt, null);
    assert.equal(ok(store.claim(hostA, 'work', sessionKey(b), 30)).acquired, false);
    assert.equal(ok(store.releaseClaim(hostA, 'work', sessionKey(b))), false);
    assert.equal(ok(store.releaseClaim(hostA, 'work', sessionKey(a))), true);
    assert.equal(ok(store.claim(hostA, 'work', sessionKey(b), 30)).acquired, true);
    ok(store.claim(hostA, 'permanent', sessionKey(a), 0));
    assert.deepEqual(store.listClaims().map(claim => claim.key), ['permanent', 'work']);
    store.close();
    const db = new DatabaseSync(path);
    try { db.prepare('UPDATE claims SET expires_at = ? WHERE key = ?').run(Date.now() - 1, 'work'); }
    finally { db.close(); }
    store = createCoordinatorStore(path, 'fleet');
    const takeover = ok(store.claim(hostA, 'work', sessionKey(a), 1));
    assert.equal(takeover.acquired, true);
    assert.equal(takeover.claim.owner, sessionKey(a));
    assert.equal(store.claim(hostB, 'work', sessionKey(a), 1).ok, false);
    assert.equal(ok(store.releaseClaim(hostA, 'work', sessionKey(a))), true);
    ok(store.claim(hostA, 'expired', sessionKey(a), 1));
    const expiredDb = new DatabaseSync(path);
    try { expiredDb.prepare('UPDATE claims SET expires_at = 1 WHERE key = ?').run('expired'); }
    finally { expiredDb.close(); }
    assert.deepEqual(store.listClaims().map(claim => claim.key), ['permanent']);
  } finally { store.close(); }
});

test('retained offline subscriptions receive and replay until seven days unseen, then expire without future broadcasts', async t => {
  const path = await fileStore(t);
  const week = 7 * 24 * 60 * 60 * 1000;
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    register(store);
    now += 91_000;
    ok(store.register(hostA, { session: a }));
    const broadcast = ok(store.send(hostA, a, { recipient: '*', body: 'offline Pi still gets this' }));
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [broadcast.id]);
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [broadcast.id]);
    now += week - 91_000;
    ok(store.register(hostA, { session: a }));
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [broadcast.id]);
    now += 60_001;
    assert.deepEqual(store.pending(hostA, 10), []);
    assert.deepEqual(store.pending(hostB, 10), []);
    assert.deepEqual(store.listSessions().map(row => row.subscription.id), [sessionKey(a)]);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: broadcast.id }).ok, false);
    const future = ok(store.send(hostA, a, { recipient: '*', body: 'not retained for dead sessions' }));
    assert.deepEqual(store.pending(hostA, 10), []);
    const resumed = ok(store.register(hostA, { session: b }));
    assert.equal(resumed.subscription.cursor, future.sequence);
    assert.deepEqual(store.pending(hostA, 10), []);
    now += 60_001;
    // Expired recipients no longer pin their unacknowledged broadcast history.
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)).map(message => message.id), [future.id]);
  } finally { store.close(); }
});

test('collection protects relevant unacknowledged deliveries, bounds history, and preserves latest receipt proof and sequence', async t => {
  const path = await fileStore(t);
  const week = 7 * 24 * 60 * 60 * 1000;
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    register(store);
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'first acknowledged' }));
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: first.id }));
    const last = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'latest acknowledged' }));
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: last.id }));
    const direct = ok(store.send(hostA, a, { recipient: sessionKey(c), body: 'still needed direct' }));
    const broadcast = ok(store.send(hostA, a, { recipient: '*', body: 'still needed broadcast' }));
    // Keep all subscriptions retained; the sender's irrelevant cursor stays at zero.
    now += week;
    register(store);
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)).map(message => message.id), [first.id, last.id, broadcast.id]);
    now += 60_001;
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)).map(message => message.id), [broadcast.id]);
    assert.deepEqual(store.pending(hostB, 10).map(frame => frame.message.id), [direct.id]);
    assert.equal(ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: last.id })).cursor, last.sequence);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: first.id }).ok, false);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(a), messageId: last.id }).ok, false);
    ok(store.acknowledge(hostB, { subscriptionId: sessionKey(c), messageId: direct.id }));
    store.pending(hostB, 10);
    ok(store.acknowledge(hostB, { subscriptionId: sessionKey(c), messageId: broadcast.id }));
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: broadcast.id }));
    now += 60_001;
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)), []);
    const db = new DatabaseSync(path);
    try { assert.equal((db.prepare('SELECT count(*) AS count FROM messages').get() as { count: number }).count, 0); }
    finally { db.close(); }
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.equal(ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: broadcast.id })).cursor, broadcast.sequence);
    assert.equal(store.acknowledge(hostB, { subscriptionId: sessionKey(b), messageId: broadcast.id }).ok, false);
    const newcomer = { ...a, piSessionId: '550e8400-e29b-41d4-a716-446655440004' };
    assert.equal(ok(store.register(hostA, { session: newcomer })).subscription.cursor, broadcast.sequence);
    // A tail snapshot is not proof that this new subscription received that ID.
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(newcomer), messageId: broadcast.id }).ok, false);
    const next = ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'new sequence' }));
    assert.equal(next.sequence, broadcast.sequence + 1);
    assert.deepEqual(store.pending(hostA, 10).map(frame => frame.message.id), [next.id]);
  } finally { store.close(); }
});

function entityRef(label: string, type = 0, namespace = 'fleet') {
  const hash = createHash('sha256').update(JSON.stringify([namespace, 0, label])).digest('hex').slice(0, 32);
  return { id: `${namespace}:entity:00:${hash}`, type, label };
}

test('entity metadata changes preserve identity, deduplicated history, routing, and persisted state', async t => {
  const path = await fileStore(t);
  let store = createCoordinatorStore(path, 'fleet');
  const generic = entityRef('Café');
  const byte = entityRef('Café', 255);
  try {
    register(store);
    ok(store.setRank(hostA, b, 3));
    ok(store.claim(hostA, 'work', sessionKey(a), 0));
    const sessions = store.listSessions();
    const claims = store.listClaims();
    const body = 'Discuss [[ Cafe\u0301 ]] and [[Café]] with [[7:Café]] and [[255:Café]].';
    const data = { untouched: '[[data only]]' };
    const direct = ok(store.send(hostA, a, { recipient: sessionKey(c), body, data }));
    const broadcast = ok(store.send(hostA, b, { recipient: '*', body: 'Again [[Café]]', visibility: 'public' }));
    ok(store.send(hostA, a, { recipient: sessionKey(b), body: 'No marker: Café' }));
    assert.equal(direct.body, body);
    assert.deepEqual(direct.data, data);
    assert.deepEqual(Object.keys(direct).sort(), ['body', 'data', 'id', 'recipient', 'sender', 'sequence', 'timestamp', 'version', 'visibility']);
    assert.deepEqual(ok(store.entity(generic.id, 0, 10)), { entity: byte, messages: [direct, broadcast] });
    assert.deepEqual(ok(store.entity(generic.id, 0, 1)).messages, [direct]);
    assert.deepEqual(ok(store.entity(generic.id, direct.sequence, 1)).messages, [broadcast]);
    assert.deepEqual(ok(store.entity(generic.id, broadcast.sequence, 10)), { entity: byte, messages: [] });
    assert.deepEqual(store.listSessions(), sessions);
    assert.deepEqual(store.listClaims(), claims);
    assert.deepEqual(store.pending(hostB, 10), [{ type: 'message', subscriptionId: sessionKey(c), message: direct }]);
    assert.deepEqual(ok(store.inbox(hostA, a, 0, 10)), [broadcast]);
    assert.deepEqual(ok(store.inbox(hostB, c, 0, 10)), [direct, broadcast]);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(c), messageId: direct.id }).ok, false);
    assert.equal(ok(store.acknowledge(hostB, { subscriptionId: sessionKey(c), messageId: direct.id })).cursor, direct.sequence);
    assert.deepEqual(ok(store.entity(entityRef('data only').id, 0, 10)), { entity: entityRef('data only'), messages: [direct] });
    for (const ref of [entityRef('café')]) {
      assert.equal(store.entity(ref.id, 0, 10).ok, false);
    }
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.deepEqual(ok(store.entity(generic.id, 0, 10)), { entity: byte, messages: [direct, broadcast] });
    const update = ok(store.send(hostA, a, { recipient: '*', body: '[[7:Café]] [[Café]]' }));
    assert.deepEqual(ok(store.entity(generic.id, 0, 10)), { entity: { ...generic, type: 7 }, messages: [direct, broadcast, update] });
    const reset = ok(store.send(hostA, a, { recipient: '*', body: '[[0:Café]] [[Café]]' }));
    assert.deepEqual(ok(store.entity(generic.id, 0, 10)), { entity: generic, messages: [direct, broadcast, update, reset] });
    const db = new DatabaseSync(path);
    try {
      assert.equal(db.prepare('SELECT count(*) AS count FROM entities').get()!.count, 2);
      assert.equal(db.prepare('SELECT count(*) AS count FROM message_entities').get()!.count, 5);
    } finally { db.close(); }
  } finally { store.close(); }
});

test('legacy type-keyed entities merge atomically, retain old ID aliases, and preserve all history on reopen', async t => {
  const path = await fileStore(t);
  let store = createCoordinatorStore(path, 'fleet');
  register(store);
  const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[alice]] [[7:alice]]' }));
  const second = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[7:alice]] [[255:alice]]' }));
  const third = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[alice]]' }));
  const sessions = store.listSessions();
  store.close();
  const legacy = (type: number) => {
    const hash = createHash('sha256').update(JSON.stringify(['fleet', type, 'alice'])).digest('hex').slice(0, 32);
    return `fleet:entity:${type.toString(16).padStart(2, '0')}:${hash}`;
  };
  const db = new DatabaseSync(path);
  try {
    db.exec(`DELETE FROM message_entities; DELETE FROM entities;
      DROP TABLE store_config; CREATE TABLE store_config (namespace TEXT NOT NULL);
      INSERT INTO store_config VALUES ('fleet')`);
    for (const type of [0, 7, 255]) db.prepare('INSERT INTO entities (id, type, label) VALUES (?, ?, ?)').run(legacy(type), type, 'alice');
    for (const [type, sequence] of [[0, first.sequence], [7, first.sequence], [7, second.sequence],
      [255, second.sequence], [0, third.sequence]]) {
      db.prepare('INSERT INTO message_entities VALUES (?, ?)').run(legacy(type!), sequence!);
    }
    // A bad canonical label must roll back the entire migration, including its version marker.
    db.prepare('UPDATE entities SET label = ? WHERE id = ?').run('different label', legacy(0));
    assert.throws(() => createCoordinatorStore(path, 'fleet'), /collision/);
    assert.equal(db.prepare('SELECT count(*) AS count FROM entities').get()!.count, 3);
    assert.equal(db.prepare('SELECT count(*) AS count FROM message_entities').get()!.count, 5);
    assert.equal(db.prepare('SELECT count(*) AS count FROM entity_aliases').get()!.count, 0);
    db.prepare('UPDATE entities SET label = ? WHERE id = ?').run('alice', legacy(0));
  } finally { db.close(); }
  store = createCoordinatorStore(path, 'fleet');
  try {
    for (const type of [0, 7, 255]) {
      assert.deepEqual(ok(store.entity(legacy(type), 0, 10)), { entity: entityRef('alice', 255), messages: [first, second, third] });
    }
    assert.deepEqual(store.listSessions(), sessions);
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)), [first, second, third]);
    const reset = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[0:alice]]' }));
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.deepEqual(ok(store.entity(legacy(255), 0, 10)), { entity: entityRef('alice'), messages: [first, second, third, reset] });
    const check = new DatabaseSync(path);
    try {
      assert.equal(check.prepare('SELECT count(*) AS count FROM entities').get()!.count, 1);
      assert.equal(check.prepare('SELECT count(*) AS count FROM message_entities').get()!.count, 4);
      assert.deepEqual(check.prepare('PRAGMA foreign_key_check').all(), []);
    } finally { check.close(); }
  } finally { store.close(); }
});

test('session names, JSON strings, claims, and releases share metadata without read-side status replay or identity changes', async t => {
  const path = await fileStore(t);
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let store = createCoordinatorStore(path, 'fleet');
  try {
    register(store);
    const before = store.listSessions().find(row => row.subscription.id === sessionKey(a))!;
    const identity = ok(store.register(hostA, { session: a, displayName: '[[0:FLEET]]' }));
    assert.deepEqual(identity.subscription, before.subscription);
    assert.equal(identity.rank, before.rank);
    assert.deepEqual(identity.entities, [entityRef('FLEET')]);
    assert.deepEqual(ok(store.entity(entityRef('FLEET').id, 0, 10)), { entity: entityRef('FLEET'), messages: [] });
    const body = 'Hey Jerry, did you see the [[153:Meteor Shower]] last night?';
    const data = { nested: ['[[7:FLEET]]', { '[[4:object key]]': '[[154:Meteor Shower]]' }], flag: true };
    const message = ok(store.send(hostA, a, { recipient: sessionKey(b), body, data }));
    assert.equal(message.body, body);
    assert.deepEqual(message.data, data);
    assert.equal(ok(store.entity(entityRef('Meteor Shower').id, 0, 10)).entity.type, 154);
    const identityAgain = ok(store.register(hostA, { session: a }));
    assert.deepEqual(identityAgain.entities, [entityRef('FLEET', 7)], 'heartbeats do not reapply the old integer');
    assert.deepEqual(identityAgain.subscription, identity.subscription);
    assert.deepEqual(ok(store.register(hostA, { session: a, displayName: '[[0:FLEET]]' })).entities,
      [entityRef('FLEET')], 'an explicit name update reasserts its integer even when its text is unchanged');
    const claimKey = 'Observe [[153:Meteor Shower]]';
    const claimed = ok(store.claim(hostA, claimKey, sessionKey(a), 0, 'Ask [[8:FLEET]] and [[1:claim only]]'));
    assert.equal(claimed.acquired, true);
    assert.equal(claimed.claim.key, claimKey);
    assert.ok(claimed.claim.entities?.some(entity => entity.id === entityRef('FLEET').id && entity.type === 8));
    assert.equal(ok(store.claim(hostA, claimKey, sessionKey(b), 0, '[[99:FLEET]]')).acquired, false);
    assert.equal(ok(store.releaseClaim(hostA, claimKey, sessionKey(b))), false);
    ok(store.send(hostA, a, { recipient: '*', body: '[[200:Meteor Shower]]' }));
    store.listSessions();
    store.listClaims();
    assert.deepEqual(ok(store.inbox(hostA, b, 0, 1)), [message], 'inbox preserves the original text and nested payload');
    assert.equal(ok(store.entity(entityRef('Meteor Shower').id, 0, 10)).entity.type, 200, 'reads do not replay old integers');
    assert.equal(ok(store.entity(entityRef('FLEET').id, 0, 10)).entity.type, 8, 'failed mutations do not change metadata');
    assert.equal(ok(store.releaseClaim(hostA, claimKey, sessionKey(a))), true);
    assert.equal(ok(store.entity(entityRef('Meteor Shower').id, 0, 10)).entity.type, 153, 'successful release processes its key');
    assert.deepEqual(store.listClaims(), []);
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    assert.deepEqual(store.listSessions().find(row => row.subscription.id === sessionKey(a))!.entities, [entityRef('FLEET', 8)]);
    assert.equal(ok(store.entity(entityRef('claim only').id, 0, 10)).entity.type, 1, 'released references survive for the retention window');
    // Active identity/claim associations protect entities even after their last explicit mention ages out.
    ok(store.claim(hostA, 'keep', sessionKey(a), 0, '[[2:held]]'));
    now += 7 * 24 * 60 * 60 * 1000;
    ok(store.register(hostA, { session: a }));
    now += 60_001;
    assert.deepEqual(store.listSessions().find(row => row.subscription.id === sessionKey(a))!.entities, [entityRef('FLEET', 8)]);
    assert.equal(ok(store.entity(entityRef('held').id, 0, 10)).entity.type, 2);
    assert.equal(store.entity(entityRef('claim only').id, 0, 10).ok, false);
  } finally { store.close(); }
});

test('entity extraction accepts message data at the existing JSON depth limit', () => {
  const store = createCoordinatorStore(':memory:', 'fleet');
  try {
    register(store);
    let data: import('../../src/protocol/index.ts').JsonValue = '[[153:Meteor Shower]]';
    for (let i = 0; i < 32; i++) data = [data];
    const message = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[7:Meteor Shower]]', data }));
    assert.deepEqual(ok(store.entity(entityRef('Meteor Shower').id, 0, 10)),
      { entity: entityRef('Meteor Shower', 153), messages: [message] });
  } finally { store.close(); }
});

test('entity queries validate identity and pagination; failed sends and identity collisions roll back registry and message writes', async t => {
  const path = await fileStore(t);
  const store = createCoordinatorStore(path, 'fleet');
  const ref = entityRef('shared');
  try {
    register(store);
    for (const id of ['', null, sessionKey(a), entityRef('shared', 0, 'other').id]) {
      assert.equal((store.entity(id as string, 0, 10) as { error: string }).error, 'invalid_request');
    }
    for (const [since, limit] of [[-1, 10], [0.5, 10], [NaN, 10], [Infinity, 10], [0, 0], [0, 501], [0, 1.5]]) {
      assert.equal((store.entity(ref.id, since!, limit!) as { error: string }).error, 'invalid_request');
    }
    assert.equal((store.entity(ref.id, 0, 10) as { error: string }).error, 'not_found');
    assert.equal(store.send(hostB, a, { recipient: '*', body: '[[shared]]' }).ok, false);
    assert.equal(store.send(hostA, a, { recipient: 'invalid', body: '[[shared]]' }).ok, false);
    assert.equal((store.entity(ref.id, 0, 10) as { error: string }).error, 'not_found');
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[shared]]' }));
    const db = new DatabaseSync(path);
    try {
      // Simulate the otherwise impractical digest collision at the persisted identity boundary.
      for (const [type, label] of [[ref.type, 'different label']] as const) {
        db.prepare('UPDATE entities SET type = ?, label = ? WHERE id = ?').run(type, label, ref.id);
        assert.throws(() => store.send(hostA, a, { recipient: sessionKey(b), body: '[[new before collision]] [[shared]]' }), /collision/i);
        assert.equal((store.entity(entityRef('new before collision').id, 0, 10) as { error: string }).error, 'not_found');
        assert.deepEqual(ok(store.inbox(hostA, b, 0, 10)), [first]);
        assert.equal(db.prepare('SELECT count(*) AS count FROM message_entities').get()!.count, 1);
      }
      db.prepare('UPDATE entities SET type = ?, label = ? WHERE id = ?').run(ref.type, ref.label, ref.id);
      assert.deepEqual(ok(store.entity(ref.id, 0, 10)), { entity: ref, messages: [first] });
    } finally { db.close(); }
  } finally { store.close(); }
});

test('entity retention follows message collection, preserves pending mentions, and recreates collected identities', async t => {
  const path = await fileStore(t);
  const week = 7 * 24 * 60 * 60 * 1000;
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let store = createCoordinatorStore(path, 'fleet');
  const shared = entityRef('shared');
  const gone = entityRef('gone', 255);
  try {
    register(store);
    const first = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[shared]] [[255:gone]]' }));
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: first.id }));
    const pending = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[shared]]' }));
    now += week;
    register(store);
    now += 60_001;
    assert.deepEqual(ok(store.entity(shared.id, 0, 10)), { entity: shared, messages: [pending] });
    assert.equal((store.entity(gone.id, 0, 10) as { error: string }).error, 'not_found');
    store.pending(hostA, 10);
    ok(store.acknowledge(hostA, { subscriptionId: sessionKey(b), messageId: pending.id }));
    now += 60_001;
    assert.equal((store.entity(shared.id, 0, 10) as { error: string }).error, 'not_found');
    const db = new DatabaseSync(path);
    try {
      assert.equal(db.prepare('SELECT count(*) AS count FROM message_entities').get()!.count, 0);
      assert.equal(db.prepare('SELECT count(*) AS count FROM entities').get()!.count, 0);
    } finally { db.close(); }
    store.close();
    store = createCoordinatorStore(path, 'fleet');
    const again = ok(store.send(hostA, a, { recipient: sessionKey(b), body: '[[255:gone]]' }));
    assert.deepEqual(ok(store.entity(gone.id, 0, 10)), { entity: gone, messages: [again] });
    assert.equal(again.sequence, pending.sequence + 1);
  } finally { store.close(); }
});

test('store validates direct public calls without relying on HTTP', () => {
  const store = createCoordinatorStore(':memory:', 'fleet');
  try {
    ok(store.register(hostA, { session: a }));
    const foreign = { ...a, namespace: 'other' };
    assert.equal(store.register(hostA, { session: foreign }).ok, false);
    assert.equal(store.send(hostA, a, { recipient: sessionKey(a), body: '' }).ok, false);
    assert.equal(store.send(hostA, a, { recipient: 'other:550e8400-e29b-41d4-a716-446655440002', body: 'x' }).ok, false);
    assert.equal(store.send(hostB, a, { recipient: '*', body: 'x' }).ok, false);
    assert.equal(store.inbox(hostA, a, -1, 10).ok, false);
    assert.equal(store.claim(hostA, 'x', sessionKey(a), -1).ok, false);
    assert.equal(store.releaseClaim(hostA, '', sessionKey(a)).ok, false);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(a), messageId: 'fleet:msg:0' }).ok, false);
    assert.equal(store.acknowledge(hostA, null as unknown as Receipt).ok, false);
    assert.equal(store.acknowledge(hostA, { subscriptionId: sessionKey(a), messageId: 'fleet:msg:1', extra: true } as unknown as Receipt).ok, false);
    assert.throws(() => store.pending(hostA, 0), TypeError);
    assert.throws(() => store.pending(null as unknown as { hostId: string }, 1), TypeError);
  } finally { store.close(); }
});

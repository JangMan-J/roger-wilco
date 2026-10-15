/**
 * Host daemon against a minimal fake v2 coordinator (docs/protocol-v2.md, test
 * plan items 1 (byte boundary), 24-36, 39 and 40, daemon side). The fake speaks
 * only the wire: it scripts frames and answers receipts, presence and
 * registration; it is not the real coordinator.
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { createHostClient } from '../../src/host/client.ts';
import { createDaemonInbox, createHostDaemon, type LocalHostInbox } from '../../src/host/index.ts';
import { createCoordinatorStore } from '../../src/server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../../src/server/control/index.ts';
import {
  MAX_PRESENCE_KEYS, MAX_STREAM_EVENT_BYTES, ROUTES, sessionKey,
  type HostConfig, type HostFrame, type Message, type Outcome, type RegistrationResult, type SessionAddress,
} from '../../src/protocol/index.ts';

type TestContext = { after(fn: () => void | Promise<void>): void; mock: { method: typeof test.mock.method } };

const STREAM = '0123456789abcdef0123456789abcdef';
const OTHER_STREAM = 'ffffffffffffffffffffffffffffffff';
const address = (n: number): SessionAddress => ({ namespace: 'fleet', sessionId: `550e8400-e29b-41d4-a716-44665544000${n}` });
const [s1, s2, s3, s4] = [1, 2, 3, 4].map(address) as [SessionAddress, SessionAddress, SessionAddress, SessionAddress];
const key = sessionKey;
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function message(seq: number, sender: SessionAddress, recipient: SessionAddress | '*', body = `m${seq}`): Message {
  return { version: 1, id: `fleet:msg:${seq}`, sequence: seq, timestamp: 1_760_000_000_000, sender: key(sender),
    recipient: recipient === '*' ? '*' : key(recipient), body, data: null, visibility: 'private' };
}
type Hello = Extract<HostFrame, { type: 'hello' }>;
const hello = (resume: number, extra: Partial<Hello> = {}): Hello => ({ type: 'hello', stream: STREAM, host: 'host-a',
  conn: randomUUID(), resume, keepaliveMs: 15_000, presenceWindowMs: 90_000, ...extra });
const member = (session: SessionAddress, start: number): HostFrame => ({ type: 'member', session: key(session), start });
const leave = (session: SessionAddress): HostFrame => ({ type: 'leave', session: key(session) });
const msg = (seq: number, sender: SessionAddress, recipient: SessionAddress | '*', body?: string): HostFrame =>
  ({ type: 'msg', seq, message: message(seq, sender, recipient, body) });
const through = (seq: number): HostFrame => ({ type: 'through', seq });
const sse = (...frames: HostFrame[]) => frames.map(frame => `event: v2\ndata: ${JSON.stringify(frame)}\n\n`).join('');

async function waitFor(check: () => boolean | Promise<boolean>, timeout = 5000, what = 'observable state'): Promise<void> {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await delay(15);
  }
  assert.fail(`Timed out waiting for ${what}`);
}
function peek<T = Record<string, unknown>>(path: string, sql: string, ...params: Array<string | number>): T[] {
  const db = new DatabaseSync(path, { readOnly: true });
  try { return db.prepare(sql).all(...params).map(row => ({ ...row }) as T); } finally { db.close(); }
}
const bookmark = (path: string) => peek<{ stream_id: string | null; bookmark: number | null }>(path, 'SELECT stream_id, bookmark FROM inbox_stream')[0]!;
const rows = (path: string) => peek<{ session_key: string; message_id: string; sequence: number; message_json: string; hash: string }>(path,
  'SELECT session_key, message_id, sequence, message_json, hash FROM inbox_messages ORDER BY sequence, session_key');

type Route = (req: IncomingMessage, res: ServerResponse, body: any, url: URL) => void | Promise<void>;
type Request = { path: string; search: string; body: any; at: number };
function reply(res: ServerResponse, status: number, value: Outcome<unknown>): void {
  if (res.writableEnded || res.destroyed) return;
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(value));
}
const ok = (res: ServerResponse, value: unknown) => reply(res, 200, { ok: true, value });
function openStream(res: ServerResponse): void {
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  res.flushHeaders();
}
const registered = (session: SessionAddress, hostId = 'host-a'): RegistrationResult =>
  ({ subscription: { id: key(session), session, hostId, cursor: 0 }, rank: 1, lastSeenAt: 1 });

type Rig = {
  path: string; inbox: LocalHostInbox; config: HostConfig; log: Request[];
  requests(path: string): Request[]; streams: number;
  start(): void; stop(): Promise<void>;
};
/** A daemon with its own inbox against a fake coordinator. Unknown routes answer 404 not_found, as a v1 coordinator does. */
async function rig(t: TestContext, routes: Record<string, Route>, options: { before?(path: string, inbox: LocalHostInbox): void | Promise<void>; start?: boolean } = {}): Promise<Rig> {
  const dir = mkdtempSync(join(tmpdir(), 'rw-v2-'));
  const path = join(dir, 'inbox.sqlite');
  const inbox = createDaemonInbox(path);
  await options.before?.(path, inbox);
  const log: Request[] = [];
  const defaults: Record<string, Route> = {
    [ROUTES.receiptV2]: (_req, res, body) => ok(res, { through: body.through }),
    [ROUTES.presence]: (_req, res) => ok(res, { unknown: [] }),
    [ROUTES.register]: (_req, res, body) => ok(res, registered(body.session)),
  };
  const table = { ...defaults, ...routes };
  const server: Server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    let raw = '';
    if (req.method === 'POST') for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : undefined;
    log.push({ path: url.pathname, search: url.search, body, at: Date.now() });
    if (req.headers.authorization !== 'Bearer secret') { reply(res, 401, { ok: false, error: 'unauthorized', message: 'Missing credential' }); return; }
    const route = table[url.pathname];
    if (!route) { reply(res, 404, { ok: false, error: 'not_found', message: 'Unknown route' }); return; }
    await route(req, res, body, url);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  const config: HostConfig = { coordinatorUrl: `http://127.0.0.1:${port}`, hostId: 'host-a', credential: 'secret', inboxPath: path };
  let daemon: ReturnType<typeof createHostDaemon> | undefined;
  let run: Promise<void> | undefined;
  const result: Rig = {
    path, inbox, config, log,
    requests: route => log.filter(entry => entry.path === route),
    get streams() { return log.filter(entry => entry.path === ROUTES.streamV2).length; },
    start() {
      daemon = createHostDaemon(config, inbox);
      run = daemon.start(new AbortController().signal);
    },
    async stop() {
      if (!daemon) return;
      const current = daemon;
      daemon = undefined;
      await current.stop(); await run;
    },
  };
  t.after(async () => {
    await result.stop();
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
    await inbox.close();
    rmSync(dir, { recursive: true, force: true });
  });
  if (options.start !== false) result.start();
  return result;
}
/** Collects daemon reports instead of printing them. */
function reports(t: TestContext): Array<{ text: string; at: number }> {
  const seen: Array<{ text: string; at: number }> = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { seen.push({ text: args.map(String).join(' '), at: Date.now() }); });
  return seen;
}
/** Resolves with each new stream response, in order, after opening it. */
function streamQueue() {
  const opened: ServerResponse[] = [];
  const route: Route = (_req, res) => { openStream(res); opened.push(res); };
  return { route, opened, next: async (n: number) => { await waitFor(() => opened.length >= n, 5000, `stream ${n}`); return opened[n - 1]!; } };
}

test('a v2 event of exactly MAX_STREAM_EVENT_BYTES is stored; one byte more aborts the connection', async t => {
  reports(t);
  for (const extra of [0, 1]) {
    const streams = streamQueue();
    const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
    const res = await streams.next(1);
    res.write(sse(hello(0), member(s1, 0)));
    await waitFor(() => bookmark(daemon.path).bookmark === 0);
    const frame = { type: 'msg', seq: 1, message: { ...message(1, s2, s1), data: '' } } as const;
    const pad = MAX_STREAM_EVENT_BYTES + extra - Buffer.byteLength(sse(frame));
    const sized = sse({ ...frame, message: { ...frame.message, data: 'x'.repeat(pad) } });
    assert.equal(Buffer.byteLength(sized), MAX_STREAM_EVENT_BYTES + extra);
    res.write(sized);
    if (extra === 0) {
      await waitFor(() => daemon.requests(ROUTES.receiptV2).length === 1);
      assert.equal(daemon.requests(ROUTES.receiptV2)[0]!.body.through, 1);
      assert.deepEqual(rows(daemon.path).map(row => row.message_id), ['fleet:msg:1']);
    } else {
      await waitFor(() => daemon.streams === 2);
      assert.deepEqual(rows(daemon.path), []);
      assert.equal(bookmark(daemon.path).bookmark, 0);
      assert.equal(daemon.requests(ROUTES.receiptV2).length, 0);
    }
  }
});

test('one chunk of frames is one FULL transaction; the rows and bookmark are visible before the receipt arrives', async t => {
  const streams = streamQueue();
  let atReceipt: { rows: number; bookmark: number | null } | undefined;
  const daemon = await rig(t, {
    [ROUTES.streamV2]: streams.route,
    [ROUTES.receiptV2]: (_req, res, body) => {
      atReceipt ??= { rows: rows(daemon.path).length, bookmark: bookmark(daemon.path).bookmark };
      ok(res, { through: body.through });
    },
  });
  const res = await streams.next(1);
  const first = hello(0);
  res.write(sse(first));
  await waitFor(() => bookmark(daemon.path).bookmark === 0);
  const levels: number[] = [];
  const originalExec = DatabaseSync.prototype.exec;
  DatabaseSync.prototype.exec = function (this: DatabaseSync, sql: string) {
    if (sql === 'COMMIT') levels.push((this.prepare('PRAGMA synchronous').get() as { synchronous: number }).synchronous);
    return originalExec.call(this, sql);
  };
  try {
    res.write(sse(member(s1, 0), msg(1, s2, s1), msg(2, s2, s1), msg(3, s2, s1), msg(4, s2, s1), through(9)));
    await waitFor(() => daemon.requests(ROUTES.receiptV2).length === 1);
  } finally { DatabaseSync.prototype.exec = originalExec; }
  assert.deepEqual(levels, [2], 'one commit, at synchronous=FULL');
  assert.deepEqual(daemon.requests(ROUTES.receiptV2).map(entry => entry.body), [{ conn: first.conn, through: 9 }]);
  assert.deepEqual(atReceipt, { rows: 4, bookmark: 9 });
});

test('a broadcast is copied to every eligible member except its sender, with the coordinator bytes and hash', async t => {
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  const receipts = () => daemon.requests(ROUTES.receiptV2).map(entry => entry.body.through);
  const res = await streams.next(1);
  const broadcast = msg(5, s1, '*');
  res.write(sse(hello(0), member(s1, 0), member(s2, 0), member(s3, 10), broadcast));
  await waitFor(() => receipts().includes(5));
  const json = JSON.stringify((broadcast as { message: Message }).message);
  assert.deepEqual(rows(daemon.path), [{ session_key: key(s2), message_id: 'fleet:msg:5', sequence: 5, message_json: json,
    hash: createHash('sha256').update(json).digest('hex') }]);
  assert.equal(JSON.parse(rows(daemon.path)[0]!.message_json).recipient, '*');
  // No eligible member: s2 left, s3 starts after 6, s1 sent it. Nothing is stored and D still advances.
  res.write(sse(leave(s2), msg(6, s1, '*')));
  await waitFor(() => receipts().includes(6));
  assert.equal(rows(daemon.path).length, 1);
  assert.equal(bookmark(daemon.path).bookmark, 6);
  // A later broadcast reaches the remaining members only; the left session gets no copy.
  res.write(sse(msg(12, s4, '*')));
  await waitFor(() => receipts().includes(12));
  assert.deepEqual(rows(daemon.path).filter(row => row.sequence === 12).map(row => row.session_key), [key(s1), key(s3)]);
});

test('a member that joins mid-stream receives the messages after its start, in order', async t => {
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  const res = await streams.next(1);
  res.write(sse(hello(50)));
  await waitFor(() => bookmark(daemon.path).bookmark === 50);
  res.write(sse(member(s1, 99)));
  await delay(50);
  res.write(sse(msg(100, s2, '*'), msg(101, s2, s1)));
  await waitFor(() => daemon.requests(ROUTES.receiptV2).some(entry => entry.body.through === 101));
  assert.equal((await daemon.inbox.bind(s1, 'owner', Date.now())).ok, true);
  assert.deepEqual((await daemon.inbox.pending(s1, 'owner')).map(item => item.id), ['fleet:msg:100', 'fleet:msg:101']);
});

test('frames at or below D are checked and skipped, even when the local row was swept; resume above D raises D at hello', async t => {
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  const receipts = () => daemon.requests(ROUTES.receiptV2).map(entry => entry.body.through);
  const first = await streams.next(1);
  first.write(sse(hello(0), member(s1, 0), msg(1, s2, s1), msg(2, s2, s1), msg(3, s2, s1), through(120)));
  await waitFor(() => receipts().includes(120));
  // The acknowledged row 2 was swept locally.
  const writer = new DatabaseSync(daemon.path);
  writer.prepare("DELETE FROM inbox_messages WHERE message_id = 'fleet:msg:2'").run();
  writer.close();
  first.end();
  const second = await streams.next(2);
  assert.equal(daemon.requests(ROUTES.streamV2)[1]!.search, `?stream=${STREAM}&after=120`);
  // The coordinator's copy lags: it resumes at 1 and replays 2 and 3.
  second.write(sse(hello(1), member(s1, 0), msg(2, s2, s1), msg(3, s2, s1), through(120), msg(121, s2, s1)));
  await waitFor(() => receipts().includes(121));
  assert.deepEqual(rows(daemon.path).map(row => row.sequence), [1, 3, 121], 'the swept row does not come back');
  assert.equal(bookmark(daemon.path).bookmark, 121);
  second.end();
  const third = await streams.next(3);
  assert.equal(daemon.requests(ROUTES.streamV2)[2]!.search, `?stream=${STREAM}&after=121`);
  third.write(sse(hello(200)));
  await waitFor(() => bookmark(daemon.path).bookmark === 200);
  assert.equal(receipts().length, 2, 'the hello alone posts no receipt');
});

type ViolationCase = {
  name: string; report: RegExp; resume?: number; first?: (resume: number) => Hello;
  chunk?: string; before?(path: string, inbox: LocalHostInbox): void | Promise<void>;
  expectRows?: number; expectBookmark: { stream_id: string | null; bookmark: number | null };
};
const violations: ViolationCase[] = [
  { name: 'a position not above the previous one', report: /Position 1 is not above 1/,
    chunk: sse(member(s1, 0), msg(1, s2, s1), msg(1, s2, s1)), expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'a position not above hello.resume', report: /Position 5 is not above 5/, resume: 5,
    chunk: sse(member(s1, 0), through(5)), expectBookmark: { stream_id: STREAM, bookmark: 5 } },
  { name: 'invalid JSON', report: /Invalid v2 frame JSON/,
    chunk: `${sse(member(s1, 0), msg(1, s2, s1))}event: v2\ndata: {broken\n\n`, expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'an unknown type', report: /Invalid v2 frame/,
    chunk: `${sse(member(s1, 0), msg(1, s2, s1))}event: v2\ndata: {"type":"pause"}\n\n`, expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'an oversized event', report: /SSE event exceeds byte limit/,
    chunk: `event: v2\ndata: ${'x'.repeat(MAX_STREAM_EVENT_BYTES)}\n\n`, expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'a directed message to a non-member', report: /Directed message fleet:msg:1 for a non-member/,
    chunk: sse(msg(1, s2, s1)), expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'a hash conflict on a dedup key', report: new RegExp(`Message identity conflict: session ${key(s1)}, message fleet:msg:1`),
    before: async (_path, inbox) => { await inbox.receive({ type: 'message', subscriptionId: key(s1), message: message(1, s2, s1, 'original') }); },
    chunk: sse(member(s1, 0), msg(1, s2, s1, 'different')), expectRows: 1, expectBookmark: { stream_id: STREAM, bookmark: 0 } },
  { name: 'a hello.host mismatch', report: /hello.host does not match/, first: resume => hello(resume, { host: 'host-b' }),
    expectBookmark: { stream_id: null, bookmark: null } },
  { name: 'a hello.stream mismatch', report: /hello.stream does not match/,
    before: path => { const db = new DatabaseSync(path); db.exec(`UPDATE inbox_stream SET stream_id = '${OTHER_STREAM}', bookmark = 7`); db.close(); },
    expectBookmark: { stream_id: OTHER_STREAM, bookmark: 7 } },
];
for (const item of violations) {
  test(`violation, ${item.name}: no commit, no receipt, D unchanged, reconnect with backoff`, async t => {
    const seen = reports(t);
    const streams = streamQueue();
    const daemon = await rig(t, { [ROUTES.streamV2]: streams.route }, { before: item.before });
    const before = bookmark(daemon.path);
    const res = await streams.next(1);
    const resume = item.resume ?? 0;
    res.write(sse((item.first ?? hello)(resume)));
    if (item.chunk !== undefined) {
      await waitFor(() => bookmark(daemon.path).bookmark === Math.max(before.bookmark ?? 0, resume));
      res.write(item.chunk);
    }
    await streams.next(2);
    const report = seen.find(entry => item.report.test(entry.text));
    assert.ok(report, `reported: ${seen.map(entry => entry.text).join(' | ')}`);
    assert.ok(daemon.requests(ROUTES.streamV2)[1]!.at - report.at >= 200, 'the reconnect waits for the backoff');
    assert.equal(rows(daemon.path).length, item.expectRows ?? 0);
    assert.deepEqual(bookmark(daemon.path), item.expectBookmark);
    assert.equal(daemon.requests(ROUTES.receiptV2).length, 0);
    const { stream_id: stream, bookmark: after } = item.expectBookmark;
    assert.equal(daemon.requests(ROUTES.streamV2)[1]!.search, after === null ? '' : `?stream=${stream}&after=${after}`);
  });
}

test('the receipt poster keeps one POST in flight and coalesces to the latest committed position', async t => {
  const streams = streamQueue();
  let release = () => {};
  const held = new Promise<void>(resolve => { release = resolve; });
  let inFlight = 0, most = 0;
  const daemon = await rig(t, {
    [ROUTES.streamV2]: streams.route,
    [ROUTES.receiptV2]: async (_req, res, body) => {
      most = Math.max(most, ++inFlight);
      if (body.through === 1) await held;
      inFlight--;
      ok(res, { through: body.through });
    },
  });
  const res = await streams.next(1);
  res.write(sse(hello(0), member(s1, 0), msg(1, s2, s1)));
  await waitFor(() => daemon.requests(ROUTES.receiptV2).length === 1);
  for (const seq of [2, 3]) {
    res.write(sse(msg(seq, s2, s1)));
    await waitFor(() => bookmark(daemon.path).bookmark === seq);
  }
  await delay(100);
  assert.equal(daemon.requests(ROUTES.receiptV2).length, 1, 'no second POST while the first is in flight');
  release();
  await waitFor(() => daemon.requests(ROUTES.receiptV2).length === 2);
  await delay(100);
  assert.deepEqual(daemon.requests(ROUTES.receiptV2).map(entry => entry.body.through), [1, 3]);
  assert.equal(most, 1);
});

test('the receipt poster retries on 503 and on a transport error without leaving the connection', async t => {
  reports(t);
  const streams = streamQueue();
  let posts = 0;
  const daemon = await rig(t, {
    [ROUTES.streamV2]: streams.route,
    [ROUTES.receiptV2]: (_req, res, body) => {
      posts++;
      if (posts === 1) reply(res, 503, { ok: false, error: 'temporarily_unavailable', message: 'busy' });
      else if (posts === 2) res.destroy();
      else ok(res, { through: body.through });
    },
  });
  const res = await streams.next(1);
  res.write(sse(hello(0), member(s1, 0), msg(1, s2, s1)));
  await waitFor(() => posts === 3);
  await delay(100);
  assert.equal(posts, 3);
  assert.deepEqual(daemon.requests(ROUTES.receiptV2).map(entry => entry.body.through), [1, 1, 1]);
  assert.equal(daemon.streams, 1);
});

test('the receipt poster aborts its connection on 409 and on 400', async t => {
  const seen = reports(t);
  const streams = streamQueue();
  const daemon = await rig(t, {
    [ROUTES.streamV2]: streams.route,
    [ROUTES.receiptV2]: (_req, res, body) => body.through === 1
      ? reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Superseded connection' })
      : reply(res, 400, { ok: false, error: 'invalid_request', message: 'Receipt is ahead of the sent position' }),
  });
  (await streams.next(1)).write(sse(hello(0), member(s1, 0), msg(1, s2, s1)));
  // The reconnect replays 1 (skipped, at or below D) and sends 2.
  (await streams.next(2)).write(sse(hello(0), member(s1, 0), msg(1, s2, s1), msg(2, s2, s1)));
  await streams.next(3);
  assert.ok(seen.some(entry => /Superseded connection/.test(entry.text)));
  assert.ok(seen.some(entry => /Receipt is ahead of the sent position/.test(entry.text)));
  assert.deepEqual(rows(daemon.path).map(row => row.sequence), [1, 2]);
});

test('the receipt poster ignores a reply that arrives after its connection ended', async t => {
  reports(t);
  const streams = streamQueue();
  let release = () => {};
  const held = new Promise<void>(resolve => { release = resolve; });
  const daemon = await rig(t, {
    [ROUTES.streamV2]: streams.route,
    [ROUTES.receiptV2]: async (_req, res) => {
      await held;
      reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Superseded connection' });
    },
  });
  const first = await streams.next(1);
  first.write(sse(hello(0), member(s1, 0), msg(1, s2, s1)));
  await waitFor(() => daemon.requests(ROUTES.receiptV2).length === 1);
  first.end();
  (await streams.next(2)).write(sse(hello(1), member(s1, 0)));
  await waitFor(() => bookmark(daemon.path).bookmark === 1);
  release();
  await delay(500);
  assert.equal(daemon.streams, 2, 'the late 409 does not abort the new connection');
  assert.equal(daemon.requests(ROUTES.receiptV2).length, 1);
});

test('with resume above C and nothing pending, a member-only chunk posts nothing and the stream stays open past a keepalive interval', { timeout: 20_000 }, async t => {
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  const res = await streams.next(1);
  res.write(sse(hello(50, { keepaliveMs: 5_000 })));
  await waitFor(() => bookmark(daemon.path).bookmark === 50);
  res.write(sse(member(s1, 40), member(s2, 50)));
  await delay(5_500);
  assert.equal(daemon.requests(ROUTES.receiptV2).length, 0);
  assert.equal(daemon.streams, 1);
});

/** A rig whose stream sends one hello and stays open, with a counting register route. */
async function registrationRig(t: TestContext, options: { conflict?: SessionAddress; v1?: boolean } = {}) {
  const streams = streamQueue();
  const posts: string[] = [];
  const unknown = new Set<string>();
  const routes: Record<string, Route> = {
    [ROUTES.register]: (_req, res, body) => {
      const session = body.session as SessionAddress;
      posts.push(key(session));
      if (options.conflict && key(session) === key(options.conflict) && posts.filter(item => item === key(session)).length > 1)
        reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Session belongs to another host' });
      else ok(res, registered(session));
    },
    [ROUTES.presence]: (_req, res, body) => {
      const value = (body.sessions as string[]).filter(item => unknown.delete(item));
      ok(res, { unknown: value });
    },
  };
  if (options.v1) routes[ROUTES.stream] = (_req, res) => { openStream(res); res.write(': idle\n\n'); };
  else routes[ROUTES.streamV2] = streams.route;
  const daemon = await rig(t, routes);
  if (!options.v1) (await streams.next(1)).write(sse(hello(0)));
  else await waitFor(() => daemon.requests(ROUTES.stream).length === 1);
  return { daemon, streams, posts, unknown };
}
function client(t: TestContext, path: string) {
  const value = createHostClient(path);
  t.after(() => value.close().catch(() => {}));
  return value;
}

test('on v2 a heartbeat that the lifetime registration answers completes locally; a leave makes the next heartbeat post', async t => {
  const { daemon, streams, posts } = await registrationRig(t);
  await waitFor(() => daemon.inbox.presenceActive);
  const socket = client(t, daemon.path);
  assert.equal((await socket.bind(s1, 'rt-1')).ok, true);
  await socket.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 1 && daemon.inbox.registeredResult(key(s1)) !== undefined);
  await socket.enqueueRegistration({ session: s1 });
  await delay(300);
  assert.deepEqual(posts, [key(s1)], 'the heartbeat sent no POST');
  assert.deepEqual(daemon.inbox.registrations(), [], 'and queued no row');
  (await streams.next(1)).write(sse(leave(s1)));
  await waitFor(() => daemon.inbox.registeredResult(key(s1)) === undefined);
  await socket.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 2);
});

test('a session that expired while no stream was open: the next hello clears its registration, so the adapter start posts at once', async t => {
  const { daemon, streams, posts } = await registrationRig(t);
  await waitFor(() => daemon.inbox.presenceActive);
  const first = client(t, daemon.path);
  assert.equal((await first.bind(s1, 'rt-1')).ok, true);
  await first.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 1 && daemon.inbox.registeredResult(key(s1)) !== undefined);
  await first.release(s1, 'rt-1');
  // The stream drops; s1 expires at the coordinator meanwhile, so the new
  // connection has no member for it and no leave (it never sent one).
  (await streams.next(1)).end();
  (await streams.next(2)).write(sse(hello(0)));
  await waitFor(() => bookmark(daemon.path).stream_id === STREAM && daemon.requests(ROUTES.presence).length >= 2);
  const second = client(t, daemon.path);
  assert.equal((await second.bind(s1, 'rt-2')).ok, true);
  await second.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 2, 2000, 'a registration POST for the restarted adapter');
});

test('presence unknown clears the registration and posts exactly once; a session now on another host gets the owner_conflict event', async t => {
  reports(t);
  const { daemon, streams, posts, unknown } = await registrationRig(t, { conflict: s2 });
  const one = client(t, daemon.path);
  const two = client(t, daemon.path);
  assert.equal((await one.bind(s1, 'rt-1')).ok, true);
  assert.equal((await two.bind(s2, 'rt-2')).ok, true);
  const events: Array<Outcome<null>> = [];
  two.onRegistrationResult(result => events.push(result));
  await one.enqueueRegistration({ session: s1 });
  await two.enqueueRegistration({ session: s2 });
  await waitFor(() => posts.length === 2 && daemon.inbox.registeredResult(key(s2)) !== undefined);
  unknown.add(key(s1)); unknown.add(key(s2));
  // A reconnect re-arms presence, whose first report goes out at once.
  (await streams.next(1)).end();
  (await streams.next(2)).write(sse(hello(0)));
  await waitFor(() => events.some(event => !event.ok && event.error === 'owner_conflict'));
  await delay(200);
  assert.equal(posts.filter(item => item === key(s1)).length, 2, 's1 posts exactly one new registration');
  assert.ok(daemon.inbox.registeredResult(key(s1)));
  assert.equal(daemon.inbox.registeredResult(key(s2)), undefined, 'the refusal withdraws s2');
});

test('without v2 presence (v1 coordinator), heartbeats still post', async t => {
  const { daemon, posts } = await registrationRig(t, { v1: true });
  const socket = client(t, daemon.path);
  assert.equal((await socket.bind(s1, 'rt-1')).ok, true);
  await socket.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 1 && daemon.inbox.registeredResult(key(s1)) !== undefined);
  await socket.enqueueRegistration({ session: s1 });
  await waitFor(() => posts.length === 2);
  assert.equal(daemon.inbox.presenceActive, false);
});

test('presence: one POST per presenceWindowMs/3 whatever the session count, listing only live leases', { timeout: 15_000 }, async t => {
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route }, {
    before: async (_path, inbox) => {
      for (let n = 0; n < 50; n++) await inbox.bind({ namespace: 'fleet', sessionId: `live-${String(n).padStart(2, '0')}` }, `rt-${n}`, Date.now());
      await inbox.bind({ namespace: 'fleet', sessionId: 'expired' }, 'old', Date.now() - 30_001);
    },
  });
  (await streams.next(1)).write(sse(hello(0, { presenceWindowMs: 15_000 })));
  await waitFor(() => daemon.requests(ROUTES.presence).length === 1);
  const started = daemon.requests(ROUTES.presence)[0]!.at;
  const [first] = daemon.requests(ROUTES.presence);
  assert.deepEqual(first!.body, { sessions: Array.from({ length: 50 }, (_, n) => `fleet:live-${String(n).padStart(2, '0')}`) });
  await waitFor(() => daemon.requests(ROUTES.presence).length === 2, 7_000);
  const gap = daemon.requests(ROUTES.presence)[1]!.at - started;
  assert.ok(gap >= 4_800 && gap <= 6_000, `interval ${gap} ms is 15000/3`);
  await delay(500);
  assert.equal(daemon.requests(ROUTES.presence).length, 2);
});

test('presence above MAX_PRESENCE_KEYS sends the first keys in key order and reports the overflow', async t => {
  const seen = reports(t);
  const streams = streamQueue();
  const ids = Array.from({ length: MAX_PRESENCE_KEYS + 1 }, (_, n) => `s-${String(MAX_PRESENCE_KEYS - n).padStart(5, '0')}`);
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route }, {
    before: async (_path, inbox) => { for (const id of ids) await inbox.bind({ namespace: 'fleet', sessionId: id }, id, Date.now()); },
  });
  (await streams.next(1)).write(sse(hello(0)));
  await waitFor(() => daemon.requests(ROUTES.presence).length === 1);
  const sent = daemon.requests(ROUTES.presence)[0]!.body.sessions as string[];
  assert.deepEqual(sent, ids.map(id => `fleet:${id}`).sort().slice(0, MAX_PRESENCE_KEYS));
  assert.ok(seen.some(entry => /first 4096 of 4097 leased sessions/.test(entry.text)));
});

test('the v2 idle deadline is 3 x hello.keepaliveMs', { timeout: 30_000 }, async t => {
  reports(t);
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  (await streams.next(1)).write(sse(hello(0, { keepaliveMs: 5_000 })));
  await waitFor(() => bookmark(daemon.path).bookmark === 0);
  const helloAt = Date.now();
  await waitFor(() => daemon.streams === 2, 20_000);
  const idle = daemon.requests(ROUTES.streamV2)[1]!.at - helloAt;
  assert.ok(idle >= 14_500 && idle < 17_000, `idle ${idle} ms is 3 x 5000 plus the backoff`);
});

test('the sweep keeps an old acknowledged row above the bookmark and removes it once D passes it', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'rw-v2-'));
  const path = join(dir, 'inbox.sqlite');
  const inbox = createDaemonInbox(path);
  t.after(async () => { await inbox.close(); rmSync(dir, { recursive: true, force: true }); });
  const now = Date.now();
  // One mock with a moving clock: a second mock of the same method would not restore cleanly.
  let clock = now;
  t.mock.method(Date, 'now', () => clock);
  await inbox.receive({ type: 'message', subscriptionId: key(s1), message: message(5, s2, s1) });
  assert.equal((await inbox.bind(s1, 'owner', now)).ok, true);
  assert.equal((await inbox.acknowledgeDelivery(s1, 'owner', { messageId: 'fleet:msg:5', sessionId: s1.sessionId, recordId: 'r5', location: 'log' })).ok, true);
  const set = (value: number) => { const db = new DatabaseSync(path); db.exec(`UPDATE inbox_stream SET stream_id = '${STREAM}', bookmark = ${value}`); db.close(); };
  set(3);
  clock = now + 7 * 86_400_000 + 61_000;
  inbox.sweep();
  assert.deepEqual(rows(path).map(row => row.sequence), [5], 'kept while above the bookmark');
  set(5);
  clock = now + 7 * 86_400_000 + 122_000;
  inbox.sweep();
  assert.deepEqual(rows(path), [], 'removed once D passes it');
});

test('with no bookmark a 404 runs one v1 stream request, and the next attempt tries /v2 after the backoff', async t => {
  reports(t);
  const daemon = await rig(t, { [ROUTES.stream]: (_req, res) => { openStream(res); res.end(': idle\n\n'); } });
  await waitFor(() => daemon.requests(ROUTES.stream).length >= 2);
  const order = daemon.log.filter(entry => entry.path === ROUTES.streamV2 || entry.path === ROUTES.stream);
  assert.deepEqual(order.slice(0, 4).map(entry => entry.path), [ROUTES.streamV2, ROUTES.stream, ROUTES.streamV2, ROUTES.stream]);
  assert.equal(order[0]!.search, '', 'the first request carries no query');
  assert.ok(order[2]!.at - order[1]!.at >= 200, `the next /v2 attempt waits for the backoff: ${JSON.stringify(order)}`);
});

test('a v1 coordinator answers the query-less /v2 stream request with 404, and a query with 400', async t => {
  const dir = mkdtempSync(join(tmpdir(), 'rw-v2-'));
  const store = createCoordinatorStore(join(dir, 'coordinator.sqlite'), 'fleet');
  const control = createControlHandler(store, createHostAuthenticator({ 'host-a': 'secret' }));
  const server = createServer((req, res) => { void control(req, res); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { await new Promise<void>(resolve => server.close(() => resolve())); store.close(); rmSync(dir, { recursive: true, force: true }); });
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const get = (search: string) => fetch(`${base}${ROUTES.streamV2}${search}`, { headers: { authorization: 'Bearer secret', accept: 'text/event-stream' } });
  assert.equal((await get('')).status, 404);
  assert.equal((await get(`?stream=${STREAM}&after=1`)).status, 400);
});

test('with a bookmark a 404 fails closed: the daemon never runs v1', async t => {
  reports(t);
  const daemon = await rig(t, { [ROUTES.stream]: (_req, res) => { openStream(res); res.write(': idle\n\n'); } }, {
    before: path => { const db = new DatabaseSync(path); db.exec(`UPDATE inbox_stream SET stream_id = '${STREAM}', bookmark = 3`); db.close(); },
  });
  await waitFor(() => daemon.streams >= 3);
  assert.equal(daemon.requests(ROUTES.stream).length, 0);
  assert.ok(daemon.requests(ROUTES.streamV2).every(entry => entry.search === `?stream=${STREAM}&after=3`));
});

test('a restored older inbox (after < C) gets 409 with no writes; after --reset-bookmark it resumes at C', async t => {
  const seen = reports(t);
  const C = 40;
  const streams = streamQueue();
  const daemon = await rig(t, {
    [ROUTES.streamV2]: (req, res, body, url) => {
      const after = url.searchParams.get('after');
      if (after !== null && Number(after) < C) { reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Bookmark is behind the receipted position' }); return; }
      return streams.route(req, res, body, url);
    },
  }, { before: path => { const db = new DatabaseSync(path); db.exec(`UPDATE inbox_stream SET stream_id = '${STREAM}', bookmark = 12`); db.close(); } });
  await waitFor(() => daemon.streams >= 2);
  assert.ok(seen.some(entry => /HTTP 409: Bookmark is behind the receipted position/.test(entry.text)));
  assert.deepEqual(bookmark(daemon.path), { stream_id: STREAM, bookmark: 12 });
  await daemon.stop();
  assert.deepEqual(daemon.inbox.resetBookmark(), { highWater: 12 });
  daemon.start();
  (await streams.next(1)).write(sse(hello(C)));
  await waitFor(() => bookmark(daemon.path).bookmark === C);
  assert.equal(daemon.requests(ROUTES.streamV2).at(-1)!.search, '');
});

test('--reset-bookmark keeps D as a skip floor: the next open sends no query, a replay below D writes nothing and a reissued id fails closed', async t => {
  const seen = reports(t);
  const streams = streamQueue();
  const daemon = await rig(t, { [ROUTES.streamV2]: streams.route });
  const receipts = () => daemon.requests(ROUTES.receiptV2).map(entry => entry.body.through);
  (await streams.next(1)).write(sse(hello(0), member(s1, 0), msg(1, s2, s1), msg(2, s2, s1), msg(3, s2, s1)));
  await waitFor(() => receipts().includes(3));
  // Row 2 was acknowledged long ago and swept.
  const writer = new DatabaseSync(daemon.path);
  writer.prepare("DELETE FROM inbox_messages WHERE message_id = 'fleet:msg:2'").run();
  writer.close();
  await daemon.stop();
  assert.deepEqual(daemon.inbox.resetBookmark(), { highWater: 3 });
  assert.deepEqual(bookmark(daemon.path), { stream_id: null, bookmark: 3 });
  daemon.start();
  // A restored coordinator log resumes at 0 and replays 1-3; the restore floor makes the next id 4.
  (await streams.next(2)).write(sse(hello(0, { stream: OTHER_STREAM }), member(s1, 0), msg(1, s2, s1), msg(2, s2, s1), msg(3, s2, s1), msg(4, s2, s1)));
  await waitFor(() => receipts().includes(4));
  assert.equal(daemon.requests(ROUTES.streamV2)[1]!.search, '', 'no stream: no query');
  assert.deepEqual(rows(daemon.path).map(row => row.sequence), [1, 3, 4], 'the swept row does not come back');
  assert.deepEqual(bookmark(daemon.path), { stream_id: OTHER_STREAM, bookmark: 4 });
  // A floor too low: a reissued id at or below D with other bytes is a conflict, not a silent skip.
  await daemon.stop();
  daemon.inbox.resetBookmark();
  daemon.start();
  (await streams.next(3)).write(sse(hello(0), member(s1, 0), msg(3, s2, s1, 'reissued')));
  await waitFor(() => seen.some(entry => entry.text.includes(`Message identity conflict: session ${key(s1)}, message fleet:msg:3`)));
  assert.deepEqual(bookmark(daemon.path), { stream_id: STREAM, bookmark: 4 }, 'D unchanged');
  assert.deepEqual(rows(daemon.path).map(row => row.sequence), [1, 3, 4]);
});

/**
 * A fake coordinator that keeps C, serves every owed message above C on each
 * connection, and accepts receipts only from the live connection up to what it sent.
 */
function modelCoordinator(owed: HostFrame[]) {
  const state = { C: 0, conn: '', sent: 0, opens: [] as string[] };
  const tail = Math.max(...owed.map(frame => (frame as { seq: number }).seq));
  const routes: Record<string, Route> = {
    [ROUTES.streamV2]: (_req, res, _body, url) => {
      state.opens.push(url.search);
      const after = url.searchParams.get('after');
      if (after !== null && Number(after) < state.C) { reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Bookmark is behind the receipted position' }); return; }
      const first = hello(state.C);
      state.conn = first.conn; state.sent = tail;
      openStream(res);
      res.write(sse(first, member(s1, 0), member(s2, 0), ...owed.filter(frame => (frame as { seq: number }).seq > state.C)));
    },
    [ROUTES.receiptV2]: (_req, res, body) => {
      if (body.through <= state.C) { ok(res, { through: state.C }); return; }
      if (body.conn !== state.conn) { reply(res, 409, { ok: false, error: 'owner_conflict', message: 'Superseded connection' }); return; }
      if (body.through > state.sent) { reply(res, 400, { ok: false, error: 'invalid_request', message: 'Receipt is ahead of the sent position' }); return; }
      state.C = body.through;
      ok(res, { through: state.C });
    },
  };
  return { state, routes, tail };
}
const crashChild = fileURLToPath(new URL('./stream-crash-child.mjs', import.meta.url));
async function crashDaemon(config: HostConfig, at: string): Promise<NodeJS.Signals | null> {
  const child = spawn(process.execPath, [crashChild, config.inboxPath, config.coordinatorUrl, at], { stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += chunk; });
  const [code, signal] = await once(child, 'exit') as [number | null, NodeJS.Signals | null];
  assert.equal(signal, 'SIGKILL', `child exited ${code}: ${stderr}`);
  return signal;
}
const owedFrames = [msg(1, s2, s1), msg(2, s1, '*'), msg(3, s2, s1), msg(5, s3, '*')];
const owedRows = [[1, key(s1)], [2, key(s2)], [3, key(s1)], [5, key(s1)], [5, key(s2)]];
const stored = (path: string) => rows(path).map(row => [row.sequence, row.session_key]);

test('crash: SIGKILL before the batch COMMIT; after restart every owed message is stored exactly once', { timeout: 20_000 }, async t => {
  reports(t);
  const model = modelCoordinator(owedFrames);
  const daemon = await rig(t, model.routes, { start: false });
  await daemon.inbox.close();
  await crashDaemon(daemon.config, 'before-commit');
  assert.deepEqual(stored(daemon.path), [], 'nothing from the batch is durable');
  assert.equal(bookmark(daemon.path).bookmark, 0, 'the hello commit is');
  assert.equal(daemon.requests(ROUTES.receiptV2).length, 0);
  const reopened = createDaemonInbox(daemon.path);
  t.after(() => reopened.close());
  const restarted = createHostDaemon(daemon.config, reopened);
  const run = restarted.start(new AbortController().signal);
  try { await waitFor(() => model.state.C === model.tail); } finally { await restarted.stop(); await run; }
  assert.deepEqual(model.state.opens, ['', `?stream=${STREAM}&after=0`]);
  assert.deepEqual(stored(daemon.path), owedRows);
});

test('crash: SIGKILL after COMMIT and before the receipt; the reconnect sends after=N, skips the replay and C reaches N', { timeout: 20_000 }, async t => {
  reports(t);
  const model = modelCoordinator(owedFrames);
  const daemon = await rig(t, model.routes, { start: false });
  await daemon.inbox.close();
  await crashDaemon(daemon.config, 'before-receipt');
  assert.deepEqual(stored(daemon.path), owedRows);
  assert.equal(bookmark(daemon.path).bookmark, model.tail);
  assert.equal(model.state.C, 0);
  const reopened = createDaemonInbox(daemon.path);
  t.after(() => reopened.close());
  const inserts: string[] = [];
  const originalPrepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function (this: DatabaseSync, sql: string) {
    const statement = originalPrepare.call(this, sql);
    if (!/^INSERT INTO inbox_messages/.test(sql)) return statement;
    const run = statement.run.bind(statement);
    statement.run = ((...args: never[]) => { inserts.push(String(args[1])); return run(...args); }) as typeof statement.run;
    return statement;
  } as typeof DatabaseSync.prototype.prepare;
  const restarted = createHostDaemon(daemon.config, reopened);
  const run = restarted.start(new AbortController().signal);
  try { await waitFor(() => model.state.C === model.tail); }
  finally { DatabaseSync.prototype.prepare = originalPrepare; await restarted.stop(); await run; }
  assert.deepEqual(model.state.opens, ['', `?stream=${STREAM}&after=${model.tail}`]);
  assert.deepEqual(inserts, [], 'replayed frames at or below D write nothing');
  assert.deepEqual(stored(daemon.path), owedRows, 'no duplicate rows');
});

test('crash: SIGKILL at the hello commit; the second connect produces the same final inbox', { timeout: 20_000 }, async t => {
  reports(t);
  const model = modelCoordinator(owedFrames);
  const daemon = await rig(t, model.routes, { start: false });
  await daemon.inbox.close();
  await crashDaemon(daemon.config, 'hello');
  assert.deepEqual(bookmark(daemon.path), { stream_id: null, bookmark: null });
  const reopened = createDaemonInbox(daemon.path);
  t.after(() => reopened.close());
  const restarted = createHostDaemon(daemon.config, reopened);
  const run = restarted.start(new AbortController().signal);
  try { await waitFor(() => model.state.C === model.tail); } finally { await restarted.stop(); await run; }
  assert.deepEqual(model.state.opens, ['', '']);
  assert.deepEqual(stored(daemon.path), owedRows);
  assert.deepEqual(bookmark(daemon.path), { stream_id: STREAM, bookmark: model.tail });
});

/**
 * End-to-end harness for host stream v2 (docs/protocol-v2.md, test plan 22 and 35-46):
 * the real coordinator (`startCoordinator`), real host daemons and real SQLite files.
 * Observation is wire-level: a fetch recorder sees every daemon request and every
 * SSE frame the daemon reads, a server-side tap counts unreceipted msg frames per
 * connection, and a TCP proxy adds delay. Nothing here changes protocol behaviour.
 */
import assert from 'node:assert/strict';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { ServerResponse } from 'node:http';
import { createServer as createTcpServer, connect as tcpConnect, type Server as TcpServer, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
import { startCoordinator } from '../../src/bin/coordinator.ts';
import { createDaemonInbox, createHostDaemon, type LocalHostInbox } from '../../src/host/index.ts';
import {
  DEFAULT_SETTINGS, ROUTES, sessionKey,
  type HostConfig, type HostFrame, type Message, type SessionAddress, type Settings,
} from '../../src/protocol/index.ts';

export const NS = 'fleet';
export const TOKENS: Record<string, string> = { 'host-a': 'token-a', 'host-b': 'token-b', 'host-c': 'token-c' };
const HOSTS_BY_TOKEN = new Map(Object.entries(TOKENS).map(([host, token]) => [`Bearer ${token}`, host]));
export const ROOT = fileURLToPath(new URL('../../', import.meta.url));
/** The last commit before host stream v2: a v1 coordinator and a v1-only daemon. */
export const PRE_V2 = '2269c2b';

type TestContext = { after(fn: () => unknown): void };

export const address = (n: number): SessionAddress =>
  ({ namespace: NS, sessionId: `550e8400-e29b-41d4-a716-${String(446655440000 + n).padStart(12, '0')}` });
export const key = sessionKey;

export async function waitFor(check: () => boolean | Promise<boolean>, what: string, timeout = 10_000): Promise<void> {
  const end = Date.now() + timeout;
  while (!await check()) {
    if (Date.now() > end) assert.fail(`Timed out waiting for ${what}`);
    await delay(10);
  }
}
export function tempDir(t: TestContext, prefix = 'rw-e2e-'): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}
export async function freePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as { port: number };
  await new Promise<void>(resolve => server.close(() => resolve()));
  return port;
}

// ---------- SQLite reads (a separate read-only handle, as an operator would) ----------

export function query<T = Record<string, unknown>>(path: string, sql: string, ...params: Array<string | number | null>): T[] {
  const db = new DatabaseSync(path, { readOnly: true });
  try { return db.prepare(sql).all(...params).map(row => ({ ...row }) as T); } finally { db.close(); }
}
export function bookmark(inboxPath: string): { stream: string | null; bookmark: number | null } {
  const [row] = query<{ stream_id: string | null; bookmark: number | null }>(inboxPath, 'SELECT stream_id, bookmark FROM inbox_stream WHERE id = 1');
  return { stream: row!.stream_id, bookmark: row!.bookmark };
}
export type InboxRow = { session_key: string; message_id: string; sequence: number; message_json: string; hash: string | null; evidence_record_id: string | null };
export function inboxRows(inboxPath: string): InboxRow[] {
  return query<InboxRow>(inboxPath, `SELECT session_key, message_id, sequence, message_json, hash, evidence_record_id
    FROM inbox_messages ORDER BY sequence, session_key`);
}
/** Stored message sequences per session key. */
export function stored(inboxPath: string): Map<string, number[]> {
  const result = new Map<string, number[]>();
  for (const row of inboxRows(inboxPath)) result.set(row.session_key, [...result.get(row.session_key) ?? [], row.sequence]);
  return result;
}
export function hostAcked(dbPath: string, hostId: string): number | undefined {
  const tables = query(dbPath, "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hosts'");
  if (!tables.length) return undefined;
  return query<{ acked: number }>(dbPath, 'SELECT acked FROM hosts WHERE host_id = ?', hostId)[0]?.acked;
}
export function coordinatorTail(dbPath: string): number {
  return query<{ tail: number }>(dbPath, "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'messages'), 0) AS tail")[0]!.tail;
}
export function cursors(dbPath: string): Map<string, number> {
  return new Map(query<{ id: string; cursor: number }>(dbPath, 'SELECT id, cursor FROM sessions').map(row => [row.id, row.cursor]));
}
/** Runs writes on the coordinator file from a second connection, as a fault injector. */
export function mutate(path: string, sql: string, ...params: Array<string | number | null>): void {
  const db = new DatabaseSync(path);
  try { db.exec('PRAGMA busy_timeout = 5000'); db.prepare(sql).run(...params); } finally { db.close(); }
}
export const sha256 = (text: string) => createHash('sha256').update(text).digest('hex');

// ---------- Coordinator ----------

export type Coordinator = {
  url: string; port: number; dbPath: string;
  runtime(): Awaited<ReturnType<typeof startCoordinator>> | undefined;
  stop(): Promise<void>; start(): Promise<void>; restart(): Promise<void>;
};
/** The real coordinator (`startCoordinator`) on a fixed port, so it can restart behind the same URL. */
export async function coordinator(t: TestContext, dir: string, settings: Partial<Settings> = {}, options: { dbPath?: string; port?: number } = {}): Promise<Coordinator> {
  const port = options.port ?? await freePort();
  const dbPath = options.dbPath ?? join(dir, 'coordinator.db');
  const full: Settings = { ...DEFAULT_SETTINGS, ...settings };
  let current: Awaited<ReturnType<typeof startCoordinator>> | undefined;
  const handle: Coordinator = {
    url: `http://127.0.0.1:${port}`, port, dbPath,
    runtime: () => current,
    async start() {
      if (current) return;
      current = await startCoordinator({ namespace: NS, databasePath: dbPath, listenHost: '127.0.0.1', port, hostCredentials: TOKENS }, full);
      onCoordinatorStart?.(current);
    },
    async stop() { const runtime = current; current = undefined; await runtime?.close(); },
    async restart() { await handle.stop(); await handle.start(); },
  };
  await handle.start();
  t.after(() => handle.stop());
  return handle;
}
let onCoordinatorStart: ((runtime: Awaited<ReturnType<typeof startCoordinator>>) => void) | undefined;

/** A coordinator in a child process: `node <bin> config.json`, the deployed shape. `bin` may be the pre-v2 tree's. */
export async function coordinatorProcess(t: TestContext, dir: string, options: {
  dbPath: string; port: number; bin?: string; settings?: string; crash?: 'after-send-commit';
}): Promise<{ url: string; child: ChildProcess; exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>; stop(): Promise<void> }> {
  const configPath = join(dir, `coordinator-${options.port}.json`);
  writeFileSync(configPath, JSON.stringify({ namespace: NS, databasePath: options.dbPath, listenHost: '127.0.0.1', port: options.port, hostCredentials: TOKENS }));
  const settingsPath = join(dir, 'roger-wilco.conf');
  writeFileSync(settingsPath, options.settings ?? '');
  const child = spawn(process.execPath, [fileURLToPath(new URL('./coordinator-child.mjs', import.meta.url)),
    options.bin ?? join(ROOT, 'src/bin/coordinator.ts'), configPath, options.crash ?? ''], {
    env: { ...process.env, ROGER_WILCO_SETTINGS: settingsPath }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr!.on('data', chunk => { stderr += chunk; });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve =>
    child.once('exit', (code, signal) => resolve({ code, signal })));
  const ready = await new Promise<{ url: string }>((resolve, reject) => {
    const lines = createInterface({ input: child.stdout! });
    lines.once('line', line => resolve(JSON.parse(line)));
    void exited.then(status => reject(new Error(`coordinator exited ${JSON.stringify(status)}: ${stderr}`)));
  });
  const stop = async () => { if (child.exitCode === null && child.signalCode === null) { child.kill('SIGTERM'); await exited; } };
  t.after(stop);
  return { url: ready.url, child, exited, stop };
}

// ---------- Daemons ----------

export type Daemon = {
  config: HostConfig; inbox(): LocalHostInbox;
  start(): void; stop(): Promise<void>; restart(): Promise<void>;
};
/** A real host daemon with its own inbox file. `stop` also closes the inbox, as a process exit does. */
export function daemon(t: TestContext, dir: string, hostId: string, url: string, options: { start?: boolean } = {}): Daemon {
  const config: HostConfig = { coordinatorUrl: url, hostId, credential: TOKENS[hostId]!, inboxPath: join(dir, hostId, 'inbox.sqlite') };
  let inbox: LocalHostInbox | undefined;
  let running: { daemon: ReturnType<typeof createHostDaemon>; done: Promise<void> } | undefined;
  const handle: Daemon = {
    config,
    inbox() { assert.ok(inbox, `${hostId} inbox is closed`); return inbox; },
    start() {
      if (running) return;
      inbox ??= createDaemonInbox(config.inboxPath);
      const instance = createHostDaemon(config, inbox);
      running = { daemon: instance, done: instance.start(new AbortController().signal) };
      void running.done.catch(() => {});
    },
    async stop() {
      const current = running;
      running = undefined;
      if (current) { await current.daemon.stop(); await current.done.catch(() => {}); }
      await inbox?.close();
      inbox = undefined;
    },
    async restart() { await handle.stop(); handle.start(); },
  };
  if (options.start !== false) handle.start();
  t.after(() => handle.stop());
  return handle;
}

// ---------- HTTP as a host ----------

export async function call(url: string, hostId: string, route: string, body?: unknown): Promise<{ status: number; result: any }> {
  const response = await fetch(new URL(route, url), {
    method: body === undefined ? 'GET' : 'POST',
    headers: { authorization: `Bearer ${TOKENS[hostId]}`, ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: response.status, result: await response.json() };
}
/** Registers directly with the host credential; returns the start (the subscription cursor). */
export async function register(url: string, hostId: string, session: SessionAddress): Promise<number> {
  const { status, result } = await call(url, hostId, ROUTES.register, { session });
  assert.equal(status, 200, JSON.stringify(result));
  return result.value.subscription.cursor;
}
export async function send(url: string, hostId: string, sender: SessionAddress, recipient: SessionAddress | '*', body: string): Promise<Message> {
  const { status, result } = await call(url, hostId, ROUTES.messages,
    { sender, message: { recipient: recipient === '*' ? '*' : key(recipient), body } });
  assert.equal(status, 200, JSON.stringify(result));
  return result.value;
}

// ---------- Wire recorder ----------

export type Exchange = { host: string | undefined; path: string; search: string; body: any; at: number; status?: number; reply?: any };
export type Seen = { host: string | undefined; connection: number; event: string; frame: any; at: number };
type Rule = { match(exchange: Exchange): boolean; action: 'hold' | 'drop-request' | 'drop-response'; release?: Promise<void>; times: number };

/**
 * Wraps the global fetch, which the daemon calls for every request. Records each
 * request (host from its credential, route, JSON body, reply status and body) and
 * passes every SSE response through a parser, so the test sees the frames as the daemon reads them.
 * Rules can hold a request until released, or lose it before or after it reaches
 * the coordinator. Installed once per test process; `reset` clears records and rules.
 */
export function wire() {
  const exchanges: Exchange[] = [];
  const frames: Seen[] = [];
  let rules: Rule[] = [];
  let connections = 0;
  const original = globalThis.fetch;
  const wrapped: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const raw = request.method === 'POST' ? await request.clone().text() : '';
    const exchange: Exchange = { host: HOSTS_BY_TOKEN.get(request.headers.get('authorization') ?? ''), path: url.pathname,
      search: url.search, body: raw ? JSON.parse(raw) : undefined, at: Date.now() };
    exchanges.push(exchange);
    const rule = rules.find(item => item.times > 0 && item.match(exchange));
    if (rule) rule.times--;
    if (rule?.action === 'hold') await rule.release;
    if (rule?.action === 'drop-request') throw new TypeError('fetch failed (injected: request lost)');
    const response = await original(input, init);
    exchange.status = response.status;
    if (rule?.action === 'drop-response') {
      await response.arrayBuffer().catch(() => {});
      throw new TypeError('fetch failed (injected: response lost)');
    }
    const type = response.headers.get('content-type') ?? '';
    if (type.startsWith('application/json')) {
      void response.clone().json().then(value => { exchange.reply = value; }, () => {});
      return response;
    }
    if (!type.startsWith('text/event-stream') || !response.body) return response;
    const connection = ++connections;
    const decoder = new TextDecoder();
    let buffer = '';
    // A pass-through, not a tee: the daemon's reads drive it, and its cancel reaches the socket.
    const theirs = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        buffer += decoder.decode(chunk, { stream: true });
        let end: number;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, end);
          buffer = buffer.slice(end + 2);
          const lines = block.split('\n');
          const event = lines.find(line => line.startsWith('event: '))?.slice(7) ?? '';
          const data = lines.filter(line => line.startsWith('data: ')).map(line => line.slice(6)).join('\n');
          if (data) {
            let frame: unknown;
            try { frame = JSON.parse(data); } catch { frame = data; }
            frames.push({ host: exchange.host, connection, event, frame, at: Date.now() });
          }
        }
        controller.enqueue(chunk);
      },
    }));
    return new Response(theirs, { status: response.status, statusText: response.statusText, headers: response.headers });
  };
  globalThis.fetch = wrapped;
  return {
    exchanges, frames,
    restore() { globalThis.fetch = original; },
    reset() { exchanges.length = 0; frames.length = 0; rules = []; },
    requests: (path: string, host?: string) => exchanges.filter(item => item.path === path && (host === undefined || item.host === host)),
    v2: (host: string, type?: HostFrame['type']) => frames.filter(item => item.host === host && item.event === 'v2' && (!type || item.frame.type === type))
      .map(item => item.frame),
    /** Holds matching requests until the returned function is called. */
    hold(match: (exchange: Exchange) => boolean, times = Infinity): () => void {
      let release!: () => void;
      const rule: Rule = { match, action: 'hold', release: new Promise<void>(resolve => { release = resolve; }), times };
      rules.push(rule);
      return () => { rule.times = 0; release(); };
    },
    /** Loses matching requests: before they reach the coordinator, or after it answered. */
    drop(match: (exchange: Exchange) => boolean, where: 'request' | 'response', times = 1): void {
      rules.push({ match, action: where === 'request' ? 'drop-request' : 'drop-response', times });
    },
  };
}
export type Wire = ReturnType<typeof wire>;

/**
 * Server-side window tap: counts, per v2 connection, the msg frames written that
 * no accepted receipt has covered yet. Receipts are seen through `advanceHost` on
 * the live store, which the handler calls before it releases the window, so the
 * count is exact at every write. Records the maximum and every write above `limit`.
 */
export function windowTap(limit: number) {
  const state = new WeakMap<ServerResponse, { host: string; sent: number[]; acked: number }>();
  const latest = new Map<string, ServerResponse>();
  const result = { max: 0, violations: [] as string[], msgFrames: 0 };
  const write = ServerResponse.prototype.write;
  ServerResponse.prototype.write = function (this: ServerResponse, chunk: unknown, ...rest: unknown[]) {
    if (typeof chunk === 'string' && chunk.startsWith('event: v2\ndata: ')) {
      const frame = JSON.parse(chunk.slice(16, chunk.indexOf('\n', 16)));
      const host = HOSTS_BY_TOKEN.get(this.req?.headers.authorization ?? '') ?? '?';
      if (frame.type === 'hello') { state.set(this, { host, sent: [], acked: -1 }); latest.set(host, this); }
      const connection = state.get(this);
      if (connection && frame.type === 'msg') {
        connection.sent.push(frame.seq);
        result.msgFrames++;
        const open = connection.sent.filter(seq => seq > connection.acked).length;
        result.max = Math.max(result.max, open);
        if (open > limit) result.violations.push(`${host}: ${open} unreceipted msg frames at seq ${frame.seq}`);
      }
    }
    return (write as (...args: unknown[]) => boolean).call(this, chunk, ...rest);
  } as typeof ServerResponse.prototype.write;
  onCoordinatorStart = runtime => {
    const advance = runtime.store.advanceHost.bind(runtime.store);
    runtime.store.advanceHost = (host, through) => {
      const response = latest.get(host.hostId);
      const connection = response && state.get(response);
      if (connection) connection.acked = Math.max(connection.acked, through);
      return advance(host, through);
    };
  };
  return { result, restore() { ServerResponse.prototype.write = write; onCoordinatorStart = undefined; } };
}

// ---------- TCP delay proxy ----------

/** Forwards TCP both ways, each chunk after `ms`, in order. `target` can change between connections. */
export async function delayProxy(t: TestContext, target: number, ms: number): Promise<{ port: number; url: string; target: number }> {
  const sockets = new Set<Socket>();
  const pipe = (from: Socket, to: Socket) => {
    let chain = Promise.resolve();
    from.on('data', chunk => {
      chain = chain.then(() => delay(ms)).then(() => { if (!to.destroyed) to.write(chunk); });
    });
    from.on('end', () => { chain = chain.then(() => delay(ms)).then(() => { to.end(); }); });
    from.on('error', () => to.destroy());
    from.on('close', () => { void chain.then(() => to.destroy()); });
  };
  const handle = { port: 0, url: '', target };
  const server: TcpServer = createTcpServer(client => {
    const upstream = tcpConnect(handle.target, '127.0.0.1');
    client.setNoDelay(true); upstream.setNoDelay(true);
    sockets.add(client); sockets.add(upstream);
    client.on('close', () => sockets.delete(client));
    upstream.on('close', () => sockets.delete(upstream));
    pipe(client, upstream);
    pipe(upstream, client);
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  handle.port = (server.address() as { port: number }).port;
  handle.url = `http://127.0.0.1:${handle.port}`;
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  return handle;
}

// ---------- The pre-v2 tree ----------

let preV2: string | undefined;
/** `src/` and `deploy/` at PRE_V2, extracted once per process into a temporary directory. */
export function preV2Tree(): string {
  if (preV2) return preV2;
  const dir = mkdtempSync(join(tmpdir(), 'rw-pre-v2-'));
  const archive = execFileSync('git', ['archive', PRE_V2, 'src', 'deploy'], { cwd: ROOT });
  execFileSync('tar', ['-x', '-C', dir], { input: archive });
  process.once('exit', () => rmSync(dir, { recursive: true, force: true }));
  return preV2 = dir;
}

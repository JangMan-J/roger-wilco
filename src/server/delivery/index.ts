import type { IncomingMessage, ServerResponse } from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { setImmediate as nextTurn } from 'node:timers/promises';
import {
  DEFAULT_SETTINGS, ROUTES, isHostReceipt, isReceipt,
  type ControlHandler, type CoordinatorStore, type FailureCode, type HostFrame, type HostPrincipal,
  type HostReceiptResult, type HostAuthenticator, type Outcome, type SessionKey, type Settings, type StreamFrame,
} from '../../protocol/index.ts';
import { bearerCredential, closedQuery, json, readBody, readBodyError } from '../http.ts';

const WRITE_DEADLINE_MS = 5_000;
/** Unreceipted msg bytes per v2 connection. The window W bounds the frame count. */
export const INFLIGHT_BYTE_CAP = 16 * 1024 * 1024;

function error(code: FailureCode, message: string): Outcome<never> {
  return { ok: false, error: code, message };
}
async function write(response: ServerResponse, frame: string, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted || response.destroyed || response.writableEnded) return false;
  if (response.write(frame)) return true;
  // Never enqueue another frame while this connection is backpressured.
  try {
    await once(response, 'drain', { signal: AbortSignal.any([signal, AbortSignal.timeout(WRITE_DEADLINE_MS)]) });
    return !signal.aborted && !response.destroyed;
  } catch { return false; }
}
/** One v2 event: `event: v2` and a single data line; no `id:` line. */
function event(frame: HostFrame): string {
  return `event: v2\ndata: ${JSON.stringify(frame)}\n\n`;
}
function acceptsEventStream(request: IncomingMessage): boolean {
  return request.method === 'GET' &&
    !!request.headers.accept?.split(',').some(type => /^text\/event-stream(?:\s*;|$)/i.test(type.trim()));
}
/** `stream` and `after` together or not at all, each once; nothing else. */
function bookmark(url: URL): { stream?: string; after?: number } | undefined {
  const names = [...url.searchParams.keys()];
  if (!url.search) return {};
  if (names.length !== 2 || !names.includes('stream') || !names.includes('after')) return undefined;
  const stream = url.searchParams.get('stream')!;
  const text = url.searchParams.get('after')!;
  if (!/^[0-9a-f]{32}$/.test(stream) || !/^(0|[1-9]\d*)$/.test(text) || !Number.isSafeInteger(Number(text))) return undefined;
  return { stream, after: Number(text) };
}

type Base = {
  abort: AbortController; response: ServerResponse;
  dirty: boolean; waiting?: () => void; wake(): void; done: Promise<void>; resolveDone(): void;
};
type V1 = Base & { version: 1; inFlight: Map<string, string> };
/** One v2 connection. `sentThrough` is the highest position written; `scanned` is the scan position. */
type V2 = Base & {
  version: 2; conn: string; acked: number; sentThrough: number; scanned: number;
  sentMembers: Map<SessionKey, number>;
  inflight: Array<{ seq: number; bytes: number }>;
  inflightBytes: number;
};
type Live = V1 | V2;

/** SQLite is the sole replay source. Subscribe before the first pending read. */
export type StreamHandler = ControlHandler & { close(): Promise<void> };
/** Serves the v1 and v2 host stream and receipt routes from one `active` map keyed by host. */
export function createStreamHandler(store: CoordinatorStore, auth: HostAuthenticator, settings: Settings = DEFAULT_SETTINGS): StreamHandler {
  const { window: W, keepaliveMs: K, presenceWindowMs } = settings;
  const active = new Map<string, Live>();
  let closed = false;
  const fail = () => { for (const connection of active.values()) { connection.abort.abort(); connection.response.destroy(); } };
  const unsubscribe = store.subscribe(hostId => {
    const targets = hostId === undefined ? [...active.values()] : active.has(hostId) ? [active.get(hostId)!] : [];
    try { for (const connection of targets) connection.wake(); }
    catch (problem) { fail(); throw problem; }
  });
  /** Runs `body` as the active connection for the host, superseding any earlier one, and cleans up. */
  async function serve<T extends Live>(request: IncomingMessage, hostId: string, connection: T, body: (connection: T) => Promise<void>) {
    const previous = active.get(hostId);
    previous?.abort.abort();
    previous?.response.destroy();
    active.set(hostId, connection);
    const disconnect = () => connection.abort.abort();
    connection.response.once('close', disconnect);
    request.once('aborted', disconnect);
    try {
      connection.response.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      connection.response.flushHeaders();
      await body(connection);
    } finally {
      connection.response.off('close', disconnect);
      request.off('aborted', disconnect);
      if (active.get(hostId) === connection) active.delete(hostId);
      if (!connection.response.destroyed) connection.response.end();
      connection.resolveDone();
    }
  }
  function base(response: ServerResponse): Base {
    let resolveDone!: () => void;
    const done = new Promise<void>(resolve => { resolveDone = resolve; });
    return {
      abort: new AbortController(), response, dirty: true, done, resolveDone,
      wake() { this.dirty = true; const resume = this.waiting; this.waiting = undefined; resume?.(); },
    };
  }
  /** Waits for a wake, an abort or `deadline`; returns at once when `ready()` already holds. */
  function wait(connection: Live, ready: () => boolean, deadline: number): Promise<void> {
    return new Promise<void>(resolve => {
      const { signal } = connection.abort;
      if (signal.aborted || ready()) { resolve(); return; }
      const finish = () => {
        clearTimeout(timer);
        signal.removeEventListener('abort', finish);
        if (connection.waiting === onWake) connection.waiting = undefined;
        resolve();
      };
      // wake() clears `waiting` before calling it. A wake that leaves `ready()` false (dirty
      // while the window is full) re-arms instead of ending the wait.
      const onWake = () => { if (ready()) finish(); else connection.waiting = onWake; };
      connection.waiting = onWake;
      const timer = setTimeout(finish, Math.max(0, deadline - Date.now()));
      signal.addEventListener('abort', finish, { once: true });
    });
  }

  async function streamV1(host: HostPrincipal, connection: V1): Promise<void> {
    const { signal } = connection.abort;
    const { response } = connection;
    let lastWrite = Date.now();
    while (!signal.aborted) {
      if (connection.dirty) {
        connection.dirty = false;
        const frames: StreamFrame[] = store.pending(host, 500, connection.inFlight.keys());
        let wrote = false;
        for (const frame of frames) {
          if (signal.aborted) break;
          // The store already skips these; the check keeps a store that ignores `inFlight` correct.
          if (connection.inFlight.has(frame.subscriptionId)) continue;
          connection.inFlight.set(frame.subscriptionId, frame.message.id);
          const payload = `id: ${frame.message.id}\nevent: message\ndata: ${JSON.stringify(frame)}\n\n`;
          if (!await write(response, payload, signal)) { connection.abort.abort(); break; }
          lastWrite = Date.now();
          wrote = true;
        }
        if (signal.aborted) break;
        if (wrote && frames.length === 500) connection.dirty = true;
        if (connection.dirty) continue;
      }
      await wait(connection, () => connection.dirty, lastWrite + K);
      if (signal.aborted) break;
      if (connection.dirty) continue;
      if (Date.now() - lastWrite >= K) {
        if (!await write(response, ': keepalive\n\n', signal)) { connection.abort.abort(); break; }
        lastWrite = Date.now();
      }
    }
  }

  async function streamV2(host: HostPrincipal, connection: V2, hello: HostFrame): Promise<void> {
    const { signal } = connection.abort;
    let lastWrite = Date.now();
    const room = () => connection.inflight.length < W && connection.inflightBytes < INFLIGHT_BYTE_CAP;
    const send = async (payload: string): Promise<boolean> => {
      // A failed or stalled write ends this connection only; destroy frees its socket at once.
      if (!await write(connection.response, payload, signal)) { connection.abort.abort(); connection.response.destroy(); return false; }
      lastWrite = Date.now();
      return true;
    };
    const through = async (): Promise<boolean> => {
      if (connection.scanned <= connection.sentThrough) return true;
      connection.sentThrough = connection.scanned;
      return send(event({ type: 'through', seq: connection.scanned }));
    };
    /** Steps 1-6: one snapshot, membership diff, msg frames, trailing through, re-arm. */
    const batch = async (): Promise<boolean> => {
      connection.dirty = false;
      // Let receipts and other hosts' requests run between spans of a long gap.
      await nextTurn();
      if (signal.aborted) return false;
      const result = store.hostBatch(host, connection.scanned, W - connection.inflight.length);
      const members = new Map(result.members.map(member => [member.session, member.start]));
      for (const session of connection.sentMembers.keys()) {
        if (!members.has(session) && !await send(event({ type: 'leave', session }))) return false;
      }
      for (const [session, start] of members) {
        if (connection.sentMembers.get(session) !== start && !await send(event({ type: 'member', session, start }))) return false;
      }
      connection.sentMembers = members;
      let scanned = result.scanned;
      let wroteMessage = false;
      for (const message of result.messages) {
        if (wroteMessage && !room()) { scanned = connection.sentThrough; break; }
        const payload = event({ type: 'msg', seq: message.sequence, message });
        const bytes = Buffer.byteLength(payload);
        connection.inflight.push({ seq: message.sequence, bytes });
        connection.inflightBytes += bytes;
        connection.sentThrough = message.sequence;
        if (!await send(payload)) return false;
        wroteMessage = true;
      }
      connection.scanned = scanned;
      if (wroteMessage && !await through()) return false;
      if (connection.scanned < result.tail && room()) connection.dirty = true;
      return true;
    };

    if (!await send(event(hello))) return;
    while (!signal.aborted) {
      if (connection.dirty && room()) {
        if (!await batch()) break;
        continue;
      }
      await wait(connection, () => connection.dirty && room(), lastWrite + K);
      if (signal.aborted) break;
      if (connection.dirty && room()) continue;
      if (Date.now() - lastWrite < K) continue;
      // Keepalive tick: advance over traffic that never woke this host, then report it.
      const before = lastWrite;
      if (room() && !await batch()) break;
      if (!await through()) break;
      if (lastWrite === before && !await send(': keepalive\n\n')) break;
    }
  }

  async function receiptV2(host: HostPrincipal, body: unknown): Promise<Outcome<HostReceiptResult>> {
    if (!isHostReceipt(body)) return error('invalid_request', 'Invalid receipt');
    const acked = store.hostAcked(host);
    if (acked === undefined) return error('owner_conflict', 'Host is not on protocol 2');
    if (body.through <= acked) return { ok: true, value: { through: acked } };
    const connection = active.get(host.hostId);
    if (!connection || connection.version !== 2 || connection.conn !== body.conn) return error('owner_conflict', 'Superseded connection');
    if (body.through > connection.sentThrough) return error('invalid_request', 'Receipt is ahead of the sent position');
    store.advanceHost(host, body.through);
    connection.acked = body.through;
    while (connection.inflight.length && connection.inflight[0]!.seq <= body.through) {
      connection.inflightBytes -= connection.inflight.shift()!.bytes;
    }
    connection.wake();
    return { ok: true, value: { through: body.through } };
  }

  const handler: ControlHandler = async (request, response) => {
    try {
      if (closed) { json(response, error('temporarily_unavailable', 'Stream handler is closed')); return; }
      const url = new URL(request.url ?? '/', 'http://localhost');
      const path = url.pathname;
      if (path !== ROUTES.stream && path !== ROUTES.receipt && path !== ROUTES.streamV2 && path !== ROUTES.receiptV2) {
        json(response, error('not_found', 'Unknown route')); return;
      }
      const identity = auth.authenticate(bearerCredential(request.headers.authorization));
      if (!identity.ok) { json(response, identity); return; }
      const host = identity.value;
      if (path !== ROUTES.streamV2 && !closedQuery(url)) { json(response, error('invalid_request', 'Unexpected query')); return; }
      if (path === ROUTES.receipt || path === ROUTES.receiptV2) {
        if (request.method !== 'POST') { json(response, error('not_found', 'Unknown route')); return; }
        let body: unknown;
        try { body = await readBody(request); }
        catch (problem) {
          const { code, message } = readBodyError(problem);
          json(response, error('invalid_request', message), code); return;
        }
        if (path === ROUTES.receiptV2) {
          let result: Outcome<HostReceiptResult>;
          try { result = await receiptV2(host, body); }
          catch (problem) { fail(); throw problem; }
          json(response, result);
          return;
        }
        if (!isReceipt(body)) { json(response, error('invalid_request', 'Invalid receipt')); return; }
        let result: ReturnType<CoordinatorStore['acknowledge']>;
        try { result = store.acknowledge(host, body); }
        catch (problem) { fail(); throw problem; }
        if (result.ok) {
          const connection = active.get(host.hostId);
          // Store notify runs before acknowledge returns, so clear flight and wake again.
          if (connection?.version === 1 && connection.inFlight.get(body.subscriptionId) === body.messageId) connection.inFlight.delete(body.subscriptionId);
          connection?.wake();
        }
        json(response, result);
        return;
      }
      if (!acceptsEventStream(request)) {
        json(response, error('invalid_request', 'Expected GET with Accept: text/event-stream')); return;
      }
      if (path === ROUTES.stream) {
        // Refuse before superseding, so a stale v1 daemon cannot end a v2 stream.
        if (store.hostAcked(host) !== undefined) { json(response, error('owner_conflict', 'Host uses protocol 2')); return; }
        await serve(request, host.hostId, { ...base(response), version: 1, inFlight: new Map() }, connection => streamV1(host, connection));
        return;
      }
      const query = bookmark(url);
      if (!query) { json(response, error('invalid_request', 'Invalid stream query')); return; }
      // openHost runs before any SSE header; a refusal is a plain JSON outcome.
      const start = store.openHost(host, query);
      if (!start.ok) { json(response, start); return; }
      const { acked, resume, stream } = start.value;
      const connection: V2 = {
        ...base(response), version: 2, conn: randomUUID(), acked, sentThrough: resume, scanned: resume,
        sentMembers: new Map(), inflight: [], inflightBytes: 0,
      };
      const hello: HostFrame = { type: 'hello', stream, host: host.hostId, conn: connection.conn, resume,
        keepaliveMs: K, presenceWindowMs };
      await serve(request, host.hostId, connection, live => streamV2(host, live, hello));
    } catch {
      if (response.headersSent) response.destroy();
      else json(response, error('temporarily_unavailable', 'Stream request failed'));
    }
  };
  return Object.assign(handler, {
    async close(): Promise<void> {
      closed = true;
      unsubscribe();
      const connections = [...active.values()];
      for (const connection of connections) {
        connection.abort.abort();
        connection.response.destroy();
      }
      await Promise.all(connections.map(connection => connection.done));
    },
  });
}

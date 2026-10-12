import type { IncomingMessage, ServerResponse } from 'node:http';
import { once } from 'node:events';
import {
  ROUTES, MAX_JSON_BYTES, isReceipt, type ControlHandler, type CoordinatorStore, type HostAuthenticator,
  type Outcome, type StreamFrame,
} from '../../protocol/index.ts';

const POLL_MS = 250;
const KEEPALIVE_MS = 15_000;
const WRITE_DEADLINE_MS = 5_000;
const BODY_DEADLINE_MS = 10_000;
const status = { invalid_request: 400, unauthorized: 401, unknown_host: 403, not_found: 404,
  owner_conflict: 409, temporarily_unavailable: 503 } as const;

function json(response: ServerResponse, result: Outcome<unknown>, code?: number): void {
  if (response.destroyed || response.writableEnded) return;
  response.writeHead(code ?? (result.ok ? 200 : status[result.error]), {
    'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store',
  });
  response.end(JSON.stringify(result));
}
function error(code: keyof typeof status, message: string): Outcome<never> {
  return { ok: false, error: code, message };
}
function wait(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise(resolve => {
    const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
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
async function readReceipt(request: IncomingMessage): Promise<unknown> {
  const type = request.headers['content-type'];
  if (typeof type !== 'string' || !/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(type))
    throw { code: 415, message: 'Content-Type must be application/json' };
  const length = request.headers['content-length'];
  if (length !== undefined && (typeof length !== 'string' || !/^\d+$/.test(length) || Number(length) > MAX_JSON_BYTES))
    throw { code: 413, message: 'Request body exceeds 1 MiB' };
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let settled = false;
    const timer = setTimeout(() => finish({ code: 408, message: 'Request body deadline exceeded' }), BODY_DEADLINE_MS);
    function cleanup() {
      clearTimeout(timer);
      request.off('data', data);
      request.off('end', end);
      request.off('error', interrupted);
      request.off('aborted', interrupted);
    }
    function finish(problem?: { code: number; message: string }, value?: unknown) {
      if (settled) return;
      settled = true;
      cleanup();
      if (problem) { request.pause(); reject(problem); }
      else resolve(value);
    }
    function data(chunk: Buffer) {
      size += chunk.length;
      if (size > MAX_JSON_BYTES) finish({ code: 413, message: 'Request body exceeds 1 MiB' });
      else chunks.push(chunk);
    }
    function end() {
      try { finish(undefined, JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { finish({ code: 400, message: 'Malformed JSON' }); }
    }
    function interrupted() { finish({ code: 400, message: 'Request body interrupted' }); }
    request.on('data', data);
    request.once('end', end);
    request.once('error', interrupted);
    request.once('aborted', interrupted);
  });
}

/** SQLite is the sole replay source. Polling after each read avoids a catch-up/live subscription gap. */
export type StreamHandler = ControlHandler & { close(): Promise<void> };
export function createStreamHandler(store: CoordinatorStore, auth: HostAuthenticator): StreamHandler {
  const active = new Map<string, { abort: AbortController; response: ServerResponse; inFlight: Map<string, string>;
    done: Promise<void>; resolveDone(): void }>();
  let closed = false;
  const handler: ControlHandler = async (request, response) => {
    try {
      if (closed) { json(response, error('temporarily_unavailable', 'Stream handler is closed')); return; }
      const url = new URL(request.url ?? '/', 'http://localhost');
      if (url.pathname !== ROUTES.stream && url.pathname !== ROUTES.receipt) {
        json(response, error('not_found', 'Unknown route')); return;
      }
      const match = typeof request.headers.authorization === 'string' && /^Bearer ([^\s]+)$/.exec(request.headers.authorization);
      const identity = auth.authenticate(match ? match[1]! : null);
      if (!identity.ok) { json(response, identity); return; }
      if (url.search) { json(response, error('invalid_request', 'Unexpected query')); return; }
      if (url.pathname === ROUTES.receipt) {
        if (request.method !== 'POST') { json(response, error('not_found', 'Unknown route')); return; }
        let body: unknown;
        try { body = await readReceipt(request); }
        catch (problem) {
          const { code, message } = problem as { code: number; message: string };
          json(response, error('invalid_request', message), code); return;
        }
        if (!isReceipt(body)) { json(response, error('invalid_request', 'Invalid receipt')); return; }
        const result = store.acknowledge(identity.value, body);
        if (result.ok) {
          const flight = active.get(identity.value.hostId)?.inFlight;
          if (flight?.get(body.subscriptionId) === body.messageId) flight.delete(body.subscriptionId);
        }
        json(response, result);
        return;
      }
      if (request.method !== 'GET' || !request.headers.accept?.split(',').some(type => /^text\/event-stream(?:\s*;|$)/i.test(type.trim()))) {
        json(response, error('invalid_request', 'Expected GET with Accept: text/event-stream')); return;
      }
      const hostId = identity.value.hostId;
      const previous = active.get(hostId);
      previous?.abort.abort();
      previous?.response.destroy();
      let resolveDone!: () => void;
      const done = new Promise<void>(resolve => { resolveDone = resolve; });
      const connection = { abort: new AbortController(), response, inFlight: new Map<string, string>(), done, resolveDone };
      active.set(hostId, connection);
      const { signal } = connection.abort;
      const disconnect = () => connection.abort.abort();
      response.once('close', disconnect);
      request.once('aborted', disconnect);
      try {
        response.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform',
          Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
        response.flushHeaders();
        let lastWrite = Date.now();
        while (!signal.aborted) {
          // Re-read from SQLite on every pass, including after idle and after receipts.
          const frames: StreamFrame[] = store.pending(identity.value, 500);
          for (const frame of frames) {
            if (signal.aborted) break;
            if (connection.inFlight.has(frame.subscriptionId)) continue;
            const payload = `id: ${frame.message.id}\nevent: message\ndata: ${JSON.stringify(frame)}\n\n`;
            connection.inFlight.set(frame.subscriptionId, frame.message.id);
            if (!await write(response, payload, signal)) { connection.abort.abort(); break; }
            lastWrite = Date.now();
          }
          if (signal.aborted) break;
          if (Date.now() - lastWrite >= KEEPALIVE_MS) {
            if (!await write(response, ': keepalive\n\n', signal)) { connection.abort.abort(); break; }
            lastWrite = Date.now();
          }
          await wait(POLL_MS, signal);
        }
      } finally {
        response.off('close', disconnect);
        request.off('aborted', disconnect);
        if (active.get(hostId) === connection) active.delete(hostId);
        if (!response.destroyed) response.end();
        connection.resolveDone();
      }
    } catch {
      if (response.headersSent) response.destroy();
      else json(response, error('temporarily_unavailable', 'Stream request failed'));
    }
  };
  return Object.assign(handler, {
    async close(): Promise<void> {
      closed = true;
      const connections = [...active.values()];
      for (const connection of connections) {
        connection.abort.abort();
        connection.response.destroy();
      }
      await Promise.all(connections.map(connection => connection.done));
    },
  });
}

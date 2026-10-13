import type { ServerResponse } from 'node:http';
import { once } from 'node:events';
import {
  ROUTES, isReceipt, type ControlHandler, type CoordinatorStore, type HostAuthenticator,
  type FailureCode, type Outcome, type StreamFrame,
} from '../../protocol/index.ts';
import { json, readBody } from '../http.ts';

const KEEPALIVE_MS = 15_000;
const WRITE_DEADLINE_MS = 5_000;

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

type Live = {
  abort: AbortController; response: ServerResponse; inFlight: Map<string, string>;
  dirty: boolean; waiting?: () => void; wake(): void; done: Promise<void>; resolveDone(): void;
};
/** SQLite is the sole replay source. Subscribe before the first pending read. */
export type StreamHandler = ControlHandler & { close(): Promise<void> };
export function createStreamHandler(store: CoordinatorStore, auth: HostAuthenticator): StreamHandler {
  const active = new Map<string, Live>();
  let closed = false;
  const fail = () => { for (const connection of active.values()) { connection.abort.abort(); connection.response.destroy(); } };
  const unsubscribe = store.subscribe(hostId => {
    const targets = hostId === undefined ? [...active.values()] : active.has(hostId) ? [active.get(hostId)!] : [];
    try { for (const connection of targets) connection.wake(); }
    catch (problem) { fail(); throw problem; }
  });
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
        try { body = await readBody(request); }
        catch (problem) {
          const { code, message } = problem as { code: number; message: string };
          json(response, error('invalid_request', message), code); return;
        }
        if (!isReceipt(body)) { json(response, error('invalid_request', 'Invalid receipt')); return; }
        let result: ReturnType<CoordinatorStore['acknowledge']>;
        try { result = store.acknowledge(identity.value, body); }
        catch (problem) { fail(); throw problem; }
        if (result.ok) {
          const connection = active.get(identity.value.hostId);
          // Store notify runs before acknowledge returns, so clear flight and wake again.
          if (connection?.inFlight.get(body.subscriptionId) === body.messageId) connection.inFlight.delete(body.subscriptionId);
          connection?.wake();
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
      const connection: Live = {
        abort: new AbortController(), response, inFlight: new Map(), dirty: true, done, resolveDone,
        wake() { this.dirty = true; const resume = this.waiting; this.waiting = undefined; resume?.(); },
      };
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
          if (connection.dirty) {
            connection.dirty = false;
            const frames: StreamFrame[] = store.pending(identity.value, 500);
            let wrote = false;
            for (const frame of frames) {
              if (signal.aborted) break;
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
          await new Promise<void>(resolve => {
            if (signal.aborted || connection.dirty) { resolve(); return; }
            const finish = () => {
              clearTimeout(timer);
              signal.removeEventListener('abort', finish);
              if (connection.waiting === finish) connection.waiting = undefined;
              resolve();
            };
            connection.waiting = finish;
            const timer = setTimeout(finish, Math.max(0, KEEPALIVE_MS - (Date.now() - lastWrite)));
            signal.addEventListener('abort', finish, { once: true });
          });
          if (signal.aborted) break;
          if (connection.dirty) continue;
          if (Date.now() - lastWrite >= KEEPALIVE_MS) {
            if (!await write(response, ': keepalive\n\n', signal)) { connection.abort.abort(); break; }
            lastWrite = Date.now();
          }
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

import type { ServerResponse } from 'node:http';
import { once } from 'node:events';
import {
  ROUTES, isReceipt, type ControlHandler, type CoordinatorStore, type HostAuthenticator,
  type FailureCode, type Outcome, type StreamFrame,
} from '../../protocol/index.ts';
import { json, readBody } from '../http.ts';

const POLL_MS = 250;
const KEEPALIVE_MS = 15_000;
const WRITE_DEADLINE_MS = 5_000;

function error(code: FailureCode, message: string): Outcome<never> {
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
        try { body = await readBody(request); }
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

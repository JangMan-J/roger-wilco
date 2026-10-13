import type { IncomingMessage, ServerResponse } from 'node:http';
import { MAX_JSON_BYTES, type FailureCode, type Outcome } from '../protocol/index.ts';

const BODY_DEADLINE_MS = 10_000;
const status: Record<FailureCode, number> = {
  invalid_request: 400, unauthorized: 401, unknown_host: 403,
  not_found: 404, owner_conflict: 409, temporarily_unavailable: 503,
};

export function bearerCredential(authorization: string | string[] | undefined): string | null {
  if (typeof authorization !== 'string') return null;
  const match = /^Bearer ([^\s]+)$/.exec(authorization);
  return match ? match[1]! : null;
}
export function closedQuery(url: URL): boolean {
  return url.search.length === 0;
}
export function openQuery(url: URL, allowed: readonly string[]): boolean {
  return [...url.searchParams.keys()].every(name => allowed.includes(name) && url.searchParams.getAll(name).length === 1);
}
export function readBodyError(error: unknown): { code: number; message: string } {
  if (error !== null && typeof error === 'object' && 'code' in error && 'message' in error) {
    const { code, message } = error as { code: unknown; message: unknown };
    if (typeof code === 'number' && typeof message === 'string') return { code, message };
  }
  return { code: 400, message: 'Malformed request body' };
}

export function json(response: ServerResponse, result: Outcome<unknown>, code?: number): void {
  if (response.writableEnded || response.destroyed) return;
  response.writeHead(code ?? (result.ok ? 200 : status[result.error]), {
    'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store',
  });
  response.end(JSON.stringify(result));
}

export function readBody(request: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const type = request.headers['content-type'];
    if (typeof type !== 'string' || !/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(type)) {
      reject({ code: 415, message: 'Content-Type must be application/json' }); return;
    }
    const length = request.headers['content-length'];
    if (length !== undefined && (typeof length !== 'string' || !/^\d+$/.test(length) || Number(length) > MAX_JSON_BYTES)) {
      reject({ code: 413, message: 'Request body exceeds 1 MiB' }); return;
    }
    const chunks: Buffer[] = [];
    let size = 0;
    let done = false;
    const timer = setTimeout(() => finish({ code: 408, message: 'Request body deadline exceeded' }), BODY_DEADLINE_MS);
    function cleanup() {
      clearTimeout(timer);
      request.off('data', onData);
      request.off('end', onEnd);
      request.off('error', onError);
      request.off('aborted', onAborted);
    }
    function finish(error?: { code: number; message: string }, value?: unknown) {
      if (done) return;
      done = true;
      cleanup();
      if (error) { request.pause(); reject(error); }
      else resolve(value);
    }
    function onData(chunk: Buffer) {
      size += chunk.length;
      if (size > MAX_JSON_BYTES) finish({ code: 413, message: 'Request body exceeds 1 MiB' });
      else chunks.push(chunk);
    }
    function onEnd() {
      try { finish(undefined, JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { finish({ code: 400, message: 'Malformed JSON' }); }
    }
    function onError() { finish({ code: 400, message: 'Request body interrupted' }); }
    function onAborted() { finish({ code: 400, message: 'Request body aborted' }); }
    request.on('data', onData);
    request.once('end', onEnd);
    request.once('error', onError);
    request.once('aborted', onAborted);
  });
}

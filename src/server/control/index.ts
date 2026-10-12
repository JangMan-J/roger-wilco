import type { IncomingMessage, ServerResponse } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';
import {
  ROUTES, isMessageInput, isRegistration, isSessionAddress, sessionKey, MAX_JSON_BYTES,
  type ControlHandler, type CoordinatorStore, type HostAuthenticator, type HostPrincipal,
  type Outcome, type SessionAddress,
} from '../../protocol/index.ts';

const REQUEST_DEADLINE_MS = 10_000;
const status: Record<string, number> = {
  invalid_request: 400, unauthorized: 401, unknown_host: 403,
  not_found: 404, owner_conflict: 409, temporarily_unavailable: 503,
};

/** Configure host IDs as keys and their bearer credentials as values. */
export function createHostAuthenticator(hostCredentials: Readonly<Record<string, string>>, knownHosts: ReadonlySet<string> = new Set(Object.keys(hostCredentials))): HostAuthenticator {
  const entries = Object.entries(hostCredentials);
  if (entries.some(([host, token]) => !host.trim() || !token.trim()) ||
      new Set(entries.map(([, token]) => token)).size !== entries.length) {
    throw new TypeError('Host IDs and credentials must be nonempty; credentials must be unique');
  }
  return {
    authenticate(bearer) {
      if (!bearer) return failure('unauthorized', 'Missing bearer credential');
      const candidate = Buffer.from(bearer);
      let hostId: string | undefined;
      for (const [id, token] of entries) {
        const expected = Buffer.from(token);
        if (candidate.length === expected.length && timingSafeEqual(candidate, expected)) hostId = id;
      }
      if (hostId && !knownHosts.has(hostId)) return { ok: false, error: 'unknown_host', message: 'Host is not provisioned' };
      return hostId ? { ok: true, value: { hostId } } : failure('unauthorized', 'Invalid bearer credential');
    },
  };
}

function failure<E extends 'invalid_request' | 'unauthorized' | 'not_found'>(error: E, message: string): Outcome<never> {
  return { ok: false, error, message };
}
function json(response: ServerResponse, result: Outcome<unknown>, code?: number): void {
  if (response.writableEnded || response.destroyed) return;
  response.writeHead(code ?? (result.ok ? 200 : status[result.error]), { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(result));
}
function keys(value: unknown, required: string[], optional: string[] = []): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    required.every(key => Object.hasOwn(value, key)) &&
    Object.keys(value).every(key => required.includes(key) || optional.includes(key));
}
function validClaimKey(key: unknown): key is string {
  return typeof key === 'string' && !!key.trim();
}
function parseInteger(text: string | null, min: number, max: number): number | null {
  if (text === null || !/^(0|[1-9]\d*)$/.test(text)) return null;
  const value = Number(text);
  return Number.isSafeInteger(value) && value >= min && value <= max ? value : null;
}
function readBody(request: IncomingMessage): Promise<unknown> {
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
    const timer = setTimeout(() => finish({ code: 408, message: 'Request body deadline exceeded' }), REQUEST_DEADLINE_MS);
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
function toolResult(result: Outcome<unknown>) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(result) }], ...(!result.ok ? { isError: true } : {}) };
}

/** Stateless MCP transport; every acting session is an explicit tool argument, checked by the store against the authenticated host. */
async function mcp(request: IncomingMessage, response: ServerResponse, store: CoordinatorStore, host: HostPrincipal, body: unknown): Promise<void> {
  const server = new McpServer({ name: 'roger-wilco', version: '1.0.0' });
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
  const address = z.object({ namespace: z.string(), piSessionId: z.string() }).strict();
  const actor = (session: SessionAddress): boolean => isSessionAddress(session);
  server.registerTool('list_sessions', { description: 'List registered session addresses and hosts.', inputSchema: {} },
    async () => toolResult({ ok: true, value: store.listSessions() }));
  server.registerTool('read_board', { description: 'Read registered sessions and active work claims.', inputSchema: {} },
    async () => toolResult({ ok: true, value: { sessions: store.listSessions(), claims: store.listClaims() } }));
  server.registerTool('register_session', { description: 'Register the current Pi session on this host.', inputSchema: { session: address, displayName: z.string().optional() } },
    async ({ session, displayName }) => toolResult(isRegistration({ session, displayName }) ? store.register(host, { session, displayName }) : failure('invalid_request', 'Invalid registration')));
  server.registerTool('send_message', { description: 'Send to a session address or broadcast to all other sessions.', inputSchema: {
    session: address, recipient: z.string(), body: z.string(), data: z.unknown().optional(), visibility: z.enum(['private', 'public']).optional(),
  } }, async ({ session, recipient, body, data, visibility }) => {
    const message = { recipient, body, data, visibility };
    return toolResult(actor(session) && isMessageInput(message) ? store.send(host, session, message) : failure('invalid_request', 'Invalid session or message'));
  });
  server.registerTool('inbox', { description: 'Query historical messages for this host-bound session.', inputSchema: {
    session: address, since: z.number().optional(), limit: z.number().optional(),
  } }, async ({ session, since = 0, limit = 100 }) => toolResult(actor(session) && Number.isSafeInteger(since) && since >= 0 &&
    Number.isSafeInteger(limit) && limit >= 1 && limit <= 500 ? store.inbox(host, session, since, limit) : failure('invalid_request', 'Invalid inbox query')));
  server.registerTool('claim_work', { description: 'Acquire or renew a claim owned by this Pi session.', inputSchema: {
    session: address, key: z.string(), ttlSeconds: z.number().optional(), note: z.string().optional(),
  } }, async ({ session, key, ttlSeconds = 900, note }) => toolResult(actor(session) && validClaimKey(key) &&
    Number.isSafeInteger(ttlSeconds) && ttlSeconds >= 0 &&
    (note === undefined || typeof note === 'string') ? store.claim(host, key, sessionKey(session), ttlSeconds, note) : failure('invalid_request', 'Invalid claim')));
  server.registerTool('release_work', { description: 'Release a claim owned by this Pi session.', inputSchema: { session: address, key: z.string() } },
    async ({ session, key }) => toolResult(actor(session) && validClaimKey(key) ? store.releaseClaim(host, key, sessionKey(session)) : failure('invalid_request', 'Invalid claim')));
  // The MCP server owns its connected transport; close once on completion, disconnect, or deadline.
  let closing: Promise<void> | undefined;
  const close = () => closing ??= server.close();
  response.once('close', () => { void close().catch(() => {}); });
  const deadline = setTimeout(() => {
    json(response, { ok: false, error: 'temporarily_unavailable', message: 'MCP request deadline exceeded' });
    void close().catch(() => {});
  }, REQUEST_DEADLINE_MS);
  try {
    await server.connect(transport);
    await transport.handleRequest(request, response, body);
  } catch {
    if (!response.headersSent) json(response, { ok: false, error: 'temporarily_unavailable', message: 'MCP request failed' });
  } finally {
    clearTimeout(deadline);
    await close();
  }
}

/** HTTP control only; stream and receipt routes belong to lane C and return 404 here. */
export function createControlHandler(store: CoordinatorStore, auth: HostAuthenticator): ControlHandler {
  return async (request, response) => {
    try {
      const url = new URL(request.url ?? '/', 'http://localhost');
      const path = url.pathname;
      if (request.method === 'GET' && path === ROUTES.health) {
        json(response, { ok: true, value: { service: 'roger-wilco', version: 1 } }); return;
      }
      const authorization = request.headers.authorization;
      const match = typeof authorization === 'string' && /^Bearer ([^\s]+)$/.exec(authorization);
      const identity = auth.authenticate(match ? match[1]! : null);
      if (!identity.ok) { json(response, identity); return; }
      if (url.search && path !== ROUTES.inbox) { json(response, failure('invalid_request', 'Unexpected query')); return; }
      const host = identity.value;
      if (request.method === 'GET' && path === ROUTES.sessions) {
        json(response, { ok: true, value: store.listSessions() }); return;
      }
      if (request.method === 'GET' && path === ROUTES.claims) {
        json(response, { ok: true, value: store.listClaims() }); return;
      }
      if (request.method === 'GET' && path === ROUTES.inbox) {
        const params = url.searchParams;
        const since = parseInteger(params.get('since'), 0, Number.MAX_SAFE_INTEGER);
        const limit = parseInteger(params.get('limit'), 1, 500);
        const session = { namespace: params.get('namespace'), piSessionId: params.get('piSessionId') };
        if ([...params.keys()].some(key => !['namespace', 'piSessionId', 'since', 'limit'].includes(key)) ||
            [...params.keys()].some(key => params.getAll(key).length !== 1) || !isSessionAddress(session) || since === null || limit === null) {
          json(response, failure('invalid_request', 'Invalid inbox query')); return;
        }
        json(response, store.inbox(host, session, since, limit)); return;
      }
      const post = [ROUTES.register, ROUTES.messages, ROUTES.claims, ROUTES.claimRelease, '/mcp'];
      if (request.method !== 'POST' || !post.includes(path)) { json(response, failure('not_found', 'Unknown route')); return; }
      let body: unknown;
      try { body = await readBody(request); }
      catch (error) {
        const { code, message } = error as { code: number; message: string };
        json(response, failure('invalid_request', message), code); return;
      }
      if (path === '/mcp') { await mcp(request, response, store, host, body); return; }
      if (path === ROUTES.register) {
        json(response, isRegistration(body) ? store.register(host, body) : failure('invalid_request', 'Invalid registration')); return;
      }
      if (path === ROUTES.messages) {
        json(response, keys(body, ['sender', 'message']) && isSessionAddress(body.sender) && isMessageInput(body.message) ?
          store.send(host, body.sender, body.message) : failure('invalid_request', 'Invalid message request')); return;
      }
      if (path === ROUTES.claims || path === ROUTES.claimRelease) {
        const acquire = path === ROUTES.claims;
        if (!keys(body, ['key', 'owner', ...(acquire ? ['ttlSeconds'] : [])], acquire ? ['note'] : []) ||
            !validClaimKey(body.key) || !isSessionAddress(body.owner) ||
            (acquire && (!Number.isSafeInteger(body.ttlSeconds) || (body.ttlSeconds as number) < 0 ||
              (body.note !== undefined && typeof body.note !== 'string')))) {
          json(response, failure('invalid_request', 'Invalid claim request')); return;
        }
        const owner = sessionKey(body.owner);
        json(response, acquire ? store.claim(host, body.key, owner, body.ttlSeconds as number, body.note as string | undefined) :
          store.releaseClaim(host, body.key, owner)); return;
      }
    } catch {
      json(response, { ok: false, error: 'temporarily_unavailable', message: 'Control request failed' });
    }
  };
}

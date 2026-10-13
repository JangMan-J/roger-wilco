import { timingSafeEqual } from 'node:crypto';
import {
  ROUTES, isEntityId, isMessageInput, isRegistration, isSessionAddress, sessionKey,
  type ControlHandler, type CoordinatorStore, type HostAuthenticator, type Outcome,
} from '../../protocol/index.ts';
import { bearerCredential, closedQuery, json, openQuery, readBody, readBodyError } from '../http.ts';

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
/** HTTP control only; stream and receipt routes belong to lane C and return 404 here. */
export function createControlHandler(store: CoordinatorStore, auth: HostAuthenticator): ControlHandler {
  return async (request, response) => {
    try {
      const url = new URL(request.url ?? '/', 'http://localhost');
      const path = url.pathname;
      if (request.method === 'GET' && path === ROUTES.health) {
        json(response, { ok: true, value: { service: 'roger-wilco', version: 1 } }); return;
      }
      const identity = auth.authenticate(bearerCredential(request.headers.authorization));
      if (!identity.ok) { json(response, identity); return; }
      if (!closedQuery(url) && path !== ROUTES.inbox && path !== ROUTES.entity) { json(response, failure('invalid_request', 'Unexpected query')); return; }
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
        if (!openQuery(url, ['namespace', 'piSessionId', 'since', 'limit']) || !isSessionAddress(session) || since === null || limit === null) {
          json(response, failure('invalid_request', 'Invalid inbox query')); return;
        }
        json(response, store.inbox(host, session, since, limit)); return;
      }
      if (request.method === 'GET' && path === ROUTES.entity) {
        const params = url.searchParams;
        const id = params.get('id');
        const since = parseInteger(params.get('since'), 0, Number.MAX_SAFE_INTEGER);
        const limit = parseInteger(params.get('limit'), 1, 500);
        if (!openQuery(url, ['id', 'since', 'limit']) || !isEntityId(id) || since === null || limit === null) {
          json(response, failure('invalid_request', 'Invalid entity query')); return;
        }
        json(response, store.entity(id, since, limit)); return;
      }
      const post: string[] = [ROUTES.register, ROUTES.rank, ROUTES.messages, ROUTES.claims, ROUTES.claimRelease];
      if (request.method !== 'POST' || !post.includes(path)) { json(response, failure('not_found', 'Unknown route')); return; }
      let body: unknown;
      try { body = await readBody(request); }
      catch (error) {
        const { code, message } = readBodyError(error);
        json(response, failure('invalid_request', message), code); return;
      }
      if (path === ROUTES.register) {
        json(response, isRegistration(body) ? store.register(host, body) : failure('invalid_request', 'Invalid registration')); return;
      }
      if (path === ROUTES.rank) {
        if (!keys(body, ['session', 'rank']) || !isSessionAddress(body.session) ||
            typeof body.rank !== 'number' || !Number.isInteger(body.rank) || body.rank < 1 || body.rank > 5) {
          json(response, failure('invalid_request', 'Invalid rank request')); return;
        }
        json(response, store.setRank(host, body.session, body.rank)); return;
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

/** Claude Code tool surface. It sees the core only through CoreAccess; it never touches the lease, delivery, or Claude Code session IDs. */
import {
  ROUTES, ownRegistrationResult, entityReference, isEntityId, isMessageInput, sessionKey,
  type Claim, type Outcome, type RegistrationResult, type SessionAddress,
} from '../protocol/index.ts';

const PRESENCE_MS = 90_000;

export type Failure = Extract<Outcome<never>, { ok: false }>;
/** The invoking session, resolved while this runtime holds its lease. */
export type BoundSession = { actor: SessionAddress; hostId: string };

/** What the Claude Code surface needs from the adapter. The Claude Code twin of the Pi adapter's CoreAccess. */
export interface CoreAccess {
  /** The adapter's own session, while bound; a failure while unbound. */
  bound(): Promise<BoundSession | Failure>;
  /** Register the invoking session, then make one coordinator call as it. A register route returns the registration. */
  call(route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>>;
}

const failure = (message: string): Failure => ({ ok: false, error: 'temporarily_unavailable', message });

/** Dispatch one roger_* tool by name. Any Claude Code tool host (CLI, MCP, plugin) registers this same dispatcher. */
export function createClaudeTools(core: CoreAccess): (name: string, params?: Record<string, unknown>) => Promise<Outcome<unknown>> {
  async function board(all = false): Promise<Outcome<unknown>> {
    const first = await core.call(ROUTES.sessions, 'GET') as Outcome<RegistrationResult[]>;
    if (!first.ok) return first;
    const second = await core.call(ROUTES.claims, 'GET');
    const now = Date.now();
    // A version 1 coordinator can still list displayName and session entities.
    const rows = first.value.map(ownRegistrationResult);
    const visible = all ? rows : rows.filter(row =>
      typeof row.lastSeenAt === 'number' && Number.isFinite(row.lastSeenAt) && now - row.lastSeenAt <= PRESENCE_MS);
    return second.ok ? { ok: true, value: { sessions: visible, claims: (second.value as Claim[]) } } : second;
  }

  return async (name, params = {}) => {
    switch (name) {
      case 'roger_identity': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        return core.call(ROUTES.register, 'POST', { session: identity.actor });
      }
      case 'roger_send': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        const message = { recipient: params.recipient, body: params.body, data: params.data, visibility: params.visibility };
        if (!isMessageInput(message)) return { ok: false, error: 'invalid_request', message: 'Invalid message' };
        return core.call(ROUTES.messages, 'POST', { sender: identity.actor, message });
      }
      case 'roger_inbox': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        const query = new URLSearchParams({ namespace: identity.actor.namespace, sessionId: identity.actor.sessionId,
          since: String(params.since ?? 0), limit: String(params.limit ?? 100) });
        return core.call(`${ROUTES.inbox}?${query}`, 'GET');
      }
      case 'roger_entity': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        const since = params.since ?? 0;
        const limit = params.limit ?? 100;
        if ((params.label === undefined) === (params.id === undefined) ||
            (params.id !== undefined && params.type !== undefined) ||
            !Number.isSafeInteger(since) || (since as number) < 0 || !Number.isSafeInteger(limit) || (limit as number) < 1 || (limit as number) > 500) {
          return { ok: false, error: 'invalid_request', message: 'Provide exactly one label or ID, type only with label, and valid since/limit bounds' };
        }
        let id = params.id as string | undefined;
        if (params.label !== undefined) {
          try { id = entityReference(identity.actor.namespace, params.label as string, params.type as number | undefined).id; }
          catch { return { ok: false, error: 'invalid_request', message: 'Invalid entity label or type' }; }
        }
        if (!isEntityId(id)) return { ok: false, error: 'invalid_request', message: 'Invalid entity ID' };
        const query = new URLSearchParams({ id: id!, since: String(since), limit: String(limit) });
        return core.call(`${ROUTES.entity}?${query}`, 'GET');
      }
      case 'roger_board':
        return board(params.all === true);
      case 'roger_claim': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        return core.call(ROUTES.claims, 'POST', { owner: identity.actor, key: params.key,
          ttlSeconds: params.ttlSeconds ?? 900, ...(params.note === undefined ? {} : { note: params.note }) });
      }
      case 'roger_release': {
        const identity = await core.bound();
        if ('ok' in identity) return identity;
        return core.call(ROUTES.claimRelease, 'POST', { owner: identity.actor, key: params.key });
      }
      default:
        return { ok: false, error: 'invalid_request', message: `Unknown tool: ${name}` };
    }
  };
}

/** The session key this surface acts as, for CLI display. */
export async function claudeAddress(core: CoreAccess): Promise<string | Failure> {
  const identity = await core.bound();
  return 'ok' in identity ? identity : sessionKey(identity.actor);
}

export { failure as claudeFailure };

/** Pi tools and commands. They see the core only through CoreAccess; they never touch the lease, delivery, or Pi session IDs. */
import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { Type } from 'typebox';
import {
  ROUTES, entityReference, isEntityId, isMessageInput, sessionKey,
  type Claim, type Outcome, type RegistrationResult, type SessionAddress,
} from '../protocol/index.ts';

const PRESENCE_MS = 90_000;

export type Failure = Extract<Outcome<never>, { ok: false }>;
/** The invoking session, resolved while this runtime holds its lease. */
export type BoundSession = { actor: SessionAddress; hostId: string };

/** What the Pi surface needs from the adapter. */
export interface CoreAccess {
  /** The invocation's own session, never a host-global identity; a failure while unbound or after a session switch. */
  bound(ctx: ExtensionContext): Promise<BoundSession | Failure>;
  /** Register the invoking session, then make one coordinator call as it. A register route returns the registration. */
  call(ctx: ExtensionContext, route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>>;
}

const text = (value: unknown) => ({ content: [{ type: 'text' as const, text: JSON.stringify(value) }], details: value });
const rankLabel = (rank: number) => rank === 1 ? 'fleet (rank 1)' : `Rank ${rank}`;

export function registerPiSurface(pi: ExtensionAPI, core: CoreAccess): void {
  async function board(ctx: ExtensionContext, all = false): Promise<Outcome<unknown>> {
    const first = await core.call(ctx, ROUTES.sessions, 'GET') as Outcome<RegistrationResult[]>;
    if (!first.ok) return first;
    const second = await core.call(ctx, ROUTES.claims, 'GET');
    const now = Date.now();
    const visible = all ? first.value : first.value.filter(row =>
      typeof row.lastSeenAt === 'number' && Number.isFinite(row.lastSeenAt) && now - row.lastSeenAt <= PRESENCE_MS);
    return second.ok ? { ok: true, value: { sessions: visible, claims: second.value } } : second;
  }
  const boardCommand = {
    description: 'Show the board and pick a session rank; use /roger all for stale registrations',
    handler: async (args: string, ctx: ExtensionContext) => {
      if (args.trim() && args.trim() !== 'all') { ctx.ui.notify('Usage: /roger [all]', 'error'); return; }
      const all = args.trim() === 'all';
      const identity = await core.bound(ctx);
      if ('ok' in identity) { ctx.ui.notify(identity.message, 'error'); return; }
      const result = await board(ctx, all) as Outcome<{ sessions: RegistrationResult[]; claims: Claim[] }>;
      if (!result.ok) { ctx.ui.notify(result.message, 'error'); return; }
      const address = sessionKey(identity.actor);
      const labels = result.value.sessions.map(row =>
        `${row.subscription.id}${row.subscription.id === address ? ' (you)' : ''} — host ${JSON.stringify(row.subscription.hostId)}` +
        (row.displayName ? ` — ${JSON.stringify(row.displayName)}` : '') + ` — ${rankLabel(row.rank)}`);
      ctx.ui.notify([
        'Roger-Wilco',
        `Your address: ${address}`,
        `Your host: ${JSON.stringify(identity.hostId)}`,
        '',
        `${all ? 'All registered sessions' : 'Recent sessions (presence within 90s)'} (${result.value.sessions.length}):`,
        ...result.value.sessions.map((row, index) => labels[index] +
          (typeof row.lastSeenAt === 'number' && Number.isFinite(row.lastSeenAt)
            ? ` — last seen ${new Date(row.lastSeenAt).toISOString()} (${Math.max(0, Math.floor((Date.now() - row.lastSeenAt) / 1000))}s ago)`
            : ' — last seen unknown')),
        '',
        `Active claims (${result.value.claims.length}):`,
        ...result.value.claims.map(claim => `${JSON.stringify(claim.key)} — ${claim.owner}`),
        'Presence is registration activity, not proof that Pi is open or owns a lease.',
        'Ranks are labels only; they do not change permissions or routing.',
        ...(!all ? ['Use /roger all to inspect stale or unknown presence.'] : []),
        'Ask your agent to send a message to a listed address with roger_send.',
      ].join('\n'), 'info');
      if (!ctx.hasUI || !labels.length) return;
      const selected = await ctx.ui.select('Choose a session to assign a rank', ['View only (no changes)', ...labels]);
      const target = result.value.sessions[labels.indexOf(selected ?? '')];
      if (!target) return;
      const ranks = [1, 2, 3, 4, 5].map(rankLabel);
      const choice = await ctx.ui.select(`Rank for ${target.subscription.id} (current: ${rankLabel(target.rank)})`, ranks);
      const rank = ranks.indexOf(choice ?? '') + 1;
      if (!rank) return;
      const updated = await core.call(ctx, ROUTES.rank, 'POST', { session: target.subscription.session, rank }) as Outcome<RegistrationResult>;
      if (!updated.ok) { ctx.ui.notify(updated.message, 'error'); return; }
      ctx.ui.notify(`${updated.value.subscription.id}: ${rankLabel(updated.value.rank)}`, 'info');
    },
  };
  pi.registerCommand('roger', boardCommand);
  pi.registerCommand('roger_board', boardCommand);
  pi.registerTool({ name: 'roger_identity', label: 'Agent Identity', description: 'Read or set this session’s display name. Entity references such as [[0:FLEET]] attach opaque integer metadata without changing the session address. Omit displayName to read the registration.',
    parameters: Type.Object({ displayName: Type.Optional(Type.String({ maxLength: 200 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      return text(await core.call(ctx, ROUTES.register, 'POST', { session: identity.actor,
        ...(params.displayName === undefined ? {} : { displayName: params.displayName }) }));
    },
  });
  pi.registerTool({ name: 'roger_send', label: 'Send Agent Message', description: 'Send a message to a session address or broadcast (*). Text and JSON strings support [[label]] and [[153:Meteor Shower]]. The integer is opaque metadata (0–255), not identity. Bare references preserve it. Use roger_entity for current metadata and retained message mentions.',
    parameters: Type.Object({ recipient: Type.String(), body: Type.String(), data: Type.Optional(Type.Unknown()),
      visibility: Type.Optional(Type.Union([Type.Literal('private'), Type.Literal('public')])) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      const message = { recipient: params.recipient, body: params.body, data: params.data, visibility: params.visibility };
      if (!isMessageInput(message)) return text({ ok: false, error: 'invalid_request', message: 'Invalid message' });
      return text(await core.call(ctx, ROUTES.messages, 'POST', { sender: identity.actor, message }));
    },
  });
  pi.registerTool({ name: 'roger_inbox', label: 'Agent Inbox', description: 'Query this session’s historical inbox.',
    parameters: Type.Object({ since: Type.Optional(Type.Integer({ minimum: 0 })), limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      const query = new URLSearchParams({ namespace: identity.actor.namespace, sessionId: identity.actor.sessionId,
        since: String(params.since ?? 0), limit: String(params.limit ?? 100) });
      return text(await core.call(ctx, `${ROUTES.inbox}?${query}`, 'GET'));
    },
  });
  pi.registerTool({ name: 'roger_entity', label: 'Entity History', description: 'Look up current entity metadata and retained message mentions by label or ID. The legacy type field is optional and never selects another identity or updates metadata. Integer meanings belong to consumers.',
    parameters: Type.Object({ label: Type.Optional(Type.String()), type: Type.Optional(Type.Integer({ minimum: 0, maximum: 255 })),
      id: Type.Optional(Type.String()), since: Type.Optional(Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      const since = params.since ?? 0;
      const limit = params.limit ?? 100;
      if ((params.label === undefined) === (params.id === undefined) ||
          (params.id !== undefined && params.type !== undefined) ||
          !Number.isSafeInteger(since) || since < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) {
        return text({ ok: false, error: 'invalid_request', message: 'Provide exactly one label or ID, type only with label, and valid since/limit bounds' });
      }
      let id = params.id;
      if (params.label !== undefined) {
        try { id = entityReference(identity.actor.namespace, params.label, params.type).id; }
        catch { return text({ ok: false, error: 'invalid_request', message: 'Invalid entity label or type' }); }
      }
      if (!isEntityId(id)) return text({ ok: false, error: 'invalid_request', message: 'Invalid entity ID' });
      const query = new URLSearchParams({ id, since: String(since), limit: String(limit) });
      return text(await core.call(ctx, `${ROUTES.entity}?${query}`, 'GET'));
    },
  });
  pi.registerTool({ name: 'roger_board', label: 'Agent Board', description: 'List sessions seen within 90 seconds and active work claims. Presence is not proof of a live Pi lease; set all to include stale registrations.',
    parameters: Type.Object({ all: Type.Optional(Type.Boolean()) }), execute: async (_id, params, _signal, _update, ctx) => text(await board(ctx, params.all)) });
  pi.registerTool({ name: 'roger_claim', label: 'Claim Work', description: 'Acquire or renew a session-owned work claim. Key and note strings support [[label]] or [[153:label]] entity metadata. Claim keys remain literal; entity metadata does not grant ownership.',
    parameters: Type.Object({ key: Type.String({ minLength: 1, maxLength: 256 }), ttlSeconds: Type.Optional(Type.Integer({ minimum: 0, maximum: 2147483647 })), note: Type.Optional(Type.String({ maxLength: 4096 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      return text(await core.call(ctx, ROUTES.claims, 'POST', { owner: identity.actor, key: params.key, ttlSeconds: params.ttlSeconds ?? 900, ...(params.note === undefined ? {} : { note: params.note }) }));
    },
  });
  pi.registerTool({ name: 'roger_release', label: 'Release Work', description: 'Release a claim owned by this session using its exact key. Entity references in a successfully released key update opaque metadata without changing entity identity.',
    parameters: Type.Object({ key: Type.String({ minLength: 1, maxLength: 256 }) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await core.bound(ctx);
      if ('ok' in identity) return text(identity);
      return text(await core.call(ctx, ROUTES.claimRelease, 'POST', { owner: identity.actor, key: params.key }));
    },
  });
}

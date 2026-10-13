import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { Type } from 'typebox';
import { randomUUID } from 'node:crypto';
import { watch, type BigIntStats, type FSWatcher } from 'node:fs';
import { open, stat } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import { readHostConfig } from '../bin/config.ts';
import {
  ROUTES, entityReference, isEntityId, isMessageInput, isNamespace, isSessionAddress, sessionKey,
  type HostInbox, type Message, type Outcome, type PiConfig, type PiDelivery,
  type PiReceipt, type SessionAddress, type RegistrationResult, type Claim,
} from '../protocol/index.ts';

const RENEW_MS = 1000;
const REQUEST_MS = 5000;
const PRESENCE_MS = 90_000;
const text = (value: unknown) => ({ content: [{ type: 'text' as const, text: JSON.stringify(value) }], details: value });
const error = (message: string): Extract<Outcome<never>, { ok: false }> => ({ ok: false, error: 'temporarily_unavailable', message });

/** Explicit config or the provisioned user config; never a per-session destination file. */
export async function loadPiConfig(path?: string): Promise<PiConfig> {
  const config = await readHostConfig(path);
  if (!isNamespace(config.namespace)) throw new Error('Invalid Pi namespace');
  return { ...config, namespace: config.namespace };
}

function currentSession(ctx: ExtensionContext, namespace: string): SessionAddress {
  const session = { namespace, piSessionId: ctx.sessionManager.getSessionId().toLowerCase() };
  if (!isSessionAddress(session)) throw new Error('Roger-Wilco requires a persistent Pi UUID session');
  return session;
}

function sameSnapshot(a: BigIntStats, b: BigIntStats): boolean {
  return a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
}

/** Cache only the receipt index, not JSONL contents, and only while the disk snapshot is unchanged. */
function receiptReader() {
  let cached: { file: string; sessionId: string; snapshot: BigIntStats; entries: Map<string, string> } | undefined;
  return async (file: string | undefined, session: SessionAddress, id: string): Promise<PiReceipt | undefined> => {
    if (!file) { cached = undefined; return undefined; }
    const handle = await open(file, 'r');
    try {
      const snapshot = await handle.stat({ bigint: true });
      if (!cached || cached.file !== file || cached.sessionId !== session.piSessionId || !sameSnapshot(cached.snapshot, snapshot)) {
        cached = undefined;
        const lines = (await handle.readFile('utf8')).split('\n');
        lines.pop(); // Ignore an incomplete final JSONL line, including a partial header.
        const header = JSON.parse(lines.shift() ?? 'null') as { type?: string; id?: string } | null;
        const entries = new Map<string, string>();
        if (header?.type === 'session' && header.id?.toLowerCase() === session.piSessionId) {
          for (const line of lines) {
            const entry = JSON.parse(line) as { type?: string; id?: string; customType?: string; details?: { deliveryId?: string } };
            if (entry.type === 'custom_message' && entry.customType === 'roger-wilco' &&
                typeof entry.details?.deliveryId === 'string' && typeof entry.id === 'string' && entry.id.length &&
                !entries.has(entry.details.deliveryId)) entries.set(entry.details.deliveryId, entry.id);
          }
        }
        // A concurrent write/replacement is not a coherent receipt snapshot. Retry on the next pass.
        if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(file, { bigint: true }))) {
          throw new Error('Pi session file changed during receipt scan');
        }
        cached = { file, sessionId: session.piSessionId, snapshot, entries };
      }
      const entryId = cached.entries.get(id);
      if (!entryId) return undefined;
      // Even cached matches must cross the disk+fsync boundary on the currently opened file.
      await handle.sync();
      if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(file, { bigint: true }))) {
        cached = undefined;
        throw new Error('Pi session file changed during receipt sync');
      }
      return { messageId: id, piSessionId: session.piSessionId, entryId, sessionFile: file };
    } finally { await handle.close(); }
  };
}

/** An exact entry in the current session JSONL is the only Pi submission receipt. */
export const persistedReceipt = receiptReader();

/** The caller owns the inbox and releases its lease after stop(). */
type RuntimeDelivery = PiDelivery & { wake(): void; resumeAfterAbort(): Promise<void>; isBound(): boolean };

export function createPiDelivery(pi: ExtensionAPI, ctx: ExtensionContext, inbox: HostInbox,
  session: SessionAddress, runtimeId: string, report: (message: string | undefined) => void = () => {}): RuntimeDelivery {
  let stopped = false;
  let bound = false;
  let watcher: FSWatcher | undefined;
  let timer: NodeJS.Timeout | undefined;
  let renewTimer: NodeJS.Timeout | undefined;
  let renewing: Promise<void> | undefined;
  let running: Promise<void> | undefined;
  let dirty = false;
  let retryDelay = 250;
  let retryAbort = false;
  const submitted = new Set<string>();
  const readReceipt = receiptReader();
  let recoveringAbort = false;
  let missingFileReported = false;
  const active = () => !stopped && ctx.sessionManager.getSessionId().toLowerCase() === session.piSessionId;
  const file = () => ctx.sessionManager.getSessionFile();
  const authority = () => inbox.authority ? inbox.authority() : runtimeId;
  const valid = (token: string | undefined) =>
    active() && bound && token !== undefined && token === authority();
  const unsubscribe = inbox.onChange?.(() => {
    if (inbox.authority && authority() === undefined) bound = false;
    wake();
  });

  function retry() {
    if (timer || !active()) return;
    timer = setTimeout(() => {
      timer = undefined;
      wake();
      if (retryAbort) void resumeAfterAbort();
    }, retryDelay);
    timer.unref();
    retryDelay = Math.min(retryDelay * 2, 5000);
  }

  async function receipt(message: Message, token: string | undefined, path: string | undefined): Promise<'persisted' | 'absent' | 'unavailable'> {
    let proof: PiReceipt | undefined;
    try {
      proof = await readReceipt(path, session, message.id);
      if (missingFileReported) report(undefined);
      missingFileReported = false;
    }
    catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') {
        if (!missingFileReported) report('Pi session file is not yet persistent; delivery remains pending');
        missingFileReported = true;
        return 'absent'; // Pi may create the file when the first message triggers a turn.
      }
      report(`Pi session receipt unavailable: ${String(e)}`);
      return 'unavailable';
    }
    if (!valid(token) || file() !== path) return 'unavailable';
    if (!proof) return 'absent';
    if (path !== proof.sessionFile) return 'unavailable';
    const result = await inbox.acknowledgePi(session, runtimeId, proof);
    if (!valid(token) || file() !== path) return 'unavailable';
    if (!result.ok) { report(`Pi receipt rejected: ${result.message}`); return 'unavailable'; }
    submitted.delete(message.id);
    if (!submitted.size) recoveringAbort = false;
    return 'persisted';
  }

  async function ownsLease(token = authority()): Promise<boolean> {
    if (!valid(token)) return false;
    const result = await inbox.renew(session, runtimeId, Date.now());
    if (!valid(token)) return false;
    if (!result.ok) {
      bound = false;
      report(`Roger-Wilco session owner conflict: ${result.message}`);
      return false;
    }
    return valid(token);
  }

  function submit(message: Message, token: string | undefined, path: string | undefined) {
    if (!valid(token) || !path || file() !== path || submitted.has(message.id)) return;
    submitted.add(message.id);
    try {
      pi.sendMessage({ customType: 'roger-wilco',
        content: JSON.stringify(message),
        display: true, details: { deliveryId: message.id } }, { deliverAs: 'followUp', triggerTurn: true });
    } catch (e) { submitted.delete(message.id); report(`Pi submission failed: ${String(e)}`); retry(); }
  }

  async function pass() {
    const token = authority();
    const path = file();
    if (!valid(token)) return;
    const pending = await inbox.pending(session, runtimeId);
    if (!valid(token)) return;
    if (file() !== path) { retry(); return; }
    for (const message of pending) {
      if (!valid(token) || file() !== path) return;
      const state = await receipt(message, token, path);
      if (state === 'persisted') { dirty = true; continue; }
      if (state === 'unavailable') { retry(); continue; }
      if (!path) { report('Pi session has no persistent file; delivery remains pending'); retry(); continue; }
      if (submitted.has(message.id)) continue;
      // The receipt read is async: a different runtime may have taken this lease meanwhile.
      if (!(await ownsLease(token))) return;
      submit(message, token, path);
    }
  }
  function wake() {
    if (!active()) return;
    dirty = true;
    if (running) return;
    running = (async () => {
      while (dirty && active() && bound) {
        dirty = false;
        await pass();
      }
    })().catch(e => {
      bound = false;
      report(`Roger-Wilco delivery failed: ${String(e)}`);
      retry();
    }).finally(() => {
      running = undefined;
      if (!timer) retryDelay = 250;
      if (dirty && active() && bound) wake();
    });
  }
  async function resumeAfterAbort() {
      if (!active() || !bound || !submitted.size || recoveringAbort) return;
      const token = authority();
      const path = file();
      if (!valid(token)) return;
      retryAbort = false;
      recoveringAbort = true;
      try {
        // The user-input pending count excludes custom follow-ups. A submitted
        // ID may also have been acknowledged by a takeover owner while this
        // runtime was unbound; only wake IDs still pending in this inbox.
        const pending = await inbox.pending(session, runtimeId);
        if (!valid(token) || file() !== path ||
            !pending.some(message => submitted.has(message.id)) || !(await ownsLease(token)) ||
            !valid(token) || file() !== path || !submitted.size) {
          recoveringAbort = false;
          return;
        }
        // Pi leaves busy follow-ups queued on abort. One ID-free turn drains them
        // without submitting the original delivery a second time.
        pi.sendMessage({ customType: 'roger-wilco-wake', content: 'Continue pending agent messages.', display: false },
          { deliverAs: 'followUp', triggerTurn: true });
      } catch (e) {
        recoveringAbort = false;
        retryAbort = true;
        if (inbox.authority && authority() === undefined) bound = false;
        report(`Roger-Wilco abort recovery failed: ${String(e)}`);
        retry();
      }
  }
  return {
    wake,
    isBound: () => valid(authority()),
    resumeAfterAbort,
    async bind(address, owner) {
      if (sessionKey(address) !== sessionKey(session) || owner !== runtimeId || !active()) return error('Session changed during binding');
      if (valid(authority())) return { ok: true, value: null };
      let lease;
      try { lease = await inbox.bind(session, runtimeId, Date.now()); }
      catch (e) { bound = false; return error(`Host unavailable: ${String(e)}`); }
      if (!lease.ok) return lease;
      if (!active() || authority() === undefined) return error('Session or connection changed during binding');
      bound = true;
      const path = file();
      if (path && !watcher) {
        try { watcher = watch(dirname(path), { persistent: false }, (_event, name) => {
          if (name === null || name === basename(file() ?? path)) wake();
        }); }
        catch (e) { report(`Session watcher unavailable: ${String(e)}`); }
      }
      renewTimer ??= setInterval(() => {
        if (!active() || renewing) return;
        renewing = (async () => {
          if (bound) await ownsLease();
          else {
            const result = await inbox.bind(session, runtimeId, Date.now());
            if (result.ok && active() && authority() !== undefined) {
              bound = true; report(undefined); wake();
              if (retryAbort) void resumeAfterAbort();
              await inbox.enqueueRegistration({ session });
            }
            else if (!result.ok && result.error !== 'owner_conflict') report(`Roger-Wilco lease recovery: ${result.message}`);
          }
        })().catch(e => {
          bound = false;
          report(`Roger-Wilco lease renewal failed: ${String(e)}`);
        }).finally(() => { renewing = undefined; });
      }, RENEW_MS);
      renewTimer.unref();
      wake();
      return { ok: true, value: null };
    },
    async stop() {
      stopped = true;
      unsubscribe?.();
      watcher?.close();
      if (timer) clearTimeout(timer);
      if (renewTimer) clearInterval(renewTimer);
      await running;
      await renewing;
      if (bound) await inbox.release(session, runtimeId);
      bound = false;
    },
  };
}

async function request(config: PiConfig, route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>> {
  try {
    const response = await fetch(new URL(route, config.coordinatorUrl), {
      method, signal: AbortSignal.timeout(REQUEST_MS),
      headers: { Authorization: `Bearer ${config.credential}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const result: unknown = await response.json();
    if (!result || typeof result !== 'object' || !('ok' in result) || typeof result.ok !== 'boolean') return error('Invalid coordinator response');
    return result as Outcome<unknown>;
  } catch (e) { return error(`Coordinator unavailable: ${String(e)}`); }
}

type Runtime = { session: SessionAddress; runtimeId: string; inbox: HostInbox; delivery: RuntimeDelivery; config: PiConfig; stopped: boolean;
  heartbeat?: NodeJS.Timeout; bindRetry?: NodeJS.Timeout };

/** One extension runtime, one current session lease. Lifecycle changes are serialized. */
export default function rogerWilco(pi: ExtensionAPI): void {
  let runtime: Runtime | undefined;
  let transition = Promise.resolve();
  function status(ctx: ExtensionContext, message: string | undefined) {
    if (ctx.hasUI) ctx.ui.setStatus('roger-wilco', message);
  }
  async function stop() {
    const old = runtime;
    runtime = undefined;
    if (!old) return;
    old.stopped = true;
    if (old.heartbeat) clearInterval(old.heartbeat);
    if (old.bindRetry) clearInterval(old.bindRetry);
    try { await old.delivery.stop(); }
    finally { await old.inbox.close(); }
  }
  function serial(operation: () => Promise<void>) {
    transition = transition.catch(() => {}).then(operation);
    return transition;
  }
  pi.on('session_start', (_event, ctx) => serial(async () => {
    await stop();
    try {
      const config = await loadPiConfig();
      const session = currentSession(ctx, config.namespace);
      if (!ctx.sessionManager.getSessionFile()) throw new Error('Roger-Wilco requires a persistent Pi session manager');
      const { createHostClient } = await import('../host/client.ts');
      const inbox = createHostClient(config.inboxPath);
      const runtimeId = randomUUID();
      const delivery = createPiDelivery(pi, ctx, inbox, session, runtimeId, message => status(ctx, message));
      const next: Runtime = { session, runtimeId, inbox, delivery, config, stopped: false };
      try {
        const result = await delivery.bind(session, runtimeId);
        runtime = next;
        if (!result.ok) {
          status(ctx, `Roger-Wilco: ${result.error}: ${result.message}; waiting for lease`);
          let retrying = false;
          next.bindRetry = setInterval(() => {
            if (runtime !== next || next.stopped || retrying) return;
            retrying = true;
            void serial(async () => {
              if (runtime !== next || next.stopped) return;
              const retry = await delivery.bind(session, runtimeId);
              if (retry.ok) {
                clearInterval(next.bindRetry);
                next.bindRetry = undefined;
                status(ctx, undefined);
              } else if (retry.error !== 'owner_conflict') status(ctx, `Roger-Wilco: ${retry.error}: ${retry.message}`);
              await inbox.enqueueRegistration({ session });
            }).catch(e => status(ctx, `Roger-Wilco lease recovery: ${String(e)}`))
              .finally(() => { retrying = false; });
          }, RENEW_MS);
          next.bindRetry.unref();
        }
        await inbox.enqueueRegistration({ session })
          .catch(e => status(ctx, `Roger-Wilco presence: ${String(e)}`));
        next.heartbeat = setInterval(() => {
          if (runtime !== next || next.stopped) return;
          void inbox.enqueueRegistration({ session }).catch(e => status(ctx, `Roger-Wilco presence: ${String(e)}`));
        }, 30_000);
        next.heartbeat.unref();
        if (result.ok) status(ctx, undefined);
        // Never wait for the coordinator on Pi's startup path; D also drains this durable queue.
        void request(config, ROUTES.register, 'POST', { session }).then(result => {
          if (runtime === next && (next.delivery.isBound() || !result.ok))
            status(ctx, result.ok ? undefined : `Roger-Wilco registration: ${result.error}: ${result.message}`);
        });
      } catch (e) {
        if (runtime === next) runtime = undefined;
        if (next.heartbeat) clearInterval(next.heartbeat);
        if (next.bindRetry) clearInterval(next.bindRetry);
        await delivery.stop(); await inbox.close(); throw e;
      }
    } catch (e) { status(ctx, `Roger-Wilco unavailable: ${String(e)}`); }
  }));
  pi.on('session_shutdown', (_event, ctx) => serial(async () => { await stop(); status(ctx, undefined); }));
  pi.on('turn_end', () => { runtime?.delivery.wake(); });
  pi.on('agent_settled', (event) => {
    runtime?.delivery.wake();
    if (event.aborted) return runtime?.delivery.resumeAfterAbort();
  });

  async function bound(ctx: ExtensionContext): Promise<{ actor: SessionAddress; owner: Runtime } | ReturnType<typeof error>> {
    await transition;
    const owner = runtime;
    if (!owner || owner.stopped || !owner.delivery.isBound()) return error('Roger-Wilco session is not bound');
    const actor = currentSession(ctx, owner.config.namespace); // Always use the invocation's session, never a host-global identity.
    if (sessionKey(actor) !== sessionKey(owner.session)) return error('Session changed; wait for binding');
    return { actor, owner };
  }
  async function action(ctx: ExtensionContext, route: string, method: 'GET' | 'POST', body?: unknown) {
    const identity = await bound(ctx);
    if ('ok' in identity) return text(identity);
    const { actor, owner } = identity;
    const registered = await request(owner.config, ROUTES.register, 'POST', { session: actor });
    if (!registered.ok) { status(ctx, `Roger-Wilco registration: ${registered.error}: ${registered.message}`); return text(registered); }
    status(ctx, undefined);
    if (runtime !== owner || owner.stopped || ctx.sessionManager.getSessionId().toLowerCase() !== actor.piSessionId) return text(error('Session changed'));
    return text(await request(owner.config, route, method, body));
  }
  async function board(ctx: ExtensionContext, all = false) {
    const sessions = await action(ctx, ROUTES.sessions, 'GET');
    const first = sessions.details as Outcome<RegistrationResult[]>;
    if (!first.ok) return sessions;
    const claims = await action(ctx, ROUTES.claims, 'GET');
    const second = claims.details as Outcome<unknown>;
    const now = Date.now();
    const visible = all ? first.value : first.value.filter(row =>
      typeof row.lastSeenAt === 'number' && Number.isFinite(row.lastSeenAt) && now - row.lastSeenAt <= PRESENCE_MS);
    return second.ok ? text({ ok: true, value: { sessions: visible, claims: second.value } }) : claims;
  }
  const rankLabel = (rank: number) => rank === 1 ? 'fleet (rank 1)' : `Rank ${rank}`;
  const boardCommand = {
    description: 'Show the board and pick a session rank; use /roger all for stale registrations',
    handler: async (args: string, ctx: ExtensionContext) => {
      if (args.trim() && args.trim() !== 'all') { ctx.ui.notify('Usage: /roger [all]', 'error'); return; }
      const all = args.trim() === 'all';
      const identity = await bound(ctx);
      if ('ok' in identity) { ctx.ui.notify(identity.message, 'error'); return; }
      const result = (await board(ctx, all)).details as Outcome<{ sessions: RegistrationResult[]; claims: Claim[] }>;
      if (!result.ok) { ctx.ui.notify(result.message, 'error'); return; }
      const address = sessionKey(identity.actor);
      const labels = result.value.sessions.map(row =>
        `${row.subscription.id}${row.subscription.id === address ? ' (you)' : ''} — host ${JSON.stringify(row.subscription.hostId)}` +
        (row.displayName ? ` — ${JSON.stringify(row.displayName)}` : '') + ` — ${rankLabel(row.rank)}`);
      ctx.ui.notify([
        'Roger-Wilco',
        `Your address: ${address}`,
        `Your host: ${JSON.stringify(identity.owner.config.hostId)}`,
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
      const updated = (await action(ctx, ROUTES.rank, 'POST', { session: target.subscription.session, rank })).details as Outcome<RegistrationResult>;
      if (!updated.ok) { ctx.ui.notify(updated.message, 'error'); return; }
      ctx.ui.notify(`${updated.value.subscription.id}: ${rankLabel(updated.value.rank)}`, 'info');
    },
  };
  pi.registerCommand('roger', boardCommand);
  pi.registerCommand('roger_board', boardCommand);
  pi.registerTool({ name: 'roger_identity', label: 'Agent Identity', description: 'Read or set this session’s display name. Entity references such as [[0:FLEET]] attach opaque integer metadata without changing the session address. Omit displayName to read the registration.',
    parameters: Type.Object({ displayName: Type.Optional(Type.String({ maxLength: 200 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
      if ('ok' in identity) return text(identity);
      return action(ctx, ROUTES.register, 'POST', { session: identity.actor,
        ...(params.displayName === undefined ? {} : { displayName: params.displayName }) });
    },
  });
  pi.registerTool({ name: 'roger_send', label: 'Send Agent Message', description: 'Send a message to a session address or broadcast (*). Text and JSON strings support [[label]] and [[153:Meteor Shower]]. The integer is opaque metadata (0–255), not identity. Bare references preserve it. Use roger_entity for current metadata and retained message mentions.',
    parameters: Type.Object({ recipient: Type.String(), body: Type.String(), data: Type.Optional(Type.Unknown()),
      visibility: Type.Optional(Type.Union([Type.Literal('private'), Type.Literal('public')])) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
      if ('ok' in identity) return text(identity);
      const message = { recipient: params.recipient, body: params.body, data: params.data, visibility: params.visibility };
      if (!isMessageInput(message)) return text({ ok: false, error: 'invalid_request', message: 'Invalid message' });
      return action(ctx, ROUTES.messages, 'POST', { sender: identity.actor, message });
    },
  });
  pi.registerTool({ name: 'roger_inbox', label: 'Agent Inbox', description: 'Query this session’s historical inbox.',
    parameters: Type.Object({ since: Type.Optional(Type.Integer({ minimum: 0 })), limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
      if ('ok' in identity) return text(identity);
      const query = new URLSearchParams({ namespace: identity.actor.namespace, piSessionId: identity.actor.piSessionId,
        since: String(params.since ?? 0), limit: String(params.limit ?? 100) });
      return action(ctx, `${ROUTES.inbox}?${query}`, 'GET');
    },
  });
  pi.registerTool({ name: 'roger_entity', label: 'Entity History', description: 'Look up current entity metadata and retained message mentions by label or ID. The legacy type field is optional and never selects another identity or updates metadata. Integer meanings belong to consumers.',
    parameters: Type.Object({ label: Type.Optional(Type.String()), type: Type.Optional(Type.Integer({ minimum: 0, maximum: 255 })),
      id: Type.Optional(Type.String()), since: Type.Optional(Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
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
      return action(ctx, `${ROUTES.entity}?${query}`, 'GET');
    },
  });
  pi.registerTool({ name: 'roger_board', label: 'Agent Board', description: 'List sessions seen within 90 seconds and active work claims. Presence is not proof of a live Pi lease; set all to include stale registrations.',
    parameters: Type.Object({ all: Type.Optional(Type.Boolean()) }), execute: async (_id, params, _signal, _update, ctx) => board(ctx, params.all) });
  pi.registerTool({ name: 'roger_claim', label: 'Claim Work', description: 'Acquire or renew a session-owned work claim. Key and note strings support [[label]] or [[153:label]] entity metadata. Claim keys remain literal; entity metadata does not grant ownership.',
    parameters: Type.Object({ key: Type.String({ minLength: 1, maxLength: 256 }), ttlSeconds: Type.Optional(Type.Integer({ minimum: 0, maximum: 2147483647 })), note: Type.Optional(Type.String({ maxLength: 4096 })) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
      if ('ok' in identity) return text(identity);
      return action(ctx, ROUTES.claims, 'POST', { owner: identity.actor, key: params.key, ttlSeconds: params.ttlSeconds ?? 900, ...(params.note === undefined ? {} : { note: params.note }) });
    },
  });
  pi.registerTool({ name: 'roger_release', label: 'Release Work', description: 'Release a claim owned by this session using its exact key. Entity references in a successfully released key update opaque metadata without changing entity identity.',
    parameters: Type.Object({ key: Type.String({ minLength: 1, maxLength: 256 }) }),
    execute: async (_id, params, _signal, _update, ctx) => {
      const identity = await bound(ctx);
      if ('ok' in identity) return text(identity);
      return action(ctx, ROUTES.claimRelease, 'POST', { owner: identity.actor, key: params.key });
    },
  });
}

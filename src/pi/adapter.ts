/**
 * The Pi adapter. It maps Pi's lifecycle onto the generic core through three boundaries:
 * session identity (identity.ts), delivery evidence (delivery.ts over evidence.ts),
 * and host access through the HostInbox socket client supplied by the caller.
 */
import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { randomUUID } from 'node:crypto';
import {
  ROUTES, sessionKey,
  type HostInbox, type Outcome, type SessionAddress,
} from '../protocol/index.ts';
import type { PiConfig } from './config.ts';
import { createPiDelivery, type PiDelivery } from './delivery.ts';
import { isPiSession, piSessionAddress, requirePersistentPiSession } from './identity.ts';
import { registerPiSurface, type CoreAccess, type Failure } from './tools.ts';

const RENEW_MS = 1000;
const HEARTBEAT_MS = 30_000;
const error = (message: string): Failure => ({ ok: false, error: 'temporarily_unavailable', message });

/** Everything the adapter needs from outside Pi. Config is resolved again at every session start. */
export type PiAdapterOptions = {
  loadConfig(): Promise<PiConfig>;
  connectHost(config: PiConfig): Promise<HostInbox>;
  request(config: PiConfig, route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>>;
};

type Runtime = { session: SessionAddress; runtimeId: string; inbox: HostInbox; delivery: PiDelivery; config: PiConfig; stopped: boolean;
  heartbeat?: NodeJS.Timeout; bindRetry?: NodeJS.Timeout; registration?: () => void };

/** One extension runtime, one current session lease. Lifecycle changes are serialized. */
export function createPiAdapter(pi: ExtensionAPI, options: PiAdapterOptions): void {
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
    old.registration?.();
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
      const config = await options.loadConfig();
      const session = requirePersistentPiSession(ctx, config.namespace);
      const inbox = await options.connectHost(config);
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
        next.registration = inbox.onRegistrationResult(result => {
          if (runtime !== next) return;
          if (next.delivery.isBound() || !result.ok)
            status(ctx, result.ok ? undefined : `Roger-Wilco registration: ${result.error}: ${result.message}`);
        });
        await inbox.enqueueRegistration({ session })
          .catch(e => status(ctx, `Roger-Wilco presence: ${String(e)}`));
        next.heartbeat = setInterval(() => {
          if (runtime !== next || next.stopped) return;
          void inbox.enqueueRegistration({ session }).catch(e => status(ctx, `Roger-Wilco presence: ${String(e)}`));
        }, HEARTBEAT_MS);
        next.heartbeat.unref();
        if (result.ok) status(ctx, undefined);
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

  async function bound(ctx: ExtensionContext): Promise<{ actor: SessionAddress; owner: Runtime } | Failure> {
    await transition;
    const owner = runtime;
    if (!owner || owner.stopped || !owner.delivery.isBound()) return error('Roger-Wilco session is not bound');
    const actor = piSessionAddress(ctx, owner.config.namespace); // Always use the invocation's session, never a host-global identity.
    if (sessionKey(actor) !== sessionKey(owner.session)) return error('Session changed; wait for binding');
    return { actor, owner };
  }
  const core: CoreAccess = {
    async bound(ctx) {
      const identity = await bound(ctx);
      return 'ok' in identity ? identity : { actor: identity.actor, hostId: identity.owner.config.hostId };
    },
    async call(ctx, route, method, body) {
      const identity = await bound(ctx);
      if ('ok' in identity) return identity;
      const { actor, owner } = identity;
      const registered = await owner.inbox.awaitRegistration({ session: actor });
      if (!registered.ok) { status(ctx, `Roger-Wilco registration: ${registered.error}: ${registered.message}`); return registered; }
      status(ctx, undefined);
      if (runtime !== owner || owner.stopped || !isPiSession(ctx, actor)) return error('Session changed');
      if (route === ROUTES.register) return registered;
      return options.request(owner.config, route, method, body);
    },
    async health() {
      const owner = runtime;
      if (!owner || owner.stopped) return error('Roger-Wilco session is not bound');
      return options.request(owner.config, ROUTES.health, 'GET');
    },
  };
  registerPiSurface(pi, core);
}

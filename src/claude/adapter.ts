/**
 * The Claude Code adapter. It maps Claude Code's lifecycle onto the generic core through three
 * seams: session identity (identity.ts), delivery evidence (transcript.ts), and host
 * access through the HostInbox socket client supplied by the caller. The core knows
 * nothing Claude-shaped; this adapter is a sibling of the Pi adapter, not a variant of it.
 */
import { randomUUID } from 'node:crypto';
import {
  ROUTES, isRegistration, sessionKey,
  type HostInbox, type Message, type Outcome, type SessionAddress,
} from '../protocol/index.ts';
import type { ClaudeConfig } from './config.ts';
import { claudeSessionAddress } from './identity.ts';
import { createClaudeTools, type BoundSession, type CoreAccess, type Failure } from './tools.ts';
import { claudeTranscriptEvidence, type ClaudeReceipt } from './transcript.ts';
import type { EvidenceSource } from '../evidence.ts';

const RENEW_MS = 1000;
/** The daemon lease lasts 30 s; renewing at this interval keeps two retries in hand. */
const LEASE_RENEW_MS = 10_000;
const HEARTBEAT_MS = 30_000;
const error = (message: string): Failure => ({ ok: false, error: 'temporarily_unavailable', message });

/** Everything Claude Code-specific the adapter cannot supply itself. Implementations: the CLI harness in cli.ts, a test fake, or a future MCP/plugin host. */
export interface ClaudeHarness {
  /** The current Claude Code session ID, as the harness knows it. */
  sessionId(): string;
  /** The session's durable transcript path; undefined while the harness has not created it. */
  transcriptPath(): string | undefined;
  /**
   * Present one message to the session. A delivery call is never evidence:
   * the adapter acknowledges only after the transcript records the delivery.
   */
  deliver(message: Message): void | Promise<void>;
}

/** Everything the adapter needs from outside Claude Code. Config is resolved again at every start. */
export type ClaudeAdapterOptions = {
  loadConfig(): Promise<ClaudeConfig>;
  connectHost(config: ClaudeConfig): Promise<HostInbox>;
  request(config: ClaudeConfig, route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>>;
};

export interface ClaudeAdapter {
  /** Bind the session lease, queue registration, and start heartbeat/lease recovery. */
  start(): Promise<void>;
  stop(): Promise<void>;
  /** The bound session address; fails while unbound. */
  bound(): Promise<BoundSession | Failure>;
  /** One coordinator call as the bound session. */
  call(route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>>;
  /** Dispatch one roger_* tool by name. */
  tool(name: string, params?: Record<string, unknown>): Promise<Outcome<unknown>>;
  /**
   * One delivery pass: acknowledge whatever the transcript already proves, then hand
   * each remaining pending message to the harness once. Returns the messages handed
   * over in this pass; they stay unacknowledged until a later pass finds transcript
   * evidence for them.
   */
  receive(): Promise<Message[]>;
  /** True while this runtime holds its lease. */
  isBound(): boolean;
}

type Runtime = {
  session: SessionAddress; runtimeId: string; inbox: HostInbox; config: ClaudeConfig;
  evidence: EvidenceSource<ClaudeReceipt>; stopped: boolean; bound: boolean;
  heartbeat?: NodeJS.Timeout; renewTimer?: NodeJS.Timeout;
};

/** One adapter runtime, one current session lease. */
export function createClaudeAdapter(harness: ClaudeHarness, options: ClaudeAdapterOptions): ClaudeAdapter {
  let runtime: Runtime | undefined;
  const submitted = new Set<string>();

  const valid = (rt: Runtime, token: string | undefined) =>
    !rt.stopped && rt.bound && token !== undefined && token === rt.inbox.authority();

  async function pass(rt: Runtime): Promise<Message[]> {
    const delivered: Message[] = [];
    const token = rt.inbox.authority();
    if (!valid(rt, token)) return delivered;
    const pending = await rt.inbox.pending(rt.session, rt.runtimeId);
    if (!valid(rt, token)) return delivered;
    for (const message of pending) {
      if (!valid(rt, token)) break;
      const location = rt.evidence.location();
      let found;
      try { found = await rt.evidence.lookup(location, message.id, message); }
      catch { continue; } // Indeterminate store: retry on a later pass, never acknowledge.
      if (found.state === 'persisted') {
        if (!rt.evidence.locatedAt(found.evidence, location!)) continue;
        const result = await rt.evidence.acknowledge(found.evidence);
        if (result.ok) submitted.delete(message.id);
        continue;
      }
      if (submitted.has(message.id)) continue;
      submitted.add(message.id);
      try { await harness.deliver(message); }
      catch { submitted.delete(message.id); continue; }
      delivered.push(message);
      // A harness may persist synchronously (tests, file-backed hosts); if the
      // transcript already proves this delivery, acknowledge it in the same pass.
      try {
        const again = await rt.evidence.lookup(rt.evidence.location(), message.id, message);
        if (again.state === 'persisted' && rt.evidence.locatedAt(again.evidence, rt.evidence.location()!)) {
          const result = await rt.evidence.acknowledge(again.evidence);
          if (result.ok) submitted.delete(message.id);
        }
      } catch { /* A later pass retries. */ }
    }
    return delivered;
  }

  const core: CoreAccess = {
    async bound() {
      const rt = runtime;
      if (!rt || rt.stopped || !rt.bound) return error('Roger-Wilco session is not bound');
      return { actor: rt.session, hostId: rt.config.hostId };
    },
    async call(route, method, body) {
      const rt = runtime;
      if (!rt || rt.stopped || !rt.bound) return error('Roger-Wilco session is not bound');
      const registration = route === ROUTES.register && isRegistration(body) ? body : { session: rt.session };
      const registered = await rt.inbox.awaitRegistration(registration);
      if (!registered.ok) return registered;
      if (runtime !== rt || rt.stopped) return error('Session changed');
      if (route === ROUTES.register) return registered;
      return options.request(rt.config, route, method, body);
    },
  };
  const tool = createClaudeTools(core);

  return {
    tool,
    bound: core.bound,
    call: core.call,
    isBound: () => !!runtime && !runtime.stopped && runtime.bound,
    async start() {
      if (runtime) return;
      const config = await options.loadConfig();
      const session = claudeSessionAddress(config.sessionId ?? harness.sessionId(), config.namespace);
      const inbox = await options.connectHost(config);
      const runtimeId = randomUUID();
      const rt: Runtime = {
        session, runtimeId, inbox, config, stopped: false, bound: false,
        evidence: claudeTranscriptEvidence(() => harness.transcriptPath(), inbox, session, runtimeId),
      };
      runtime = rt;
      try {
        const lease = await inbox.bind(session, runtimeId);
        rt.bound = lease.ok;
        let lastRenewAt = Date.now();
        rt.renewTimer = setInterval(() => {
          if (rt.stopped || runtime !== rt) return;
          void (async () => {
            if (rt.bound) {
              if (Date.now() - lastRenewAt < LEASE_RENEW_MS) return;
              const renewed = await inbox.renew(session, runtimeId);
              if (!renewed.ok) rt.bound = false;
              else lastRenewAt = Date.now();
            } else {
              const bound = await inbox.bind(session, runtimeId);
              if (bound.ok) { rt.bound = true; await inbox.enqueueRegistration({ session }).catch(() => {}); }
            }
          })().catch(() => { rt.bound = false; });
        }, RENEW_MS);
        rt.renewTimer.unref();
        await inbox.enqueueRegistration({ session }).catch(() => {});
        rt.heartbeat = setInterval(() => {
          if (rt.stopped || runtime !== rt) return;
          void inbox.enqueueRegistration({ session }).catch(() => {});
        }, HEARTBEAT_MS);
        rt.heartbeat.unref();
      } catch (cause) {
        runtime = undefined;
        await inbox.close();
        throw cause;
      }
    },
    async stop() {
      const rt = runtime;
      runtime = undefined;
      if (!rt) return;
      rt.stopped = true;
      if (rt.heartbeat) clearInterval(rt.heartbeat);
      if (rt.renewTimer) clearInterval(rt.renewTimer);
      await rt.inbox.close();
      rt.bound = false;
    },
    async receive() {
      const rt = runtime;
      if (!rt || rt.stopped || !rt.bound) return [];
      return pass(rt);
    },
  };
}

export { claudeSessionAddress, sessionKey };

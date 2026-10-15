import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { watch, type FSWatcher } from 'node:fs';
import { basename, dirname } from 'node:path';
import {
  sessionKey,
  type HostInbox, type Message, type Outcome, type SessionAddress,
} from '../protocol/index.ts';
import { isPiSession } from './identity.ts';
import { deliveryEntry, sessionFileEvidence } from './session-file.ts';

export type { EvidenceSource, EvidenceLookup } from './evidence.ts';
export { persistedReceipt, receiptEvidence, type PiReceipt } from './session-file.ts';

/** Submits a Pi message, then observes the persistent session file. It never acknowledges the void sendMessage call. */
export interface PiDelivery {
  bind(session: SessionAddress, runtimeId: string): Promise<Outcome<null>>;
  stop(): Promise<void>;
  wake(): void;
  resumeAfterAbort(): Promise<void>;
  isBound(): boolean;
}

const RENEW_MS = 1000;
/** The daemon lease lasts 30 s; renewing at this interval keeps two retries in hand. */
const LEASE_RENEW_MS = 10_000;
const error = (message: string): Extract<Outcome<never>, { ok: false }> => ({ ok: false, error: 'temporarily_unavailable', message });

/** The caller owns the inbox and releases its lease after stop(). */
export function createPiDelivery(pi: ExtensionAPI, ctx: ExtensionContext, inbox: HostInbox,
  session: SessionAddress, runtimeId: string, report: (message: string | undefined) => void = () => {}): PiDelivery {
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
  let lastRenewAt = 0;
  const evidence = sessionFileEvidence(ctx, inbox, session, runtimeId);
  let recoveringAbort = false;
  let missingFileReported = false;
  const active = () => !stopped && isPiSession(ctx, session);
  const file = () => evidence.location();
  const authority = () => inbox.authority();
  const valid = (token: string | undefined) =>
    active() && bound && token !== undefined && token === authority();
  const unsubscribe = inbox.onChange(() => {
    if (authority() === undefined) bound = false;
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
    let found;
    try { found = await evidence.lookup(path, message.id); }
    catch (e) {
      report(`Pi session receipt unavailable: ${String(e)}`);
      return 'unavailable';
    }
    if (found.state === 'store_missing') {
      if (!missingFileReported) report('Pi session file is not yet persistent; delivery remains pending');
      missingFileReported = true;
      return 'absent'; // Pi may create the file when the first message triggers a turn.
    }
    if (missingFileReported) report(undefined);
    missingFileReported = false;
    if (!valid(token) || file() !== path) return 'unavailable';
    if (found.state === 'absent') return 'absent';
    const proof = found.evidence;
    if (!path || !evidence.locatedAt(proof, path)) return 'unavailable';
    const result = await evidence.acknowledge(proof);
    if (!valid(token) || file() !== path) return 'unavailable';
    if (!result.ok) { report(`Pi receipt rejected: ${result.message}`); return 'unavailable'; }
    submitted.delete(message.id);
    if (!submitted.size) recoveringAbort = false;
    return 'persisted';
  }

  async function ownsLease(token = authority()): Promise<boolean> {
    if (!valid(token)) return false;
    // Delivery passes call this before every submission; the socket renewal
    // itself is throttled, the in-memory authority check is not.
    if (Date.now() - lastRenewAt < LEASE_RENEW_MS) return valid(token);
    const result = await inbox.renew(session, runtimeId);
    if (result.ok) lastRenewAt = Date.now();
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
      // A directed message wakes the session; a broadcast is recorded without
      // starting a turn, and the model sees it on its next turn.
      pi.sendMessage(deliveryEntry(message), { deliverAs: 'followUp', triggerTurn: message.recipient !== '*' });
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
            !pending.some(message => submitted.has(message.id) && message.recipient !== '*') || !(await ownsLease(token)) ||
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
        if (authority() === undefined) bound = false;
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
      try { lease = await inbox.bind(session, runtimeId); }
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
            const result = await inbox.bind(session, runtimeId);
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


import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';
import { watch, type BigIntStats, type FSWatcher } from 'node:fs';
import { open, stat } from 'node:fs/promises';
import { basename, dirname } from 'node:path';
import {
  sessionKey,
  type HostInbox, type Message, type Outcome, type PiDelivery, type PiReceipt, type SessionAddress,
} from '../protocol/index.ts';

const RENEW_MS = 1000;
const error = (message: string): Extract<Outcome<never>, { ok: false }> => ({ ok: false, error: 'temporarily_unavailable', message });

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
  const readReceipt = receiptReader();
  let recoveringAbort = false;
  let missingFileReported = false;
  const active = () => !stopped && ctx.sessionManager.getSessionId().toLowerCase() === session.piSessionId;
  const file = () => ctx.sessionManager.getSessionFile();
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
    const result = await inbox.renew(session, runtimeId);
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


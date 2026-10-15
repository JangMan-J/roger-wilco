/**
 * Muse's answer to the delivery-evidence seam: an exact delivery ID recorded in
 * the session's own durable transcript (`session.jsonl`), verified across fsync.
 * The adapter never writes to the transcript; the Muse runtime owns it. A message
 * counts as persisted only after the harness recorded the delivery in that file,
 * so an acknowledgement never rests on a delivery call alone.
 */
import type { BigIntStats } from 'node:fs';
import { open, readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';
import type { DeliveryEvidence, HostInbox, SessionAddress } from '../protocol/index.ts';
import type { EvidenceSource } from '../evidence.ts';

/** Muse's receipt: the transcript line that carries one delivery, and the file it was read from. */
export type MuseReceipt = { messageId: string; recordId: string; transcript: string; message?: unknown };

/** The Muse edge of the evidence seam: a transcript line ordinal is the record, its session file is the location. */
export function museReceiptEvidence(session: SessionAddress, receipt: MuseReceipt): DeliveryEvidence {
  return { messageId: receipt.messageId, sessionId: session.sessionId, recordId: receipt.recordId, location: receipt.transcript };
}

/** Find `sessions/<year>/<month>/<day>/<sessionId>/session.jsonl` (or a shallower nesting) under the Muse sessions root. */
export async function findMuseTranscript(sessionId: string, sessionsRoot: string, depth = 4): Promise<string | undefined> {
  if (depth < 0) return undefined;
  let entries;
  try { entries = await readdir(sessionsRoot, { withFileTypes: true }); }
  catch { return undefined; }
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isDirectory()) continue;
    if (entry.name === sessionId) {
      const candidate = join(sessionsRoot, entry.name, 'session.jsonl');
      try {
        if ((await stat(candidate)).isFile()) return candidate;
      } catch { /* keep looking */ }
      continue;
    }
    const nested = await findMuseTranscript(sessionId, join(sessionsRoot, entry.name), depth - 1);
    if (nested) return nested;
  }
  return undefined;
}

/**
 * True only when `value` records this adapter's own delivery of `messageId`:
 * an object shaped `{event:'delivered', message:{id, sender, body, ...}}`, possibly
 * nested inside transcript frames or JSON-encoded tool output. A bare mention of
 * the ID anywhere else (discussion, quoted text, another delivery) is not evidence.
 */
export function isDeliveredRecord(value: unknown, messageId: string, depth = 0, expected?: Record<string, unknown>): boolean {
  if (depth > 8) return false;
  if (typeof value === 'string') {
    if (!value.includes(messageId) || value.length > 1024 * 1024) return false;
    try { return isDeliveredRecord(JSON.parse(value) as unknown, messageId, depth + 1, expected); }
    catch { return false; }
  }
  if (Array.isArray(value)) return value.some(item => isDeliveredRecord(item, messageId, depth + 1, expected));
  if (!value || typeof value !== 'object') return false;
  const record = value as Record<string, unknown>;
  // A record that declares a non-durable durability never counts, however it is shaped.
  if ('durability' in record && record.durability !== 'durable') return false;
  const message = record.message as Record<string, unknown> | undefined;
  if (record.event === 'delivered' && message && typeof message === 'object' && !Array.isArray(message) &&
      message.id === messageId && typeof message.sender === 'string' && typeof message.body === 'string') {
    // When the caller knows the real message, every field must match it exactly:
    // a forged record naming a predictable future ID is not this delivery.
    if (!expected) return true;
    return message.sequence === expected.sequence && message.timestamp === expected.timestamp &&
      message.sender === expected.sender && message.recipient === expected.recipient &&
      message.body === expected.body && message.visibility === expected.visibility &&
      JSON.stringify(message.data ?? null) === JSON.stringify(expected.data ?? null);
  }
  return Object.values(record).some(item => isDeliveredRecord(item, messageId, depth + 1, expected));
}

/** Session IDs named by `stream` records in a parsed transcript value. */
export function transcriptSessionIds(value: unknown, depth = 0): string[] {
  if (depth > 8) return [];
  if (typeof value === 'string') {
    if (!value.includes('"session"') || value.length > 1024 * 1024) return [];
    try { return transcriptSessionIds(JSON.parse(value) as unknown, depth + 1); }
    catch { return []; }
  }
  if (!value || typeof value !== 'object') return [];
  if (Array.isArray(value)) return value.flatMap(item => transcriptSessionIds(item, depth + 1));
  const record = value as Record<string, unknown>;
  const stream = record.stream as Record<string, unknown> | undefined;
  const own = stream && stream.kind === 'session' && typeof stream.id === 'string' ? [stream.id] : [];
  return [...own, ...Object.values(record).flatMap(item => transcriptSessionIds(item, depth + 1))];
}

function sameSnapshot(a: BigIntStats, b: BigIntStats): boolean {
  return a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
}

/** An exact delivery ID in the current transcript is the only Muse delivery receipt. */
export function museTranscriptEvidence(
  transcriptPath: () => string | undefined,
  inbox: HostInbox,
  session: SessionAddress,
  runtimeId: string,
): EvidenceSource<MuseReceipt> {
  return {
    location: () => transcriptPath(),
    async lookup(location, messageId, expected) {
      if (!location) return { state: 'store_missing' };
      let handle;
      try { handle = await open(location, 'r'); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { state: 'store_missing' };
        throw error;
      }
      try {
        const snapshot = await handle.stat({ bigint: true });
        const text = await handle.readFile('utf8');
        const lines = text.split('\n');
        // Ignore an incomplete final line: the runtime may still be writing it.
        if (!text.endsWith('\n')) lines.pop();
        // If the transcript names its session and it is not this one, nothing
        // in it is evidence for this session.
        const boundIds = new Set<string>();
        for (const line of lines) {
          if (!line.includes('"stream"')) continue;
          try { for (const id of transcriptSessionIds(JSON.parse(line) as unknown)) boundIds.add(id); }
          catch { /* an unparseable line proves nothing either way */ }
        }
        if (boundIds.size && !boundIds.has(session.sessionId)) return { state: 'absent' };
        const expectedRecord = expected && typeof expected === 'object' ? expected as Record<string, unknown> : undefined;
        let recordId: string | undefined;
        for (let index = 0; index < lines.length; index++) {
          if (!lines[index]!.includes(messageId)) continue;
          let parsed: unknown;
          try { parsed = JSON.parse(lines[index]!); }
          catch { continue; }
          if (isDeliveredRecord(parsed, messageId, 0, expectedRecord)) { recordId = `line-${index + 1}`; break; }
        }
        if (!recordId) return { state: 'absent' };
        // Even a matched line must cross the disk+fsync boundary on the currently opened file.
        await handle.sync();
        if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(location, { bigint: true }))) {
          throw new Error('Muse transcript changed during receipt scan');
        }
        return { state: 'persisted', evidence: { messageId, recordId, transcript: location } };
      } finally { await handle.close(); }
    },
    locatedAt: (receipt, location) => receipt.transcript === location,
    acknowledge: receipt => inbox.acknowledgeDelivery(session, runtimeId, museReceiptEvidence(session, receipt)),
  };
}

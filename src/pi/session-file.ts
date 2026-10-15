/** Pi's answer to the delivery-evidence seam: an exact roger-wilco entry in the current session JSONL, verified across fsync. */
import type { ExtensionContext } from '@earendil-works/pi-coding-agent';
import type { BigIntStats } from 'node:fs';
import { open, stat } from 'node:fs/promises';
import type { DeliveryEvidence, HostInbox, Message, SessionAddress } from '../protocol/index.ts';
import type { EvidenceSource } from './evidence.ts';
import { piSessionId } from './identity.ts';

const DELIVERY_TYPE = 'roger-wilco';

/** Pi's receipt: the session JSONL entry that carries one delivery, and the file it was read from. */
export type PiReceipt = { messageId: string; entryId: string; sessionFile: string };

/** The Pi edge of the evidence seam: a JSONL entry ID is the record, its session file is the location. */
export function receiptEvidence(session: SessionAddress, receipt: PiReceipt): DeliveryEvidence {
  return { messageId: receipt.messageId, sessionId: session.sessionId, recordId: receipt.entryId, location: receipt.sessionFile };
}

/** The Pi custom message that carries one delivery. The receipt reader matches exactly this shape. */
export function deliveryEntry(message: Message) {
  return { customType: DELIVERY_TYPE, content: JSON.stringify(message), display: true, details: { deliveryId: message.id } };
}

/**
 * An entry without content is matched on its details alone. An entry that carries
 * content is evidence only when that content is the delivered message itself, with
 * the same message ID as the delivery ID in its details.
 */
function contentMatches(content: unknown, deliveryId: string): boolean {
  if (content === undefined) return true;
  if (typeof content !== 'string') return false;
  try {
    const message = JSON.parse(content) as { id?: unknown } | null;
    return !!message && typeof message === 'object' && message.id === deliveryId;
  } catch { return false; }
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
      if (!cached || cached.file !== file || cached.sessionId !== session.sessionId || !sameSnapshot(cached.snapshot, snapshot)) {
        cached = undefined;
        const lines = (await handle.readFile('utf8')).split('\n');
        lines.pop(); // Ignore an incomplete final JSONL line, including a partial header.
        const header = JSON.parse(lines.shift() ?? 'null') as { type?: string; id?: string } | null;
        const entries = new Map<string, string>();
        if (header?.type === 'session' && header.id != null && piSessionId(header.id) === session.sessionId) {
          for (const line of lines) {
            const entry = JSON.parse(line) as { type?: string; id?: string; customType?: string; content?: unknown; details?: { deliveryId?: string } };
            if (entry.type === 'custom_message' && entry.customType === DELIVERY_TYPE &&
                typeof entry.details?.deliveryId === 'string' && typeof entry.id === 'string' && entry.id.length &&
                contentMatches(entry.content, entry.details.deliveryId) &&
                !entries.has(entry.details.deliveryId)) entries.set(entry.details.deliveryId, entry.id);
          }
        }
        // A concurrent write/replacement is not a coherent receipt snapshot. Retry on the next pass.
        if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(file, { bigint: true }))) {
          throw new Error('Pi session file changed during receipt scan');
        }
        cached = { file, sessionId: session.sessionId, snapshot, entries };
      }
      const entryId = cached.entries.get(id);
      if (!entryId) return undefined;
      // Even cached matches must cross the disk+fsync boundary on the currently opened file.
      await handle.sync();
      if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(file, { bigint: true }))) {
        cached = undefined;
        throw new Error('Pi session file changed during receipt sync');
      }
      return { messageId: id, entryId, sessionFile: file };
    } finally { await handle.close(); }
  };
}

/** An exact entry in the current session JSONL is the only Pi submission receipt. */
export const persistedReceipt = receiptReader();

/** The session-file evidence for one bound session and runtime. */
export function sessionFileEvidence(ctx: ExtensionContext, inbox: HostInbox,
  session: SessionAddress, runtimeId: string): EvidenceSource<PiReceipt> {
  const readReceipt = receiptReader();
  return {
    location: () => ctx.sessionManager.getSessionFile(),
    async lookup(location, messageId) {
      try {
        const receipt = await readReceipt(location, session, messageId);
        return receipt ? { state: 'persisted', evidence: receipt } : { state: 'absent' };
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return { state: 'store_missing' };
        throw e;
      }
    },
    locatedAt: (receipt, location) => receipt.sessionFile === location,
    acknowledge: receipt => inbox.acknowledgeDelivery(session, runtimeId, receiptEvidence(session, receipt)),
  };
}

/**
 * Claude Code's answer to the delivery-evidence seam: the adapter's exact delivered
 * record inside a tool result that Claude Code wrote to the session's own transcript
 * (`<projects>/<encoded-cwd>/<sessionId>.jsonl`), verified across fsync.
 *
 * The proof is that the model saw the delivery, not that someone typed it. Only a
 * `tool_result` block on a main-chain transcript line stamped with this session's ID
 * counts. The same record pasted into a human prompt, quoted by the assistant, or
 * written by a subagent is not evidence. The adapter never writes to the transcript.
 */
import type { BigIntStats } from 'node:fs';
import { open, readdir, stat } from 'node:fs/promises';
import { join } from 'node:path';
import type { DeliveryEvidence, HostInbox, SessionAddress } from '../protocol/index.ts';
import type { EvidenceSource } from '../evidence.ts';

/** Claude Code's receipt: the transcript line that carries one delivery, and the file it was read from. */
export type ClaudeReceipt = { messageId: string; recordId: string; transcript: string };

/** The Claude Code edge of the evidence seam: a transcript line's uuid is the record, its session file is the location. */
export function claudeReceiptEvidence(session: SessionAddress, receipt: ClaudeReceipt): DeliveryEvidence {
  return { messageId: receipt.messageId, sessionId: session.sessionId, recordId: receipt.recordId, location: receipt.transcript };
}

/** Find `<projectsRoot>/<any project>/<sessionId>.jsonl`. */
export async function findClaudeTranscript(sessionId: string, projectsRoot: string): Promise<string | undefined> {
  let entries;
  try { entries = await readdir(projectsRoot, { withFileTypes: true }); }
  catch { return undefined; }
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!entry.isDirectory()) continue;
    const candidate = join(projectsRoot, entry.name, `${sessionId}.jsonl`);
    try { if ((await stat(candidate)).isFile()) return candidate; }
    catch { /* keep looking */ }
  }
  return undefined;
}

/**
 * True only when `value` is this adapter's delivered record for `messageId`:
 * `{event:'delivered', message:{id, sender, body, ...}}`. When the real message is
 * known, every field must match it, so a forged record naming a future ID fails.
 */
export function isDeliveredRecord(value: unknown, messageId: string, expected?: Record<string, unknown>): boolean {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  const message = record.message as Record<string, unknown> | undefined;
  if (record.event !== 'delivered' || !message || typeof message !== 'object' || Array.isArray(message) ||
      message.id !== messageId || typeof message.sender !== 'string' || typeof message.body !== 'string') return false;
  if (!expected) return true;
  return message.sequence === expected.sequence && message.timestamp === expected.timestamp &&
    message.sender === expected.sender && message.recipient === expected.recipient &&
    message.body === expected.body && message.visibility === expected.visibility &&
    JSON.stringify(message.data ?? null) === JSON.stringify(expected.data ?? null);
}

/** The text of every tool result on one transcript line, as the model received it. Human prompts yield nothing. */
export function toolResultTexts(line: Record<string, unknown>): string[] {
  if (line.type !== 'user' || line.isSidechain === true) return [];
  const content = (line.message as Record<string, unknown> | undefined)?.content;
  if (!Array.isArray(content)) return [];
  const texts: string[] = [];
  for (const block of content) {
    if (!block || typeof block !== 'object' || (block as Record<string, unknown>).type !== 'tool_result') continue;
    const inner = (block as Record<string, unknown>).content;
    if (typeof inner === 'string') texts.push(inner);
    else if (Array.isArray(inner)) {
      for (const part of inner) {
        if (part && typeof part === 'object' && (part as Record<string, unknown>).type === 'text' &&
            typeof (part as Record<string, unknown>).text === 'string') texts.push((part as Record<string, unknown>).text as string);
      }
    }
  }
  return texts;
}

/** True when one output line of `text` is exactly a delivered record for `messageId`. */
function outputProves(text: string, messageId: string, expected?: Record<string, unknown>): boolean {
  for (const row of text.split('\n')) {
    if (!row.includes(messageId)) continue;
    try { if (isDeliveredRecord(JSON.parse(row) as unknown, messageId, expected)) return true; }
    catch { /* a partial or wrapped line proves nothing */ }
  }
  return false;
}

function sameSnapshot(a: BigIntStats, b: BigIntStats): boolean {
  return a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
}

/** A tool result in this session's transcript carrying the exact delivered record is the only Claude Code receipt. */
export function claudeTranscriptEvidence(
  transcriptPath: () => string | undefined,
  inbox: HostInbox,
  session: SessionAddress,
  runtimeId: string,
): EvidenceSource<ClaudeReceipt> {
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
        // Ignore an incomplete final line: Claude Code may still be writing it.
        if (!text.endsWith('\n')) lines.pop();
        const expectedRecord = expected && typeof expected === 'object' ? expected as Record<string, unknown> : undefined;
        let recordId: string | undefined;
        for (const raw of lines) {
          if (!raw.includes(messageId)) continue;
          let line: Record<string, unknown>;
          try { line = JSON.parse(raw) as Record<string, unknown>; }
          catch { continue; }
          if (!line || typeof line !== 'object' || line.sessionId !== session.sessionId || typeof line.uuid !== 'string') continue;
          if (toolResultTexts(line).some(output => outputProves(output, messageId, expectedRecord))) { recordId = line.uuid; break; }
        }
        if (!recordId) return { state: 'absent' };
        // Even a matched line must cross the disk+fsync boundary on the currently opened file.
        await handle.sync();
        if (!sameSnapshot(snapshot, await handle.stat({ bigint: true })) || !sameSnapshot(snapshot, await stat(location, { bigint: true }))) {
          throw new Error('Claude Code transcript changed during receipt scan');
        }
        return { state: 'persisted', evidence: { messageId, recordId, transcript: location } };
      } finally { await handle.close(); }
    },
    locatedAt: (receipt, location) => receipt.transcript === location,
    acknowledge: receipt => inbox.acknowledgeDelivery(session, runtimeId, claudeReceiptEvidence(session, receipt)),
  };
}

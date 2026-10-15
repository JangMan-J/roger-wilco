/**
 * The Claude Code adapter's only mapping from a Claude Code session to a core session address.
 * A Claude Code session ID is an opaque token supplied by the harness (CLAUDE_CODE_SESSION_ID,
 * set in every Bash tool environment); it crosses into the core unchanged apart from the
 * core's own session-key rules. Nothing else in the adapter reads or folds a session ID.
 */
import { isSessionAddress, type SessionAddress } from '../protocol/index.ts';

/** The address of the Claude Code session identified by `sessionId`. */
export function claudeSessionAddress(sessionId: string, namespace: string): SessionAddress {
  const session = { namespace, sessionId };
  if (!isSessionAddress(session)) throw new Error('Roger-Wilco requires a Claude Code session id (set CLAUDE_CODE_SESSION_ID or pass --session)');
  return session;
}

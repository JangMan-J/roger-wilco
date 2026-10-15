/**
 * The Muse adapter's only mapping from a Muse session to a core session address.
 * A Muse session ID is an opaque token supplied by the harness (MUSE_SESSION_ID);
 * it crosses into the core unchanged apart from the core's own session-key rules.
 * Nothing else in the adapter reads or folds a Muse session ID.
 */
import { isSessionAddress, type SessionAddress } from '../protocol/index.ts';

/** The address of the Muse session identified by `sessionId`. */
export function museSessionAddress(sessionId: string, namespace: string): SessionAddress {
  const session = { namespace, sessionId };
  if (!isSessionAddress(session)) throw new Error('Roger-Wilco requires a Muse session id (set MUSE_SESSION_ID or pass --session)');
  return session;
}

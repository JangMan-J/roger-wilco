/**
 * The Pi adapter's only mapping from a Pi session to a core session address.
 * A Pi session ID is a UUID; it always crosses into the core in lowercase.
 * Nothing else in the adapter reads or folds a Pi session ID.
 */
import type { ExtensionContext } from '@earendil-works/pi-coding-agent';
import { isSessionAddress, type SessionAddress } from '../protocol/index.ts';

type PiSession = Pick<ExtensionContext, 'sessionManager'>;

/** The canonical session ID for a Pi session ID, including one read from a session file header. */
export function piSessionId(id: string): string {
  return id.toLowerCase();
}

/** The address of the session that `ctx` currently belongs to. */
export function piSessionAddress(ctx: PiSession, namespace: string): SessionAddress {
  const session = { namespace, sessionId: piSessionId(ctx.sessionManager.getSessionId()) };
  if (!isSessionAddress(session)) throw new Error('Roger-Wilco requires a persistent Pi UUID session');
  return session;
}

/** Same as piSessionAddress, and the Pi session must also have a persistent session file. */
export function requirePersistentPiSession(ctx: PiSession, namespace: string): SessionAddress {
  const session = piSessionAddress(ctx, namespace);
  if (!ctx.sessionManager.getSessionFile()) throw new Error('Roger-Wilco requires a persistent Pi session manager');
  return session;
}

/** True while `ctx` still belongs to `session`. */
export function isPiSession(ctx: PiSession, session: SessionAddress): boolean {
  return piSessionId(ctx.sessionManager.getSessionId()) === session.sessionId;
}

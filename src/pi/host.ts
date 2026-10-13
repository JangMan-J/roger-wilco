/** Clients for the generic core: the local host socket and the coordinator HTTP API. Nothing here is Pi-specific. */
import type { HostInbox, Outcome } from '../protocol/index.ts';
import type { PiConfig } from './config.ts';

const REQUEST_MS = 5000;
const error = (message: string): Extract<Outcome<never>, { ok: false }> => ({ ok: false, error: 'temporarily_unavailable', message });

/** The host daemon through its socket client. Loaded lazily, at session start. */
export async function connectHost(config: PiConfig): Promise<HostInbox> {
  const { createHostClient } = await import('../host/client.ts');
  return createHostClient(config.inboxPath);
}

export async function requestCoordinator(config: PiConfig, route: string, method: 'GET' | 'POST', body?: unknown): Promise<Outcome<unknown>> {
  try {
    const response = await fetch(new URL(route, config.coordinatorUrl), {
      method, signal: AbortSignal.timeout(REQUEST_MS),
      headers: { Authorization: `Bearer ${config.credential}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const result: unknown = await response.json();
    if (!result || typeof result !== 'object' || !('ok' in result) || typeof result.ok !== 'boolean') return error('Invalid coordinator response');
    return result as Outcome<unknown>;
  } catch (e) { return error(`Coordinator unavailable: ${String(e)}`); }
}

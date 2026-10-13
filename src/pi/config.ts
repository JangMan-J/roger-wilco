import { readHostConfig } from '../bin/config.ts';
import { isNamespace, type HostConfig } from '../protocol/index.ts';

/** The host's provisioned config plus the namespace. Same trusted host credential; the sender is always the bound Pi session. */
export type PiConfig = HostConfig & { namespace: string };

/** Explicit config or the provisioned user config; never a per-session destination file. */
export async function loadPiConfig(path?: string): Promise<PiConfig> {
  const config = await readHostConfig(path);
  if (!isNamespace(config.namespace)) throw new Error('Invalid Pi namespace');
  return { ...config, namespace: config.namespace };
}

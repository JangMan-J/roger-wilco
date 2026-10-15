import { homedir } from 'node:os';
import { join } from 'node:path';
import { readHostConfig } from '../bin/config.ts';
import { isNamespace, type HostConfig } from '../protocol/index.ts';

/** The host's provisioned config plus the namespace and Muse session locating fields. */
export type MuseConfig = HostConfig & {
  namespace: string;
  /** Explicit Muse session id; defaults to the harness-provided MUSE_SESSION_ID. */
  sessionId?: string;
  /** Explicit transcript path; defaults to discovery under the Muse sessions root. */
  transcriptPath?: string;
  /** Root holding `<date>/<sessionId>/session.jsonl`; defaults to the Muse data directory. */
  sessionsRoot: string;
};

export function defaultSessionsRoot(): string {
  return join(process.env.XDG_DATA_HOME ?? join(homedir(), '.local/share'), 'muse', 'sessions');
}

/** Explicit config or the provisioned user config; never a per-session destination file. */
export async function loadMuseConfig(path?: string): Promise<MuseConfig> {
  const config = await readHostConfig(path);
  if (!isNamespace(config.namespace)) throw new Error('Invalid Muse namespace');
  return { ...config, namespace: config.namespace, sessionsRoot: defaultSessionsRoot() };
}

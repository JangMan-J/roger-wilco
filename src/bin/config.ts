import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { isAbsolute, join, resolve } from 'node:path';
import { isNamespace, type HostConfig } from '../protocol/index.ts';

export type CoordinatorConfig = {
  namespace: string;
  databasePath: string;
  listenHost: string;
  port: number;
  hostCredentials: Record<string, string>;
};
export function homePath(path: string): string {
  return path.startsWith('~/') ? join(homedir(), path.slice(2)) : resolve(path);
}
async function json(path: string): Promise<Record<string, unknown>> {
  const value: unknown = JSON.parse(await readFile(homePath(path), 'utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Configuration must be an object');
  return value as Record<string, unknown>;
}
function text(value: unknown, name: string): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`Invalid ${name}`);
  return value;
}
export async function readCoordinatorConfig(path = process.env.ROGER_WILCO_SERVER_CONFIG ?? join(homedir(), '.config/roger-wilco/coordinator.json')): Promise<CoordinatorConfig> {
  const value = await json(path);
  if (Object.keys(value).some(key => !['namespace', 'databasePath', 'listenHost', 'port', 'hostCredentials'].includes(key))) throw new Error('Unknown coordinator configuration field');
  if (!isNamespace(value.namespace)) throw new Error('Invalid coordinator namespace');
  const port = value.port ?? 7088;
  if (!Number.isInteger(port) || (port as number) < 0 || (port as number) > 65535) throw new Error('Invalid coordinator port');
  const credentials = value.hostCredentials;
  if (!credentials || typeof credentials !== 'object' || Array.isArray(credentials) || !Object.keys(credentials).length) throw new Error('Configure at least one host credential');
  for (const [host, token] of Object.entries(credentials)) {
    text(host, 'host ID'); text(token, 'credential');
  }
  return {
    namespace: value.namespace,
    databasePath: homePath(text(value.databasePath, 'databasePath')),
    listenHost: text(value.listenHost ?? '127.0.0.1', 'listenHost'),
    port: port as number, hostCredentials: credentials as Record<string, string>,
  };
}
export async function readHostConfig(path = process.env.ROGER_WILCO_CONFIG ?? join(homedir(), '.config/roger-wilco/host.json')): Promise<HostConfig> {
  const value = await json(path);
  if (Object.keys(value).some(key => !['namespace', 'coordinatorUrl', 'hostId', 'credential', 'inboxPath'].includes(key))) throw new Error('Unknown host configuration field');
  const coordinatorUrl = text(value.coordinatorUrl, 'coordinatorUrl');
  const url = new URL(coordinatorUrl);
  if (url.username || url.password || url.search || url.hash ||
      !(url.protocol === 'https:' || (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)))) {
    throw new Error('Coordinator requires HTTPS, except loopback development');
  }
  const pathValue = text(value.inboxPath, 'inboxPath');
  if (!isAbsolute(pathValue) && !pathValue.startsWith('~/')) throw new Error('inboxPath must be absolute or home-relative');
  return { coordinatorUrl, hostId: text(value.hostId, 'hostId'), credential: text(value.credential, 'credential'), inboxPath: homePath(pathValue) };
}

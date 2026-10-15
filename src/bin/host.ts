#!/usr/bin/env node
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHostDaemon, createDaemonInbox } from '../host/index.ts';
import { readHostConfig } from './config.ts';

export async function runHost(configPath?: string) {
  const config = await readHostConfig(configPath);
  await mkdir(dirname(config.inboxPath), { recursive: true, mode: 0o700 });
  const inbox = createDaemonInbox(config.inboxPath);
  const daemon = createHostDaemon(config, inbox);
  const abort = new AbortController();
  let stopped: Promise<void> | undefined;
  const stop = () => stopped ??= (async () => {
    abort.abort();
    await daemon.stop();
    await inbox.close();
  })();
  process.once('SIGINT', () => { void stop(); });
  process.once('SIGTERM', () => { void stop(); });
  console.log(JSON.stringify({ service: 'roger-wilco-host', hostId: config.hostId }));
  try { await daemon.start(abort.signal); }
  finally { await stop(); }
}
/**
 * Clear the inbox bookmark so the next connect resumes at the coordinator's
 * receipted position. Takes the inbox singleton lock, so it fails while a
 * daemon runs. Prints the highest position this inbox has seen.
 */
export async function resetBookmark(configPath?: string): Promise<{ highWater: number }> {
  const config = await readHostConfig(configPath);
  const inbox = createDaemonInbox(config.inboxPath);
  try { return inbox.resetBookmark(); }
  finally { await inbox.close(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const args = process.argv.slice(2);
  if (args.includes('--help')) {
    console.log('Usage: roger-wilco-host [config.json]\n       roger-wilco-host --reset-bookmark [config.json]\nDefault: $ROGER_WILCO_CONFIG or ~/.config/roger-wilco/host.json');
  } else if (args[0] === '--reset-bookmark') {
    console.log(JSON.stringify(await resetBookmark(args[1])));
  } else { await runHost(args[0]); }
}

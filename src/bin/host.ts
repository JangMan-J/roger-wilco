#!/usr/bin/env node
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHostDaemon, createHostInbox } from '../host/index.ts';
import { readHostConfig } from './config.ts';

export async function runHost(configPath?: string) {
  const config = await readHostConfig(configPath);
  await mkdir(dirname(config.inboxPath), { recursive: true, mode: 0o700 });
  const inbox = createHostInbox(config.inboxPath);
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
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.includes('--help')) {
    console.log('Usage: roger-wilco-host [config.json]\nDefault: $ROGER_WILCO_CONFIG or ~/.config/roger-wilco/host.json');
  } else { await runHost(process.argv[2]); }
}

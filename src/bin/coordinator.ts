#!/usr/bin/env node
import { createServer } from 'node:http';
import { mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createCoordinatorStore } from '../server/store.ts';
import { createControlHandler, createHostAuthenticator } from '../server/control/index.ts';
import { createStreamHandler } from '../server/delivery/index.ts';
import { ROUTES, type Settings } from '../protocol/index.ts';
import { readCoordinatorConfig, type CoordinatorConfig } from './config.ts';
import { readSettings } from './settings.ts';

const STREAM_ROUTES: ReadonlySet<string> = new Set([ROUTES.stream, ROUTES.receipt, ROUTES.streamV2, ROUTES.receiptV2]);

/** Settings default to the settings file (`$ROGER_WILCO_SETTINGS` or ~/.config/roger-wilco/roger-wilco.conf). */
export async function startCoordinator(config: CoordinatorConfig, settings?: Settings) {
  settings ??= await readSettings();
  await mkdir(dirname(config.databasePath), { recursive: true, mode: 0o700 });
  const store = createCoordinatorStore(config.databasePath, config.namespace, { retentionMs: settings.retentionMs });
  const auth = createHostAuthenticator(config.hostCredentials);
  const control = createControlHandler(store, auth, settings);
  const stream = createStreamHandler(store, auth, settings);
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? '/', 'http://localhost').pathname;
    const handler = STREAM_ROUTES.has(path) ? stream : control;
    void handler(request, response).catch(error => {
      console.error('Roger-Wilco request failed:', error instanceof Error ? error.message : 'unknown error');
      if (!response.headersSent) response.writeHead(500, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ ok: false, error: 'temporarily_unavailable', message: 'Request failed' }));
    });
  });
  server.requestTimeout = 15_000;
  server.headersTimeout = 10_000;
  server.keepAliveTimeout = 5_000;
  try {
    await new Promise<void>((ready, reject) => {
      server.once('error', reject);
      server.listen(config.port, config.listenHost, () => { server.off('error', reject); ready(); });
    });
  } catch (error) { store.close(); throw error; }
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Missing coordinator address');
  let closing: Promise<void> | undefined;
  return {
    url: `http://${config.listenHost.includes(':') ? `[${config.listenHost}]` : config.listenHost}:${address.port}`,
    store,
    close(): Promise<void> {
      return closing ??= (async () => {
        await stream.close();
        await new Promise<void>(done => { server.close(() => done()); server.closeAllConnections(); });
        store.close();
      })();
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.includes('--help')) {
    console.log('Usage: roger-wilco-coordinator [config.json]\nDefault: $ROGER_WILCO_SERVER_CONFIG or ~/.config/roger-wilco/coordinator.json\n' +
      'Settings: $ROGER_WILCO_SETTINGS or ~/.config/roger-wilco/roger-wilco.conf (absent: defaults)');
  } else {
    const runtime = await startCoordinator(await readCoordinatorConfig(process.argv[2]));
    console.log(JSON.stringify({ service: 'roger-wilco-coordinator', url: runtime.url }));
    const stop = () => { void runtime.close().then(() => { process.exitCode = 0; }); };
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
  }
}

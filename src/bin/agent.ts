#!/usr/bin/env node
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { runAgent } from '../agent/tracer.ts';

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.includes('--help')) {
    console.log(`Usage: roger-wilco-agent <config.json> <register|send|serve> [recipient] [body]
Config: coordinatorUrl, credential, namespace, sessionId, inboxPath, evidencePath
Optional: runtimeId, displayName
sessionId is one opaque token, for example tracer-1.`);
  } else {
    try { await runAgent(process.argv.slice(2)); }
    catch (error) {
      console.error(error instanceof Error ? error.message : 'Agent failed');
      process.exitCode = 1;
    }
  }
}

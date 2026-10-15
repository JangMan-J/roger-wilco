#!/usr/bin/env node
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { runMuseCli } from '../muse/cli.ts';

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.includes('--help')) {
    console.log(`Usage: roger-wilco-muse [--config <host.json>] [--session <id>] [--transcript <session.jsonl>] <command>
Commands: address | register | board [--all] | send <recipient|*> <body> | inbox |
  identity | entity <label> | claim <key> [note] | release <key> | receive | serve
Config: ROGER_WILCO_CONFIG or ~/.config/roger-wilco/host.json (same host config as every harness).
Session: MUSE_SESSION_ID from the Muse harness, or --session. Transcript evidence is
read from the Muse session transcript; a delivered message is acknowledged only after
that transcript records it, so run receive again after Muse has persisted the output.`);
  } else {
    try { await runMuseCli(process.argv.slice(2)); }
    catch (error) {
      console.error(error instanceof Error ? error.message : 'Muse adapter failed');
      process.exitCode = 1;
    }
  }
}

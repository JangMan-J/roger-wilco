#!/usr/bin/env node
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { runClaudeCli } from '../claude/cli.ts';

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (process.argv.includes('--help')) {
    console.log(`Usage: roger-wilco-claude [--config <host.json>] [--session <id>] [--transcript <session.jsonl>] <command>
Commands: address | register | board [--all] | send <recipient|*> <body> | inbox |
  identity | entity <label> | claim <key> [note] | release <key> | receive | serve
Config: ROGER_WILCO_CONFIG or ~/.config/roger-wilco/host.json (same host config as every harness).
Session: CLAUDE_CODE_SESSION_ID from the Claude Code Bash tool, or --session. Evidence is
read from the session transcript under $CLAUDE_CONFIG_DIR/projects (default ~/.claude/projects).
A delivered message is acknowledged only after Claude Code records it as a tool result in that
transcript, so run receive again in a later Bash call.`);
  } else {
    try { await runClaudeCli(process.argv.slice(2)); }
    catch (error) {
      console.error(error instanceof Error ? error.message : 'Claude Code adapter failed');
      process.exitCode = 1;
    }
  }
}

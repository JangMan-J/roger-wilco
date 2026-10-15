/**
 * The Claude Code CLI harness: the adapter driven from Claude Code's Bash tool.
 * `receive` prints each delivered message as one JSON line; Claude Code records that
 * output as a tool result in the session transcript, and a later pass acknowledges it
 * from that transcript evidence. Nothing is acknowledged on print alone.
 */
import { setTimeout as delay } from 'node:timers/promises';
import { connectHost, requestCoordinator } from '../host/http.ts';
import { sessionKey, type Message } from '../protocol/index.ts';
import { createClaudeAdapter, type ClaudeHarness } from './adapter.ts';
import { loadClaudeConfig, type ClaudeConfig } from './config.ts';
import { findClaudeTranscript } from './transcript.ts';

export type ClaudeCliOptions = { configPath?: string; sessionId?: string; transcriptPath?: string };

/** Build the CLI harness for one Claude Code session. Transcript discovery happens before the adapter starts. */
export async function createCliHarness(config: ClaudeConfig, overrides: ClaudeCliOptions): Promise<ClaudeHarness & { sessionIdValue: string }> {
  const sessionIdValue = overrides.sessionId ?? config.sessionId ?? process.env.CLAUDE_CODE_SESSION_ID ?? '';
  const transcriptPath = overrides.transcriptPath ?? config.transcriptPath ??
    (sessionIdValue ? await findClaudeTranscript(sessionIdValue, config.projectsRoot) : undefined);
  return {
    sessionIdValue,
    sessionId: () => sessionIdValue,
    transcriptPath: () => transcriptPath,
    deliver: (message: Message) => {
      console.log(JSON.stringify({ event: 'delivered', message }));
    },
  };
}

async function withAdapter<T>(overrides: ClaudeCliOptions, run: (adapter: ReturnType<typeof createClaudeAdapter>) => Promise<T>): Promise<T> {
  const config = { ...(await loadClaudeConfig(overrides.configPath)), ...(overrides.sessionId ? { sessionId: overrides.sessionId } : {}) };
  const harness = await createCliHarness(config, overrides);
  const adapter = createClaudeAdapter(harness, {
    loadConfig: async () => config,
    connectHost,
    request: requestCoordinator,
  });
  await adapter.start();
  try { return await run(adapter); }
  finally { await adapter.stop(); }
}

function print(value: unknown): void {
  console.log(JSON.stringify(value));
}

/** Run one CLI command. Usage is in the help text of `src/bin/claude.ts`. */
export async function runClaudeCli(args: string[]): Promise<void> {
  const overrides: ClaudeCliOptions = {};
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === '--config') overrides.configPath = args[++i];
    else if (arg === '--session') overrides.sessionId = args[++i];
    else if (arg === '--transcript') overrides.transcriptPath = args[++i];
    else rest.push(arg);
  }
  const [command, ...tail] = rest;
  if (!command) throw new Error('Usage: roger-wilco-claude [--config <host.json>] [--session <id>] <address|register|board|send|inbox|identity|entity|claim|release|receive|serve>');

  if ((command === 'register' || command === 'identity') && tail.length)
    throw new Error(`${command} takes no name; a call sign is an entity, for example [[0:FLEET]] in a message`);
  if (command === 'serve') {
    if (tail.length) throw new Error('serve takes no arguments');
    await withAdapter(overrides, async adapter => {
      const identity = await adapter.bound();
      if ('ok' in identity) throw new Error(identity.message);
      print({ service: 'roger-wilco-claude', event: 'ready', session: sessionKey(identity.actor) });
      const abort = new AbortController();
      const stop = () => abort.abort();
      process.once('SIGINT', stop);
      process.once('SIGTERM', stop);
      while (!abort.signal.aborted) {
        await adapter.receive().catch(() => {});
        await delay(1000);
      }
    });
    return;
  }

  await withAdapter(overrides, async adapter => {
    const identity = await adapter.bound();
    if ('ok' in identity) throw new Error(identity.message);
    switch (command) {
      case 'address':
        print({ session: sessionKey(identity.actor) });
        return;
      case 'register':
        print(await adapter.tool('roger_identity', {}));
        return;
      case 'board':
        print(await adapter.tool('roger_board', { all: tail.includes('--all') || tail.includes('all') }));
        return;
      case 'send': {
        const [recipient, ...body] = tail;
        if (!recipient || !body.length) throw new Error('Usage: roger-wilco-claude send <recipient|*> <body>');
        print(await adapter.tool('roger_send', { recipient, body: body.join(' ') }));
        return;
      }
      case 'inbox':
        print(await adapter.tool('roger_inbox', {}));
        return;
      case 'identity':
        print(await adapter.tool('roger_identity', {}));
        return;
      case 'entity': {
        const label = tail.join(' ');
        if (!label) throw new Error('Usage: roger-wilco-claude entity <label>');
        print(await adapter.tool('roger_entity', { label }));
        return;
      }
      case 'claim': {
        const [key, ...note] = tail;
        if (!key) throw new Error('Usage: roger-wilco-claude claim <key> [note]');
        print(await adapter.tool('roger_claim', { key, ...(note.length ? { note: note.join(' ') } : {}) }));
        return;
      }
      case 'release': {
        if (!tail[0]) throw new Error('Usage: roger-wilco-claude release <key>');
        print(await adapter.tool('roger_release', { key: tail.join(' ') }));
        return;
      }
      case 'receive':
        // The deliver callback already printed each message; this pass also
        // acknowledged any earlier delivery the transcript now proves.
        await adapter.receive();
        return;
      default:
        throw new Error(`Unknown command: ${command}`);
    }
  });
}

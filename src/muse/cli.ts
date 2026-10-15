/**
 * The Muse CLI harness: the adapter driven from Muse's shell surface.
 * `receive` prints each delivered message as one JSON line; Muse records that
 * tool output in the session transcript, and a later pass acknowledges it from
 * that transcript evidence. Nothing is acknowledged on print alone.
 */
import { setTimeout as delay } from 'node:timers/promises';
import { connectHost, requestCoordinator } from '../host/http.ts';
import { sessionKey, type Message } from '../protocol/index.ts';
import { createMuseAdapter, type MuseHarness } from './adapter.ts';
import { loadMuseConfig, type MuseConfig } from './config.ts';
import { findMuseTranscript } from './transcript.ts';

export type MuseCliOptions = { configPath?: string; sessionId?: string; transcriptPath?: string };

/** Build the CLI harness for one Muse session. Transcript discovery happens before the adapter starts. */
export async function createCliHarness(config: MuseConfig, overrides: MuseCliOptions): Promise<MuseHarness & { sessionIdValue: string }> {
  const sessionIdValue = overrides.sessionId ?? config.sessionId ?? process.env.MUSE_SESSION_ID ?? '';
  const transcriptPath = overrides.transcriptPath ?? config.transcriptPath ??
    (sessionIdValue ? await findMuseTranscript(sessionIdValue, config.sessionsRoot) : undefined);
  return {
    sessionIdValue,
    sessionId: () => sessionIdValue,
    transcriptPath: () => transcriptPath,
    deliver: (message: Message) => {
      console.log(JSON.stringify({ event: 'delivered', message }));
    },
  };
}

async function withAdapter<T>(overrides: MuseCliOptions, run: (adapter: ReturnType<typeof createMuseAdapter>) => Promise<T>): Promise<T> {
  const config = { ...(await loadMuseConfig(overrides.configPath)), ...(overrides.sessionId ? { sessionId: overrides.sessionId } : {}) };
  const harness = await createCliHarness(config, overrides);
  const adapter = createMuseAdapter(harness, {
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

/** Run one CLI command. Usage is in the help text of `src/bin/muse.ts`. */
export async function runMuseCli(args: string[]): Promise<void> {
  const overrides: MuseCliOptions = {};
  const rest: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === '--config') overrides.configPath = args[++i];
    else if (arg === '--session') overrides.sessionId = args[++i];
    else if (arg === '--transcript') overrides.transcriptPath = args[++i];
    else rest.push(arg);
  }
  const [command, ...tail] = rest;
  if (!command) throw new Error('Usage: roger-wilco-muse [--config <host.json>] [--session <id>] <address|register|board|send|inbox|identity|entity|claim|release|receive|serve>');

  if ((command === 'register' || command === 'identity') && tail.length)
    throw new Error(`${command} takes no name; a call sign is an entity, for example [[0:FLEET]] in a message`);
  if (command === 'serve') {
    if (tail.length) throw new Error('serve takes no arguments');
    await withAdapter(overrides, async adapter => {
      const identity = await adapter.bound();
      if ('ok' in identity) throw new Error(identity.message);
      print({ service: 'roger-wilco-muse', event: 'ready', session: sessionKey(identity.actor) });
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
        if (!recipient || !body.length) throw new Error('Usage: roger-wilco-muse send <recipient|*> <body>');
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
        if (!label) throw new Error('Usage: roger-wilco-muse entity <label>');
        print(await adapter.tool('roger_entity', { label }));
        return;
      }
      case 'claim': {
        const [key, ...note] = tail;
        if (!key) throw new Error('Usage: roger-wilco-muse claim <key> [note]');
        print(await adapter.tool('roger_claim', { key, ...(note.length ? { note: note.join(' ') } : {}) }));
        return;
      }
      case 'release': {
        if (!tail[0]) throw new Error('Usage: roger-wilco-muse release <key>');
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

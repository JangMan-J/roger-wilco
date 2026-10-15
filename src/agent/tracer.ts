/** Non-Pi agent. Talks to the coordinator over HTTP and to the host daemon over its socket. */
import { open, readFile, mkdir } from 'node:fs/promises';
import { isAbsolute, dirname, resolve } from 'node:path';
import {
  ROUTES, isMessage, isNamespace, isOutcome, isRegistrationResult, isSessionId, isSessionKey, sessionKey,
  type HostInbox, type Message, type RegistrationResult, type SessionAddress,
} from '../protocol/index.ts';
import { createHostClient } from '../host/client.ts';

const REQUEST_MS = 10_000;
const RENEW_MS = 10_000;

export type AgentConfig = {
  coordinatorUrl: string;
  credential: string;
  namespace: string;
  sessionId: string;
  inboxPath: string;
  evidencePath: string;
  runtimeId: string;
  displayName?: string;
};

export type EvidenceRecord = { id: string; messageId: string; message: Message };

export function agentSession(config: AgentConfig): SessionAddress {
  return { namespace: config.namespace, sessionId: config.sessionId };
}

export function parseAgentConfig(value: unknown): AgentConfig {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Configuration must be an object');
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some(key => !['coordinatorUrl', 'credential', 'namespace', 'sessionId', 'inboxPath', 'evidencePath', 'runtimeId', 'displayName'].includes(key)))
    throw new Error('Unknown agent configuration field');
  const coordinatorUrl = text(record.coordinatorUrl, 'coordinatorUrl');
  const url = new URL(coordinatorUrl);
  const loopback = url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  if (url.username || url.password || url.search || url.hash || (url.protocol !== 'https:' && !loopback))
    throw new Error('Coordinator requires HTTPS, except loopback');
  if (!isNamespace(record.namespace)) throw new Error('Invalid namespace');
  if (!isSessionId(record.sessionId)) throw new Error('Invalid session id');
  const inboxPath = text(record.inboxPath, 'inboxPath');
  const evidencePath = text(record.evidencePath, 'evidencePath');
  if (!isAbsolute(inboxPath) || !isAbsolute(evidencePath)) throw new Error('inboxPath and evidencePath must be absolute');
  const runtimeId = record.runtimeId === undefined ? 'tracer' : text(record.runtimeId, 'runtimeId');
  if (runtimeId.length > 200) throw new Error('Invalid runtimeId');
  if (record.displayName !== undefined && (typeof record.displayName !== 'string' || record.displayName.length > 200))
    throw new Error('Invalid displayName');
  return {
    coordinatorUrl, credential: text(record.credential, 'credential'), namespace: record.namespace,
    sessionId: record.sessionId, inboxPath, evidencePath, runtimeId,
    ...(record.displayName === undefined ? {} : { displayName: record.displayName }),
  };
}

export async function readAgentConfig(path: string): Promise<AgentConfig> {
  return parseAgentConfig(JSON.parse(await readFile(path, 'utf8')));
}

export async function registerSession(client: HostInbox, session: SessionAddress, displayName?: string): Promise<RegistrationResult> {
  const registration = displayName === undefined ? { session } : { session, displayName };
  const result = await client.awaitRegistration(registration);
  if (!result.ok) throw new Error(`${result.error}: ${result.message}`);
  if (!isRegistrationResult(result.value) || result.value.subscription.id !== sessionKey(session))
    throw new Error('Invalid registration response');
  return result.value;
}

export async function sendMessage(config: AgentConfig, recipient: string, body: string): Promise<Message> {
  if (!isSessionKey(recipient) || typeof body !== 'string' || !body) throw new TypeError('Invalid message');
  const response = await fetch(new URL(ROUTES.messages, config.coordinatorUrl), {
    method: 'POST',
    headers: { authorization: `Bearer ${config.credential}`, 'content-type': 'application/json' },
    body: JSON.stringify({ sender: agentSession(config), message: { recipient, body } }),
    signal: AbortSignal.timeout(REQUEST_MS),
  });
  const result: unknown = await response.json().catch(() => undefined);
  if (!isOutcome(result)) throw new Error('Invalid send response');
  if (!result.ok) throw new Error(`${result.error}: ${result.message}`);
  if (!isMessage(result.value)) throw new Error('Invalid send response');
  return result.value;
}

/**
 * Append-only JSONL. A record id is evidence only after this fsyncs the line.
 * The id derives from the message sequence, which is unique within the namespace,
 * so concurrent deliveries that read the same file snapshot still get distinct ids.
 */
export async function persistEvidence(path: string, message: Message): Promise<EvidenceRecord> {
  if (!isMessage(message)) throw new TypeError('Invalid message');
  const sessionFile = resolve(path);
  await mkdir(dirname(sessionFile), { recursive: true, mode: 0o700 });
  const handle = await open(sessionFile, 'a+', 0o600);
  try {
    const size = (await handle.stat()).size;
    const buffer = Buffer.alloc(size);
    if (size) {
      const read = await handle.read(buffer, 0, size, 0);
      if (read.bytesRead !== size) throw new Error('Evidence file changed during read');
    }
    const text = buffer.toString('utf8');
    const records = parseEvidence(text);
    const existing = records.find(record => record.messageId === message.id);
    if (existing) {
      await handle.sync();
      return existing;
    }
    const id = `rec-${message.sequence}`;
    const line = `${JSON.stringify({ id, messageId: message.id, message })}\n`;
    // A torn tail has no newline. Seal it before appending so the new record stays one line.
    if (size > 0 && !text.endsWith('\n')) await handle.write('\n');
    await handle.write(line);
    await handle.sync();
    const directory = await open(dirname(sessionFile), 'r');
    try { await directory.sync(); }
    finally { await directory.close(); }
    return { id, messageId: message.id, message };
  } finally { await handle.close(); }
}

export async function readEvidence(path: string): Promise<EvidenceRecord[]> {
  try { return parseEvidence(await readFile(path, 'utf8')); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
}

/** Pending host-daemon messages. Each one is fsynced before the socket acknowledgement. */
export async function persistAndAcknowledge(client: HostInbox, session: SessionAddress, runtimeId: string, evidencePath: string): Promise<EvidenceRecord[]> {
  const delivered: EvidenceRecord[] = [];
  for (;;) {
    const pending = await client.pending(session, runtimeId);
    if (!pending.length) return delivered;
    for (const message of pending) {
      const record = await persistEvidence(evidencePath, message);
      // The tracer's own evidence: the JSONL record id inside its append-only evidence file.
      const result = await client.acknowledgeDelivery(session, runtimeId, {
        messageId: message.id, sessionId: session.sessionId, recordId: record.id, location: resolve(evidencePath),
      });
      if (!result.ok) throw new Error(`${result.error}: ${result.message}`);
      delivered.push(record);
    }
  }
}

export async function serve(config: AgentConfig, signal: AbortSignal): Promise<void> {
  const client = createHostClient(config.inboxPath);
  const session = agentSession(config);
  const wake = new Wake();
  const unsubscribe = client.onChange(() => wake.signal());
  signal.addEventListener('abort', () => { wake.signal(); void client.close(); }, { once: true });
  try {
    await registerSession(client, session, config.displayName);
    await holdLease(client, session, config.runtimeId);
    console.log(JSON.stringify({ service: 'roger-wilco-agent', event: 'ready', session: sessionKey(session) }));
    wake.signal();
    while (!signal.aborted) {
      if (wake.consume()) {
        await holdLease(client, session, config.runtimeId);
        const delivered = await persistAndAcknowledge(client, session, config.runtimeId, config.evidencePath);
        for (const record of delivered) {
          console.log(JSON.stringify({ event: 'delivered', recordId: record.id, messageId: record.messageId, body: record.message.body }));
        }
      }
      await wake.wait(RENEW_MS, signal);
    }
  } catch (error) {
    if (!signal.aborted) throw error;
  } finally {
    unsubscribe();
    await client.close();
  }
}

export async function runAgent(args: string[]): Promise<void> {
  const [configPath, command, ...rest] = args;
  if (!configPath || !command) throw new Error('Usage: roger-wilco-agent <config.json> <register|send|serve> [recipient] [body]');
  const config = await readAgentConfig(configPath);
  if (command === 'register') {
    const client = createHostClient(config.inboxPath);
    try {
      const result = await registerSession(client, agentSession(config), config.displayName);
      console.log(JSON.stringify({ event: 'registered', session: result.subscription.id }));
    } finally { await client.close(); }
    return;
  }
  if (command === 'send') {
    const recipient = rest[0];
    const body = rest.slice(1).join(' ');
    if (!recipient || !body) throw new Error('Usage: roger-wilco-agent <config.json> send <recipient> <body>');
    console.log(JSON.stringify({ event: 'sent', message: await sendMessage(config, recipient, body) }));
    return;
  }
  if (command === 'serve') {
    if (rest.length) throw new Error('serve takes no arguments');
    const abort = new AbortController();
    const stop = () => abort.abort();
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
    await serve(config, abort.signal);
    return;
  }
  throw new Error(`Unknown command: ${command}`);
}

async function holdLease(client: HostInbox, session: SessionAddress, runtimeId: string): Promise<void> {
  const renewed = await client.renew(session, runtimeId);
  if (renewed.ok) return;
  const bound = await client.bind(session, runtimeId);
  if (!bound.ok) throw new Error(`${bound.error}: ${bound.message}`);
}

function parseEvidence(text: string): EvidenceRecord[] {
  if (!text) return [];
  const lines = text.split('\n');
  lines.pop(); // Drop the empty end after a newline, or a torn tail that is not yet a record.
  const records: EvidenceRecord[] = [];
  for (const line of lines) {
    if (!line) continue;
    let value: unknown;
    try { value = JSON.parse(line); }
    catch { continue; }
    if (!isEvidence(value)) throw new Error('Corrupt evidence record');
    records.push(value);
  }
  return records;
}

function isEvidence(value: unknown): value is EvidenceRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return Object.keys(record).length === 3 && typeof record.id === 'string' && record.id.length > 0 &&
    typeof record.messageId === 'string' && isMessage(record.message) && record.message.id === record.messageId;
}

function text(value: unknown, name: string): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`Invalid ${name}`);
  return value;
}

class Wake {
  private flagged = false;
  private readonly waiters = new Set<() => void>();
  signal(): void {
    this.flagged = true;
    for (const waiter of this.waiters) waiter();
    this.waiters.clear();
  }
  consume(): boolean {
    const flagged = this.flagged;
    this.flagged = false;
    return flagged;
  }
  wait(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise(resolve => {
      let timer: NodeJS.Timeout;
      const done = () => {
        clearTimeout(timer);
        signal.removeEventListener('abort', done);
        this.waiters.delete(done);
        resolve();
      };
      timer = setTimeout(done, ms);
      this.waiters.add(done);
      if (this.flagged || signal.aborted) done();
      else signal.addEventListener('abort', done, { once: true });
    });
  }
}

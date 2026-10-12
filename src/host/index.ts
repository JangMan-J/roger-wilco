import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import {
  isRegistration, isSessionAddress, isStreamFrame, sessionKey, ROUTES, MAX_STREAM_EVENT_BYTES,
  type HostConfig, type HostInbox, type Message, type Outcome, type PiReceipt,
  type Registration, type SessionAddress, type StreamFrame,
} from '../protocol/index.ts';

const LEASE_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;
const STREAM_IDLE_MS = 45_000;
const RETRY_MIN_MS = 250;
const RETRY_MAX_MS = 10_000;

type LeaseRow = { runtime_id: string; expires_at: number };
type RegistrationRow = { session_key: string; registration_json: string };
type PendingRow = { message_json: string };

class LocalHostInbox implements HostInbox {
  private readonly db: DatabaseSync;
  private closed = false;

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        session_key TEXT NOT NULL,
        message_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        message_json TEXT NOT NULL,
        receipt_entry_id TEXT,
        receipt_session_file TEXT,
        PRIMARY KEY (session_key, message_id)
      );
      CREATE INDEX IF NOT EXISTS messages_due ON messages(session_key, receipt_entry_id, sequence);
      CREATE TABLE IF NOT EXISTS leases (
        session_key TEXT PRIMARY KEY,
        runtime_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS registrations (
        session_key TEXT PRIMARY KEY,
        registration_json TEXT NOT NULL
      );
    `);
  }

  private tx<T>(work: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = work();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  async receive(frame: StreamFrame): Promise<{ inserted: boolean }> {
    if (!isStreamFrame(frame)) throw new TypeError('Invalid stream frame');
    const result = this.db.prepare(`INSERT OR IGNORE INTO messages
      (session_key, message_id, sequence, message_json) VALUES (?, ?, ?, ?)`).run(
      frame.subscriptionId, frame.message.id, frame.message.sequence, JSON.stringify(frame.message));
    return { inserted: result.changes === 1 };
  }

  async bind(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>> {
    return this.setLease(session, runtimeId, now, false);
  }

  async renew(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>> {
    return this.setLease(session, runtimeId, now, true);
  }

  private setLease(session: SessionAddress, runtimeId: string, now: number, renew: boolean): Outcome<{ leaseExpiresAt: number }> {
    if (!isSessionAddress(session) || !runtimeId || !Number.isSafeInteger(now) || now < 0) return failure('invalid_request', 'Invalid lease input');
    return this.tx(() => {
      const key = sessionKey(session);
      const row = this.db.prepare('SELECT runtime_id, expires_at FROM leases WHERE session_key = ?').get(key) as LeaseRow | undefined;
      if (renew && (!row || row.runtime_id !== runtimeId || row.expires_at <= now)) return failure('owner_conflict', 'Lease no longer owned');
      if (!renew && row && row.expires_at > now && row.runtime_id !== runtimeId) return failure('owner_conflict', 'Session already active');
      // A lease is always bounded by the current clock, even on a repeated bind.
      const expires = now + LEASE_MS;
      this.db.prepare(`INSERT INTO leases (session_key, runtime_id, expires_at) VALUES (?, ?, ?)
        ON CONFLICT(session_key) DO UPDATE SET runtime_id = excluded.runtime_id, expires_at = excluded.expires_at`)
        .run(key, runtimeId, expires);
      return { ok: true, value: { leaseExpiresAt: expires } };
    });
  }

  async release(session: SessionAddress, runtimeId: string): Promise<void> {
    if (!isSessionAddress(session) || !runtimeId) return;
    this.db.prepare('DELETE FROM leases WHERE session_key = ? AND runtime_id = ?').run(sessionKey(session), runtimeId);
  }

  private owns(session: SessionAddress, runtimeId: string): boolean {
    const row = this.db.prepare('SELECT runtime_id, expires_at FROM leases WHERE session_key = ?')
      .get(sessionKey(session)) as LeaseRow | undefined;
    return !!row && row.runtime_id === runtimeId && row.expires_at > Date.now();
  }

  async pending(session: SessionAddress, runtimeId: string): Promise<Message[]> {
    if (!isSessionAddress(session) || !runtimeId || !this.owns(session, runtimeId)) return [];
    const rows = this.db.prepare(`SELECT message_json FROM messages WHERE session_key = ? AND receipt_entry_id IS NULL
      ORDER BY sequence ASC`).all(sessionKey(session)) as PendingRow[];
    return rows.map(row => JSON.parse(row.message_json) as Message);
  }

  async acknowledgePi(session: SessionAddress, runtimeId: string, proof: PiReceipt): Promise<Outcome<null>> {
    if (!isSessionAddress(session) || !runtimeId || !proof || proof.piSessionId !== session.piSessionId ||
      typeof proof.messageId !== 'string' || !proof.messageId.startsWith(`${session.namespace}:msg:`) ||
      typeof proof.entryId !== 'string' || !proof.entryId ||
      typeof proof.sessionFile !== 'string' || !proof.sessionFile) return failure('invalid_request', 'Invalid Pi receipt');
    return this.tx(() => {
      if (!this.owns(session, runtimeId)) return failure('owner_conflict', 'Session lease is not owned');
      const row = this.db.prepare(`SELECT message_json, receipt_entry_id, receipt_session_file FROM messages
        WHERE session_key = ? AND message_id = ?`).get(sessionKey(session), proof.messageId) as
        (PendingRow & { receipt_entry_id: string | null; receipt_session_file: string | null }) | undefined;
      if (!row) return failure('not_found', 'Message not in session inbox');
      if (row.receipt_entry_id !== null) {
        return row.receipt_entry_id === proof.entryId && row.receipt_session_file === proof.sessionFile
          ? { ok: true, value: null } : failure('invalid_request', 'Conflicting Pi receipt');
      }
      this.db.prepare(`UPDATE messages SET receipt_entry_id = ?, receipt_session_file = ?
        WHERE session_key = ? AND message_id = ?`).run(proof.entryId, proof.sessionFile, sessionKey(session), proof.messageId);
      return { ok: true, value: null };
    });
  }

  async enqueueRegistration(registration: Registration): Promise<void> {
    if (!isRegistration(registration)) throw new TypeError('Invalid registration');
    this.db.prepare(`INSERT INTO registrations (session_key, registration_json) VALUES (?, ?)
      ON CONFLICT(session_key) DO UPDATE SET registration_json = excluded.registration_json`)
      .run(sessionKey(registration.session), JSON.stringify(registration));
  }

  registrations(): RegistrationRow[] {
    return this.db.prepare('SELECT session_key, registration_json FROM registrations ORDER BY session_key').all() as RegistrationRow[];
  }

  completeRegistration(row: RegistrationRow): void {
    // A concurrent re-enqueue is not consumed by a response to the earlier request.
    this.db.prepare('DELETE FROM registrations WHERE session_key = ? AND registration_json = ?')
      .run(row.session_key, row.registration_json);
  }

  async close(): Promise<void> {
    if (!this.closed) { this.closed = true; this.db.close(); }
  }
}

function failure(code: 'invalid_request' | 'owner_conflict' | 'not_found', message: string): Outcome<never> {
  return { ok: false, error: code, message };
}

export function createHostInbox(path: string): HostInbox {
  return new LocalHostInbox(path);
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise(resolve => {
    const timer = setTimeout(done, ms);
    function done() { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); }
    signal.addEventListener('abort', done, { once: true });
  });
}

async function jsonPost(base: URL, route: string, credential: string, body: unknown, signal: AbortSignal): Promise<unknown> {
  const response = await fetch(new URL(route, base), {
    method: 'POST', headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
  });
  if (!response.ok) throw new Error(`${route}: HTTP ${response.status}`);
  return response.json();
}

/** SSE parser: byte-limited records, streaming UTF-8, CRLF, multiline data and comments. */
async function* frames(stream: ReadableStream<Uint8Array>, signal: AbortSignal, onBytes: () => void): AsyncGenerator<StreamFrame> {
  const reader = stream.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let line = '', data: string[] = [], event = '', bytes = 0;
  const field = (raw: string): StreamFrame | undefined => {
    const text = raw.endsWith('\r') ? raw.slice(0, -1) : raw;
    if (!text) {
      const payload = data.join('\n');
      const kind = event;
      data = []; event = ''; bytes = 0;
      if (!payload || kind !== 'message') return undefined;
      let value: unknown;
      try { value = JSON.parse(payload); } catch { throw new Error('Invalid SSE JSON'); }
      if (!isStreamFrame(value)) throw new Error('Invalid SSE frame');
      return value;
    }
    if (text.startsWith(':')) return undefined;
    const sep = text.indexOf(':');
    const name = sep < 0 ? text : text.slice(0, sep);
    const value = sep < 0 ? '' : text.slice(sep + 1).replace(/^ /, '');
    if (name === 'event') event = value;
    if (name === 'data') data.push(value);
    return undefined;
  };
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value.byteLength) onBytes();
      const chunk = decoder.decode(value, { stream: true });
      let start = 0;
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] !== '\n') continue;
        line += chunk.slice(start, i);
        bytes += Buffer.byteLength(line) + 1;
        if (bytes > MAX_STREAM_EVENT_BYTES) throw new Error('SSE event exceeds byte limit');
        const frame = field(line);
        line = ''; start = i + 1;
        if (frame) yield frame;
      }
      line += chunk.slice(start);
      if (bytes + Buffer.byteLength(line) > MAX_STREAM_EVENT_BYTES) throw new Error('SSE event exceeds byte limit');
    }
    decoder.decode(); // reject an incomplete UTF-8 suffix
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function createHostDaemon(config: HostConfig, inbox: HostInbox): { start(signal: AbortSignal): Promise<void>; stop(): Promise<void> } {
  if (!(inbox instanceof LocalHostInbox)) throw new TypeError('Host daemon requires the durable local inbox');
  const base = new URL(config.coordinatorUrl.endsWith('/') ? config.coordinatorUrl : `${config.coordinatorUrl}/`);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash ||
      !config.hostId || !config.credential) throw new TypeError('Invalid host configuration');
  let controller: AbortController | undefined;
  let loops: Promise<void>[] = [];
  let external: AbortSignal | undefined;
  const onAbort = () => controller?.abort();
  let lastError = '';
  let lastReport = 0;
  const report = (error: unknown) => {
    const text = error instanceof Error ? error.message : String(error);
    // Repeated outages are visible without flooding stderr or exposing credentials.
    if (text !== lastError || Date.now() - lastReport > 30_000) {
      console.error('roger-wilco host:', text);
      lastError = text;
      lastReport = Date.now();
    }
  };
  const registrationLoop = async (signal: AbortSignal) => {
    let backoff = RETRY_MIN_MS;
    while (!signal.aborted) {
      try {
        for (const row of inbox.registrations()) {
          if (signal.aborted) break;
          const registration = JSON.parse(row.registration_json) as Registration;
          const response = await jsonPost(base, ROUTES.register, config.credential, registration, signal) as
            { ok?: boolean; value?: { subscription?: { id?: string; hostId?: string } } };
          if (response?.ok !== true || response.value?.subscription?.id !== row.session_key ||
              response.value.subscription.hostId !== config.hostId) throw new Error('Invalid registration response');
          inbox.completeRegistration(row);
        }
        backoff = RETRY_MIN_MS;
        await delay(500, signal);
      } catch (error) {
        if (!signal.aborted) report(error);
        await delay(backoff, signal);
        backoff = Math.min(backoff * 2, RETRY_MAX_MS);
      }
    }
  };
  const streamLoop = async (signal: AbortSignal) => {
    let backoff = RETRY_MIN_MS;
    while (!signal.aborted) {
      const connection = new AbortController();
      const streamSignal = AbortSignal.any([signal, connection.signal]);
      let deadline: ReturnType<typeof setTimeout> | undefined;
      const armDeadline = (ms: number, reason: string) => {
        clearTimeout(deadline);
        deadline = setTimeout(() => connection.abort(new Error(reason)), ms);
      };
      try {
        armDeadline(REQUEST_TIMEOUT_MS, 'Host stream response timed out');
        const response = await fetch(new URL(ROUTES.stream, base), {
          headers: { authorization: `Bearer ${config.credential}`, accept: 'text/event-stream' }, signal: streamSignal,
        });
        if (!response.ok || !response.body || !response.headers.get('content-type')?.toLowerCase().startsWith('text/event-stream')) {
          await response.body?.cancel();
          throw new Error(`Stream unavailable: HTTP ${response.status}`);
        }
        armDeadline(STREAM_IDLE_MS, 'Host stream idle timed out');
        backoff = RETRY_MIN_MS;
        for await (const frame of frames(response.body, streamSignal, () => armDeadline(STREAM_IDLE_MS, 'Host stream idle timed out'))) {
          await inbox.receive(frame); // transaction commits before receipt is sent
          const receipt = { subscriptionId: frame.subscriptionId, messageId: frame.message.id };
          const ack = await jsonPost(base, ROUTES.receipt, config.credential, receipt, signal) as
            { ok?: boolean; value?: { id?: string; hostId?: string } };
          if (ack?.ok !== true || ack.value?.id !== receipt.subscriptionId || ack.value.hostId !== config.hostId) {
            throw new Error('Invalid receipt response');
          }
        }
        if (!signal.aborted) throw new Error('Host stream ended');
      } catch (error) {
        if (!signal.aborted) report(connection.signal.aborted ? connection.signal.reason : error);
      } finally {
        clearTimeout(deadline);
        connection.abort();
      }
      await delay(backoff, signal);
      backoff = Math.min(backoff * 2, RETRY_MAX_MS);
    }
  };
  return {
    async start(signal: AbortSignal) {
      if (controller) throw new Error('Host daemon already started');
      controller = new AbortController();
      external = signal;
      signal.addEventListener('abort', onAbort, { once: true });
      if (signal.aborted) controller.abort();
      loops = [registrationLoop(controller.signal), streamLoop(controller.signal)];
      await Promise.all(loops);
    },
    async stop() {
      controller?.abort();
      await Promise.all(loops);
      external?.removeEventListener('abort', onAbort);
      controller = undefined;
      loops = [];
    },
  };
}

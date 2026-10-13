import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { listenLocal } from './ipc.ts';
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
const HISTORY_MS = 7 * 86400_000;
const SWEEP_MS = 60_000;

type LeaseRow = { runtime_id: string; expires_at: number };
type RegistrationRow = { session_key: string; registration_json: string };
type PendingRow = { message_json: string };

export class LocalHostInbox implements HostInbox {
  private readonly db: DatabaseSync;
  private readonly lock: DatabaseSync;
  private closed = false;
  private nextSweepAt = 0;
  private readonly changes = new Set<(key: string) => void>();
  private readonly registrationChanges = new Set<() => void>();
  private readonly faults = new Set<() => void>();

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    // A separate inode provides a crash-released, process-wide singleton lock.
    // Never unlink it: doing so would allow two independent owners.
    this.lock = new DatabaseSync(`${path}.lock`);
    try {
      this.lock.exec('PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE');
    } catch (error) { this.lock.close(); throw error; }
    this.db = new DatabaseSync(path);
    try {
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000; PRAGMA journal_size_limit = 1048576');
    this.tx(() => {
      const version = this.db.prepare('PRAGMA user_version').get()!.user_version;
      if (version !== 0 && version !== 2) throw new Error(`Unsupported inbox schema version: ${version}`);
      if (version === 0) {
        // Old writers remain attached to this inode. Empty read-only views stop
        // both existing handles and constructors from recreating writer tables.
        this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        session_key TEXT NOT NULL,
        message_id TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        message_json TEXT NOT NULL,
        receipt_entry_id TEXT,
        receipt_session_file TEXT,
        acknowledged_at INTEGER,
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
      const columns = this.db.prepare('PRAGMA table_info(messages)').all();
      if (!columns.some(column => column.name === 'acknowledged_at'))
        this.db.exec('ALTER TABLE messages ADD COLUMN acknowledged_at INTEGER');
      // Give receipts from older versions a full retention window after migration.
      this.db.prepare('UPDATE messages SET acknowledged_at = ? WHERE receipt_entry_id IS NOT NULL AND acknowledged_at IS NULL')
        .run(Date.now());
        this.db.exec(`
          DROP INDEX IF EXISTS messages_due;
          DROP INDEX IF EXISTS messages_acknowledged;
          ALTER TABLE messages RENAME TO inbox_messages;
          ALTER TABLE leases RENAME TO inbox_leases;
          ALTER TABLE registrations RENAME TO inbox_registrations;
          CREATE VIEW messages AS SELECT * FROM inbox_messages WHERE 0;
          CREATE VIEW leases AS SELECT * FROM inbox_leases WHERE 0;
          CREATE VIEW registrations AS SELECT * FROM inbox_registrations WHERE 0;
          CREATE INDEX inbox_messages_due ON inbox_messages(session_key, receipt_entry_id, sequence);
          CREATE INDEX inbox_messages_acknowledged ON inbox_messages(acknowledged_at);
          PRAGMA user_version = 2;
        `);
      }
      // Connection grants never survive a daemon restart.
      this.db.exec('DELETE FROM inbox_leases');
    });
    this.sweep();
    } catch (error) { this.db.close(); this.lock.close(); throw error; }
  }

  onChange(listener: () => void): () => void { return this.subscribe(listener); }
  subscribe(listener: (key: string) => void): () => void {
    this.changes.add(listener);
    return () => this.changes.delete(listener);
  }
  onRegistration(listener: () => void): () => void {
    this.registrationChanges.add(listener);
    return () => this.registrationChanges.delete(listener);
  }
  onFault(listener: () => void): () => void {
    this.faults.add(listener);
    return () => this.faults.delete(listener);
  }
  private notify<T>(listeners: Set<(value: T) => void>, value: T): void {
    let fault: unknown;
    for (const listener of listeners) {
      try { listener(value); } catch (error) { fault = error; }
    }
    if (fault !== undefined) {
      for (const listener of this.faults) { try { listener(); } catch {} }
      throw fault;
    }
  }
  private changed(key: string): void {
    this.notify(this.changes, key);
  }

  sweep(): void {
    const now = Date.now();
    if (now < this.nextSweepAt) return;
    this.tx(() => {
      this.db.prepare('DELETE FROM inbox_messages WHERE acknowledged_at < ? AND receipt_entry_id IS NOT NULL').run(now - HISTORY_MS);
      this.db.prepare('DELETE FROM inbox_leases WHERE expires_at <= ?').run(now);
    });
    this.nextSweepAt = now + SWEEP_MS;
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
    const result = this.db.prepare(`INSERT OR IGNORE INTO inbox_messages
      (session_key, message_id, sequence, message_json) VALUES (?, ?, ?, ?)`).run(
      frame.subscriptionId, frame.message.id, frame.message.sequence, JSON.stringify(frame.message));
    if (result.changes === 1) this.changed(frame.subscriptionId);
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
      const row = this.db.prepare('SELECT runtime_id, expires_at FROM inbox_leases WHERE session_key = ?').get(key) as LeaseRow | undefined;
      if (renew && (!row || row.runtime_id !== runtimeId || row.expires_at <= now)) return failure('owner_conflict', 'Lease no longer owned');
      if (!renew && row && row.expires_at > now && row.runtime_id !== runtimeId) return failure('owner_conflict', 'Session already active');
      // A lease is always bounded by the current clock, even on a repeated bind.
      const expires = now + LEASE_MS;
      this.db.prepare(`INSERT INTO inbox_leases (session_key, runtime_id, expires_at) VALUES (?, ?, ?)
        ON CONFLICT(session_key) DO UPDATE SET runtime_id = excluded.runtime_id, expires_at = excluded.expires_at`)
        .run(key, runtimeId, expires);
      return { ok: true, value: { leaseExpiresAt: expires } };
    });
  }

  async release(session: SessionAddress, runtimeId: string): Promise<void> {
    if (!isSessionAddress(session) || !runtimeId) return;
    this.db.prepare('DELETE FROM inbox_leases WHERE session_key = ? AND runtime_id = ?').run(sessionKey(session), runtimeId);
  }

  private owns(session: SessionAddress, runtimeId: string): boolean {
    const row = this.db.prepare('SELECT runtime_id, expires_at FROM inbox_leases WHERE session_key = ?')
      .get(sessionKey(session)) as LeaseRow | undefined;
    return !!row && row.runtime_id === runtimeId && row.expires_at > Date.now();
  }

  async pending(session: SessionAddress, runtimeId: string): Promise<Message[]> {
    if (!isSessionAddress(session) || !runtimeId || !this.owns(session, runtimeId)) return [];
    const rows = this.db.prepare(`SELECT message_json FROM inbox_messages WHERE session_key = ? AND receipt_entry_id IS NULL
      ORDER BY sequence ASC`).all(sessionKey(session)) as PendingRow[];
    return rows.map(row => JSON.parse(row.message_json) as Message);
  }

  pendingPage(session: SessionAddress, owner: string, afterSequence = 0): Outcome<{ messages: Message[]; more: boolean }> {
    if (!this.owns(session, owner)) return failure('owner_conflict', 'Session lease is not owned');
    const rows = this.db.prepare(`SELECT message_json FROM inbox_messages
      WHERE session_key = ? AND receipt_entry_id IS NULL AND sequence > ? ORDER BY sequence LIMIT 2`)
      .all(sessionKey(session), afterSequence) as PendingRow[];
    return { ok: true, value: { messages: rows.slice(0, 1).map(row => JSON.parse(row.message_json)), more: rows.length > 1 } };
  }

  async acknowledgePi(session: SessionAddress, runtimeId: string, proof: PiReceipt): Promise<Outcome<null>> {
    if (!isSessionAddress(session) || !runtimeId || !proof || proof.piSessionId !== session.piSessionId ||
      typeof proof.messageId !== 'string' || !proof.messageId.startsWith(`${session.namespace}:msg:`) ||
      typeof proof.entryId !== 'string' || !proof.entryId ||
      typeof proof.sessionFile !== 'string' || !proof.sessionFile) return failure('invalid_request', 'Invalid Pi receipt');
    let changed = false;
    const result: Outcome<null> = this.tx(() => {
      if (!this.owns(session, runtimeId)) return failure('owner_conflict', 'Session lease is not owned');
      const row = this.db.prepare(`SELECT message_json, receipt_entry_id, receipt_session_file FROM inbox_messages
        WHERE session_key = ? AND message_id = ?`).get(sessionKey(session), proof.messageId) as
        (PendingRow & { receipt_entry_id: string | null; receipt_session_file: string | null }) | undefined;
      if (!row) return failure('not_found', 'Message not in session inbox');
      if (row.receipt_entry_id !== null) {
        return row.receipt_entry_id === proof.entryId && row.receipt_session_file === proof.sessionFile
          ? { ok: true, value: null } : failure('invalid_request', 'Conflicting Pi receipt');
      }
      this.db.prepare(`UPDATE inbox_messages SET receipt_entry_id = ?, receipt_session_file = ?, acknowledged_at = ?
        WHERE session_key = ? AND message_id = ?`).run(proof.entryId, proof.sessionFile, Date.now(), sessionKey(session), proof.messageId);
      changed = true;
      return { ok: true, value: null };
    });
    if (changed) this.changed(sessionKey(session));
    return result;
  }

  async enqueueRegistration(registration: Registration): Promise<void> {
    if (!isRegistration(registration)) throw new TypeError('Invalid registration');
    const result = this.db.prepare(`INSERT INTO inbox_registrations (session_key, registration_json) VALUES (?, ?)
      ON CONFLICT(session_key) DO UPDATE SET registration_json = excluded.registration_json
      WHERE registration_json <> excluded.registration_json`)
      .run(sessionKey(registration.session), JSON.stringify(registration));
    if (result.changes) this.notify(this.registrationChanges, undefined);
  }

  registrations(): RegistrationRow[] {
    this.sweep();
    return this.db.prepare('SELECT session_key, registration_json FROM inbox_registrations ORDER BY session_key').all() as RegistrationRow[];
  }

  completeRegistration(row: RegistrationRow): void {
    // A concurrent re-enqueue is not consumed by a response to the earlier request.
    this.db.prepare('DELETE FROM inbox_registrations WHERE session_key = ? AND registration_json = ?')
      .run(row.session_key, row.registration_json);
  }

  async close(): Promise<void> {
    if (!this.closed) { this.closed = true; this.changes.clear(); this.registrationChanges.clear(); this.faults.clear(); this.db.close(); this.lock.close(); }
  }
}

function failure(code: 'invalid_request' | 'owner_conflict' | 'not_found', message: string): Outcome<never> {
  return { ok: false, error: code, message };
}

// Do not restore the old createHostInbox export. A cached legacy Pi adapter
// imports that name lazily; it must not acquire the new daemon-only writer.
export function createDaemonInbox(path: string): HostInbox {
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
async function* frames(stream: ReadableStream<Uint8Array>, signal: AbortSignal, onBytes: () => void,
  report: (error: Error) => void): AsyncGenerator<StreamFrame> {
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
      try { value = JSON.parse(payload); }
      catch { report(new Error('Skipping invalid SSE JSON without a receipt')); return undefined; }
      if (!isStreamFrame(value)) {
        report(new Error('Skipping unsupported or invalid SSE frame without a receipt'));
        return undefined;
      }
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
  let ipc: Awaited<ReturnType<typeof listenLocal>> | undefined;
  let opening: Promise<Awaited<ReturnType<typeof listenLocal>>> | undefined;
  let sweepTimer: NodeJS.Timeout | undefined;
  let unsubscribeRegistration: (() => void) | undefined;
  let registrationDirty = true;
  let registrationWake: (() => void) | undefined;
  const kickRegistrations = () => { registrationDirty = true; registrationWake?.(); };
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
      if (!registrationDirty) {
        await new Promise<void>(resolve => {
          const done = () => { registrationWake = undefined; signal.removeEventListener('abort', done); resolve(); };
          registrationWake = done;
          signal.addEventListener('abort', done, { once: true });
          if (registrationDirty || signal.aborted) done();
        });
      }
      if (signal.aborted) break;
      registrationDirty = false;
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
      } catch (error) {
        if (!signal.aborted) report(error);
        await delay(backoff, signal);
        backoff = Math.min(backoff * 2, RETRY_MAX_MS);
        // Retry known failed work, not an empty-queue discovery scan.
        registrationDirty = true;
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
        kickRegistrations();
        backoff = RETRY_MIN_MS;
        for await (const frame of frames(response.body, streamSignal, () => armDeadline(STREAM_IDLE_MS, 'Host stream idle timed out'), report)) {
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
      const current = controller;
      external = signal;
      signal.addEventListener('abort', onAbort, { once: true });
      if (signal.aborted) controller.abort();
      unsubscribeRegistration = inbox.onRegistration(kickRegistrations);
      opening = listenLocal(config, inbox);
      ipc = await opening;
      if (current.signal.aborted) { await ipc.close(); return; }
      sweepTimer = setInterval(() => inbox.sweep(), SWEEP_MS);
      sweepTimer.unref();
      registrationDirty = true;
      loops = [registrationLoop(current.signal), streamLoop(current.signal)];
      await Promise.all(loops);
    },
    async stop() {
      controller?.abort();
      clearInterval(sweepTimer);
      await opening?.catch(() => {});
      await Promise.all(loops);
      await ipc?.close();
      unsubscribeRegistration?.();
      external?.removeEventListener('abort', onAbort);
      controller = undefined;
      loops = [];
    },
  };
}

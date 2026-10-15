import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { listenLocal } from './ipc.ts';
import {
  isDeliveryEvidence, isFailureCode, isMessage, isOutcome, isRegistration, isRegistrationResult, isSessionAddress, ownRegistration, ownRegistrationResult, isStreamFrame, isSubscription, outcomeValue, sessionKey, ROUTES, MAX_STREAM_EVENT_BYTES,
  type FailureCode, type HostConfig, type Message, type Outcome, type DeliveryEvidence, type RegistrationResult,
  type Registration, type SessionAddress, type StreamFrame,
} from '../protocol/index.ts';

const LEASE_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;
const STREAM_IDLE_MS = 45_000;
const RETRY_MIN_MS = 250;
const RETRY_MAX_MS = 10_000;
const HISTORY_MS = 7 * 86400_000;
const SWEEP_MS = 60_000;
/** Current inbox schema. 2 fenced legacy writers; 3 made delivery evidence and queued addresses agent-neutral. */
const SCHEMA_VERSION = 3;
/** Session-address field name that schema 2 and older stored in queued registration JSON. Migration input only. */
const LEGACY_SESSION_FIELD = 'piSessionId';

type RegistrationRow = { session_key: string; registration_json: string; generation: number };
type PendingRow = { message_json: string; hash: string | null };
/** Insert of a frame the SSE parser already validated. Not exported, so only
 * the stream path in this module can skip the check that receive() makes. */
const ingestParsed = Symbol('ingestParsed');
/** The packet identity: SHA-256 over the exact stored message JSON text. */
function packetHash(json: string): string {
  return createHash('sha256').update(json).digest('hex');
}

export class LocalHostInbox {
  private readonly db: DatabaseSync;
  private readonly lock: DatabaseSync;
  private closed = false;
  private nextSweepAt = 0;
  private readonly changes = new Set<(key: string) => void>();
  private readonly registrationChanges = new Set<() => void>();
  private readonly registrationResults = new Set<(key: string, registrationJson: string, generation: number, result: Outcome<RegistrationResult>) => void>();
  private readonly registrationAttempts = new Set<(key: string, registrationJson: string, generation: number) => void>();
  private readonly registrationGenerations = new Map<string, number>();
  private readonly registrationAttempting = new Map<string, number>();
  private readonly registrationQueued = new Set<(key: string, registrationJson: string, generation: number) => void>();
  private readonly faults = new Set<() => void>();
  /** Connection grants. A daemon restart drops every grant, so they live only
   * in memory. The inbox_leases table remains for the legacy fence views. */
  private readonly leases = new Map<string, { runtimeId: string; expiresAt: number }>();
  /** Latest successful registration result per session in this daemon process. */
  private readonly registered = new Map<string, RegistrationResult>();

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
      let version = this.db.prepare('PRAGMA user_version').get()!.user_version;
      if (version !== 0 && version !== 2 && version !== SCHEMA_VERSION) throw new Error(`Unsupported inbox schema version: ${version}`);
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
        version = 2;
      }
      if (version === 2) migrateNeutralEvidence(this.db);
      const inboxColumns = this.db.prepare('PRAGMA table_info(inbox_messages)').all() as Array<{ name: string }>;
      if (!inboxColumns.some(column => column.name === 'hash')) {
        this.db.exec('ALTER TABLE inbox_messages ADD COLUMN hash TEXT');
        // Rows already stored were verified when their frames were ingested;
        // hash them once here so later reads are equality checks.
        const backfill = this.db.prepare('UPDATE inbox_messages SET hash = ? WHERE message_id = ? AND session_key = ?');
        for (const row of this.db.prepare('SELECT session_key, message_id, message_json FROM inbox_messages').all() as
          Array<{ session_key: string; message_id: string; message_json: string }>)
          backfill.run(packetHash(row.message_json), row.message_id, row.session_key);
      }
      // Older daemons stored grants here. This daemon keeps them in memory
      // and never writes this table; clear rows an older daemon left behind.
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
  onRegistrationResult(listener: (key: string, registrationJson: string, generation: number, result: Outcome<RegistrationResult>) => void): () => void {
    this.registrationResults.add(listener);
    return () => this.registrationResults.delete(listener);
  }
  onRegistrationAttempt(listener: (key: string, registrationJson: string, generation: number) => void): () => void {
    this.registrationAttempts.add(listener);
    return () => this.registrationAttempts.delete(listener);
  }
  onRegistrationQueued(listener: (key: string, registrationJson: string, generation: number) => void): () => void {
    this.registrationQueued.add(listener);
    return () => this.registrationQueued.delete(listener);
  }
  /** Generation of the attempt currently in flight for a session, if any. */
  registrationAttemptGeneration(key: string): number | undefined {
    return this.registrationAttempting.get(key);
  }
  isCurrentRegistration(row: RegistrationRow): boolean {
    return (this.registrationGenerations.get(row.session_key) ?? 0) === row.generation;
  }
  reportRegistrationAttempt(key: string, registrationJson: string, generation: number): void {
    this.registrationAttempting.set(key, generation);
    let fault: unknown;
    for (const listener of this.registrationAttempts) {
      try { listener(key, registrationJson, generation); } catch (problem) { fault = problem; }
    }
    if (fault !== undefined) {
      for (const listener of this.faults) { try { listener(); } catch {} }
      throw fault;
    }
  }
  /** The last completed registration result per session, for waiters that subscribed mid-flight. */
  private readonly registrationCompletions = new Map<string, { generation: number; result: Outcome<RegistrationResult> }>();
  registrationCompletion(key: string, generation: number): Outcome<RegistrationResult> | undefined {
    const done = this.registrationCompletions.get(key);
    return done && done.generation === generation ? done.result : undefined;
  }
  /** The result that answers an awaitRegistration without a coordinator
   * POST: the session registered successfully during this daemon's lifetime,
   * and no registration for the session is queued or in flight. A pending row
   * would land at the coordinator after this answer, so the caller waits for it. */
  registeredResult(key: string): RegistrationResult | undefined {
    const result = this.registered.get(key);
    if (!result) return undefined;
    if (this.registrationAttempting.has(key)) return undefined;
    if (this.db.prepare('SELECT 1 FROM inbox_registrations WHERE session_key = ?').get(key)) return undefined;
    return result;
  }
  reportRegistration(key: string, registrationJson: string, generation: number, result: Outcome<RegistrationResult>): void {
    this.registrationCompletions.set(key, { generation, result });
    // An outage keeps the last result; a coordinator refusal withdraws it.
    if (result.ok) this.registered.set(key, result.value);
    else if (result.error !== 'temporarily_unavailable') this.registered.delete(key);
    if (this.registrationAttempting.get(key) === generation) this.registrationAttempting.delete(key);
    let fault: unknown;
    for (const listener of this.registrationResults) {
      try { listener(key, registrationJson, generation, result); } catch (problem) { fault = problem; }
    }
    if (fault !== undefined) {
      for (const listener of this.faults) { try { listener(); } catch {} }
      throw fault;
    }
  }
  private emitQueued(key: string, registrationJson: string, generation: number): void {
    let fault: unknown;
    for (const listener of this.registrationQueued) {
      try { listener(key, registrationJson, generation); } catch (problem) { fault = problem; }
    }
    if (fault !== undefined) {
      for (const listener of this.faults) { try { listener(); } catch {} }
      throw fault;
    }
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
      this.db.prepare('DELETE FROM inbox_messages WHERE acknowledged_at < ? AND evidence_record_id IS NOT NULL').run(now - HISTORY_MS);
    });
    for (const [key, lease] of this.leases) if (lease.expiresAt <= now) this.leases.delete(key);
    this.nextSweepAt = now + SWEEP_MS;
  }

  /** Runs work at synchronous=NORMAL, then restores the connection's FULL default. */
  private normalSync<T>(work: () => T): T {
    this.db.exec('PRAGMA synchronous = NORMAL');
    try { return work(); } finally { this.db.exec('PRAGMA synchronous = FULL'); }
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
    return this[ingestParsed](frame);
  }

  /** Commits at synchronous=FULL before it returns, so the coordinator
   * receipt that follows never covers a message a power loss could drop. */
  [ingestParsed](frame: StreamFrame): { inserted: boolean } {
    const messageJson = JSON.stringify(frame.message);
    const result = this.db.prepare(`INSERT OR IGNORE INTO inbox_messages
      (session_key, message_id, sequence, message_json, hash) VALUES (?, ?, ?, ?, ?)`).run(
      frame.subscriptionId, frame.message.id, frame.message.sequence, messageJson, packetHash(messageJson));
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
    const key = sessionKey(session);
    const lease = this.leases.get(key);
    if (renew && (!lease || lease.runtimeId !== runtimeId || lease.expiresAt <= now)) return failure('owner_conflict', 'Lease no longer owned');
    if (!renew && lease && lease.expiresAt > now && lease.runtimeId !== runtimeId) return failure('owner_conflict', 'Session already active');
    // A lease is always bounded by the current clock, even on a repeated bind.
    const expiresAt = now + LEASE_MS;
    this.leases.set(key, { runtimeId, expiresAt });
    return { ok: true, value: { leaseExpiresAt: expiresAt } };
  }

  async release(session: SessionAddress, runtimeId: string): Promise<void> {
    if (!isSessionAddress(session) || !runtimeId) return;
    const key = sessionKey(session);
    if (this.leases.get(key)?.runtimeId === runtimeId) this.leases.delete(key);
  }

  /** The runtime holding the live lease for a session, if any. */
  leaseHolder(session: SessionAddress): string | undefined {
    const lease = this.leases.get(sessionKey(session));
    return lease && lease.expiresAt > Date.now() ? lease.runtimeId : undefined;
  }

  private owns(session: SessionAddress, runtimeId: string): boolean {
    return this.leaseHolder(session) === runtimeId;
  }

  async pending(session: SessionAddress, runtimeId: string): Promise<Message[]> {
    if (!isSessionAddress(session) || !runtimeId || !this.owns(session, runtimeId)) return [];
    const rows = this.db.prepare(`SELECT message_json, hash FROM inbox_messages WHERE session_key = ? AND evidence_record_id IS NULL
      ORDER BY sequence ASC`).all(sessionKey(session)) as PendingRow[];
    return rows.map(row => storedMessage(row.message_json, row.hash));
  }

  pendingPage(session: SessionAddress, owner: string, afterSequence = 0): Outcome<{ messages: Message[]; more: boolean }> {
    if (!this.owns(session, owner)) return failure('owner_conflict', 'Session lease is not owned');
    const rows = this.db.prepare(`SELECT message_json, hash FROM inbox_messages
      WHERE session_key = ? AND evidence_record_id IS NULL AND sequence > ? ORDER BY sequence LIMIT 33`)
      .all(sessionKey(session), afterSequence) as PendingRow[];
    // A page is bounded by count and by bytes, so a few large messages cost
    // one exchange each and many small messages share one. At least the
    // first message is always included, whatever its size.
    const messages: Message[] = [];
    let bytes = 0;
    for (const row of rows) {
      bytes += Buffer.byteLength(row.message_json);
      if (messages.length && (messages.length >= 32 || bytes > 256 * 1024)) break;
      messages.push(storedMessage(row.message_json, row.hash));
    }
    return { ok: true, value: { messages, more: rows.length > messages.length } };
  }

  async acknowledgeDelivery(session: SessionAddress, runtimeId: string, evidence: DeliveryEvidence): Promise<Outcome<null>> {
    if (!isSessionAddress(session) || !runtimeId || !isDeliveryEvidence(evidence) || evidence.sessionId !== session.sessionId ||
      !evidence.messageId.startsWith(`${session.namespace}:msg:`)) return failure('invalid_request', 'Invalid delivery evidence');
    let changed = false;
    // An acknowledgement commits at synchronous=NORMAL. Power loss can drop
    // only the latest NORMAL commits, and a dropped acknowledgement leaves the
    // message pending: it is redelivered, and the adapter acknowledges it
    // again from the evidence in its own session store. The next FULL commit
    // (an inbox insert from receive) syncs these WAL frames too.
    const result: Outcome<null> = this.normalSync(() => this.tx(() => {
      if (!this.owns(session, runtimeId)) return failure('owner_conflict', 'Session lease is not owned');
      const row = this.db.prepare(`SELECT message_json, evidence_record_id, evidence_location FROM inbox_messages
        WHERE session_key = ? AND message_id = ?`).get(sessionKey(session), evidence.messageId) as
        (PendingRow & { evidence_record_id: string | null; evidence_location: string | null }) | undefined;
      if (!row) return failure('not_found', 'Message not in session inbox');
      if (row.evidence_record_id !== null) {
        return row.evidence_record_id === evidence.recordId && row.evidence_location === evidence.location
          ? { ok: true, value: null } : failure('invalid_request', 'Conflicting delivery evidence');
      }
      this.db.prepare(`UPDATE inbox_messages SET evidence_record_id = ?, evidence_location = ?, acknowledged_at = ?
        WHERE session_key = ? AND message_id = ?`).run(evidence.recordId, evidence.location, Date.now(), sessionKey(session), evidence.messageId);
      changed = true;
      return { ok: true, value: null };
    }));
    if (changed) this.changed(sessionKey(session));
    return result;
  }

  /** Enqueue and return the effective queued bytes with their generation.
   * The generation bumps only when the effective bytes change, so a daemon
   * snapshot, its attempt, and its result all carry the same generation. */
  async enqueueRegistration(registration: Registration): Promise<{ registrationJson: string; generation: number }> {
    if (!isRegistration(registration)) throw new TypeError('Invalid registration');
    const key = sessionKey(registration.session);
    const existing = this.db.prepare('SELECT registration_json FROM inbox_registrations WHERE session_key = ?')
      .get(key) as { registration_json: string } | undefined;
    const generation = () => this.registrationGenerations.get(key) ?? 0;
    // A version 1 peer's displayName is dropped here, so the queue holds one
    // canonical row per session and an older daemon's named row is replaced.
    const registrationJson = JSON.stringify(ownRegistration(registration));
    const result = this.db.prepare(`INSERT INTO inbox_registrations (session_key, registration_json) VALUES (?, ?)
      ON CONFLICT(session_key) DO UPDATE SET registration_json = excluded.registration_json
      WHERE registration_json <> excluded.registration_json`)
      .run(key, registrationJson);
    if (result.changes) {
      const next = generation() + 1;
      this.registrationGenerations.set(key, next);
      this.emitQueued(key, registrationJson, next);
      this.notify(this.registrationChanges, undefined);
      return { registrationJson, generation: next };
    }
    return { registrationJson: existing?.registration_json ?? registrationJson, generation: generation() };
  }

  registrations(): RegistrationRow[] {
    this.sweep();
    const rows = this.db.prepare('SELECT session_key, registration_json FROM inbox_registrations ORDER BY session_key')
      .all() as Array<{ session_key: string; registration_json: string }>;
    return rows.map(row => ({ ...row, generation: this.registrationGenerations.get(row.session_key) ?? 0 }));
  }

  completeRegistration(row: RegistrationRow): void {
    // A concurrent re-enqueue is not consumed by a response to the earlier
    // request. The generation guard also covers X->Y->X, where the newer
    // row has bytes identical to the attempted snapshot.
    if (!this.isCurrentRegistration(row)) return;
    this.db.prepare('DELETE FROM inbox_registrations WHERE session_key = ? AND registration_json = ?')
      .run(row.session_key, row.registration_json);
  }

  async close(): Promise<void> {
    if (!this.closed) { this.closed = true; this.changes.clear(); this.registrationChanges.clear(); this.registrationResults.clear(); this.registrationAttempts.clear(); this.registrationQueued.clear(); this.registrationGenerations.clear(); this.registrationAttempting.clear(); this.faults.clear(); this.leases.clear(); this.registered.clear(); this.db.close(); this.lock.close(); }
  }
}

function failure(code: 'invalid_request' | 'owner_conflict' | 'not_found', message: string): Outcome<never> {
  return { ok: false, error: code, message };
}
function storedMessage(json: string, hash?: string | null): Message {
  // A hashed row was fully verified when its frame was ingested. The
  // equality check is the whole verification here: bytes that are not the
  // verified bytes are rejected, not re-validated.
  if (hash) {
    if (packetHash(json) !== hash) throw new Error('Message hash mismatch');
    return JSON.parse(json) as Message;
  }
  const value: unknown = JSON.parse(json);
  if (!isMessage(value)) throw new TypeError('Invalid stored message');
  return value;
}
/** A row queued by an older daemon can carry displayName; it is not posted. */
function storedRegistration(json: string): Registration {
  const value: unknown = JSON.parse(json);
  if (!isRegistration(value)) throw new TypeError('Invalid stored registration');
  return ownRegistration(value);
}

/**
 * Schema 2 -> 3, inside the constructor transaction. Evidence columns take
 * agent-neutral names, and queued registrations take the current session
 * address field, so the daemon can still post them. Keys, messages, cursors
 * and evidence values are unchanged. The legacy `messages` view keeps its
 * old column names, so it stays an empty, column-compatible fence. An
 * unparseable registration row stays as it was; the next enqueue for its
 * session replaces it, as before.
 */
function migrateNeutralEvidence(db: DatabaseSync): void {
  db.exec(`
    DROP VIEW messages;
    ALTER TABLE inbox_messages RENAME COLUMN receipt_entry_id TO evidence_record_id;
    ALTER TABLE inbox_messages RENAME COLUMN receipt_session_file TO evidence_location;
    CREATE VIEW messages AS SELECT session_key, message_id, sequence, message_json,
      evidence_record_id AS receipt_entry_id, evidence_location AS receipt_session_file, acknowledged_at
      FROM inbox_messages WHERE 0;
  `);
  const update = db.prepare('UPDATE inbox_registrations SET registration_json = ? WHERE session_key = ?');
  for (const row of db.prepare('SELECT session_key, registration_json FROM inbox_registrations').all() as Array<{ session_key: string; registration_json: string }>) {
    let value: unknown;
    try { value = JSON.parse(row.registration_json); } catch { continue; }
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const session = (value as { session?: unknown }).session;
    if (!session || typeof session !== 'object' || Array.isArray(session)) continue;
    const { namespace, [LEGACY_SESSION_FIELD]: sessionId, ...rest } = session as Record<string, unknown>;
    if (sessionId === undefined || Object.keys(rest).length) continue;
    // Spread keeps key order, so the bytes match what a current client enqueues.
    const next = { ...(value as Record<string, unknown>), session: { namespace, sessionId } };
    if (isRegistration(next)) update.run(JSON.stringify(next), row.session_key);
  }
  db.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
}

// Do not restore the old createHostInbox export. A cached pre-daemon adapter
// imports that name lazily; it must not acquire the new daemon-only writer.
export function createDaemonInbox(path: string): LocalHostInbox {
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

type CoordinatorError = Error & { code?: FailureCode };

async function jsonPost(base: URL, route: string, credential: string, body: unknown, signal: AbortSignal): Promise<unknown> {
  const response = await fetch(new URL(route, base), {
    method: 'POST', headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
  });
  if (!response.ok) {
    // Preserve the coordinator's real failure code instead of flattening it.
    const failure: unknown = await response.json().catch(() => undefined);
    const error: CoordinatorError = new Error(`${route}: HTTP ${response.status}`);
    if (isOutcome(failure) && !failure.ok) {
      error.code = failure.error;
      error.message = failure.message;
    }
    throw error;
  }
  return await response.json() as unknown;
}
function registrationFailure(error: unknown): Outcome<RegistrationResult> {
  // Only a decoded coordinator code is trusted; transport errors carry
  // unrelated codes (ECONNREFUSED, timeouts) that are not FailureCodes.
  const code = (error as CoordinatorError | undefined)?.code;
  return { ok: false, error: isFailureCode(code) ? code : 'temporarily_unavailable', message: error instanceof Error ? error.message : String(error) };
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

export function createHostDaemon(config: HostConfig, inbox: LocalHostInbox): { start(signal: AbortSignal): Promise<void>; stop(): Promise<void> } {
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
      let failed = false;
      let firstError: unknown;
      try {
        for (const row of inbox.registrations()) {
          if (signal.aborted) break;
          // A row replaced after the snapshot is never posted; the newer
          // generation is picked up by the next pass its enqueue triggered.
          if (!inbox.isCurrentRegistration(row)) continue;
          try {
            const registration = storedRegistration(row.registration_json);
            inbox.reportRegistrationAttempt(row.session_key, row.registration_json, row.generation);
            const value = outcomeValue(await jsonPost(base, ROUTES.register, config.credential, registration, signal), isRegistrationResult);
            if (!value || value.subscription.id !== row.session_key || value.subscription.hostId !== config.hostId)
              throw new Error('Invalid registration response');
            inbox.completeRegistration(row);
            // A version 1 coordinator can still return displayName and session entities.
            inbox.reportRegistration(row.session_key, row.registration_json, row.generation, { ok: true, value: ownRegistrationResult(value) });
          } catch (error) {
            // One bad row must not starve the rest of the pass; it stays queued.
            failed = true;
            firstError ??= error;
            inbox.reportRegistration(row.session_key, row.registration_json, row.generation, registrationFailure(error));
          }
        }
      } catch (error) {
        failed = true;
        firstError ??= error;
      }
      if (failed) {
        if (!signal.aborted) report(firstError);
        await delay(backoff, signal);
        backoff = Math.min(backoff * 2, RETRY_MAX_MS);
        // Retry known failed work, not an empty-queue discovery scan.
        registrationDirty = true;
      } else backoff = RETRY_MIN_MS;
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
        // Receipts are posted on a per-subscription chain: the stream keeps
        // reading while posts are in flight, order per subscription is
        // preserved for the coordinator's contiguous-cursor check, and a
        // failed post leaves the cursor unmoved so replay reposts it.
        const receiptChains = new Map<string, Promise<void>>();
        for await (const frame of frames(response.body, streamSignal, () => armDeadline(STREAM_IDLE_MS, 'Host stream idle timed out'), report)) {
          // frames() validated this frame; the insert commits before the receipt is sent.
          inbox[ingestParsed](frame);
          const receipt = { subscriptionId: frame.subscriptionId, messageId: frame.message.id };
          const prior = receiptChains.get(frame.subscriptionId) ?? Promise.resolve();
          receiptChains.set(frame.subscriptionId, prior.then(async () => {
            const ack = outcomeValue(await jsonPost(base, ROUTES.receipt, config.credential, receipt, signal), isSubscription);
            if (!ack || ack.id !== receipt.subscriptionId || ack.hostId !== config.hostId) throw new Error('Invalid receipt response');
          }).catch(error => { if (!signal.aborted) report(error); }));
        }
        await Promise.all(receiptChains.values());
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

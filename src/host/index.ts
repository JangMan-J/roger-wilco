import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { listenLocal } from './ipc.ts';
import {
  isDeliveryEvidence, isFailureCode, isHostFrame, isHostReceiptResult, isMessage, isOutcome, isPresenceResult, isRegistration, isRegistrationResult,
  isSessionAddress, ownRegistration, ownRegistrationResult, isStreamFrame, isSubscription, outcomeValue, presenceIntervalMs, sessionKey, streamIdleMs,
  ROUTES, MAX_PRESENCE_KEYS, MAX_STREAM_EVENT_BYTES,
  type FailureCode, type HostConfig, type HostFrame, type Message, type Outcome, type DeliveryEvidence, type RegistrationResult,
  type Registration, type SessionAddress, type SessionKey, type StreamFrame,
} from '../protocol/index.ts';

const LEASE_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;
/** v1 stream idle deadline. A v2 stream derives its own from `hello.keepaliveMs`. */
const STREAM_IDLE_MS = 45_000;
const RETRY_MIN_MS = 250;
const RETRY_MAX_MS = 10_000;
const HISTORY_MS = 7 * 86400_000;
const SWEEP_MS = 60_000;
/** Current inbox schema. 2 fenced legacy writers; 3 made delivery evidence and queued addresses agent-neutral;
 * 4 added the host stream v2 bookmark (`inbox_stream`). */
const SCHEMA_VERSION = 4;
/** A v2 ingest transaction holds at most this many frames, or this many bytes of frame JSON. */
const BATCH_FRAMES = 256;
const BATCH_BYTES = 8 * 1024 * 1024;
/** Session-address field name that schema 2 and older stored in queued registration JSON. Migration input only. */
const LEGACY_SESSION_FIELD = 'piSessionId';

type RegistrationRow = { session_key: string; registration_json: string };
type PendingRow = { message_json: string; hash: string | null };
/** Insert of a frame the SSE parser already validated. Not exported, so only
 * the stream path in this module can skip the check that receive() makes. */
const ingestParsed = Symbol('ingestParsed');
/** v2 hello and batch commits. Not exported: only the v2 stream path in this module calls them. */
const commitHello = Symbol('commitHello');
const ingestHostFrames = Symbol('ingestHostFrames');
type HelloFrame = Extract<HostFrame, { type: 'hello' }>;
/** Membership and position of one v2 connection. Membership is never stored (I6). */
type StreamState = { members: Map<SessionKey, number>; last: number; namespace?: string };
/** A frame or stream that breaks a v2 rule. The batch rolls back and the connection aborts. */
class StreamViolation extends Error {}
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
  private readonly registrationResults = new Set<(key: string, result: Outcome<RegistrationResult>) => void>();
  private readonly registrationAttempts = new Set<(key: string) => void>();
  /** Sessions whose registration POST is in flight. */
  private readonly registrationAttempting = new Set<string>();
  private readonly faults = new Set<() => void>();
  /** Connection grants. A daemon restart drops every grant, so they live only
   * in memory. The inbox_leases table remains for the legacy fence views. */
  private readonly leases = new Map<string, { runtimeId: string; expiresAt: number }>();
  /** Latest successful registration result per session in this daemon process. */
  private readonly registered = new Map<string, RegistrationResult>();
  /** The last failed attempt per session. A failed row stays queued until an attempt succeeds. */
  private readonly registrationFailures = new Map<string, Outcome<RegistrationResult>>();
  /** Set by the daemon when it arms presence after the first v2 hello of this
   * process; never reset. While it holds, presence keeps registrations fresh,
   * so a heartbeat that `registeredResult` answers completes locally. */
  presenceActive = false;
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
      if (version !== 0 && version !== 2 && version !== 3 && version !== SCHEMA_VERSION) throw new Error(`Unsupported inbox schema version: ${version}`);
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
      if (version === 2) { migrateNeutralEvidence(this.db); version = 3; }
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
      if (version === 3) {
        // Schema 3 -> 4. The bookmark is NULL until the first v2 hello commits.
        this.db.exec(`
          CREATE TABLE inbox_stream (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            stream_id TEXT,
            bookmark INTEGER,
            CHECK ((stream_id IS NULL) = (bookmark IS NULL))
          );
          INSERT INTO inbox_stream (id) VALUES (1);
          PRAGMA user_version = 4;
        `);
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
  onRegistrationResult(listener: (key: string, result: Outcome<RegistrationResult>) => void): () => void {
    this.registrationResults.add(listener);
    return () => this.registrationResults.delete(listener);
  }
  onRegistrationAttempt(listener: (key: string) => void): () => void {
    this.registrationAttempts.add(listener);
    return () => this.registrationAttempts.delete(listener);
  }
  isRegistrationAttempting(key: string): boolean {
    return this.registrationAttempting.has(key);
  }
  reportRegistrationAttempt(key: string): void {
    this.registrationAttempting.add(key);
    this.emit(this.registrationAttempts, listener => listener(key));
  }
  /** The result that answers a registration without a coordinator POST: the
   * session registered successfully during this daemon's lifetime, and no
   * registration for the session is queued or in flight. A pending row would
   * land at the coordinator after this answer, so the caller waits for it. */
  registeredResult(key: string): RegistrationResult | undefined {
    const result = this.registered.get(key);
    if (!result) return undefined;
    if (this.registrationAttempting.has(key)) return undefined;
    if (this.queuedRegistration(key) !== undefined) return undefined;
    return result;
  }
  /** The failure of the last attempt for the queued row, while that row waits for its retry. */
  queuedFailure(key: string): Outcome<RegistrationResult> | undefined {
    if (this.registrationAttempting.has(key) || this.queuedRegistration(key) === undefined) return undefined;
    return this.registrationFailures.get(key);
  }
  /** Drop the lifetime registration, so the next heartbeat for the session posts.
   * Called on a v2 `leave` and on presence `unknown`. */
  forgetRegistration(key: string): void {
    this.registered.delete(key);
  }
  reportRegistration(key: string, result: Outcome<RegistrationResult>): void {
    // An outage keeps the last result; a coordinator refusal withdraws it.
    if (result.ok) this.registered.set(key, result.value);
    else if (result.error !== 'temporarily_unavailable') this.registered.delete(key);
    if (result.ok) this.registrationFailures.delete(key);
    else this.registrationFailures.set(key, result);
    this.registrationAttempting.delete(key);
    this.emit(this.registrationResults, listener => listener(key, result));
  }
  /** Calls every listener; if any throws, notifies fault listeners and rethrows. */
  private emit<L>(listeners: Set<L>, call: (listener: L) => void): void {
    let fault: unknown;
    for (const listener of listeners) {
      try { call(listener); } catch (problem) { fault = problem; }
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
  private changed(key: string): void {
    this.emit(this.changes, listener => listener(key));
  }

  sweep(): void {
    const now = Date.now();
    if (now < this.nextSweepAt) return;
    this.tx(() => {
      // With a bookmark, an acknowledged row goes only once D has passed it, so a
      // v2 replay (which never inserts at or below D) cannot bring it back.
      this.db.prepare(`DELETE FROM inbox_messages WHERE acknowledged_at < ? AND evidence_record_id IS NOT NULL
        AND sequence <= COALESCE((SELECT bookmark FROM inbox_stream WHERE id = 1), sequence)`).run(now - HISTORY_MS);
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

  /** Queue a registration. Every request for a session queues the same bytes,
   * so an identical row is neither rewritten nor announced again. */
  async enqueueRegistration(registration: Registration): Promise<void> {
    if (!isRegistration(registration)) throw new TypeError('Invalid registration');
    const key = sessionKey(registration.session);
    // A version 1 peer's displayName is dropped here, so the queue holds one
    // canonical row per session and an older daemon's named row is replaced.
    const registrationJson = JSON.stringify(ownRegistration(registration));
    const result = this.db.prepare(`INSERT INTO inbox_registrations (session_key, registration_json) VALUES (?, ?)
      ON CONFLICT(session_key) DO UPDATE SET registration_json = excluded.registration_json
      WHERE registration_json <> excluded.registration_json`)
      .run(key, registrationJson);
    if (result.changes) this.emit(this.registrationChanges, listener => listener());
  }

  /** The queued registration bytes for a session, if any. */
  queuedRegistration(key: string): string | undefined {
    const row = this.db.prepare('SELECT registration_json FROM inbox_registrations WHERE session_key = ?')
      .get(key) as { registration_json: string } | undefined;
    return row?.registration_json;
  }

  registrations(): RegistrationRow[] {
    this.sweep();
    return this.db.prepare('SELECT session_key, registration_json FROM inbox_registrations ORDER BY session_key')
      .all() as RegistrationRow[];
  }

  /** Remove the posted row only if the queue still holds the posted bytes. */
  completeRegistration(row: RegistrationRow): void {
    this.db.prepare('DELETE FROM inbox_registrations WHERE session_key = ? AND registration_json = ?')
      .run(row.session_key, row.registration_json);
  }

  /** Session keys that hold a live lease, in key order. */
  leasedSessions(now = Date.now()): SessionKey[] {
    return [...this.leases].filter(([, lease]) => lease.expiresAt > now).map(([key]) => key).sort();
  }

  /** The v2 bookmark: the coordinator log it belongs to and D. Both NULL until the first hello commits. */
  streamPosition(): { stream: string | null; bookmark: number | null } {
    const row = this.db.prepare('SELECT stream_id, bookmark FROM inbox_stream WHERE id = 1').get() as
      { stream_id: string | null; bookmark: number | null };
    return { stream: row.stream_id, bookmark: row.bookmark };
  }

  /** Clear the bookmark in one transaction, for `--reset-bookmark`. `highWater`
   * is the highest position this inbox has seen, the floor a coordinator restore needs. */
  resetBookmark(): { highWater: number } {
    return this.tx(() => {
      const row = this.db.prepare(`SELECT MAX(COALESCE((SELECT bookmark FROM inbox_stream WHERE id = 1), 0),
        COALESCE((SELECT MAX(sequence) FROM inbox_messages), 0)) AS high`).get() as { high: number };
      this.db.prepare('UPDATE inbox_stream SET stream_id = NULL, bookmark = NULL WHERE id = 1').run();
      return { highWater: row.high };
    });
  }

  /** Hello commit, at FULL: D := max(D, resume). `resume > D` is accepted because the
   * coordinator states that nothing is owed up to `resume`. Returns the new D. */
  [commitHello](hello: HelloFrame): number {
    return this.tx(() => {
      const { stream, bookmark } = this.streamPosition();
      if (stream !== null && stream !== hello.stream) throw new StreamViolation('hello.stream does not match the stored bookmark');
      const next = Math.max(bookmark ?? 0, hello.resume);
      this.db.prepare('UPDATE inbox_stream SET stream_id = ?, bookmark = ? WHERE id = 1').run(hello.stream, next);
      return next;
    });
  }

  /**
   * One v2 batch in one FULL transaction, with the bookmark in the same
   * transaction (I3). Applies membership in order, skips positions at or below
   * D, copies each message to its local recipients, and advances D. Throws
   * StreamViolation and rolls back on a rule violation; `state.last` is then
   * restored. Returns whether the batch held a position frame.
   */
  [ingestHostFrames](batch: HostFrame[], state: StreamState): boolean {
    const before = state.last;
    const touched = new Set<SessionKey>();
    let positioned = false;
    const namespace = (key: string) => {
      const ns = key.slice(0, key.indexOf(':'));
      state.namespace ??= ns;
      if (ns !== state.namespace) throw new StreamViolation(`Session ${key} is outside the stream namespace`);
    };
    try {
      this.tx(() => {
        const start = this.streamPosition().bookmark ?? 0;
        let bookmark = start;
        const insert = this.db.prepare(`INSERT INTO inbox_messages (session_key, message_id, sequence, message_json, hash)
          VALUES (?, ?, ?, ?, ?) ON CONFLICT(session_key, message_id) DO NOTHING`);
        const stored = this.db.prepare('SELECT hash FROM inbox_messages WHERE session_key = ? AND message_id = ?');
        for (const frame of batch) {
          if (frame.type === 'hello') throw new StreamViolation('Repeated hello');
          if (frame.type === 'member') { namespace(frame.session); state.members.set(frame.session, frame.start); continue; }
          if (frame.type === 'leave') {
            namespace(frame.session);
            state.members.delete(frame.session);
            this.forgetRegistration(frame.session);
            continue;
          }
          if (frame.seq <= state.last) throw new StreamViolation(`Position ${frame.seq} is not above ${state.last}`);
          state.last = frame.seq;
          positioned = true;
          if (frame.seq <= bookmark) continue; // replay of the stored range: no write
          if (frame.type === 'msg') {
            const message = frame.message;
            namespace(message.sender);
            const json = JSON.stringify(message);
            const hash = packetHash(json);
            let targets: SessionKey[];
            if (message.recipient === '*') {
              targets = [...state.members].filter(([key, from]) => from < frame.seq && key !== message.sender).map(([key]) => key);
            } else {
              const from = state.members.get(message.recipient);
              if (from === undefined || from >= frame.seq) throw new StreamViolation(`Directed message ${message.id} for a non-member ${message.recipient}`);
              targets = [message.recipient];
            }
            for (const key of targets) {
              if (insert.run(key, message.id, frame.seq, json, hash).changes === 1) { touched.add(key); continue; }
              const row = stored.get(key, message.id) as { hash: string | null } | undefined;
              if (row?.hash !== hash) throw new StreamViolation(`Message identity conflict: session ${key}, message ${message.id}`);
            }
          }
          bookmark = frame.seq;
        }
        if (bookmark !== start) this.db.prepare('UPDATE inbox_stream SET bookmark = ? WHERE id = 1').run(bookmark);
      });
    } catch (error) {
      state.last = before;
      throw error;
    }
    for (const key of touched) this.changed(key);
    return positioned;
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      for (const set of [this.changes, this.registrationChanges, this.registrationResults, this.registrationAttempts, this.registrationAttempting, this.faults]) set.clear();
      this.leases.clear(); this.registered.clear(); this.registrationFailures.clear(); this.db.close(); this.lock.close();
    }
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
  db.exec('PRAGMA user_version = 3');
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

type CoordinatorError = Error & { code?: FailureCode; status?: number };

async function jsonPost(base: URL, route: string, credential: string, body: unknown, signal: AbortSignal): Promise<unknown> {
  const response = await fetch(new URL(route, base), {
    method: 'POST', headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
  });
  if (!response.ok) throw await coordinatorError(route, response);
  return await response.json() as unknown;
}
/** A non-2xx reply, with the coordinator's real failure code and message when it sent one. */
async function coordinatorError(route: string, response: Response): Promise<CoordinatorError> {
  const failure: unknown = await response.json().catch(() => undefined);
  const error: CoordinatorError = new Error(`${route}: HTTP ${response.status}`);
  error.status = response.status;
  if (isOutcome(failure) && !failure.ok) {
    error.code = failure.error;
    error.message = failure.message;
  }
  return error;
}
function registrationFailure(error: unknown): Outcome<RegistrationResult> {
  // Only a decoded coordinator code is trusted; transport errors carry
  // unrelated codes (ECONNREFUSED, timeouts) that are not FailureCodes.
  const code = (error as CoordinatorError | undefined)?.code;
  return { ok: false, error: isFailureCode(code) ? code : 'temporarily_unavailable', message: error instanceof Error ? error.message : String(error) };
}
function eventStream(response: Response): response is Response & { body: ReadableStream<Uint8Array> } {
  return response.ok && !!response.body && !!response.headers.get('content-type')?.toLowerCase().startsWith('text/event-stream');
}

type SseEvent = { event: string; data: string };
/**
 * SSE parser: byte-limited events, streaming UTF-8, CRLF, multiline data and
 * comments. Yields the complete events of each read, cut into batches of at
 * most BATCH_FRAMES events or BATCH_BYTES of data. An event counts every byte
 * of its lines, terminators included, and may not exceed MAX_STREAM_EVENT_BYTES.
 */
async function* sseBatches(stream: ReadableStream<Uint8Array>, signal: AbortSignal, onBytes: () => void): AsyncGenerator<SseEvent[]> {
  const reader = stream.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let line = '', data: string[] = [], event = '', bytes = 0;
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value.byteLength) onBytes();
      const chunk = decoder.decode(value, { stream: true });
      const batches: SseEvent[][] = [];
      let batch: SseEvent[] = [], batchBytes = 0;
      let start = 0;
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] !== '\n') continue;
        line += chunk.slice(start, i);
        start = i + 1;
        bytes += Buffer.byteLength(line) + 1;
        if (bytes > MAX_STREAM_EVENT_BYTES) throw new Error('SSE event exceeds byte limit');
        const text = line.endsWith('\r') ? line.slice(0, -1) : line;
        line = '';
        if (!text) {
          const payload = data.join('\n');
          if (payload) {
            if (batch.length >= BATCH_FRAMES || batchBytes >= BATCH_BYTES) { batches.push(batch); batch = []; batchBytes = 0; }
            batch.push({ event, data: payload });
            batchBytes += payload.length;
          }
          data = []; event = ''; bytes = 0;
          continue;
        }
        if (text.startsWith(':')) continue;
        const sep = text.indexOf(':');
        const name = sep < 0 ? text : text.slice(0, sep);
        const field = sep < 0 ? '' : text.slice(sep + 1).replace(/^ /, '');
        if (name === 'event') event = field;
        if (name === 'data') data.push(field);
      }
      line += chunk.slice(start);
      if (bytes + Buffer.byteLength(line) > MAX_STREAM_EVENT_BYTES) throw new Error('SSE event exceeds byte limit');
      if (batch.length) batches.push(batch);
      for (const item of batches) yield item;
    }
    decoder.decode(); // reject an incomplete UTF-8 suffix
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** v1 frames: events other than `message` are ignored; an invalid frame is skipped without a receipt. */
async function* frames(stream: ReadableStream<Uint8Array>, signal: AbortSignal, onBytes: () => void,
  report: (error: Error) => void): AsyncGenerator<StreamFrame> {
  for await (const batch of sseBatches(stream, signal, onBytes)) {
    for (const { event, data } of batch) {
      if (event !== 'message') continue;
      let value: unknown;
      try { value = JSON.parse(data); }
      catch { report(new Error('Skipping invalid SSE JSON without a receipt')); continue; }
      if (!isStreamFrame(value)) { report(new Error('Skipping unsupported or invalid SSE frame without a receipt')); continue; }
      yield value;
    }
  }
}

/** v2 frames: every event must be a valid `event: v2` frame; anything else aborts the connection. */
function hostFrame({ event, data }: SseEvent): HostFrame {
  if (event !== 'v2') throw new StreamViolation(`Unexpected SSE event type ${JSON.stringify(event)}`);
  let value: unknown;
  try { value = JSON.parse(data); } catch { throw new StreamViolation('Invalid v2 frame JSON'); }
  if (!isHostFrame(value)) throw new StreamViolation('Invalid v2 frame');
  return value;
}

/** A canonical session key back to its address: the namespace before the first colon. */
function keyAddress(key: SessionKey): SessionAddress {
  const colon = key.indexOf(':');
  return { namespace: key.slice(0, colon), sessionId: key.slice(colon + 1) };
}

export function createHostDaemon(config: HostConfig, inbox: LocalHostInbox): { start(signal: AbortSignal): Promise<void>; stop(): Promise<void> } {
  const base = new URL(config.coordinatorUrl.endsWith('/') ? config.coordinatorUrl : `${config.coordinatorUrl}/`);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash ||
      !config.hostId || !config.credential) throw new TypeError('Invalid host configuration');
  const headers = { authorization: `Bearer ${config.credential}`, accept: 'text/event-stream' };
  let controller: AbortController | undefined;
  let loops: Promise<void>[] = [];
  let external: AbortSignal | undefined;
  let ipc: Awaited<ReturnType<typeof listenLocal>> | undefined;
  let opening: Promise<Awaited<ReturnType<typeof listenLocal>>> | undefined;
  let sweepTimer: NodeJS.Timeout | undefined;
  let presenceTimer: NodeJS.Timeout | undefined;
  let presenceBusy = false;
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
        for (const { session_key: key } of inbox.registrations()) {
          if (signal.aborted) break;
          // Post the row as it is now: a newer enqueue may have replaced the
          // snapshot, and a completed one has removed it.
          const json = inbox.queuedRegistration(key);
          if (json === undefined) continue;
          try {
            const registration = storedRegistration(json);
            inbox.reportRegistrationAttempt(key);
            const value = outcomeValue(await jsonPost(base, ROUTES.register, config.credential, registration, signal), isRegistrationResult);
            if (!value || value.subscription.id !== key || value.subscription.hostId !== config.hostId)
              throw new Error('Invalid registration response');
            inbox.completeRegistration({ session_key: key, registration_json: json });
            // A version 1 coordinator can still return displayName and session entities.
            inbox.reportRegistration(key, { ok: true, value: ownRegistrationResult(value) });
          } catch (error) {
            // One bad row must not starve the rest of the pass; it stays queued.
            failed = true;
            firstError ??= error;
            inbox.reportRegistration(key, registrationFailure(error));
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

  /** One presence report: the sessions with a live lease. Each key the coordinator
   * did not refresh is forgotten and registered again. */
  const presenceTick = async (signal: AbortSignal) => {
    if (presenceBusy || signal.aborted) return;
    presenceBusy = true;
    try {
      let sessions = inbox.leasedSessions();
      if (sessions.length > MAX_PRESENCE_KEYS) {
        report(new Error(`Presence lists the first ${MAX_PRESENCE_KEYS} of ${sessions.length} leased sessions`));
        sessions = sessions.slice(0, MAX_PRESENCE_KEYS);
      }
      const value = outcomeValue(await jsonPost(base, ROUTES.presence, config.credential, { sessions }, signal), isPresenceResult);
      if (!value) throw new Error('Invalid presence response');
      const sent = new Set(sessions);
      for (const key of value.unknown) {
        if (!sent.has(key)) continue;
        inbox.forgetRegistration(key);
        await inbox.enqueueRegistration({ session: keyAddress(key) });
      }
    } catch (error) {
      if (!signal.aborted) report(error);
    } finally { presenceBusy = false; }
  };
  /** (Re)arm presence from a hello: one report now, then every presenceWindowMs / 3. */
  const armPresence = (presenceWindowMs: number, signal: AbortSignal) => {
    inbox.presenceActive = true;
    clearInterval(presenceTimer);
    presenceTimer = setInterval(() => void presenceTick(signal), presenceIntervalMs(presenceWindowMs));
    presenceTimer.unref();
    void presenceTick(signal);
  };

  /**
   * Receipts for one v2 connection. At most one POST is in flight, and values
   * coalesce to the latest committed position. A 400 or 409 aborts the
   * connection; a transport error or any other status retries with backoff.
   */
  const receiptPoster = (conn: string, resume: number, connection: AbortController, signal: AbortSignal) => {
    let committed = resume, receipted = resume, busy = false, closed = false, backoff = RETRY_MIN_MS;
    const pump = async (): Promise<void> => {
      if (busy || closed || committed <= receipted) return;
      busy = true;
      const through = committed;
      try {
        const value = outcomeValue(await jsonPost(base, ROUTES.receiptV2, config.credential, { conn, through }, signal), isHostReceiptResult);
        if (closed) return;
        if (!value || value.through < through) { connection.abort(new Error('Invalid receipt response')); return; }
        receipted = through;
        backoff = RETRY_MIN_MS;
      } catch (error) {
        if (closed || signal.aborted) return;
        const status = (error as CoordinatorError).status;
        if (status === 400 || status === 409) { connection.abort(error); return; }
        report(error);
        await delay(backoff, connection.signal);
        backoff = Math.min(backoff * 2, RETRY_MAX_MS);
      } finally { busy = false; }
      return pump();
    };
    return {
      offer(position: number) { if (position > committed) committed = position; void pump(); },
      close() { closed = true; },
    };
  };

  /** One v2 connection after its SSE headers: hello, then batches until EOF or a violation. */
  const runV2 = async (body: ReadableStream<Uint8Array>, connection: AbortController, streamSignal: AbortSignal,
    armDeadline: (ms: number, reason: string) => void, opened: () => void, signal: AbortSignal) => {
    armDeadline(REQUEST_TIMEOUT_MS, 'Host stream hello timed out');
    let idleMs = 0;
    let state: StreamState | undefined;
    let poster: ReturnType<typeof receiptPoster> | undefined;
    try {
      for await (const batch of sseBatches(body, streamSignal, () => { if (state) armDeadline(idleMs, 'Host stream idle timed out'); })) {
        // Parse the whole batch first, so a bad event commits nothing from it.
        const parsed = batch.map(hostFrame);
        if (!state) {
          const hello = parsed.shift()!;
          if (hello.type !== 'hello') throw new StreamViolation('The first v2 frame is not hello');
          if (hello.host !== config.hostId) throw new StreamViolation('hello.host does not match this host');
          inbox[commitHello](hello);
          state = { members: new Map(), last: hello.resume };
          idleMs = streamIdleMs(hello.keepaliveMs);
          armDeadline(idleMs, 'Host stream idle timed out');
          poster = receiptPoster(hello.conn, hello.resume, connection, signal);
          armPresence(hello.presenceWindowMs, signal);
          opened();
        }
        if (parsed.length && inbox[ingestHostFrames](parsed, state)) poster!.offer(state.last);
      }
      if (!streamSignal.aborted) throw new Error('Host stream ended');
    } finally { poster?.close(); }
  };

  /** One v1 connection: the unchanged v1 loop. */
  const runV1 = async (streamSignal: AbortSignal, armDeadline: (ms: number, reason: string) => void, opened: () => void, signal: AbortSignal) => {
    armDeadline(REQUEST_TIMEOUT_MS, 'Host stream response timed out');
    const response = await fetch(new URL(ROUTES.stream, base), { headers, signal: streamSignal });
    if (!eventStream(response)) {
      await response.body?.cancel();
      throw new Error(`Stream unavailable: HTTP ${response.status}`);
    }
    armDeadline(STREAM_IDLE_MS, 'Host stream idle timed out');
    opened();
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
  };

  const streamLoop = async (signal: AbortSignal) => {
    let backoff = RETRY_MIN_MS;
    const opened = () => { backoff = RETRY_MIN_MS; kickRegistrations(); };
    while (!signal.aborted) {
      const connection = new AbortController();
      const streamSignal = AbortSignal.any([signal, connection.signal]);
      let deadline: ReturnType<typeof setTimeout> | undefined;
      const armDeadline = (ms: number, reason: string) => {
        clearTimeout(deadline);
        deadline = setTimeout(() => connection.abort(new Error(reason)), ms);
      };
      try {
        const { stream, bookmark } = inbox.streamPosition();
        // With no bookmark the first request carries no query, so a v1
        // coordinator answers 404 (unknown route), not 400 (unexpected query).
        const query = bookmark === null ? '' : `?${new URLSearchParams({ stream: stream!, after: String(bookmark) })}`;
        armDeadline(REQUEST_TIMEOUT_MS, 'Host stream response timed out');
        const response = await fetch(new URL(`${ROUTES.streamV2}${query}`, base), { headers, signal: streamSignal });
        if (response.status === 404 && bookmark === null) {
          await response.body?.cancel();
          await runV1(streamSignal, armDeadline, opened, signal);
        } else if (!eventStream(response)) {
          const failure = await coordinatorError(ROUTES.streamV2, response);
          throw new Error(`Stream unavailable: HTTP ${response.status}${failure.code ? `: ${failure.message}` : ''}`);
        } else await runV2(response.body, connection, streamSignal, armDeadline, opened, signal);
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
      clearInterval(presenceTimer);
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

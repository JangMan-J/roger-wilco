import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import {
  deliveryId, isMessageInput, isNamespace, isReceipt, isRegistration,
  isSessionAddress, isSessionKey, sessionKey, PROTOCOL_VERSION,
  type Claim, type CoordinatorStore, type HostPrincipal, type Message,
  type MessageInput, type Outcome, type Receipt, type Registration,
  type RegistrationResult, type SessionAddress, type StreamFrame, type Subscription,
} from '../protocol/index.ts';

const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MAINTENANCE_MS = 60_000;
const COLLECTION_LIMIT = 5000;

const schema = `
CREATE TABLE IF NOT EXISTS store_config (namespace TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY, host_id TEXT NOT NULL, display_name TEXT,
  last_seen_at INTEGER NOT NULL DEFAULT 0, rank INTEGER NOT NULL DEFAULT 1,
  initial_cursor INTEGER NOT NULL, cursor INTEGER NOT NULL, offered INTEGER
);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER NOT NULL,
  sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL,
  data TEXT NOT NULL, visibility TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_recipient ON messages(recipient, id);
CREATE INDEX IF NOT EXISTS messages_timestamp ON messages(timestamp);
CREATE TABLE IF NOT EXISTS claims (
  key TEXT PRIMARY KEY, owner TEXT NOT NULL, note TEXT,
  claimed_at INTEGER NOT NULL, expires_at INTEGER
);`;

type SessionRow = { id: string; host_id: string; display_name: string | null; last_seen_at: number; rank: number; cursor: number; initial_cursor: number; offered: number | null };
type MessageRow = { id: number; timestamp: number; sender: string; recipient: string; body: string; data: string; visibility: 'private' | 'public' };
type ClaimRow = { key: string; owner: string; note: string | null; claimed_at: number; expires_at: number | null };
const failure = <T>(error: 'invalid_request' | 'not_found' | 'owner_conflict', message: string): Outcome<T> => ({ ok: false, error, message });
const validHost = (host: HostPrincipal): boolean => !!host && typeof host.hostId === 'string' && host.hostId.length > 0;
const validLimit = (limit: number): boolean => Number.isSafeInteger(limit) && limit > 0 && limit <= 500;
const validCursor = (cursor: number): boolean => Number.isSafeInteger(cursor) && cursor >= 0;
const addressOf = (key: string): SessionAddress => {
  const index = key.indexOf(':');
  return { namespace: key.slice(0, index), piSessionId: key.slice(index + 1) };
};

class Store implements CoordinatorStore {
  private readonly db: DatabaseSync;
  private readonly namespace: string;
  private readonly rotation = new Map<string, string>();
  private nextMaintenance = 0;
  private readonly pendingRows: StatementSync;
  private readonly offer: StatementSync;
  private readonly sessionRow: StatementSync;
  private readonly messageRow: StatementSync;
  private readonly claimRow: StatementSync;

  constructor(path: string, namespace: string) {
    if (!isNamespace(namespace)) throw new TypeError('Invalid namespace');
    this.namespace = namespace;
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    try {
      this.db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA journal_size_limit = 16777216;');
      this.db.exec(schema);
      const columns = this.db.prepare('PRAGMA table_info(sessions)').all() as { name: string }[];
      if (!columns.some(column => column.name === 'last_seen_at'))
        this.db.exec('ALTER TABLE sessions ADD COLUMN last_seen_at INTEGER NOT NULL DEFAULT 0');
      if (!columns.some(column => column.name === 'rank'))
        this.db.exec('ALTER TABLE sessions ADD COLUMN rank INTEGER NOT NULL DEFAULT 1');
      this.db.exec(`CREATE INDEX IF NOT EXISTS sessions_host ON sessions(host_id, id);
        CREATE INDEX IF NOT EXISTS sessions_last_seen ON sessions(last_seen_at);
        CREATE INDEX IF NOT EXISTS sessions_cursor ON sessions(cursor)`);
      this.transaction(() => {
        const row = this.db.prepare('SELECT namespace FROM store_config').get() as { namespace: string } | undefined;
        if (row && row.namespace !== namespace) throw new Error('Coordinator database namespace mismatch');
        if (!row) this.db.prepare('INSERT INTO store_config (namespace) VALUES (?)').run(namespace);
        // Pre-presence databases get one retention window, not immediate data loss.
        this.db.prepare('UPDATE sessions SET last_seen_at = ? WHERE last_seen_at = 0').run(Date.now());
      });
      this.sessionRow = this.db.prepare('SELECT * FROM sessions WHERE id = ?');
      this.messageRow = this.db.prepare('SELECT * FROM messages WHERE id = ?');
      this.claimRow = this.db.prepare('SELECT * FROM claims WHERE key = ?');
      // Two indexed seeks per retained subscription, one bounded result set; no JS N+1 reads.
      this.pendingRows = this.db.prepare(`SELECT m.*, s.id AS subscription_id, s.offered
        FROM sessions s JOIN messages m ON m.id = (
          SELECT MIN(id) FROM (
            SELECT MIN(id) AS id FROM messages WHERE recipient = s.id AND id > s.cursor
            UNION ALL
            SELECT MIN(id) AS id FROM messages WHERE recipient = '*' AND sender != s.id AND id > s.cursor
          )
        ) WHERE s.host_id = ? ORDER BY (s.id <= ?), s.id LIMIT ?`);
      this.offer = this.db.prepare('UPDATE sessions SET offered = ? WHERE id = ?');
      this.maintain();
    } catch (error) { this.db.close(); throw error; }
  }

  private transaction<T>(action: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = action();
      this.db.exec('COMMIT');
      return result;
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  private maintain(): void {
    const now = Date.now();
    if (now < this.nextMaintenance) return;
    this.transaction(() => {
      this.db.prepare('DELETE FROM sessions WHERE last_seen_at < ?').run(now - RETENTION_MS);
      // A sender or unrelated low cursor must not pin history. Broadcasts remain
      // until every retained recipient has acknowledged, or its registration expires.
      this.db.prepare(`DELETE FROM messages WHERE id IN (
        SELECT id FROM messages WHERE timestamp < ?
          AND NOT EXISTS (SELECT 1 FROM sessions WHERE id = messages.recipient AND cursor < messages.id)
          AND (recipient != '*' OR NOT EXISTS (
            SELECT 1 FROM sessions WHERE id != messages.sender AND cursor < messages.id
          )) ORDER BY timestamp LIMIT ?
      )`).run(now - RETENTION_MS, COLLECTION_LIMIT);
    });
    this.nextMaintenance = now + MAINTENANCE_MS;
  }
  private session(id: string): SessionRow | undefined { return this.sessionRow.get(id) as SessionRow | undefined; }
  private subscription(row: SessionRow): Subscription {
    return { id: row.id, session: addressOf(row.id), hostId: row.host_id, cursor: row.cursor };
  }
  private registration(row: SessionRow): RegistrationResult {
    return { subscription: this.subscription(row), ...(row.display_name === null ? {} : { displayName: row.display_name }),
      lastSeenAt: row.last_seen_at, rank: row.rank };
  }
  private message(row: MessageRow): Message {
    return { version: PROTOCOL_VERSION, id: deliveryId(this.namespace, row.id), sequence: row.id,
      timestamp: row.timestamp, sender: row.sender, recipient: row.recipient, body: row.body,
      data: JSON.parse(row.data), visibility: row.visibility };
  }
  private claimValue(row: ClaimRow): Claim {
    return { key: row.key, owner: row.owner, note: row.note, claimedAt: row.claimed_at, expiresAt: row.expires_at };
  }
  private assigned(host: HostPrincipal, session: SessionAddress): Outcome<SessionRow> {
    if (!validHost(host) || !isSessionAddress(session) || session.namespace !== this.namespace)
      return failure('invalid_request', 'Invalid host or session');
    this.maintain();
    const row = this.session(sessionKey(session));
    if (!row) return failure('not_found', 'Session is not registered');
    if (row.host_id !== host.hostId) return failure('owner_conflict', 'Session belongs to another host');
    return { ok: true, value: row };
  }

  register(host: HostPrincipal, request: Registration): Outcome<RegistrationResult> {
    if (!validHost(host) || !isRegistration(request) || request.session.namespace !== this.namespace)
      return failure('invalid_request', 'Invalid registration');
    this.maintain();
    return this.transaction(() => {
      const id = sessionKey(request.session);
      let row = this.session(id);
      if (row && row.host_id !== host.hostId) return failure('owner_conflict', 'Session belongs to another host');
      const now = Date.now();
      if (!row) {
        // AUTOINCREMENT's durable watermark survives collection of the entire history.
        const tail = (this.db.prepare("SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'messages'), 0) AS tail")
          .get() as { tail: number }).tail;
        this.db.prepare('INSERT INTO sessions (id, host_id, display_name, last_seen_at, initial_cursor, cursor) VALUES (?, ?, ?, ?, ?, ?)')
          .run(id, host.hostId, request.displayName ?? null, now, tail, tail);
      } else {
        this.db.prepare('UPDATE sessions SET display_name = COALESCE(?, display_name), last_seen_at = ? WHERE id = ?')
          .run(request.displayName ?? null, now, id);
      }
      return { ok: true, value: this.registration(this.session(id)!) };
    });
  }
  setRank(host: HostPrincipal, session: SessionAddress, rank: number): Outcome<RegistrationResult> {
    if (!validHost(host) || !isSessionAddress(session) || session.namespace !== this.namespace ||
        !Number.isInteger(rank) || rank < 1 || rank > 5)
      return failure('invalid_request', 'Invalid rank update');
    this.maintain();
    return this.transaction(() => {
      const id = sessionKey(session);
      if (!this.session(id)) return failure('not_found', 'Session is not registered');
      // Trusted-user metadata: no owner restriction, presence refresh, or cursor change.
      this.db.prepare('UPDATE sessions SET rank = ? WHERE id = ?').run(rank, id);
      return { ok: true, value: this.registration(this.session(id)!) };
    });
  }
  listSessions(): RegistrationResult[] {
    this.maintain();
    return (this.db.prepare('SELECT * FROM sessions ORDER BY id').all() as SessionRow[])
      .map(row => this.registration(row));
  }
  listClaims(): Claim[] {
    this.maintain();
    return this.transaction(() => {
      const now = Date.now();
      this.db.prepare('DELETE FROM claims WHERE expires_at IS NOT NULL AND expires_at <= ?').run(now);
      return (this.db.prepare('SELECT * FROM claims ORDER BY key').all() as ClaimRow[])
        .map(row => this.claimValue(row));
    });
  }
  send(host: HostPrincipal, sender: SessionAddress, input: MessageInput): Outcome<Message> {
    const assignment = this.assigned(host, sender);
    if (!assignment.ok) return assignment;
    if (!isMessageInput(input) || (input.recipient !== '*' && !input.recipient.startsWith(`${this.namespace}:`)))
      return failure('invalid_request', 'Invalid message');
    return this.transaction(() => {
      const result = this.db.prepare('INSERT INTO messages (timestamp, sender, recipient, body, data, visibility) VALUES (?, ?, ?, ?, ?, ?)')
        .run(Date.now(), sessionKey(sender), input.recipient, input.body, JSON.stringify(input.data ?? null), input.visibility ?? 'private');
      if (!Number.isSafeInteger(result.lastInsertRowid)) throw new RangeError('Message sequence exceeds safe integer range');
      return { ok: true, value: this.message(this.messageRow.get(result.lastInsertRowid) as MessageRow) };
    });
  }
  private next(id: string, cursor: number): MessageRow | undefined {
    return this.db.prepare(`SELECT * FROM messages WHERE id > ? AND
      (recipient = ? OR (recipient = '*' AND sender != ?)) ORDER BY id LIMIT 1`)
      .get(cursor, id, id) as MessageRow | undefined;
  }
  inbox(host: HostPrincipal, session: SessionAddress, since: number, limit: number): Outcome<Message[]> {
    const assignment = this.assigned(host, session);
    if (!assignment.ok) return assignment;
    if (!validCursor(since) || !validLimit(limit)) return failure('invalid_request', 'Invalid pagination');
    const id = sessionKey(session);
    const rows = this.db.prepare(`SELECT * FROM messages WHERE id > ? AND
      (recipient = ? OR (recipient = '*' AND sender != ?)) ORDER BY id LIMIT ?`)
      .all(since, id, id, limit) as MessageRow[];
    return { ok: true, value: rows.map(row => this.message(row)) };
  }
  pending(host: HostPrincipal, limit: number): StreamFrame[] {
    if (!validHost(host) || !validLimit(limit)) throw new TypeError('Invalid host or limit');
    this.maintain();
    return this.transaction(() => {
      const rows = this.pendingRows.all(host.hostId, this.rotation.get(host.hostId) ?? '', limit) as
        (MessageRow & { subscription_id: string; offered: number | null })[];
      if (!rows.length) { this.rotation.delete(host.hostId); return []; }
      const frames = rows.map(row => {
        // Record the exact item offered before the stream writes it. No later item can bypass it.
        if (row.offered !== row.id) this.offer.run(row.id, row.subscription_id);
        return { type: 'message' as const, subscriptionId: row.subscription_id, message: this.message(row) };
      });
      this.rotation.set(host.hostId, rows.at(-1)!.subscription_id);
      return frames;
    });
  }
  acknowledge(host: HostPrincipal, receipt: Receipt): Outcome<Subscription> {
    if (!validHost(host) || !isReceipt(receipt) || !receipt.subscriptionId.startsWith(`${this.namespace}:`))
      return failure('invalid_request', 'Invalid receipt');
    this.maintain();
    return this.transaction(() => {
      const row = this.session(receipt.subscriptionId);
      if (!row) return failure('not_found', 'Subscription does not exist');
      if (row.host_id !== host.hostId) return failure('owner_conflict', 'Subscription belongs to another host');
      const sequence = Number(receipt.messageId.slice(`${this.namespace}:msg:`.length));
      if (sequence <= row.cursor) {
        // The cursor itself proves the latest ACK even after message collection.
        // Older retries require retained routing history; an initial tail is not an ACK.
        if (sequence === row.cursor && sequence > row.initial_cursor)
          return { ok: true, value: this.subscription(row) };
        const past = this.messageRow.get(sequence) as MessageRow | undefined;
        if (sequence > row.initial_cursor && past &&
            (past.recipient === row.id || (past.recipient === '*' && past.sender !== row.id)))
          return { ok: true, value: this.subscription(row) };
        return failure('invalid_request', 'Message was not delivered to this subscription');
      }
      const next = this.next(row.id, row.cursor);
      if (!next || next.id !== sequence || row.offered !== sequence)
        return failure('invalid_request', 'Message is not the next offered delivery');
      this.db.prepare('UPDATE sessions SET cursor = ?, offered = NULL WHERE id = ?').run(sequence, row.id);
      return { ok: true, value: this.subscription(this.session(row.id)!) };
    });
  }
  claim(host: HostPrincipal, key: string, owner: string, ttlSeconds: number, note?: string): Outcome<{ claim: Claim; acquired: boolean }> {
    if (!validHost(host) || typeof key !== 'string' || key.length < 1 || key.length > 256 || !isSessionKey(owner) ||
        !Number.isSafeInteger(ttlSeconds) || ttlSeconds < 0 || ttlSeconds > 2147483647 ||
        (note !== undefined && (typeof note !== 'string' || note.length > 4096))) return failure('invalid_request', 'Invalid claim');
    const assignment = this.assigned(host, addressOf(owner));
    if (!assignment.ok) return assignment;
    return this.transaction(() => {
      const now = Date.now();
      const expires = ttlSeconds === 0 ? null : now + ttlSeconds * 1000;
      const result = this.db.prepare(`INSERT INTO claims (key, owner, note, claimed_at, expires_at)
        VALUES (?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET
        owner = excluded.owner, note = excluded.note, claimed_at = excluded.claimed_at,
        expires_at = excluded.expires_at WHERE claims.owner = excluded.owner
        OR (claims.expires_at IS NOT NULL AND claims.expires_at <= excluded.claimed_at)`)
        .run(key, owner, note ?? null, now, expires);
      const claim = this.claimValue(this.claimRow.get(key) as ClaimRow);
      return { ok: true, value: { claim, acquired: result.changes > 0 } };
    });
  }
  releaseClaim(host: HostPrincipal, key: string, owner: string): Outcome<boolean> {
    if (!validHost(host) || typeof key !== 'string' || key.length < 1 || key.length > 256 || !isSessionKey(owner))
      return failure('invalid_request', 'Invalid release');
    const assignment = this.assigned(host, addressOf(owner));
    if (!assignment.ok) return assignment;
    return this.transaction(() => ({ ok: true, value: this.db.prepare('DELETE FROM claims WHERE key = ? AND owner = ?').run(key, owner).changes > 0 }));
  }
  close(): void { this.db.close(); }
}

export function createCoordinatorStore(dbPath: string, namespace: string): CoordinatorStore {
  return new Store(dbPath, namespace);
}

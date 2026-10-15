import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';
import {
  deliveryId, entityReference, extractEntities, isEntityId, isMessage, isMessageInput, isNamespace, isPresence, isReceipt,
  isRegistration, isSessionAddress, isSessionKey, isSettingValue, sessionKey, DEFAULT_SETTINGS, PROTOCOL_VERSION,
  type Claim, type CoordinatorStore, type EntityHistory, type EntityRef, type HostBatch, type HostPrincipal, type HostStart,
  type Message, type JsonValue, type MessageInput, type Outcome, type Presence, type PresenceResult, type Receipt,
  type Registration, type RegistrationResult, type SessionAddress, type StreamFrame, type Subscription,
} from '../protocol/index.ts';

const MAINTENANCE_MS = 60_000;
/** Message ids one hostBatch call examines at most, however far behind the host is. */
export const SCAN_SPAN = 4096;
const STREAM_ID = /^[0-9a-f]{32}$/;
const COLLECTION_LIMIT = 5000;
const VERIFIED_CACHE_LIMIT = 4096;
// The verified-message cache is also bounded by stored text (body plus data JSON, in
// UTF-16 code units). Parsed JSON can take about 20 times its text length in heap, so
// 4 MiB of text bounds the cache near 100 MB in the worst case. A row above the entry
// limit is verified on every read instead, so a page of large rows cannot flush the cache.
const VERIFIED_CACHE_TEXT = 4 * 1024 * 1024;
const VERIFIED_ENTRY_TEXT = 64 * 1024;

const schema = `
CREATE TABLE IF NOT EXISTS store_config (namespace TEXT NOT NULL);
-- display_name is unused. It stays so older databases open unchanged.
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
CREATE TABLE IF NOT EXISTS entities (
  id TEXT PRIMARY KEY, type INTEGER NOT NULL, label TEXT NOT NULL, last_seen_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS entity_sources (
  entity_id TEXT NOT NULL REFERENCES entities(id),
  kind TEXT NOT NULL CHECK (kind IN ('session', 'claim')), source_key TEXT NOT NULL,
  PRIMARY KEY (entity_id, kind, source_key)
);
CREATE INDEX IF NOT EXISTS entity_sources_source ON entity_sources(kind, source_key);
CREATE TABLE IF NOT EXISTS entity_aliases (
  alias_id TEXT PRIMARY KEY, entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS entity_aliases_entity ON entity_aliases(entity_id);
CREATE TABLE IF NOT EXISTS message_entities (
  entity_id TEXT NOT NULL REFERENCES entities(id),
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  PRIMARY KEY (entity_id, message_id)
);
CREATE INDEX IF NOT EXISTS message_entities_message ON message_entities(message_id);
CREATE TABLE IF NOT EXISTS claims (
  key TEXT PRIMARY KEY, owner TEXT NOT NULL, note TEXT,
  claimed_at INTEGER NOT NULL, expires_at INTEGER
);`;

type SessionRow = { id: string; host_id: string; last_seen_at: number; rank: number; cursor: number; initial_cursor: number; offered: number | null };
type MessageRow = { id: number; timestamp: number; sender: string; recipient: string; body: string; data: string; visibility: 'private' | 'public'; hash: string | null };
/** The packet identity: SHA-256 over the exact JSON text of the verified message. */
function messageHash(message: Message): string {
  return createHash('sha256').update(JSON.stringify(message)).digest('hex');
}
type ClaimRow = { key: string; owner: string; note: string | null; claimed_at: number; expires_at: number | null };
const failure = <T>(error: 'invalid_request' | 'not_found' | 'owner_conflict', message: string): Outcome<T> => ({ ok: false, error, message });
/** A new random coordinator log id: 32 lowercase hex digits. */
export const newStreamId = (): string => randomBytes(16).toString('hex');
const validHost = (host: HostPrincipal): boolean => !!host && typeof host.hostId === 'string' && host.hostId.length > 0;
const validLimit = (limit: number): boolean => Number.isSafeInteger(limit) && limit > 0 && limit <= 500;
const validCursor = (cursor: number): boolean => Number.isSafeInteger(cursor) && cursor >= 0;
const validClaimKey = (key: unknown): key is string => typeof key === 'string' && key.length <= 256 && key.trim().length > 0;
function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}
const addressOf = (key: string): SessionAddress => {
  const index = key.indexOf(':');
  return { namespace: key.slice(0, index), sessionId: key.slice(index + 1) };
};

class Store implements CoordinatorStore {
  private readonly db: DatabaseSync;
  private readonly namespace: string;
  private readonly retentionMs: number;
  private readonly rotation = new Map<string, string>();
  private readonly listeners = new Set<(hostId: string | undefined) => void>();
  /**
   * Verified Message objects by row id, so each stored hash is checked once per process.
   * Rows are never updated after commit and AUTOINCREMENT never reuses an id. The cache
   * holds no delivery state: cursors and offered marks are read from SQLite on every call.
   * Insertion order is eviction order; `verifiedText` is the sum of the entries' text sizes.
   */
  private readonly verified = new Map<number, { message: Message; text: number }>();
  private verifiedText = 0;
  private nextMaintenance = 0;
  private readonly pendingRows: StatementSync;
  private readonly offer: StatementSync;
  private readonly sessionRow: StatementSync;
  private readonly messageRow: StatementSync;
  private readonly claimRow: StatementSync;
  private readonly entityRow: StatementSync;
  private readonly insertEntity: StatementSync;
  private readonly insertMention: StatementSync;
  private readonly hostRow: StatementSync;
  private readonly hostMembers: StatementSync;
  private readonly hostRows: StatementSync;

  constructor(path: string, namespace: string, retentionMs: number) {
    if (!isNamespace(namespace)) throw new TypeError('Invalid namespace');
    if (!isSettingValue('retentionMs', retentionMs)) throw new RangeError('Invalid retention');
    this.namespace = namespace;
    this.retentionMs = retentionMs;
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    try {
      // Durability classes. Commits run at synchronous=NORMAL: in WAL mode they survive a
      // process crash, and an OS crash or power loss can roll back the most recent ones.
      // send() commits at FULL, because a message insert lost after a success response is
      // a lost message; a FULL commit also syncs every earlier WAL frame. claim() and the
      // openHost() that creates a hosts row (the v2 cut-over) also commit at FULL. The offered
      // mark is connection state. A lost cursor update replays one message, which the daemon
      // deduplicates by message ID (INSERT OR IGNORE) and receipts again. A lost v2 receipt
      // only lowers C, which replays frames the daemon skips.
      this.db.exec(`PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;
        PRAGMA busy_timeout = 5000; PRAGMA journal_size_limit = 16777216;`);
      this.db.exec(schema);
      const entityColumns = this.db.prepare('PRAGMA table_info(entities)').all() as { name: string }[];
      if (!entityColumns.some(column => column.name === 'last_seen_at'))
        this.db.exec('ALTER TABLE entities ADD COLUMN last_seen_at INTEGER NOT NULL DEFAULT 0');
      const messageColumns = this.db.prepare('PRAGMA table_info(messages)').all() as { name: string }[];
      if (!messageColumns.some(column => column.name === 'hash'))
        this.db.exec('ALTER TABLE messages ADD COLUMN hash TEXT');
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
      // Host stream v2 delta. A hosts row means that host is on protocol 2; acked is C(h).
      // stream_id names this coordinator log; it is set once and kept on every later open.
      this.transaction(() => {
        this.db.exec('CREATE TABLE IF NOT EXISTS hosts (host_id TEXT PRIMARY KEY, acked INTEGER NOT NULL)');
        const configColumns = this.db.prepare('PRAGMA table_info(store_config)').all() as { name: string }[];
        if (!configColumns.some(column => column.name === 'stream_id'))
          this.db.exec('ALTER TABLE store_config ADD COLUMN stream_id TEXT');
        this.db.prepare('UPDATE store_config SET stream_id = ? WHERE stream_id IS NULL').run(newStreamId());
      });
      this.migrateEntityMetadata();
      // Only registration display names created session entity sources. Those names
      // are gone, so the sources are removed here; the entities stay and age out under
      // the entity retention rule. Running this on every open is idempotent.
      this.db.exec("DELETE FROM entity_sources WHERE kind = 'session'");
      // Rows written before packet hashes existed are verified once, here,
      // and hashed; after this pass every read is an equality check.
      this.transaction(() => {
        const update = this.db.prepare('UPDATE messages SET hash = ? WHERE id = ?');
        for (const row of this.db.prepare('SELECT * FROM messages WHERE hash IS NULL').all() as MessageRow[])
          update.run(messageHash(this.message(row)), row.id);
      });
      this.sessionRow = this.db.prepare('SELECT * FROM sessions WHERE id = ?');
      this.messageRow = this.db.prepare('SELECT * FROM messages WHERE id = ?');
      this.claimRow = this.db.prepare('SELECT * FROM claims WHERE key = ?');
      this.entityRow = this.db.prepare('SELECT * FROM entities WHERE id = ?');
      this.insertEntity = this.db.prepare(`INSERT INTO entities (id, type, label, last_seen_at) VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET type = COALESCE(?, entities.type), last_seen_at = excluded.last_seen_at`);
      this.insertMention = this.db.prepare('INSERT INTO message_entities (entity_id, message_id) VALUES (?, ?)');
      // Two indexed seeks per retained subscription, one bounded result set; no JS N+1 reads.
      this.pendingRows = this.db.prepare(`SELECT m.*, s.id AS subscription_id, s.offered
        FROM sessions s JOIN messages m ON m.id = (
          SELECT MIN(id) FROM (
            SELECT MIN(id) AS id FROM messages WHERE recipient = s.id AND id > s.cursor
            UNION ALL
            SELECT MIN(id) AS id FROM messages WHERE recipient = '*' AND sender != s.id AND id > s.cursor
          )
        ) WHERE s.host_id = ? AND s.id NOT IN (SELECT value FROM json_each(?))
        ORDER BY (s.id <= ?), s.id LIMIT ?`);
      this.offer = this.db.prepare('UPDATE sessions SET offered = ? WHERE id = ?');
      this.hostRow = this.db.prepare('SELECT acked FROM hosts WHERE host_id = ?');
      this.hostMembers = this.db.prepare('SELECT id, cursor FROM sessions WHERE host_id = ? ORDER BY id');
      // Directed rows above the recipient's start, plus each broadcast once when some member
      // other than its sender has a start below it. Both branches seek the recipient index.
      this.hostRows = this.db.prepare(`SELECT m.* FROM sessions s JOIN messages m ON m.recipient = s.id
          WHERE s.host_id = :host AND m.id > :from AND m.id <= :upto AND m.id > s.cursor
        UNION ALL
        SELECT m.* FROM messages m
          WHERE m.recipient = '*' AND m.id > :from AND m.id <= :upto
            AND EXISTS (SELECT 1 FROM sessions s WHERE s.host_id = :host AND s.cursor < m.id AND s.id <> m.sender)
        ORDER BY id LIMIT :limit`);
      this.maintain();
    } catch (error) { this.db.close(); throw error; }
  }

  /** `full` commits at synchronous=FULL; SQLite rejects a safety-level change inside a transaction. */
  private transaction<T>(action: () => T, full = false): T {
    if (full) this.db.exec('PRAGMA synchronous = FULL');
    try {
      this.db.exec('BEGIN IMMEDIATE');
      try {
        const result = action();
        this.db.exec('COMMIT');
        return result;
      } catch (error) { this.db.exec('ROLLBACK'); throw error; }
    } finally { if (full) this.db.exec('PRAGMA synchronous = NORMAL'); }
  }
  /** A read snapshot: every statement inside sees the same committed state. */
  private snapshot<T>(action: () => T): T {
    this.db.exec('BEGIN DEFERRED');
    try {
      const result = action();
      this.db.exec('COMMIT');
      return result;
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  /** The durable message watermark; AUTOINCREMENT never reissues an id at or below it. */
  private tail(): number {
    return (this.db.prepare("SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'messages'), 0) AS tail")
      .get() as { tail: number }).tail;
  }
  private acked(hostId: string): number | undefined {
    return (this.hostRow.get(hostId) as { acked: number } | undefined)?.acked;
  }
  private migrateEntityMetadata(): void {
    this.transaction(() => {
      const columns = this.db.prepare('PRAGMA table_info(store_config)').all() as { name: string }[];
      if (!columns.some(column => column.name === 'entity_metadata_version'))
        this.db.exec('ALTER TABLE store_config ADD COLUMN entity_metadata_version INTEGER NOT NULL DEFAULT 0');
      const { entity_metadata_version: version } = this.db.prepare('SELECT entity_metadata_version FROM store_config')
        .get() as { entity_metadata_version: number };
      if (version === 1) return;
      if (version !== 0) throw new Error('Unsupported entity metadata version');
      const rows = this.db.prepare('SELECT * FROM entities').all() as EntityRef[];
      for (const row of rows) {
        const canonical = entityReference(this.namespace, row.label);
        const legacyHash = createHash('sha256').update(JSON.stringify([this.namespace, row.type, row.label])).digest('hex').slice(0, 32);
        if (row.id !== `${this.namespace}:entity:${row.type.toString(16).padStart(2, '0')}:${legacyHash}`)
          throw new Error('Entity identity collision');
        if (canonical.id === row.id) continue;
        const existing = this.db.prepare('SELECT label FROM entities WHERE id = ?').get(canonical.id);
        if (existing && existing.label !== canonical.label) throw new Error('Entity identity collision');
        this.db.prepare('INSERT OR IGNORE INTO entities (id, type, label) VALUES (?, 0, ?)').run(canonical.id, canonical.label);
        this.db.prepare(`INSERT OR IGNORE INTO message_entities (entity_id, message_id)
          SELECT ?, message_id FROM message_entities WHERE entity_id = ?`).run(canonical.id, row.id);
        this.db.prepare('DELETE FROM message_entities WHERE entity_id = ?').run(row.id);
        this.db.prepare('INSERT INTO entity_aliases (alias_id, entity_id) VALUES (?, ?)').run(row.id, canonical.id);
        this.db.prepare('DELETE FROM entities WHERE id = ?').run(row.id);
      }
      // Replay only indexed mentions, not older messages from before entity tracking.
      // Sequence order and marker order define the latest explicit metadata.
      const update = this.db.prepare(`UPDATE entities SET type = ? WHERE id = ?
        AND EXISTS (SELECT 1 FROM message_entities WHERE entity_id = entities.id AND message_id = ?)`);
      for (const row of this.db.prepare(`SELECT id, body FROM messages
        WHERE EXISTS (SELECT 1 FROM message_entities WHERE message_id = messages.id) ORDER BY id`).iterate()) {
        for (const entity of extractEntities(this.namespace, row.body as string)) {
          if (entity.type !== undefined) update.run(entity.type, entity.id, row.id);
        }
      }
      this.db.prepare(`UPDATE entities SET last_seen_at = COALESCE(
        (SELECT MAX(m.timestamp) FROM message_entities e JOIN messages m ON m.id = e.message_id WHERE e.entity_id = entities.id), ?
      )`).run(Date.now());
      this.db.exec('UPDATE store_config SET entity_metadata_version = 1');
    });
  }
  private maintain(): void {
    const now = Date.now();
    if (now < this.nextMaintenance) return;
    this.transaction(() => {
      const cutoff = now - this.retentionMs;
      this.db.prepare('DELETE FROM sessions WHERE last_seen_at < ?').run(cutoff);
      this.db.prepare('DELETE FROM claims WHERE expires_at IS NOT NULL AND expires_at <= ?').run(now);
      this.db.exec("DELETE FROM entity_sources WHERE kind = 'claim' AND NOT EXISTS (SELECT 1 FROM claims WHERE key = source_key)");
      // A sender or unrelated low cursor must not pin history. A message stays while a retained
      // session it is owed to has a start (cursor) below it and its host has not receipted it.
      // A v1 host has no hosts row (acked -1), which leaves the per-session cursor rule.
      this.db.prepare(`DELETE FROM messages WHERE id IN (
        SELECT m.id FROM messages m WHERE m.timestamp < ?
          AND NOT EXISTS (SELECT 1 FROM sessions s LEFT JOIN hosts h ON h.host_id = s.host_id
            WHERE s.id = m.recipient AND s.cursor < m.id AND COALESCE(h.acked, -1) < m.id)
          AND (m.recipient <> '*' OR NOT EXISTS (SELECT 1 FROM sessions s LEFT JOIN hosts h ON h.host_id = s.host_id
            WHERE s.id <> m.sender AND s.cursor < m.id AND COALESCE(h.acked, -1) < m.id))
          ORDER BY m.timestamp LIMIT ?
      )`).run(cutoff, COLLECTION_LIMIT);
      this.db.prepare(`DELETE FROM entities WHERE last_seen_at < ? AND NOT EXISTS (
        SELECT 1 FROM message_entities WHERE entity_id = entities.id
      ) AND NOT EXISTS (SELECT 1 FROM entity_sources WHERE entity_id = entities.id)`).run(cutoff);
    });
    this.nextMaintenance = now + MAINTENANCE_MS;
  }
  private session(id: string): SessionRow | undefined { return this.sessionRow.get(id) as SessionRow | undefined; }
  private subscription(row: SessionRow): Subscription {
    return { id: row.id, session: addressOf(row.id), hostId: row.host_id, cursor: row.cursor };
  }
  private claimEntities(key: string): { entities?: EntityRef[] } {
    const entities = this.db.prepare(`SELECT e.id, e.type, e.label FROM entities e JOIN entity_sources s ON s.entity_id = e.id
      WHERE s.kind = 'claim' AND s.source_key = ? ORDER BY e.id`).all(key) as EntityRef[];
    return entities.length ? { entities: entities.map(entity => ({ ...entity })) } : {};
  }
  /** Called only within the accepted mutation's transaction, never while reading or delivering. */
  private trackEntities(values: JsonValue[], source?: { kind: 'claim'; key: string }): string[] {
    if (source) this.db.prepare('DELETE FROM entity_sources WHERE kind = ? AND source_key = ?').run(source.kind, source.key);
    const entities = extractEntities(this.namespace, ...values);
    for (const entity of entities) {
      const existing = this.entityRow.get(entity.id) as EntityRef | undefined;
      if (existing && existing.label !== entity.label) throw new Error('Entity identity collision');
      this.insertEntity.run(entity.id, entity.type ?? 0, entity.label, Date.now(), entity.type ?? null);
      if (source) this.db.prepare('INSERT INTO entity_sources (entity_id, kind, source_key) VALUES (?, ?, ?)')
        .run(entity.id, source.kind, source.key);
    }
    return entities.map(entity => entity.id);
  }
  /** All claim entities in a single joined query, grouped by claim key. */
  private claimEntitiesByKey(): Map<string, EntityRef[]> {
    const rows = this.db.prepare(`SELECT s.source_key AS source_key, e.id, e.type, e.label FROM entities e
      JOIN entity_sources s ON s.entity_id = e.id WHERE s.kind = 'claim' ORDER BY s.source_key, e.id`)
      .all() as Array<EntityRef & { source_key: string }>;
    const byKey = new Map<string, EntityRef[]>();
    for (const row of rows) {
      const list = byKey.get(row.source_key) ?? [];
      list.push({ id: row.id, type: row.type, label: row.label });
      byKey.set(row.source_key, list);
    }
    return byKey;
  }
  private registration(row: SessionRow): RegistrationResult {
    return { subscription: this.subscription(row), lastSeenAt: row.last_seen_at, rank: row.rank };
  }
  private message(row: MessageRow): Message {
    const cached = this.verified.get(row.id);
    if (cached) return cached.message;
    let data: unknown;
    try { data = JSON.parse(row.data); }
    catch { throw new Error(row.hash ? 'Message hash mismatch' : 'Invalid stored message'); }
    const message = { version: PROTOCOL_VERSION, id: deliveryId(this.namespace, row.id), sequence: row.id,
      timestamp: row.timestamp, sender: row.sender, recipient: row.recipient, body: row.body,
      data, visibility: row.visibility };
    // A hashed row was fully verified at write time by this store and at
    // input time by the sending adapter. The equality check below is the
    // whole read-path verification: bytes that are not the verified bytes
    // are rejected, not re-validated.
    if (row.hash) {
      if (messageHash(message as Message) !== row.hash) throw new Error('Message hash mismatch');
      // Only committed, hashed rows are cached; send() reads its own row before the
      // hash exists, and a rolled-back insert can release its id for reuse.
      const text = row.body.length + row.data.length;
      if (text > VERIFIED_ENTRY_TEXT) return message as Message;
      for (const [id, entry] of this.verified) {
        if (this.verified.size < VERIFIED_CACHE_LIMIT && this.verifiedText + text <= VERIFIED_CACHE_TEXT) break;
        this.verified.delete(id);
        this.verifiedText -= entry.text;
      }
      this.verified.set(row.id, { message: deepFreeze(message as Message), text });
      this.verifiedText += text;
      return message as Message;
    }
    if (!isMessage(message)) throw new TypeError('Invalid stored message');
    return message;
  }
  private claimValue(row: ClaimRow, entities?: EntityRef[]): Claim {
    const found = entities ?? this.claimEntities(row.key).entities;
    return { key: row.key, owner: row.owner, note: row.note, claimedAt: row.claimed_at, expiresAt: row.expires_at,
      ...(found?.length ? { entities: found } : {}) };
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

  subscribe(listener: (hostId: string | undefined) => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }
  /** All listeners run. A later throw must not hide an earlier wake. */
  private notify(hostId: string | undefined): void {
    let fault: unknown;
    for (const listener of [...this.listeners]) {
      try { listener(hostId); }
      catch (error) { fault ??= error; }
    }
    if (fault !== undefined) throw fault;
  }
  register(host: HostPrincipal, request: Registration): Outcome<RegistrationResult> {
    if (!validHost(host) || !isRegistration(request) || request.session.namespace !== this.namespace)
      return failure('invalid_request', 'Invalid registration');
    this.maintain();
    // No stream wake: a registration makes nothing deliverable. A new session starts at
    // the tail, and an existing one keeps its cursor and host.
    return this.transaction((): Outcome<RegistrationResult> => {
      const id = sessionKey(request.session);
      let row = this.session(id);
      if (row && row.host_id !== host.hostId) return failure('owner_conflict', 'Session belongs to another host');
      const now = Date.now();
      if (!row) {
        // AUTOINCREMENT's durable watermark survives collection of the entire history.
        const tail = this.tail();
        this.db.prepare('INSERT INTO sessions (id, host_id, last_seen_at, initial_cursor, cursor) VALUES (?, ?, ?, ?, ?)')
          .run(id, host.hostId, now, tail, tail);
      } else {
        this.db.prepare('UPDATE sessions SET last_seen_at = ? WHERE id = ?').run(now, id);
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
      this.db.exec("DELETE FROM entity_sources WHERE kind = 'claim' AND NOT EXISTS (SELECT 1 FROM claims WHERE key = source_key)");
      const entities = this.claimEntitiesByKey();
      return (this.db.prepare('SELECT * FROM claims ORDER BY key').all() as ClaimRow[])
        .map(row => this.claimValue(row, entities.get(row.key)));
    });
  }
  send(host: HostPrincipal, sender: SessionAddress, input: MessageInput): Outcome<Message> {
    const assignment = this.assigned(host, sender);
    if (!assignment.ok) return assignment;
    if (!isMessageInput(input) || (input.recipient !== '*' && !input.recipient.startsWith(`${this.namespace}:`)))
      return failure('invalid_request', 'Invalid message');
    let recipientHost: string | undefined;
    const outcome: Outcome<Message> = this.transaction(() => {
      const result = this.db.prepare('INSERT INTO messages (timestamp, sender, recipient, body, data, visibility) VALUES (?, ?, ?, ?, ?, ?)')
        .run(Date.now(), sessionKey(sender), input.recipient, input.body, JSON.stringify(input.data ?? null), input.visibility ?? 'private');
      if (!Number.isSafeInteger(result.lastInsertRowid)) throw new RangeError('Message sequence exceeds safe integer range');
      for (const id of this.trackEntities([input.body, input.data ?? null])) {
        this.insertMention.run(id, result.lastInsertRowid);
      }
      // A directed message can only be delivered on the recipient's host.
      recipientHost = input.recipient === '*' ? undefined : this.session(input.recipient)?.host_id;
      const row = this.messageRow.get(result.lastInsertRowid) as MessageRow;
      const value = this.message(row);
      this.db.prepare('UPDATE messages SET hash = ? WHERE id = ?').run(messageHash(value), row.id);
      return { ok: true, value };
    }, true);
    if (outcome.ok) {
      // Transport notification, not a session wake: a broadcast notifies every
      // host so each stores it; a directed send notifies only the recipient's
      // host, and an unregistered recipient notifies none.
      if (input.recipient === '*') this.notify(undefined);
      else if (recipientHost) this.notify(recipientHost);
    }
    return outcome;
  }
  entity(id: string, since: number, limit: number): Outcome<EntityHistory> {
    if (!isEntityId(id) || !id.startsWith(`${this.namespace}:entity:`) || !validCursor(since) || !validLimit(limit))
      return failure('invalid_request', 'Invalid entity query');
    this.maintain();
    const alias = this.db.prepare('SELECT entity_id FROM entity_aliases WHERE alias_id = ?').get(id);
    const entity = this.entityRow.get(alias ? alias.entity_id : id) as EntityRef | undefined;
    if (!entity) return failure('not_found', 'Entity is not tracked');
    const rows = this.db.prepare(`SELECT m.* FROM message_entities e JOIN messages m ON m.id = e.message_id
      WHERE e.entity_id = ? AND e.message_id > ? ORDER BY e.message_id LIMIT ?`)
      .all(entity.id, since, limit) as MessageRow[];
    return { ok: true, value: { entity: { id: entity.id, type: entity.type, label: entity.label },
      messages: rows.map(row => this.message(row)) } };
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
  pending(host: HostPrincipal, limit: number, inFlight: Iterable<string> = []): StreamFrame[] {
    if (!validHost(host) || !validLimit(limit)) throw new TypeError('Invalid host or limit');
    const skip = JSON.stringify([...inFlight]);
    this.maintain();
    return this.transaction(() => {
      // In-flight subscriptions are excluded in SQL, so their rows are neither read nor
      // verified, and they do not count toward the limit.
      const rows = this.pendingRows.all(host.hostId, skip, this.rotation.get(host.hostId) ?? '', limit) as
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
    const outcome: Outcome<Subscription> = this.transaction(() => {
      const row = this.session(receipt.subscriptionId);
      if (!row) return failure('not_found', 'Subscription does not exist');
      if (row.host_id !== host.hostId) return failure('owner_conflict', 'Subscription belongs to another host');
      // Serialised with openHost by BEGIN IMMEDIATE: a v1 receipt either counts in the
      // cut-over's MIN(cursor) or is refused after it.
      if (this.acked(host.hostId) !== undefined) return failure('owner_conflict', 'Host uses protocol 2');
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
    if (outcome.ok) this.notify(host.hostId);
    return outcome;
  }
  claim(host: HostPrincipal, key: string, owner: string, ttlSeconds: number, note?: string): Outcome<{ claim: Claim; acquired: boolean }> {
    if (!validHost(host) || !validClaimKey(key) || !isSessionKey(owner) ||
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
      if (result.changes > 0) this.trackEntities([key, note ?? null], { kind: 'claim', key });
      const claim = this.claimValue(this.claimRow.get(key) as ClaimRow);
      return { ok: true, value: { claim, acquired: result.changes > 0 } };
      // A claim granted and then lost to a power failure could be granted again to
      // another owner, so it commits at FULL like a send. A lost release only keeps
      // the claim until it expires, which is safe at NORMAL.
    }, true);
  }
  releaseClaim(host: HostPrincipal, key: string, owner: string): Outcome<boolean> {
    if (!validHost(host) || !validClaimKey(key) || !isSessionKey(owner))
      return failure('invalid_request', 'Invalid release');
    const assignment = this.assigned(host, addressOf(owner));
    if (!assignment.ok) return assignment;
    return this.transaction(() => {
      const released = this.db.prepare('DELETE FROM claims WHERE key = ? AND owner = ?').run(key, owner).changes > 0;
      if (released) {
        this.db.prepare("DELETE FROM entity_sources WHERE kind = 'claim' AND source_key = ?").run(key);
        this.trackEntities([key]);
      }
      return { ok: true, value: released };
    });
  }
  openHost(host: HostPrincipal, bookmark: { stream?: string; after?: number } = {}): Outcome<HostStart> {
    const { stream, after } = bookmark;
    if (!validHost(host) || (stream !== undefined && (typeof stream !== 'string' || !STREAM_ID.test(stream))) ||
        (after !== undefined && !validCursor(after))) return failure('invalid_request', 'Invalid host stream bookmark');
    this.maintain();
    // Creating the row is the cut-over: it freezes cursors as starts and refuses v1
    // receipts, so it commits at FULL. The check and BEGIN run with no await between them.
    const creating = this.acked(host.hostId) === undefined;
    return this.transaction((): Outcome<HostStart> => {
      const tail = this.tail();
      const id = (this.db.prepare('SELECT stream_id FROM store_config').get() as { stream_id: string }).stream_id;
      if (stream !== undefined && stream !== id) return failure('owner_conflict', 'Bookmark belongs to another coordinator log');
      if (after !== undefined && after > tail) return failure('owner_conflict', 'Bookmark is ahead of the coordinator tail');
      const minStart = (this.db.prepare('SELECT MIN(cursor) AS start FROM sessions WHERE host_id = ?')
        .get(host.hostId) as { start: number | null }).start;
      let acked = this.acked(host.hostId);
      if (acked === undefined) {
        // A lower C only replays more; it never loses a message.
        acked = Math.min(minStart ?? tail, after ?? Number.MAX_SAFE_INTEGER);
        this.db.prepare('INSERT INTO hosts (host_id, acked) VALUES (?, ?)').run(host.hostId, acked);
      } else if (after !== undefined && after < acked) {
        return failure('owner_conflict', 'Bookmark is behind the receipted position');
      }
      // The coordinator's own knowledge, never the daemon's `after`, and never written to C.
      return { ok: true, value: { acked, resume: Math.max(acked, minStart ?? tail), stream: id } };
    }, creating);
  }
  hostBatch(host: HostPrincipal, from: number, limit: number): HostBatch {
    if (!validHost(host) || !validCursor(from) || !Number.isSafeInteger(limit) || limit < 1)
      throw new TypeError('Invalid host batch');
    this.maintain();
    return this.snapshot(() => {
      const tail = this.tail();
      const members = (this.hostMembers.all(host.hostId) as { id: string; cursor: number }[])
        .map(row => ({ session: row.id, start: row.cursor }));
      const upto = Math.max(from, Math.min(tail, from + SCAN_SPAN));
      const rows = this.hostRows.all({ host: host.hostId, from, upto, limit }) as MessageRow[];
      const scanned = rows.length === limit ? rows.at(-1)!.id : upto;
      return { members, messages: rows.map(row => this.message(row)), scanned, tail };
    });
  }
  advanceHost(host: HostPrincipal, through: number): number | undefined {
    if (!validHost(host) || !validCursor(through)) throw new TypeError('Invalid host receipt');
    return this.transaction(() => {
      this.db.prepare('UPDATE hosts SET acked = ? WHERE host_id = ? AND acked < ?').run(through, host.hostId, through);
      return this.acked(host.hostId);
    });
  }
  hostAcked(host: HostPrincipal): number | undefined {
    return validHost(host) ? this.acked(host.hostId) : undefined;
  }
  presence(host: HostPrincipal, request: Presence): Outcome<PresenceResult> {
    if (!validHost(host) || !isPresence(request)) return failure('invalid_request', 'Invalid presence');
    this.maintain();
    // No notify: presence makes nothing deliverable and never changes a start.
    return this.transaction((): Outcome<PresenceResult> => {
      if (this.acked(host.hostId) === undefined) return failure('owner_conflict', 'Host is not on protocol 2');
      const now = Date.now();
      const refresh = this.db.prepare('UPDATE sessions SET last_seen_at = ? WHERE id = ? AND host_id = ?');
      const unknown = request.sessions.filter(key => Number(refresh.run(now, key, host.hostId).changes) === 0);
      return { ok: true, value: { unknown } };
    });
  }
  close(): void { this.db.close(); }
}

/** `retentionMs` is the registration expiry and collection age, from settings. */
export function createCoordinatorStore(dbPath: string, namespace: string,
  options: { retentionMs?: number } = {}): CoordinatorStore {
  return new Store(dbPath, namespace, options.retentionMs ?? DEFAULT_SETTINGS.retentionMs);
}

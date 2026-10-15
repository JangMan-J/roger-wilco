import { DatabaseSync, type StatementSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';
import {
  deliveryId, entityReference, extractEntities, isEntityId, isMessage, isMessageInput, isNamespace, isReceipt, isRegistration,
  isSessionAddress, isSessionKey, sessionKey, PROTOCOL_VERSION,
  type Claim, type CoordinatorStore, type EntityHistory, type EntityRef, type HostPrincipal, type Message,
  type JsonValue, type MessageInput, type Outcome, type Receipt, type Registration,
  type RegistrationResult, type SessionAddress, type StreamFrame, type Subscription,
} from '../protocol/index.ts';

const RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const MAINTENANCE_MS = 60_000;
const COLLECTION_LIMIT = 5000;
const VERIFIED_CACHE_LIMIT = 4096;

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

type SessionRow = { id: string; host_id: string; display_name: string | null; last_seen_at: number; rank: number; cursor: number; initial_cursor: number; offered: number | null };
type MessageRow = { id: number; timestamp: number; sender: string; recipient: string; body: string; data: string; visibility: 'private' | 'public'; hash: string | null };
/** The packet identity: SHA-256 over the exact JSON text of the verified message. */
function messageHash(message: Message): string {
  return createHash('sha256').update(JSON.stringify(message)).digest('hex');
}
type ClaimRow = { key: string; owner: string; note: string | null; claimed_at: number; expires_at: number | null };
const failure = <T>(error: 'invalid_request' | 'not_found' | 'owner_conflict', message: string): Outcome<T> => ({ ok: false, error, message });
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
  private readonly rotation = new Map<string, string>();
  private readonly listeners = new Set<(hostId: string | undefined) => void>();
  /**
   * Verified Message objects by row id, so each stored hash is checked once per process.
   * Rows are never updated after commit and AUTOINCREMENT never reuses an id. The cache
   * holds no delivery state: cursors and offered marks are read from SQLite on every call.
   */
  private readonly verified = new Map<number, Message>();
  private nextMaintenance = 0;
  private readonly pendingRows: StatementSync;
  private readonly offer: StatementSync;
  private readonly sessionRow: StatementSync;
  private readonly messageRow: StatementSync;
  private readonly claimRow: StatementSync;
  private readonly entityRow: StatementSync;
  private readonly insertEntity: StatementSync;
  private readonly insertMention: StatementSync;

  constructor(path: string, namespace: string) {
    if (!isNamespace(namespace)) throw new TypeError('Invalid namespace');
    this.namespace = namespace;
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    try {
      // Durability classes. Commits run at synchronous=NORMAL: in WAL mode they survive a
      // process crash, and an OS crash or power loss can roll back the most recent ones.
      // send() commits at FULL, because a message insert lost after a success response is
      // a lost message; a FULL commit also syncs every earlier WAL frame. The offered mark
      // is connection state. A lost cursor update replays one message, which the daemon
      // deduplicates by message ID (INSERT OR IGNORE) and receipts again.
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
      this.migrateEntityMetadata();
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
      this.db.prepare('DELETE FROM sessions WHERE last_seen_at < ?').run(now - RETENTION_MS);
      this.db.prepare('DELETE FROM claims WHERE expires_at IS NOT NULL AND expires_at <= ?').run(now);
      this.db.exec(`DELETE FROM entity_sources WHERE
        (kind = 'session' AND NOT EXISTS (SELECT 1 FROM sessions WHERE id = source_key)) OR
        (kind = 'claim' AND NOT EXISTS (SELECT 1 FROM claims WHERE key = source_key))`);
      // A sender or unrelated low cursor must not pin history. Broadcasts remain
      // until every retained recipient has acknowledged, or its registration expires.
      this.db.prepare(`DELETE FROM messages WHERE id IN (
        SELECT id FROM messages WHERE timestamp < ?
          AND NOT EXISTS (SELECT 1 FROM sessions WHERE id = messages.recipient AND cursor < messages.id)
          AND (recipient != '*' OR NOT EXISTS (
            SELECT 1 FROM sessions WHERE id != messages.sender AND cursor < messages.id
          )) ORDER BY timestamp LIMIT ?
      )`).run(now - RETENTION_MS, COLLECTION_LIMIT);
      this.db.prepare(`DELETE FROM entities WHERE last_seen_at < ? AND NOT EXISTS (
        SELECT 1 FROM message_entities WHERE entity_id = entities.id
      ) AND NOT EXISTS (SELECT 1 FROM entity_sources WHERE entity_id = entities.id)`).run(now - RETENTION_MS);
    });
    this.nextMaintenance = now + MAINTENANCE_MS;
  }
  private session(id: string): SessionRow | undefined { return this.sessionRow.get(id) as SessionRow | undefined; }
  private subscription(row: SessionRow): Subscription {
    return { id: row.id, session: addressOf(row.id), hostId: row.host_id, cursor: row.cursor };
  }
  private sourceEntities(kind: 'session' | 'claim', key: string): { entities?: EntityRef[] } {
    const entities = this.db.prepare(`SELECT e.id, e.type, e.label FROM entities e JOIN entity_sources s ON s.entity_id = e.id
      WHERE s.kind = ? AND s.source_key = ? ORDER BY e.id`).all(kind, key) as EntityRef[];
    return entities.length ? { entities: entities.map(entity => ({ ...entity })) } : {};
  }
  /** Called only within the accepted mutation's transaction, never while reading or delivering. */
  private trackEntities(values: JsonValue[], source?: { kind: 'session' | 'claim'; key: string }): string[] {
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
  /** All entities for one source kind in a single joined query, grouped by source key. */
  private sourceEntitiesByKey(kind: 'session' | 'claim'): Map<string, EntityRef[]> {
    const rows = this.db.prepare(`SELECT s.source_key AS source_key, e.id, e.type, e.label FROM entities e
      JOIN entity_sources s ON s.entity_id = e.id WHERE s.kind = ? ORDER BY s.source_key, e.id`)
      .all(kind) as Array<EntityRef & { source_key: string }>;
    const byKey = new Map<string, EntityRef[]>();
    for (const row of rows) {
      const list = byKey.get(row.source_key) ?? [];
      list.push({ id: row.id, type: row.type, label: row.label });
      byKey.set(row.source_key, list);
    }
    return byKey;
  }
  private registration(row: SessionRow, entities?: EntityRef[]): RegistrationResult {
    const found = entities ?? this.sourceEntities('session', row.id).entities;
    return { subscription: this.subscription(row), ...(row.display_name === null ? {} : { displayName: row.display_name }),
      lastSeenAt: row.last_seen_at, rank: row.rank, ...(found?.length ? { entities: found } : {}) };
  }
  private message(row: MessageRow): Message {
    const cached = this.verified.get(row.id);
    if (cached) return cached;
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
      if (this.verified.size >= VERIFIED_CACHE_LIMIT) this.verified.delete(this.verified.keys().next().value!);
      this.verified.set(row.id, deepFreeze(message as Message));
      return message as Message;
    }
    if (!isMessage(message)) throw new TypeError('Invalid stored message');
    return message;
  }
  private claimValue(row: ClaimRow, entities?: EntityRef[]): Claim {
    const found = entities ?? this.sourceEntities('claim', row.key).entities;
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
        const tail = (this.db.prepare("SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'messages'), 0) AS tail")
          .get() as { tail: number }).tail;
        this.db.prepare('INSERT INTO sessions (id, host_id, display_name, last_seen_at, initial_cursor, cursor) VALUES (?, ?, ?, ?, ?, ?)')
          .run(id, host.hostId, request.displayName ?? null, now, tail, tail);
      } else {
        this.db.prepare('UPDATE sessions SET display_name = COALESCE(?, display_name), last_seen_at = ? WHERE id = ?')
          .run(request.displayName ?? null, now, id);
      }
      // Heartbeats omit the name. An explicitly supplied name is a metadata write.
      if (request.displayName !== undefined)
        this.trackEntities([request.displayName], { kind: 'session', key: id });
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
    const entities = this.sourceEntitiesByKey('session');
    return (this.db.prepare('SELECT * FROM sessions ORDER BY id').all() as SessionRow[])
      .map(row => this.registration(row, entities.get(row.id)));
  }
  listClaims(): Claim[] {
    this.maintain();
    return this.transaction(() => {
      const now = Date.now();
      this.db.prepare('DELETE FROM claims WHERE expires_at IS NOT NULL AND expires_at <= ?').run(now);
      this.db.exec("DELETE FROM entity_sources WHERE kind = 'claim' AND NOT EXISTS (SELECT 1 FROM claims WHERE key = source_key)");
      const entities = this.sourceEntitiesByKey('claim');
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
    });
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
  close(): void { this.db.close(); }
}

export function createCoordinatorStore(dbPath: string, namespace: string): CoordinatorStore {
  return new Store(dbPath, namespace);
}

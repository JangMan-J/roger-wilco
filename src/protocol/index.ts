/** Roger-Wilco wire and lane contracts, version 1. No reference-project imports. */
import { createHash } from 'node:crypto';

const ENTITY_CONTROLS = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/u;
export const PROTOCOL_VERSION = 1 as const;
/** Both raw JSON requests and normalized MessageInput JSON are bounded to 1 MiB. */
export const MAX_JSON_BYTES = 1024 * 1024;
/** Reserve space for v1's bounded namespace and session id, safe-integer metadata, defaults and SSE framing. */
export const MAX_STREAM_EVENT_BYTES = MAX_JSON_BYTES + 1024;
/** Three ids of this length still fit in MAX_STREAM_EVENT_BYTES. */
export const MAX_SESSION_ID_LENGTH = 128;
export const ROUTES = {
  health: '/health',
  register: '/v1/sessions/register',
  rank: '/v1/sessions/rank',
  entity: '/v1/entities',
  sessions: '/v1/sessions', messages: '/v1/messages', inbox: '/v1/inbox',
  claims: '/v1/claims', claimRelease: '/v1/claims/release',
  stream: '/v1/hosts/stream', receipt: '/v1/hosts/receipts',
} as const;
export type Visibility = 'private' | 'public';
/** `sessionId` is one opaque session id. */
export type SessionAddress = { namespace: string; sessionId: string };
export type SessionKey = string; // `${namespace}:${canonical session id}`; one colon; never an entity label or host ID
export type DeliveryId = string; // `${namespace}:msg:${positive SQLite message id}`
export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type FailureCode = 'invalid_request' | 'unauthorized' | 'unknown_host' | 'not_found' | 'owner_conflict' | 'temporarily_unavailable';
export type Outcome<T> = { ok: true; value: T } | { ok: false; error: FailureCode; message: string };
export type MessageInput = { recipient: SessionKey | '*'; body: string; data?: JsonValue; visibility?: Visibility };
export type Message = {
  version: typeof PROTOCOL_VERSION;
  id: DeliveryId;
  sequence: number; // safe integer, unique within namespace, monotonically increasing
  timestamp: number; // Unix milliseconds
  sender: SessionKey;
  recipient: SessionKey | '*';
  body: string;
  data: JsonValue | null;
  visibility: Visibility; // public is metadata only, not confidentiality
};
export type Subscription = { id: SessionKey; session: SessionAddress; hostId: string; cursor: number };
/** A registration names the session only. Call signs are entities that consumers associate with an address. */
export type Registration = { session: SessionAddress };
export type RegistrationResult = { subscription: Subscription; rank: number; lastSeenAt?: number };
export type Claim = { key: string; owner: SessionKey; note: string | null; claimedAt: number; expiresAt: number | null; entities?: EntityRef[] };
export type HostPrincipal = { hostId: string }; // resolved from bearer credential, never from request body
export type HostConfig = { coordinatorUrl: string; hostId: string; credential: string; inboxPath: string };
export type Receipt = { subscriptionId: SessionKey; messageId: DeliveryId };
export type StreamFrame = { type: 'message'; subscriptionId: SessionKey; message: Message };
/**
 * Adapter-supplied evidence that one message is durably persisted in the session's own store.
 * `recordId` names the record inside that store; `location` names the store. The host compares
 * both on a repeated acknowledgement and never interprets them.
 */
export type DeliveryEvidence = { messageId: DeliveryId; sessionId: string; recordId: string; location: string };
/** `type` is the legacy field name for opaque integer metadata, never identity or authority. */
export type EntityRef = { id: string; type: number; label: string };
/** An absent integer references the entity without changing its metadata. */
export type EntityMention = { id: string; type?: number; label: string };
export type EntityHistory = { entity: EntityRef; messages: Message[] };

/** Lane A. Every operation that changes registration/cursor/claim state is one SQLite transaction. */
export interface CoordinatorStore {
  /** Post-commit replay invalidation; undefined means all connected hosts. */
  subscribe(listener: (hostId: string | undefined) => void): () => void;
  register(host: HostPrincipal, request: Registration): Outcome<RegistrationResult>;
  /** User-assigned metadata (1=fleet, through 5), not an authority or routing permission. */
  setRank(host: HostPrincipal, session: SessionAddress, rank: number): Outcome<RegistrationResult>;
  listSessions(): RegistrationResult[];
  listClaims(): Claim[];
  send(host: HostPrincipal, sender: SessionAddress, input: MessageInput): Outcome<Message>;
  inbox(host: HostPrincipal, session: SessionAddress, since: number, limit: number): Outcome<Message[]>;
  entity(id: string, since: number, limit: number): Outcome<EntityHistory>;
  /**
   * Atomic tail snapshot on first registration; retry MUST return unchanged cursor.
   * `inFlight` lists subscription IDs the caller's connection already holds an offer for;
   * they are skipped before any row is read.
   */
  pending(host: HostPrincipal, limit: number, inFlight?: Iterable<string>): StreamFrame[];
  /** Only this host's subscription, and only a delivered contiguous item, advances its cursor. */
  acknowledge(host: HostPrincipal, receipt: Receipt): Outcome<Subscription>;
  claim(host: HostPrincipal, key: string, owner: SessionKey, ttlSeconds: number, note?: string): Outcome<{ claim: Claim; acquired: boolean }>;
  releaseClaim(host: HostPrincipal, key: string, owner: SessionKey): Outcome<boolean>;
  close(): void;
}

/** Adapter-facing socket client. The daemon's SQLite inbox is a separate module and does not implement this. */
export interface HostInbox {
  /** Commit hints. The adapter retains hints that arrive during reconciliation. */
  onChange(listener: () => void): () => void;
  /** Local transport grant, invalidated on disconnect. Not the caller-supplied runtime id. */
  authority(): string | undefined;
  /** One live runtime per session. The daemon clock bounds the lease; callers do not supply one. */
  bind(session: SessionAddress, runtimeId: string): Promise<Outcome<{ leaseExpiresAt: number }>>;
  renew(session: SessionAddress, runtimeId: string): Promise<Outcome<{ leaseExpiresAt: number }>>;
  release(session: SessionAddress, runtimeId: string): Promise<void>;
  /** At most one bounded page batch. The daemon stores the full pending set. */
  pending(session: SessionAddress, runtimeId: string): Promise<Message[]>;
  /** Only after the adapter observed the exact durable record in the session's current store; evidence is retained. */
  acknowledgeDelivery(session: SessionAddress, runtimeId: string, evidence: DeliveryEvidence): Promise<Outcome<null>>;
  /** Durable local registration queue: an unavailable coordinator must not block agent startup. */
  enqueueRegistration(registration: Registration): Promise<void>;
  /** Wait until the daemon finishes this registration attempt. Startup does not use this. */
  awaitRegistration(registration: Registration): Promise<Outcome<RegistrationResult>>;
  /** Daemon registration outcome for this connection's session. */
  onRegistrationResult(listener: (result: Outcome<null>) => void): () => void;
  close(): Promise<void>;
}

export type ControlHandler = (request: import('node:http').IncomingMessage, response: import('node:http').ServerResponse) => Promise<void>;

/** Lane B authentication binds a credential to one configured host. */
export interface HostAuthenticator {
  authenticate(bearer: string | null): Outcome<HostPrincipal>;
}

export function sessionKey(address: SessionAddress): SessionKey {
  if (!isSessionAddress(address)) throw new TypeError('Invalid session address');
  // Canonicalize only a UUID shape. Other ids keep the supplied case.
  const id = UUID_SESSION_ID.test(address.sessionId) ? address.sessionId.toLowerCase() : address.sessionId;
  return `${address.namespace}:${id}`;
}
export function deliveryId(namespace: string, sequence: number): DeliveryId {
  if (!isNamespace(namespace) || !Number.isSafeInteger(sequence) || sequence < 1) throw new TypeError('Invalid message identity');
  return `${namespace}:msg:${sequence}`;
}
export function entityReference(namespace: string, label: string, type = 0): EntityRef {
  if (!isNamespace(namespace) || !Number.isInteger(type) || type < 0 || type > 255 ||
      typeof label !== 'string' || !label.isWellFormed() || ENTITY_CONTROLS.test(label))
    throw new TypeError('Invalid entity reference');
  const canonical = label.normalize('NFC').trim();
  if (!canonical || canonical.length > 256) throw new TypeError('Entity label must contain 1–256 characters');
  // Keep existing generic IDs. The zero slot is fixed, not the current metadata.
  const hash = createHash('sha256').update(JSON.stringify([namespace, 0, canonical])).digest('hex').slice(0, 32);
  return { id: `${namespace}:entity:00:${hash}`, type, label: canonical };
}
export function isEntityId(value: unknown): value is string {
  return typeof value === 'string' && /^[a-z][a-z0-9-]{0,62}:entity:[0-9a-f]{2}:[0-9a-f]{32}$/.test(value);
}
/** Shared text syntax, including strings nested in JSON. No workflow or harness interpretation. */
export function extractEntities(namespace: string, ...values: JsonValue[]): EntityMention[] {
  if (!isNamespace(namespace) || !values.every(isJson)) throw new TypeError('Invalid entity text');
  const entities = new Map<string, EntityMention>();
  function visit(value: JsonValue): void {
    if (Array.isArray(value)) { for (const item of value) visit(item); return; }
    if (value !== null && typeof value === 'object') {
      for (const [key, item] of Object.entries(value)) { visit(key); visit(item); }
      return;
    }
    if (typeof value !== 'string') return;
    for (const match of value.matchAll(/(?<![\\[])\[\[([^[\]\r\n]*)\]\](?!\])/g)) {
      if (ENTITY_CONTROLS.test(match[1]!)) continue;
      const token = match[1]!.trim();
      const typed = /^(\d+):(.*)$/u.exec(token);
      try {
        const entity = entityReference(namespace, typed ? typed[2]! : token, typed ? Number(typed[1]) : 0);
        // Last explicit metadata wins; a bare mention never resets it.
        if (typed) entities.set(entity.id, entity);
        else if (!entities.has(entity.id)) entities.set(entity.id, { id: entity.id, label: entity.label });
      } catch (error) {
        if (!(error instanceof TypeError)) throw error;
      }
    }
  }
  for (const value of values) visit(value);
  return [...entities.values()];
}
export function isNamespace(value: unknown): value is string {
  return typeof value === 'string' && /^[a-z][a-z0-9-]{0,62}$/.test(value);
}
/**
 * One opaque session id: RFC 3986 unreserved characters, 1–128, case preserved.
 * No colon: keys split on the first colon. Unreserved URL characters, so a query
 * value cannot smuggle another parameter. ASCII only: Unicode case folding can change length.
 */
const SESSION_ID = new RegExp(`^[A-Za-z0-9._~-]{1,${MAX_SESSION_ID_LENGTH}}$`);
/** Legacy UUID shape. sessionKey folds a match to lowercase. Not a second accepted grammar. Do not tighten this expression. */
const UUID_SESSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export function isSessionId(value: unknown): value is string {
  return typeof value === 'string' && SESSION_ID.test(value);
}
export function isSessionAddress(value: unknown): value is SessionAddress {
  if (!record(value)) return false;
  return hasKeys(value, ['namespace', 'sessionId']) && isNamespace(value.namespace) && isSessionId(value.sessionId);
}
/** Canonical key only: a UUID segment is lowercase; any other segment is the supplied spelling. */
export function isSessionKey(value: unknown): value is SessionKey {
  if (typeof value !== 'string') return false;
  const colon = value.indexOf(':');
  const address = { namespace: value.slice(0, colon), sessionId: value.slice(colon + 1) };
  return colon > 0 && isSessionAddress(address) && value === sessionKey(address);
}
/**
 * Version 1 peers may still send `displayName`; it is accepted when it has its old
 * shape and ignored. Use `ownRegistration` before storing or forwarding.
 */
export function isRegistration(value: unknown): value is Registration {
  return record(value) && hasKeys(value, ['session', 'displayName']) && isSessionAddress(value.session) &&
    (value.displayName === undefined || (typeof value.displayName === 'string' && value.displayName.length <= 200));
}
/** The registration without fields from older peers. */
export function ownRegistration(registration: Registration): Registration {
  return { session: registration.session };
}
export function isMessageInput(value: unknown): value is MessageInput {
  return record(value) && hasKeys(value, ['recipient', 'body', 'data', 'visibility']) &&
    (value.recipient === '*' || isSessionKey(value.recipient)) &&
    typeof value.body === 'string' && value.body.length > 0 && value.body.length <= 65536 &&
    (value.visibility === undefined || value.visibility === 'private' || value.visibility === 'public') &&
    (value.data === undefined || isJson(value.data)) &&
    // Normalization can expand numeric literals; check before any store insertion,
    // including direct store calls that have no HTTP request-size guard.
    Buffer.byteLength(JSON.stringify(value)) <= MAX_JSON_BYTES;
}
/** Shape only. The host also binds `sessionId` and the message namespace to the acknowledging session. */
export function isDeliveryEvidence(value: unknown): value is DeliveryEvidence {
  return record(value) && hasKeys(value, ['messageId', 'sessionId', 'recordId', 'location']) &&
    typeof value.messageId === 'string' && isSessionId(value.sessionId) &&
    typeof value.recordId === 'string' && value.recordId.length > 0 &&
    typeof value.location === 'string' && value.location.length > 0;
}
export function isReceipt(value: unknown): value is Receipt {
  if (!record(value) || !hasKeys(value, ['subscriptionId', 'messageId']) ||
    !isSessionKey(value.subscriptionId) || typeof value.messageId !== 'string') return false;
  const match = /^([a-z][a-z0-9-]{0,62}):msg:([1-9][0-9]*)$/.exec(value.messageId);
  return !!match && Number.isSafeInteger(Number(match[2])) && match[1] === value.subscriptionId.split(':', 1)[0];
}
const FAILURE_CODES: readonly FailureCode[] = ['invalid_request', 'unauthorized', 'unknown_host', 'not_found', 'owner_conflict', 'temporarily_unavailable'];
export function isFailureCode(value: unknown): value is FailureCode {
  return typeof value === 'string' && FAILURE_CODES.includes(value as FailureCode);
}
export function isOutcome(value: unknown): value is Outcome<unknown> {
  if (!record(value)) return false;
  if (value.ok === true) return hasKeys(value, ['ok', 'value']) && Object.hasOwn(value, 'value');
  return value.ok === false && hasKeys(value, ['ok', 'error', 'message']) && isFailureCode(value.error) && typeof value.message === 'string';
}
/** Accept a successful coordinator outcome only when its value matches. */
export function outcomeValue<T>(value: unknown, accept: (value: unknown) => value is T): T | undefined {
  return isOutcome(value) && value.ok && accept(value.value) ? value.value : undefined;
}
export function isSubscription(value: unknown): value is Subscription {
  return record(value) && hasKeys(value, ['id', 'session', 'hostId', 'cursor']) &&
    isSessionKey(value.id) && isSessionAddress(value.session) && sessionKey(value.session) === value.id &&
    typeof value.hostId === 'string' && value.hostId.length > 0 &&
    Number.isSafeInteger(value.cursor) && (value.cursor as number) >= 0;
}
export function isEntityRef(value: unknown): value is EntityRef {
  return record(value) && hasKeys(value, ['id', 'type', 'label']) && isEntityId(value.id) &&
    Number.isInteger(value.type) && (value.type as number) >= 0 && (value.type as number) <= 255 &&
    typeof value.label === 'string' && value.label.length > 0 && value.label.length <= 256;
}
/**
 * A version 1 coordinator or daemon may still return `displayName` and session
 * `entities`; they are accepted when they have their old shape and ignored.
 * Use `ownRegistrationResult` before storing or returning.
 */
export function isRegistrationResult(value: unknown): value is RegistrationResult {
  return record(value) && hasKeys(value, ['subscription', 'rank', 'lastSeenAt', 'displayName', 'entities']) &&
    isSubscription(value.subscription) && Number.isInteger(value.rank) && (value.rank as number) >= 1 && (value.rank as number) <= 5 &&
    (value.lastSeenAt === undefined || (Number.isSafeInteger(value.lastSeenAt) && (value.lastSeenAt as number) >= 0)) &&
    (value.displayName === undefined || (typeof value.displayName === 'string' && value.displayName.length <= 200)) &&
    (value.entities === undefined || (Array.isArray(value.entities) && value.entities.every(isEntityRef)));
}
/** The registration result without fields from older peers. */
export function ownRegistrationResult(result: RegistrationResult): RegistrationResult {
  return { subscription: result.subscription, rank: result.rank, ...(result.lastSeenAt === undefined ? {} : { lastSeenAt: result.lastSeenAt }) };
}
export function isStreamFrame(value: unknown): value is StreamFrame {
  return record(value) && hasKeys(value, ['type', 'subscriptionId', 'message']) &&
    value.type === 'message' && isSessionKey(value.subscriptionId) && isMessage(value.message) &&
    (value.message.recipient === value.subscriptionId ||
      (value.message.recipient === '*' && value.message.sender !== value.subscriptionId &&
        value.message.sender.split(':', 1)[0] === value.subscriptionId.split(':', 1)[0]));
}
export function isMessage(value: unknown): value is Message {
  if (!record(value) || !hasKeys(value, ['version', 'id', 'sequence', 'timestamp', 'sender', 'recipient', 'body', 'data', 'visibility'])) return false;
  const { version, id, sequence, timestamp, sender, recipient, body, data, visibility } = value;
  return version === PROTOCOL_VERSION && typeof id === 'string' && Number.isSafeInteger(sequence) && (sequence as number) > 0 &&
    typeof sender === 'string' && isSessionKey(sender) && id === deliveryId(sender.split(':', 1)[0]!, sequence as number) &&
    Number.isSafeInteger(timestamp) && (timestamp as number) >= 0 && (recipient === '*' || isSessionKey(recipient)) &&
    typeof body === 'string' && body.length > 0 && body.length <= 65536 &&
    (recipient === '*' || recipient.split(':', 1)[0] === sender.split(':', 1)[0]) &&
    (visibility === 'private' || visibility === 'public') && isJson(data);
}
export function isJson(value: unknown): value is JsonValue {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  return jsonTree(value, new Set(), 0);
}
function jsonTree(value: unknown, seen: Set<object>, depth: number): boolean {
  if (depth > 32) return false;
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (!Array.isArray(value) && !record(value)) return false;
  if (seen.has(value)) return false;
  seen.add(value);
  let valid = true;
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      if (!(index in value) || !jsonTree(value[index], seen, depth + 1)) { valid = false; break; }
    }
  } else {
    valid = Object.values(value).every(item => jsonTree(item, seen, depth + 1));
  }
  seen.delete(value);
  return valid;
}
function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}
function hasKeys(value: Record<string, unknown>, keys: string[]): boolean {
  return Object.keys(value).every(key => keys.includes(key));
}

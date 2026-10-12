/** Roger-Wilco wire and lane contracts, version 1. No reference-project imports. */
export const PROTOCOL_VERSION = 1 as const;
/** Both raw JSON requests and normalized MessageInput JSON are bounded to 1 MiB. */
export const MAX_JSON_BYTES = 1024 * 1024;
/** Reserve space for v1's bounded namespace/UUID, safe-integer metadata, defaults and SSE framing. */
export const MAX_STREAM_EVENT_BYTES = MAX_JSON_BYTES + 1024;
export const ROUTES = {
  health: '/health',
  register: '/v1/sessions/register',
  rank: '/v1/sessions/rank',
  sessions: '/v1/sessions', messages: '/v1/messages', inbox: '/v1/inbox',
  claims: '/v1/claims', claimRelease: '/v1/claims/release',
  stream: '/v1/hosts/stream', receipt: '/v1/hosts/receipts',
} as const;
export type Visibility = 'private' | 'public';
export type SessionAddress = { namespace: string; piSessionId: string };
export type SessionKey = string; // `${namespace}:${piSessionId}`, never a display name or host ID
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
export type Registration = { session: SessionAddress; displayName?: string };
export type RegistrationResult = { subscription: Subscription; rank: number; displayName?: string; lastSeenAt?: number };
export type Claim = { key: string; owner: SessionKey; note: string | null; claimedAt: number; expiresAt: number | null };
export type HostPrincipal = { hostId: string }; // resolved from bearer credential, never from request body
export type HostConfig = { coordinatorUrl: string; hostId: string; credential: string; inboxPath: string };
export type PiConfig = HostConfig & { namespace: string }; // same trusted host credential; sender always bound Pi session
export type Receipt = { subscriptionId: SessionKey; messageId: DeliveryId };
export type StreamFrame = { type: 'message'; subscriptionId: SessionKey; message: Message };
export type PiReceipt = { messageId: DeliveryId; piSessionId: string; entryId: string; sessionFile: string };

/** Lane A. Every operation that changes registration/cursor/claim state is one SQLite transaction. */
export interface CoordinatorStore {
  register(host: HostPrincipal, request: Registration): Outcome<RegistrationResult>;
  /** User-assigned metadata (1=fleet, through 5), not an authority or routing permission. */
  setRank(host: HostPrincipal, session: SessionAddress, rank: number): Outcome<RegistrationResult>;
  listSessions(): RegistrationResult[];
  listClaims(): Claim[];
  send(host: HostPrincipal, sender: SessionAddress, input: MessageInput): Outcome<Message>;
  inbox(host: HostPrincipal, session: SessionAddress, since: number, limit: number): Outcome<Message[]>;
  /** Atomic tail snapshot on first registration; retry MUST return unchanged cursor. */
  pending(host: HostPrincipal, limit: number): StreamFrame[];
  /** Only this host's subscription, and only a delivered contiguous item, advances its cursor. */
  acknowledge(host: HostPrincipal, receipt: Receipt): Outcome<Subscription>;
  claim(host: HostPrincipal, key: string, owner: SessionKey, ttlSeconds: number, note?: string): Outcome<{ claim: Claim; acquired: boolean }>;
  releaseClaim(host: HostPrincipal, key: string, owner: SessionKey): Outcome<boolean>;
  close(): void;
}

/** Lane D owns one shared SQLite inbox per host; never an inbound listener. */
export interface HostInbox {
  /** Commit before returning. Dedup lasts until seven days after Pi ACK; later replay checks Pi's JSONL. */
  receive(frame: StreamFrame): Promise<{ inserted: boolean }>;
  /** One live runtime per session; lease expiration must preserve pending messages. */
  bind(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>>;
  renew(session: SessionAddress, runtimeId: string, now: number): Promise<Outcome<{ leaseExpiresAt: number }>>;
  release(session: SessionAddress, runtimeId: string): Promise<void>;
  pending(session: SessionAddress, runtimeId: string): Promise<Message[]>;
  /** Only after exact JSONL entry observed and file fsynced by lane E; identity is retained. */
  acknowledgePi(session: SessionAddress, runtimeId: string, proof: PiReceipt): Promise<Outcome<null>>;
  /** Durable local registration/outbox queue: unavailable coordinator must not block Pi startup. */
  enqueueRegistration(registration: Registration): Promise<void>;
  close(): Promise<void>;
}

export type CreateCoordinatorStore = (dbPath: string, namespace: string) => CoordinatorStore;
export type CreateHostInbox = (path: string) => HostInbox;
export type ControlHandler = (request: import('node:http').IncomingMessage, response: import('node:http').ServerResponse) => Promise<void>;
export type CreateControlHandler = (store: CoordinatorStore, auth: HostAuthenticator) => ControlHandler;
export type CreateStreamHandler = (store: CoordinatorStore, auth: HostAuthenticator) => ControlHandler;
export type CreateHostDaemon = (config: HostConfig, inbox: HostInbox) => { start(signal: AbortSignal): Promise<void>; stop(): Promise<void> };

/** Lane C uses SQLite replay, not an event-only in-memory queue. */
export interface StreamDelivery {
  subscribe(host: HostPrincipal, signal: AbortSignal): AsyncIterable<StreamFrame>;
  acknowledge(host: HostPrincipal, receipt: Receipt): Outcome<Subscription>;
}
/** Lane B authentication binds a credential to one configured host. */
export interface HostAuthenticator {
  authenticate(bearer: string | null): Outcome<HostPrincipal>;
}
/** Lane E submits a Pi message then observes the persistent session file, never ACKs the void sendMessage call. */
export interface PiDelivery {
  bind(session: SessionAddress, runtimeId: string): Promise<Outcome<null>>;
  stop(): Promise<void>;
}

export function sessionKey(address: SessionAddress): SessionKey {
  if (!isSessionAddress(address)) throw new TypeError('Invalid session address');
  return `${address.namespace}:${address.piSessionId.toLowerCase()}`;
}
export function deliveryId(namespace: string, sequence: number): DeliveryId {
  if (!isNamespace(namespace) || !Number.isSafeInteger(sequence) || sequence < 1) throw new TypeError('Invalid message identity');
  return `${namespace}:msg:${sequence}`;
}
export function isNamespace(value: unknown): value is string {
  return typeof value === 'string' && /^[a-z][a-z0-9-]{0,62}$/.test(value);
}
export function isSessionAddress(value: unknown): value is SessionAddress {
  if (!record(value)) return false;
  return hasKeys(value, ['namespace', 'piSessionId']) && isNamespace(value.namespace) && typeof value.piSessionId === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.piSessionId);
}
export function isSessionKey(value: unknown): value is SessionKey {
  if (typeof value !== 'string') return false;
  const colon = value.indexOf(':');
  const address = { namespace: value.slice(0, colon), piSessionId: value.slice(colon + 1) };
  return colon > 0 && isSessionAddress(address) && value === sessionKey(address);
}
export function isRegistration(value: unknown): value is Registration {
  return record(value) && hasKeys(value, ['session', 'displayName']) && isSessionAddress(value.session) &&
    (value.displayName === undefined || (typeof value.displayName === 'string' && value.displayName.length <= 200));
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
export function isReceipt(value: unknown): value is Receipt {
  if (!record(value) || !hasKeys(value, ['subscriptionId', 'messageId']) ||
    !isSessionKey(value.subscriptionId) || typeof value.messageId !== 'string') return false;
  const match = /^([a-z][a-z0-9-]{0,62}):msg:([1-9][0-9]*)$/.exec(value.messageId);
  return !!match && Number.isSafeInteger(Number(match[2])) && match[1] === value.subscriptionId.split(':', 1)[0];
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

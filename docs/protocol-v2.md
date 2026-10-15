# Delivery protocol version 2

Basis: `~/Projects/roger-wilco`, branch `claude`, at `95ef674` ("Coordinator: commit claims at FULL"). The three candidate designs were written against `2357460`. Several of their premises no longer hold on `95ef674`:

- The coordinator connection already defaults to `synchronous=NORMAL`. `send` and `claim` already commit at FULL (`store.ts:115-122`, `transaction(..., full)` at `:176-186`, `send` at `:427`, `claim` at `:513-534`; `contract.md:394`).
- `register` no longer notifies the stream (`store.ts:355-356`).
- The daemon already keeps leases in memory. It also has a per-lifetime `registered` map that `reportRegistration` clears when the coordinator refuses a registration (`host/index.ts:47-51`, `:191-196`).
- After an OS crash, a NORMAL cursor update can roll back over several acknowledged messages (`contract.md:397`).

A separate branch removes the registration `displayName` field: registration becomes `{ session }`, and an incoming `displayName` is accepted and ignored. Nothing in this spec depends on display names.

This spec starts from design 0 and fixes every fatal flaw the judges found:

| Flaw | Fix |
|---|---|
| The lifetime registration entry is never cleared | Cleared on presence `unknown` and on `leave` (§Presence) |
| The daemon expires sessions on its own clock, and the visibility gate hides them | Removed. Membership comes only from the coordinator, on the stream. There is no daemon session table, visibility gate, backfill or `inbox_broadcasts` |
| A registration completes between the snapshot and the hello | The same removal fixes it |
| The v1 fallback is hidden by the visibility gate | The gate is removed |
| An idle host pins collection | Coordinator `through` frames, plus per-message collection |
| An open with `after` moves the receipted copy forward on the daemon's word | The coordinator never moves its copy forward on a daemon claim |
| The bookmark is `message.sequence` | Frames carry `seq` separately from the message |
| Settings live in `coordinator.json` | Settings move to a `.conf` file |

Taken from design 1: membership frames from the same snapshot as the messages; `through` frames; bounded scans; connection-bound receipts. Taken from design 2: `stream` log identity; `resume ≤` the coordinator's own copy; fail on a dedup hash conflict.

Rejected because they add complexity with no need: the uptime clock, the epoch (including a per-host membership epoch), `inbox_id`, `collected_through`/`history_gap`, `route_host` and its trigger, the members route, daemon session and broadcast tables, a `prev` chain, a reserved `path` field, an offered-boundary set, a `foreign` presence result, and a daemon history window derived from `retention`.

## Goals and non-goals

### Goals

1. One bookmark per host, kept durably by the daemon. It replaces per-session coordinator cursors for v2 hosts.
2. Up to W message frames in flight per host stream. One cumulative receipt, "stored through N", covers them.
3. Batched daemon commits: one FULL transaction per batch, with the bookmark in the same transaction.
4. A broadcast crosses the wire once per host. The daemon copies it locally to every member whose start is below the broadcast's sequence, except the sender.
5. Presence is one request per host per interval. Per-session heartbeat POSTs stop.
6. Collection is bounded by the slowest relevant host bookmark. An idle host does not pin history.
7. v2 runs beside v1. Migration from v1 cursors loses nothing. It adds no duplicates beyond what message-ID dedup absorbs.

### Non-goals

- No change to the message envelope. `Message.version` stays 1, so the JSON bytes, the SHA-256 packet hash and the `ns:msg:<n>` ids are identical over v1 and v2.
- No change to the socket protocol (`HostInbox`), leases, delivery evidence or session acknowledgement.
- No change to registration expiry semantics. Expiry stays wall-clock `last_seen_at`, as in v1.
- No change to the delivery contract. Broadcasts wake no session.
- No new `FailureCode`.
- No peering. Peering is deferred (§Deferred).
- No consumer- or harness-specific concept in the core.

### Terms

| Term | Meaning |
|---|---|
| `seq` | Coordinator log position = `messages.id`. In v2, for a local-origin message, `seq === message.sequence`. |
| `start(s)` | The point after which session `s` is owed messages. For a v2 host it is `sessions.cursor`, frozen. For a new session it is the tail at registration. For a migrated session it is its v1 cursor. |
| Owed to host h | A directed `m` to a member `s` of h with `start(s) < m.seq`; or a broadcast `m` with some member `s` of h where `start(s) < m.seq` and `s ≠ m.sender`. |
| `D` | The daemon bookmark, durable in the inbox. |
| `C` | The coordinator's receipted copy, `hosts.acked`. |
| Position frame | A `msg` or `through` frame. |
| `sentThrough` | The highest position the coordinator has sent on a connection. It starts at `resume`. |

## Invariants

Each invariant is tested in §Test plan.

- **I1 Prefix-committed log.** The coordinator is one process with one SQLite connection. Every write is a synchronous `BEGIN IMMEDIATE` transaction with no `await` inside, so the committed ids always form a prefix. `sqlite_sequence.seq` is a committed tail. "Stored through N" therefore has one meaning.
- **I2 Relevance is fixed at send time.** A new session's `start` is the tail when its registration commits. A message sent earlier is never owed to it. A session cannot change host while it is registered (`owner_conflict`).
- **I3 No skip.** `D` means that every message owed to this host with `seq ≤ D` is committed in this inbox. `D` advances only inside the transaction that stores those rows.
- **I4 Commit before receipt.** A receipt `through = N` is sent only after a FULL commit with `D ≥ N`.
- **I5 Connection-bound receipts.** The coordinator advances `C` only to a value no higher than `sentThrough` of the live connection that the receipt names. The daemon posts only positions it has committed. It commits position frames in order, from one ordered connection that aborts on any event it cannot parse. Between consecutive position frames, nothing is owed. `C` never decreases, except through the operator rollback or restore in §Migration. **The coordinator never sets `C` from a daemon claim** (`after`). Therefore `C ≤ D` always holds.
- **I6 One membership truth.** Which sessions a host serves, and their starts, comes only from the coordinator `sessions` table. The table is read in the same snapshot as the messages it filters. It is sent as `member`/`leave` frames, diffed against the membership last sent on the connection, ahead of the messages they govern.
- **I7 Per-session order.** A session's rows are inserted in ascending `seq`. No later step inserts a lower `seq` for that session. There is no backfill.
- **I8 Exact copies; broadcasts wake no session.** A broadcast copy is the exact `message_json` and hash, with `recipient: '*'`. Adapters decide wake from `message.recipient` (`pi/delivery.ts`), so broadcasts wake nothing. The daemon's `changed` hint stays a transport hint.
- **I9 Exact acknowledgement is unchanged.** Bookmarks, receipts and presence never count as session acknowledgement.
- **I10 Collection safety.** A message owed to a registered session and not yet stored on its host is never collected.

## Wire (routes, frames, receipts, version negotiation)

### Version negotiation

The route selects the version. There is no handshake field.

- **Coordinator.** A v2 coordinator serves `/v1/hosts/*` and `/v2/hosts/*` from one `active` map keyed by `hostId`.
  - A host with a `hosts` row is on v2. For such a host, `GET /v1/hosts/stream` and `POST /v1/hosts/receipts` return `409 owner_conflict` "Host uses protocol 2". The stream route answers **before** it supersedes any live connection, so a stale v1 daemon cannot kill a v2 stream.
  - A v2 open supersedes any live v1 or v2 connection for the host.
- **Daemon.**
  - **Bookmark NULL.** The daemon sends `GET /v2/hosts/stream` with **no query**.
    - A v1 coordinator routes `/v2/*` to the control handler. After authentication it returns `404 not_found` "Unknown route" (`control/index.ts`). On that 404 the daemon issues one v1 stream request with the unchanged v1 loop. When that stream ends, the next attempt tries `/v2` again after the normal backoff.
    - A query on that first request would get `400 invalid_request` from `closedQuery` instead, so the first request MUST carry no query.
  - **Bookmark present.** The daemon MUST NOT fall back. It reports 404, 400 and 409, then backs off and retries.
- **Health.** `GET /health` returns `{service:'roger-wilco', version:1, presenceWindowMs:P}`.

### Routes

| Request | Body / query | Result |
|---|---|---|
| `GET /v2/hosts/stream[?stream=S&after=D]` | `Accept: text/event-stream`. `stream` and `after` appear together or not at all, each at most once. Any other key returns 400. `stream` matches `^[0-9a-f]{32}$`. `after` is a decimal safe integer ≥ 0. | `openHost` runs **before any SSE header**. On failure, it returns a JSON `Outcome` (below). On success: SSE, with `hello` first. |
| `POST /v2/hosts/receipts` | `HostReceipt` | `Outcome<HostReceiptResult>` |
| `POST /v2/hosts/presence` | `Presence` | `Outcome<PresenceResult>` |
| `POST /v1/sessions/register` | unchanged | Unchanged. For a v2 host, `subscription.cursor` is the frozen `start`. |
| `/v1/hosts/stream`, `/v1/hosts/receipts` | unchanged | Unchanged for v1 hosts. 409 for v2 hosts. |
| All other routes | unchanged | unchanged |

`src/bin/coordinator.ts:20` sends `ROUTES.stream`, `ROUTES.receipt`, `ROUTES.streamV2` and `ROUTES.receiptV2` to the stream handler. `ROUTES.presence` uses only the store, so it goes to the control handler.

Stream open errors (JSON, no SSE headers):

| Condition | Result |
|---|---|
| Bad query | `400 invalid_request` |
| `stream` ≠ `store_config.stream_id` | `409 owner_conflict` "Bookmark belongs to another coordinator log" |
| `after > tail` | `409 owner_conflict` "Bookmark is ahead of the coordinator tail" |
| `after < C` (row existed before this open) | `409 owner_conflict` "Bookmark is behind the receipted position" |
| Auth | `401` / `403`, as today |

### Frames

Every v2 event is `event: v2` with one `data:` line of JSON. The v1 parser ignores events whose type is not `message` (`host/index.ts:532`). There is no SSE `id:` line, and `Last-Event-ID` is ignored. Keepalive is the comment `: keepalive`. Validators accept exact keys only and safe integers only.

| `type` | Fields | Meaning |
|---|---|---|
| `hello` | `stream` (32 lowercase hex), `host`, `conn` (lowercase UUID), `resume` (≥ 0), `keepaliveMs`, `presenceWindowMs` | Always the first event. `keepaliveMs` and `presenceWindowMs` must lie within the fixed bounds of §Settings (`SETTING_BOUNDS`). |
| `member` | `session` (SessionKey), `start` (int ≥ 0) | Adds a member or replaces its start. Takes effect for the frames that follow. |
| `leave` | `session` | The session is no longer registered to this host. Stop fan-out to it. Its local rows are kept. |
| `msg` | `seq` (> 0), `message` (v1 `Message`) | A message owed to this host. |
| `through` | `seq` (> 0) | Nothing else is owed to this host up to `seq`. |

The daemon enforces these rules. Any violation aborts the connection. The current batch is not committed and no receipt is sent.

1. `hello` comes first and exactly once. `hello.host === config.hostId`.
2. Position frames have strictly increasing `seq`. The first one has `seq > hello.resume`, and each later one has `seq >` the previous position frame's `seq`.
3. `msg`: `isMessage(message)`, `seq === message.sequence` (local origin, v2), and the sender's namespace equals the stream namespace. A directed `msg` must name a current member `s` with `start(s) < seq`. A broadcast `msg` may have no eligible local member.
4. An invalid JSON payload, an unknown `type`, or an oversized event (more than the existing `MAX_STREAM_EVENT_BYTES = MAX_JSON_BYTES + 1024`) aborts the connection. In v1 the parser skipped such a frame and moved on (`host/index.ts:535-539`). v2 does not. A v2 `msg` frame has less overhead than a v1 frame, because it has no `id:` line and no `subscriptionId`.

Example:

```
GET /v2/hosts/stream?stream=7f3c0a1e9b2d4c6f8a0b1c2d3e4f5061&after=120

event: v2
data: {"type":"hello","stream":"7f3c0a1e9b2d4c6f8a0b1c2d3e4f5061","host":"host-a","conn":"6f1c2d3e-0a9b-4c8d-9e7f-102938475665","resume":118,"keepaliveMs":15000,"presenceWindowMs":90000}

event: v2
data: {"type":"member","session":"fleet:550e8400-e29b-41d4-a716-446655440000","start":118}

event: v2
data: {"type":"msg","seq":121,"message":{"version":1,"id":"fleet:msg:121","sequence":121,"timestamp":1760500000000,"sender":"fleet:a1","recipient":"*","body":"Build is green.","data":null,"visibility":"private"}}

event: v2
data: {"type":"through","seq":131}

POST /v2/hosts/receipts {"conn":"6f1c2d3e-0a9b-4c8d-9e7f-102938475665","through":131}
-> {"ok":true,"value":{"through":131}}
```

In the example the daemon's `D` is 120 and the coordinator's `C` is 118. The coordinator resumes at its own copy, and the daemon skips frames at or below 120.

### Receipts

`POST /v2/hosts/receipts` with body `{conn, through}`. The coordinator checks, in order:

1. Authentication gives host `h`. The body is an exact `HostReceipt`, `conn` is a UUID, and `through` is a safe integer ≥ 0. Otherwise `400`.
2. If there is no `hosts` row for `h`: `409` "Host is not on protocol 2".
3. If `through ≤ C`: `200 {through: C}`. Nothing changes. This covers an idempotent retry and a late receipt from any connection.
4. If `active[h]` is absent, is v1, or has `conn ≠ body.conn`: `409 owner_conflict` "Superseded connection". After a coordinator restart every `conn` is unknown and gets this answer.
5. If `through > live.sentThrough`: `400` "Receipt is ahead of the sent position".
6. Otherwise: `C := through` (`UPDATE hosts SET acked = :n WHERE host_id = :h AND acked < :n`, NORMAL). Then set `live.acked := through`, release every in-flight `msg` entry with `seq ≤ through` (window count and bytes), wake this connection, and return `200 {through}`.

Meaning: "this inbox has durably stored every message owed to this host with `seq ≤ through`." The receipt is cumulative. Any value in `(C, sentThrough]` is sound, because nothing is owed between two consecutive position frames, and the daemon commits position frames in order (I5). An honest daemon posts only positions it has committed.

### Presence

`POST /v2/hosts/presence` with body `{sessions: SessionKey[]}`. The keys must be canonical and unique, and there may be at most `MAX_PRESENCE_KEYS = 4096`.

Reply: `{unknown: SessionKey[]}`. It lists every key that was not refreshed on this host, because the key is absent or registered to another host.

The coordinator never creates a session from presence, never wakes a stream, and never changes a start. A host with no `hosts` row gets `409`.

### Protocol module additions (`src/protocol/index.ts`)

```ts
export const HOST_STREAM_VERSION = 2 as const;          // PROTOCOL_VERSION (envelope) stays 1
export const MAX_PRESENCE_KEYS = 4096;
/** Fixed bounds and defaults; the one source for the settings parser and isHostFrame. */
export const SETTING_BOUNDS = {
  window:           { min: 1,          max: 256,         default: 32 },
  retentionMs:      { min: 86_400_000, max: 7_776_000_000, default: 604_800_000 },
  keepaliveMs:      { min: 5_000,      max: 60_000,      default: 15_000 },
  presenceWindowMs: { min: 15_000,     max: 900_000,     default: 90_000 },
} as const;
// ROUTES += streamV2:'/v2/hosts/stream', receiptV2:'/v2/hosts/receipts', presence:'/v2/hosts/presence'
export type HostFrame =
  | { type: 'hello'; stream: string; host: string; conn: string; resume: number;
      keepaliveMs: number; presenceWindowMs: number }
  | { type: 'member'; session: SessionKey; start: number }
  | { type: 'leave'; session: SessionKey }
  | { type: 'msg'; seq: number; message: Message }
  | { type: 'through'; seq: number };
export type HostReceipt = { conn: string; through: number };
export type HostReceiptResult = { through: number };
export type Presence = { sessions: SessionKey[] };
export type PresenceResult = { unknown: SessionKey[] };
```

New validators: `isHostFrame` (which checks `stream` against `^[0-9a-f]{32}$` and the hello values against `SETTING_BOUNDS`), `isHostReceipt`, `isHostReceiptResult`, `isPresence`, `isPresenceResult`. `MAX_STREAM_EVENT_BYTES` is reused unchanged.

## Coordinator store

### Schema delta

The delta is applied in one constructor transaction. Like the existing migrations (`store.ts:124-134`), each step is guarded:

```sql
CREATE TABLE IF NOT EXISTS hosts (
  host_id TEXT PRIMARY KEY,          -- a row means: this host is on protocol 2
  acked   INTEGER NOT NULL           -- C(h): receipted "stored through"
);
-- only if PRAGMA table_info(store_config) lacks stream_id:
ALTER TABLE store_config ADD COLUMN stream_id TEXT;
UPDATE store_config SET stream_id = :random32hex WHERE stream_id IS NULL;
```

There are no other schema changes. For a host with a `hosts` row, `sessions.cursor` is the frozen `start` and `sessions.offered` is unused. No new per-session column exists. There is no in-memory membership cache. Membership is read from `sessions` in every batch.

The store takes its retention from settings: `createCoordinatorStore(path, namespace, { retentionMs })`, replacing `RETENTION_MS`.

### Sync classes (fixed in code)

The connection default stays `synchronous=NORMAL`, as today. FULL is set **before** `BEGIN` and restored after `COMMIT`. Otherwise node:sqlite throws "Safety level may not be changed inside a transaction" (`store.ts:175`).

| Transaction | Class | Reason |
|---|---|---|
| `send` | FULL (existing) | A lost insert after success is a lost message. It also syncs every earlier WAL frame. |
| `claim` | FULL (existing since `95ef674`) | A granted claim lost to power failure could be granted to another owner. |
| `openHost` that **creates** the `hosts` row (cut-over) | FULL | It freezes cursors and starts refusing v1 receipts. |
| `register`, rank, claim release | NORMAL (unchanged) | WAL loses only a suffix. Any `send` routed by a registration syncs it. Presence `unknown` repairs a lost registration of a leased session. |
| v2 receipt, presence, maintenance | NORMAL | Losing one of these only lowers `C`, ages `last_seen_at` or delays collection. Each case is safe (§Crash and reconnect). |

### `openHost(host, {stream?, after?}) → Outcome<HostStart>`

This runs as one `BEGIN IMMEDIATE` transaction. It runs at FULL exactly when no `hosts` row exists. That check runs synchronously just before `BEGIN`, with no `await` in between.

```
tail := COALESCE((SELECT seq FROM sqlite_sequence WHERE name='messages'), 0)
if stream given and stream ≠ store_config.stream_id → owner_conflict 'Bookmark belongs to another coordinator log'
if after given and after > tail                     → owner_conflict 'Bookmark is ahead of the coordinator tail'
row := SELECT acked FROM hosts WHERE host_id = :h
if row absent:                                         -- cut-over from v1, or a first-ever v2 host
   C := COALESCE((SELECT MIN(cursor) FROM sessions WHERE host_id = :h), tail)
   if after given: C := min(C, after)                  -- lower C only replays more; never loses
   INSERT INTO hosts(host_id, acked) VALUES (:h, :C)
else:
   C := row.acked
   if after given and after < C → owner_conflict 'Bookmark is behind the receipted position'
minStart := (SELECT MIN(cursor) FROM sessions WHERE host_id = :h)        -- NULL if no sessions
resume   := max(C, minStart ?? tail)                   -- coordinator's own knowledge; never written to C
return { C, resume, stream: store_config.stream_id }
```

`after > C` is accepted, but the stream resumes from the coordinator's knowledge, never from `after`. The daemon skips replayed frames `≤ D`. A faulty daemon therefore cannot make the coordinator skip or collect anything.

### `hostBatch(host, from, n) → HostBatch`

`maintain()` runs first, as `pending` does today. Then one read transaction (`BEGIN DEFERRED … COMMIT`):

```
tail := sqlite_sequence('messages')
members := SELECT id, cursor AS start FROM sessions WHERE host_id = :h ORDER BY id
upto := min(tail, from + SCAN_SPAN)                    -- SCAN_SPAN = 4096, fixed
rows :=
  SELECT m.* FROM sessions s JOIN messages m ON m.recipient = s.id
    WHERE s.host_id = :h AND m.id > :from AND m.id <= :upto AND m.id > s.cursor
  UNION ALL
  SELECT m.* FROM messages m
    WHERE m.recipient = '*' AND m.id > :from AND m.id <= :upto
      AND EXISTS (SELECT 1 FROM sessions s WHERE s.host_id = :h AND s.cursor < m.id AND s.id <> m.sender)
  ORDER BY id LIMIT :n
scanned := rows.length === n ? rows.at(-1).id : upto
return { members, rows: rows.map(message), scanned, tail }   -- message(row) does the hash-equality check
```

Both branches use `messages_recipient(recipient, id)` and `sessions_host(host_id, id)`. If a host's only eligible session for a broadcast is its sender, the host gets no frame for that broadcast, and `through` covers it. The work per call is bounded by `SCAN_SPAN` ids, however far behind the host is. The handler loop (below) keeps calling until `scanned` reaches `tail`, so a host far behind crosses the gap within one wake.

### Other store methods

- **`advanceHost(host, through)`.** NORMAL, monotone update as in §Receipts. Returns the new `C`.
- **`hostAcked(host): number | undefined`.**
- **`presence(host, keys)`.** One NORMAL transaction:
  - For each key, `UPDATE sessions SET last_seen_at = :now WHERE id = :k AND host_id = :h`.
  - A key with 0 changes goes to `unknown`.
  - No `notify`.
- **`register`.** Unchanged: a new session gets `cursor = initial_cursor = tail`, NORMAL, no notify.
- **`acknowledge` (v1).** Inside its transaction, if a `hosts` row exists for `row.host_id`, it returns `owner_conflict` "Host uses protocol 2". This method and `openHost` are both `BEGIN IMMEDIATE` in one process, so a v1 receipt is ordered strictly before the cut-over (and counted in `MIN(cursor)`) or strictly after it (and refused).
- **`pending` (v1).** Unchanged. The v1 stream refuses v2 hosts at open, using `hostAcked(host) !== undefined`.

### Collection (`maintain`, every 60 s at most, NORMAL)

```sql
-- expiry, unchanged rule
DELETE FROM sessions WHERE last_seen_at < :cutoff;
-- messages: one rule for v1 and v2 hosts
DELETE FROM messages WHERE id IN (
  SELECT m.id FROM messages m WHERE m.timestamp < :cutoff AND NOT EXISTS (
    SELECT 1 FROM sessions s LEFT JOIN hosts h ON h.host_id = s.host_id
    WHERE s.cursor < m.id AND COALESCE(h.acked, -1) < m.id
      AND (s.id = m.recipient OR (m.recipient = '*' AND s.id <> m.sender)))
  ORDER BY m.timestamp LIMIT 5000);
-- claims, entity_sources, entities: unchanged
```

`:cutoff = now − retention`.

- **v1 hosts.** `acked` is NULL, so the rule reduces exactly to today's (`store.ts:239-245`). Unrelated low cursors still do not keep old messages.
- **v2 hosts.** A message owed to `s` on h and not yet stored has `m.id > start(s)` and `m.id > D ≥ C`, so it is kept (I10).
- **Idle hosts.** An idle connected host advances `C` through `through` frames. A host with no sessions pins nothing. An offline host pins only messages owed to its sessions, until those sessions expire. This is the same as v1.

### Stream handler (`src/server/delivery/index.ts`)

`createStreamHandler(store, auth, settings)` takes `W` and `K` (keepalive) from settings. The `Live` record gains these fields:

```
{ version: 1 | 2, conn, acked, sentThrough, scanned,
  sentMembers: Map<SessionKey, number>,          // membership last written on this connection
  inflight: Array<{ seq: number; bytes: number }>, // unreceipted msg frames, ascending
  inflightBytes }
```

`room` means `inflight.length < W and inflightBytes < 16 MiB`. The v1 path is unchanged.

**Open.**
1. Authenticate and parse the query (`stream` and `after` only).
2. Call `store.openHost`. On `!ok`, return the JSON error.
3. Supersede `active[h]`, whatever its version.
4. Set `conn = randomUUID()`, `acked = C`, `sentThrough = scanned = resume`, `sentMembers = new Map()`, `inflight = []`, `dirty = true`.
5. Write `hello`.

**Loop**, while not aborted:
1. If `dirty` and `room`: set `dirty := false`, yield with `await setImmediate()` (`node:timers/promises`), then call `batch := hostBatch(h, scanned, W − inflight.length)`.
2. Diff `batch.members` against `sentMembers`. Write `leave` for keys that are gone, and `member` for keys that are new or whose start has changed. Then set `sentMembers := batch.members`.
3. Write each row as `msg`. Push `{seq, bytes}` to `inflight`, add the bytes to `inflightBytes`, and set `sentThrough := seq`. Stop early at the byte cap, but always send at least one frame.
4. Set `scanned := batch.scanned` (or the last written `seq` if the loop stopped early).
5. If the batch wrote any `msg` and `scanned > sentThrough`, write `through(scanned)` and set `sentThrough := scanned`.
6. If `scanned < batch.tail` and `room` still holds, set `dirty := true`. Continue.
7. Wait. The wait returns at once only when `dirty` **and** `room` both hold. A `dirty` flag set while the window or byte cap is full does not end the wait. Otherwise the wait ends on a wake, an abort or the keepalive deadline K.
   - **Wakes** set `dirty` and end the wait: a directed send to this host, any broadcast, or an accepted receipt on this connection. Registration and presence do not wake.
   - **On the keepalive tick:** if `room`, run one batch (steps 1–6). Then, if `scanned > sentThrough`, write `through(scanned)` and set `sentThrough := scanned`. If nothing was written for K, write `: keepalive`.

Writes use the existing `write()` with drain and the 5 s deadline. A failed write aborts the connection, so a stalled host never blocks another. The `setImmediate` yield before every batch lets receipts and other hosts' requests run while one host crosses a large gap.

## Daemon store, fan-out, and start points

### Inbox schema 4

The 3→4 step runs in the constructor transaction, after the earlier steps. `migrateNeutralEvidence` writes `PRAGMA user_version = 3` explicitly, not `${SCHEMA_VERSION}`, and the local `version` becomes 3 after it. The 3→4 step then runs for every opened version below 4:

```sql
CREATE TABLE inbox_stream (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  stream_id TEXT,                  -- coordinator log the bookmark belongs to
  bookmark INTEGER,                -- D; NULL until the first v2 hello commits
  CHECK ((stream_id IS NULL) = (bookmark IS NULL))
);
INSERT INTO inbox_stream (id) VALUES (1);
PRAGMA user_version = 4;
```

- The version check becomes `{0, 2, 3, 4}`.
- A pre-v2 daemon fails closed on 4 through its existing "Unsupported inbox schema version" check.
- `inbox_messages`, `inbox_registrations` and the fence views are unchanged. Broadcast copies stay one row per session, so `pending`, `ack` and evidence do not change.
- Membership is not stored. It belongs to the connection and is rebuilt from the coordinator snapshot on every connect (I6).
- `deploy/rollback-inbox.ts` gains `--stream-clients-stopped INBOX-PATH`, which runs the 4→3 step (drop `inbox_stream`, set `user_version = 3`). `--legacy-clients-stopped` and `--contract-clients-stopped` also accept version 4 and run the 4→3 step first.

### Connect and hello

1. If `bookmark IS NULL`, the daemon sends `GET /v2/hosts/stream` with no query. Otherwise it sends `?stream=<stream_id>&after=<bookmark>`. The header deadline is 10 s.
2. Response handling:
   - `404` with a NULL bookmark: issue one v1 stream request with the unchanged v1 loop. When it ends, the next attempt tries `/v2` after the normal backoff.
   - Any other non-SSE reply: report, back off between 250 ms and 10 s, and retry.
3. The first event must be a valid `hello` within 10 s. Checks:
   - `host === config.hostId`;
   - if `stream_id` is stored, `hello.stream === stream_id`.
4. Then one FULL transaction:
   ```
   stream_id := hello.stream
   bookmark  := max(bookmark ?? 0, hello.resume)
   ```
   `resume > D` is accepted, because the coordinator has stated that nothing is owed up to `resume`. `resume < D` means the coordinator copy lags, and the replayed frames `≤ D` are skipped.
5. In memory: `members := new Map()`, `last := hello.resume`, `lastReceipted := hello.resume`, idle deadline `3 × keepaliveMs`. The presence timer is (re)armed at `presenceWindowMs / 3`, with the first tick immediately, and `inbox.presenceActive := true` (§Registration).

### Ingest: batching, start-point filter, local fan-out

The SSE parser yields one array of complete events per `reader.read()` chunk, cut at 256 frames or 8 MiB of frame JSON. Each array is one `BEGIN IMMEDIATE` transaction at FULL.

```
touched := ∅
for f in batch, in order:
  member : members.set(f.session, f.start)
  leave  : members.delete(f.session); inbox.forgetRegistration(f.session)
  msg | through:
     require f.seq > last                                   else VIOLATION('position order')
     last := f.seq
     if f.seq <= D: continue                                -- replay of stored range: no write
     if msg:
        json := JSON.stringify(f.message); hash := sha256(json)
        targets := f.message.recipient === '*'
           ? [s for (s, start) in members if start < f.seq and s ≠ f.message.sender]
           : (members.get(rcpt) < f.seq ? [rcpt] : VIOLATION('directed message for a non-member'))
        for s in targets:
           INSERT INTO inbox_messages (session_key, message_id, sequence, message_json, hash)
             VALUES (s, f.message.id, f.seq, json, hash) ON CONFLICT(session_key, message_id) DO NOTHING
           if changes = 0 and (SELECT hash ... WHERE session_key=s AND message_id=id) ≠ hash
              → VIOLATION('message identity conflict')
           else if changes = 1: touched += s
     D := f.seq
UPDATE inbox_stream SET bookmark = D
COMMIT
then: changed(s) once per s in touched; receipts.offer(conn, last)
VIOLATION: ROLLBACK; restore `last` to its value before the batch; report; abort connection; D unchanged; reconnect with backoff
```

- **Start-point filter.** `start(s) < seq`, and for a broadcast also `s ≠ sender`. This is the same predicate the coordinator applied in the same snapshot (§hostBatch), so the two cannot disagree. A session that registers mid-stream gets its `member` frame in the first batch read after its registration commits. Every message owed to it has `seq > start ≥` every earlier scan (I2), so the frame always arrives before such a message, and nothing needs backfill (I7).
- **Exact copies (I8).** Every copy is the same `json` and `hash`, with `recipient` left as `'*'`.
- A batch that holds only replayed frames (`≤ D`) writes nothing. Its receipt value `last ≤ D` is already durable. A batch with no position frame (only `member`/`leave`) leaves `last` unchanged, so it posts nothing.

### Receipt poster

The poster is bound to one connection. At most one POST is in flight. Values are coalesced to the latest committed `last`, and the poster posts only when `last > lastReceipted`. `lastReceipted` starts at `hello.resume`.

| Reply | Action |
|---|---|
| Valid `{through ≥ posted}` | Set `lastReceipted` |
| Transport error or `503` | Retry with backoff while the connection lives. The coordinator window stalls on its own. |
| `400` or `409` | Abort this connection. The reconnect resolves it. |
| Any reply after the connection ended | Ignored |

### Registration

`registered` is the existing per-lifetime map. `reportRegistration` already clears it on a non-transient refusal. Two new `LocalHostInbox` members expose what the daemon loop needs:

- `forgetRegistration(key)`: `registered.delete(key)`. Called on `leave` and on presence `unknown`.
- `presenceActive: boolean`: set by the daemon when it arms the presence timer after the first v2 hello of this process. It is never reset in that process. Presence keeps running across reconnects, with the last hello's `presenceWindowMs`.

When `presenceActive` holds, the IPC `enqueueRegistration` handler completes locally exactly when the existing predicate `inbox.registeredResult(registration)` returns a result (key in `registered`, no queued row, no attempt in flight). "Completes locally" means no row, no POST, and an unchanged IPC reply. The adapters' 30 s heartbeat timers therefore cost nothing and need no change. When `presenceActive` is false, nothing changes. `completeRegistration` is unchanged: fan-out never depends on a registration response. The `displayName` clause in `registeredResult` (`host/index.ts:186`) and the IPC waiter's `bare` flag (`ipc.ts:133`, `:147`, `:159`) are removed with the names branch. This rule does not use them.

### Sweep

```sql
DELETE FROM inbox_messages
 WHERE acknowledged_at < :now - HISTORY_MS AND evidence_record_id IS NOT NULL
   AND sequence <= COALESCE((SELECT bookmark FROM inbox_stream WHERE id = 1), sequence);
```

`HISTORY_MS` stays the fixed 7 d constant (`host/index.ts:17`). With a NULL bookmark (v1 mode) the rule is today's. With a bookmark, an acknowledged row is swept only after `D` has passed it. A v2 replay never inserts at or below `D`, so a swept row can never come back as a duplicate. Unacknowledged rows never expire.

## Presence

- **Daemon.** While `presenceActive` holds, every `presenceWindowMs / 3` the daemon sends one `POST /v2/hosts/presence` with the keys of sessions that hold a live in-memory lease (`leases` entries with `expiresAt > now`). Above `MAX_PRESENCE_KEYS`, which is far outside the operating envelope, it sends the first 4096 keys in key order and reports the overflow.
  - Each key in `unknown`: `inbox.forgetRegistration(key)`, then `enqueueRegistration({session})`, which now posts. A key now registered to another host gets `owner_conflict` from `register`. The existing `reportRegistration` path then clears `registered` and sends the registration-result event to that session's socket.
- **Coordinator.** Presence refreshes `sessions.last_seen_at`, which is both the board's "seen" time and the expiry clock.
  - A session with no lease is not refreshed and expires after `retention`, as a v1 session with no running adapter did. A running adapter without the lease exists only while another runtime of the same session holds it (so the key is in presence), or until its bind retry succeeds.
  - A registration lost to coordinator power loss comes back as `unknown` on the next tick. It is then re-registered with `start = tail`.
  - A session that expires sends `leave`, which clears `registered`. A later start of its adapter therefore posts at once. Presence cannot cover this case, because an expired session has no lease.
- **Board.** The board's "present" filter is `presenceWindowMs`. `createControlHandler(store, auth, settings)` serves it in `/health`. Adapter tools (`pi|claude|muse/tools.ts`) read it once per process through a new `CoreAccess.health()`, which fetches `GET /health` without registering. They fall back to 90 s when the field is absent (a v1 coordinator).
- **`requirements.md`.** "Presence is the registration and heartbeat record" becomes "Presence is the registration and host presence record."

## Settings and derivations

The settings file is `~/.config/roger-wilco/roger-wilco.conf`, overridden by `$ROGER_WILCO_SETTINGS`. Only the coordinator reads it (`src/bin/settings.ts`, called from `startCoordinator`). The daemon receives the values it needs in `hello`, so there is one source of truth.

- Format: `key = value` lines and `#` comments. Durations take the units `s`, `m`, `h` or `d`.
- An unknown key or an out-of-bounds value fails startup. An absent file means the defaults apply. Bounds and defaults come from `SETTING_BOUNDS`.
- Deployment facts (URLs, credentials, paths, namespace, listen address) stay in `coordinator.json` and `host.json`.

| Key | Default | Bounds (fixed) | Meaning |
|---|---|---|---|
| `window` | 32 | 1–256 | W: unreceipted `msg` frames per host stream (coordinator only) |
| `retention` | 7d | 1d–90d | Registration expiry and message collection age (coordinator only) |
| `keepalive` | 15s | 5s–60s | SSE keepalive and `through` cadence |
| `presence_window` | 90s | 15s–15m | How stale the board may be |

Derived values have no setting of their own. Each is computed in one place:

| Value | Formula | Today |
|---|---|---|
| Daemon stream idle timeout | `3 × keepaliveMs` (from `hello`) | 45 s constant |
| Presence report interval | `presenceWindowMs / 3` (from `hello`) | 30 s per-session heartbeat |
| Board "present" filter | `presenceWindowMs` (via `/health`) | `PRESENCE_MS` in adapters |
| Registration expiry, collection cutoff | `retention` | 7 d constants |
| `through` cadence | `keepaliveMs` | — |

Fixed in code, not settings:
- **Sync classes:** the coordinator table above; daemon hello and ingest commits at FULL; daemon acks at NORMAL (unchanged).
- **Limits:** `MAX_JSON_BYTES`, `MAX_STREAM_EVENT_BYTES` (v1 and v2), `MAX_PRESENCE_KEYS = 4096`, `SETTING_BOUNDS`.
- **Stream mechanics:** `SCAN_SPAN = 4096`, in-flight byte cap 16 MiB, daemon batch cap 256 frames or 8 MiB.
- **Timing:** request and header deadline 10 s, write deadline 5 s, lease 30 s, backoff 250 ms–10 s, maintenance 60 s, collection batch 5000, daemon history `HISTORY_MS = 7 d`.
- **Contract rules:** strictly increasing position `seq`, cumulative receipts bounded by `sentThrough`, and SHA-256 over the exact message JSON.

## Crash and reconnect

| Failure | Durable state | In doubt | Resolution |
|---|---|---|---|
| Daemon dies before the batch COMMIT | `D` unchanged | Frames received but not stored | Reconnect with `after=D`. `resume ≤ D`, so the frames are re-sent. No receipt was sent. |
| Daemon dies after COMMIT, before or during the receipt POST | `D = N`, `C < N` | Whether `C` moved | Reconnect with `after=N`. `resume = max(C, minStart) ≤ N`. Frames `≤ N` are checked for order and skipped, with no writes. The first committed position is receipted and `C` catches up. |
| Receipt response lost or timed out | `D = N` | `C ∈ [old, N]` | Retry the cumulative value. `N ≤ C` is an idempotent 200. The next reconnect also covers it. |
| Late receipt from a superseded connection | — | — | `≤ C` gives 200 with no change. Otherwise `409`, which the old poster ignores because its connection is gone. |
| Coordinator process restart | `C`, messages | In-flight window, `sentMembers` | Every `conn` is unknown (409 or EOF). The daemon reconnects, and a full membership snapshot is sent again. |
| Coordinator power loss | A suffix of NORMAL commits lost | `C` may be lower; `last_seen_at` older; maintenance and registrations lost | `C ≤ D` still holds, so the reconnect replays and skips. A lost registration of a leased session comes back as presence `unknown` and is re-registered. `send` and `claim` are FULL, so no message or grant is lost. |
| `send` committed, response lost to the sender | Message durable | Sender's view | Unchanged from v1. Delivery is unaffected. |
| Cut-over committed, daemon crashed before the hello commit | `hosts` row, `D = NULL` | — | Reconnect without a query. `resume = max(C0, minStart)`. Rows already stored hit `(session, id)` dedup. |
| v1 receipt racing the cut-over | — | — | Serialised by `BEGIN IMMEDIATE`. If it commits before, it is counted in `MIN(cursor)`. If after, it gets `409`, and the message is replayed and deduplicated. |
| Session registers mid-stream | `sessions` row | Its start at the daemon | A `member` frame from the same snapshot precedes every message owed to it (I2, I6). There is no race with the registration response. |
| Session expires at the coordinator | Row deleted | — | `leave` precedes later frames, and `registered` is cleared. The next registration (presence `unknown` or adapter start) posts and gets `start = tail` (v1 expiry semantics). |
| Invalid frame, position order violation, directed message to a non-member, dedup hash conflict | `D` unchanged | — | The batch rolls back. Report, abort, back off, reconnect. Nothing is skipped. |
| `after < C` | — | The inbox went backwards (restored file) or two daemons share one credential | `409`, fail closed. The operator runs `roger-wilco-host --reset-bookmark` (§Coordinator restore). The next connect resumes at `C`. |
| `after > tail` or `stream` mismatch | — | Coordinator DB replaced or restored | `409`, fail closed. Follow §Coordinator restore. |
| Daemon DB lost (no bookmark, row present) | `C` | Local undelivered rows (disk loss, outside the contract) | Resume at `max(C, minStart)` and log it. |
| Host offline longer than `retention` | Its sessions expired | Nothing owed (v1 semantics) | `resume = max(C, tail)`. The scan emits `through` frames only. |
| Host far behind `tail` (old `C`, or a cut-over from a stale `MIN(cursor)`) | — | — | The loop re-arms while `scanned < tail` and yields between spans, so it reaches the tail within one wake. |
| Daemon stops reading | — | — | The write deadline (5 s) closes that stream only. Other hosts are unaffected. |

### Coordinator restore

A restored coordinator database reissues `ns:msg:<n>` ids above its backup tail. Message ids do not contain `stream_id`, so rotating the stream alone does not prevent reuse. The procedure is:

1. Keep the coordinator stopped.
2. On every host, stop the daemon and run `roger-wilco-host --reset-bookmark [config.json]`. The command takes the inbox singleton lock and fails if a daemon holds it. In one transaction it reads `highWater = max(bookmark ?? 0, MAX(inbox_messages.sequence) ?? 0)`, sets `stream_id` and `bookmark` to NULL, and prints `{"highWater": N}`. A host still on the v1 daemon binary has no such command. The operator reads `SELECT MAX(sequence) FROM inbox_messages` there instead.
3. Run `node deploy/restore-coordinator.ts --sequence-floor N DB-PATH` with N at least the maximum `highWater`. In one FULL transaction it sets the `messages` row of `sqlite_sequence` to `max(seq, N)` (inserting the row if it is absent), and sets `store_config.stream_id` to a new random value.
4. Start the coordinator, then the daemons. Each daemon resumes at the restored `C`. Ids at or below the backup tail are the same messages with the same bytes, so dedup absorbs them. New ids are above every id stored on any host.

A dedup hash conflict after this means that the floor was too low or an inbox was not counted. It stays fail-closed and reports the session and message id. The core cannot decide which of two messages with one id is real, so recovery is a manual operator decision. There is no automatic quarantine.

`--reset-bookmark` alone (step 2 without a coordinator restore) is also the remedy for `after < C`.

## Migration from v1

1. **Coordinator first.** Deploy the v2 coordinator. Startup adds `hosts` (empty) and `store_config.stream_id`. Every host stays on v1, and the collection rule reduces to the v1 rule.
2. **One host at a time.** Stop the v1 daemon. The singleton lock guarantees that only one runs. Start the v2 daemon, which upgrades the inbox schema to 4 (`bookmark = NULL`) and opens `/v2/hosts/stream` with no query.
3. **Cut-over** (`openHost`, FULL):
   - `C0 = MIN(cursor)` over h's sessions (the tail if there are none);
   - insert `hosts(h, C0)`.
   - From that commit on, h's cursors are frozen as starts, and v1 receipts and the v1 stream for h get 409.
   - The supersede aborts any live v1 connection for h.
   - The first batch sends every session as `member{start = cursor}`.
4. **No loss.** Take any message `x` owed to `s` with `x > cursor_s` (not receipted under v1).
   - `x > cursor_s ≥ C0` and `resume = C0`, so `x` is scanned.
   - A directed `x` passes the coordinator filter `x > cursor_s`.
   - A broadcast `x` passes both filters (`start < x`, `sender ≠ s`).
   - Collection kept `x`, because `cursor_s < x`, `acked` was NULL until the cut-over, and `C0 < x` after it.
5. **No duplicates beyond dedup.**
   - Neither filter copies anything `≤ cursor_s` to `s`. This holds even when those rows were acknowledged and swept locally, which matters for a session whose cursor lagged behind other sessions on its host.
   - The re-sent rows for `s` are those above its durable cursor that v1 had already stored. This can be several rows: an OS crash can roll the NORMAL cursor back over several acknowledged messages (`contract.md:397`). They are recent, because only a NORMAL suffix rolls back and v1 re-offers an unreceipted row on every stream connection. They are therefore present. `ON CONFLICT DO NOTHING` absorbs them, and the hashes match because the envelope is unchanged.
   - After the hello commit, the sweep keeps every row above `D = C0` until the replay passes it (§Sweep).
   - A row swept under the v1 rule before the cut-over could still be re-sent. That needs a v1 receipt to fail for longer than `HISTORY_MS` while the session stays registered. v1 already re-delivers such a row today, so the cut-over adds no new case.
   - Per-session order holds: v1 rows are a contiguous prefix per subscription, and v2 rows follow in `seq` order.
6. **Mixed fleet.**
   - v1 hosts keep per-session cursors.
   - v2 hosts get one frame per broadcast. Directed messages cross between v1 and v2 in both directions unchanged.
   - Collection uses the single rule above.
7. **Rollback for host h.** Stop the daemon. Then:
   - `deploy/rollback-host-v2.ts --host h`:
     ```sql
     UPDATE sessions SET cursor = MAX(cursor, (SELECT acked FROM hosts WHERE host_id = :h)), offered = NULL WHERE host_id = :h;
     DELETE FROM hosts WHERE host_id = :h;
     ```
     This is sound because everything owed with `seq ≤ C ≤ D` is stored.
   - `node deploy/rollback-inbox.ts --stream-clients-stopped INBOX-PATH` drops `inbox_stream` (4→3).
   - The order of these two steps does not affect safety. A v1 daemon against a v2 host gets 409 and stalls loudly. If the inbox step is skipped and the v2 daemon reconnects, re-creation takes `C0 = min(MIN(cursor), after)`, which only replays more.
   - An older coordinator binary requires every host to be rolled back first.
8. **Removal.** One release after the last v1 host cuts over, remove `/v1/hosts/*`, `pendingRows`, `rotation`, `acknowledge`, the daemon v1 loop, and the columns `sessions.offered` and `sessions.initial_cursor` (`ALTER TABLE … DROP COLUMN`, guarded by `table_info`). `sessions.cursor` stays as the start.
9. **Clients.** The Rust interop client uses control routes only, so it is unaffected.
10. **Contract.** `docs/contract.md` gains a "Host stream v2" section that restates this wire, the receipt rules, the sync classes and §Coordinator restore. These existing lines are edited:
    - `:207` board 90 seconds → `presenceWindowMs` from `/health`, default 90 s.
    - `:217`, `:223` seven days → `retention`, default 7 d.
    - `:398` "The host daemon repeats registration on its heartbeat" → v1 hosts heartbeat; v2 hosts re-register on presence `unknown` and `leave`.
    - `:403` idle deadline 45 seconds → `3 × keepaliveMs` on v2 (45 s on v1).
    - `:420` "The current inbox schema is version 3" → version 4, with the 4→3 rollback flag.
    - `:469` "A queued heartbeat always posts" → on v1; on v2 a heartbeat that `registeredResult` answers completes locally.
    - `:479` local rows removed seven days after ACK → plus "and, on v2, only at or below the bookmark".

## Test plan

`node --test`. Crash tests reopen **both** SQLite files and use SIGKILL on a child process (the `test/host/crash-child.mjs` pattern). Power-loss durability rests on the FULL classes and is stated, not tested.

**Protocol (`test/protocol/contract.test.ts`)**
1. Each `HostFrame` variant accepts its exact example. Each variant rejects:
   - extra or missing keys;
   - non-safe or negative integers, and `seq = 0`;
   - `msg.seq ≠ message.sequence`;
   - a `message` that fails `isMessage`;
   - an unknown `type`;
   - a `stream` that is not 32 lowercase hex;
   - `hello.keepaliveMs` or `hello.presenceWindowMs` outside `SETTING_BOUNDS`.

   A v2 event at `MAX_STREAM_EVENT_BYTES` is accepted, and one byte more is rejected.
2. `isHostReceipt`, `isPresence` and `isPresenceResult` reject a non-UUID `conn`, a negative `through`, non-canonical keys, duplicates, more than 4096 keys, and a `foreign` key.
3. A v2 `msg.message` is byte-identical to the v1 frame's message for the same row, with the same hash. v1 golden frames still validate.

**Coordinator store (`test/store/store.test.ts`)**

4. **Cut-over.** Cursors {10, 50, 90} give `acked = 10` and members with starts {10, 50, 90}. Cursors are unchanged. A second open is a no-op. A host with no sessions gives `acked = tail`. `after` on creation lowers `C0` and never raises it.
5. **Open rules.**
   - `after > tail` gives 409.
   - A foreign `stream` gives 409.
   - On an existing row, `after < C` gives 409.
   - `after > C` gives `resume = max(C, minStart)` and **does not change `C`**.
   - With no sessions, `resume = tail`.
   - A second constructor run on the same file does not fail and keeps `stream_id`.
6. **`hostBatch` filter.**
   - Includes directed messages only above `start`.
   - Includes each broadcast once.
   - Excludes another host's directed messages and messages to unregistered recipients.
   - Excludes a broadcast whose only eligible host session is its sender.
   - Returns ids in order and honours `n`.
   - Bounds `scanned` by `SCAN_SPAN` and returns `tail`.
   - 10 000 irrelevant messages produce no rows, and `scanned` advances by span.
7. **Snapshot.** Register `s` between two batches. The second batch returns `members` containing `s`. A broadcast committed before the registration is not eligible for `s`; one committed after it is.
8. **I1.** Interleaved `send`s with a rolled-back insert: a reader never sees id k+1 without k.
9. **v1 ack racing cut-over.** If it commits first, it is counted in `C0`. If it commits second, it gets 409 and the cursor is unchanged.
10. **Collection.**
    - An old message above a v2 host's `acked` with an eligible member is kept, and collected after `acked` passes it.
    - A v1 host's cursor still pins its own messages.
    - An unrelated low cursor does not pin.
    - A host with no sessions pins nothing.
    - The sender does not pin its own broadcast.
    - Mixed fleet: both rules hold together.
11. **Presence.**
    - Updates only this host's sessions.
    - Returns as `unknown` both absent keys and keys registered to another host.
    - Creates nothing and does not notify.
    - Gives 409 for a v1 host.
12. **Register.** No notify. A retry keeps `cursor`.
13. **Sync classes.** A probe asserts `PRAGMA synchronous`:
    - 2 (FULL) inside the creating `openHost`; inside `send` and `claim` as a regression check;
    - 1 (NORMAL) inside receipts, presence, maintenance, `register`, and a non-creating `openHost`;
    - 1 outside transactions.

    Setting the pragma inside `BEGIN` throws.
14. **Rollback.** The script sets `cursor = MAX(cursor, acked)` and deletes the row. The v1 stream then serves the host.

**Stream handler (`test/delivery/delivery.test.ts`)**

15. **Order.** `hello` first, membership before any `msg`, and strictly increasing position `seq` above `resume`. The open writes no SSE header when `openHost` fails.
16. **Window.**
    - With W=4 and 10 pending, exactly 4 `msg` frames go out before any receipt.
    - While those 4 are unreceipted, the process stays responsive: another host's receipt and a `/health` request are answered before the first receipt for this host arrives.
    - A receipt at the 2nd frame releases exactly 2.
    - The 16 MiB byte cap stops early but always sends at least one frame.
17. **Receipts.**
    - `≤ C` from any `conn` gives 200 unchanged.
    - Any value in `(C, sentThrough]` advances `C`, including a value between two sent frames.
    - N above `sentThrough`, a stale `conn`, a v1 body, or a coordinator restart gives 400 or 409, as specified, with no change.
    - Another host's credential cannot move this row.
18. **`through`.**
    - Other hosts' traffic only: a `through` on the keepalive tick, then a comment when nothing moved.
    - A trailing `through` after a `msg` batch.
    - An idle connected host's `C` advances.
19. **Supersede.**
    - A v2 open replaces a live v1 stream.
    - A v1 open or receipt for a v2 host gets 409 and does **not** supersede the v2 stream.
    - A second v2 open aborts the first.
20. **Wakes.** A broadcast wakes all v2 streams. A directed send wakes only its host. Registration and presence wake none.
21. **Isolation.** A stalled host never delays another. A backpressured write past 5 s closes only that stream. `close()` returns promptly.
22. **Expiry.** A session expired between batches produces `leave` before any later frame, computed against `sentMembers`. With a real daemon, no copy of a later broadcast is stored for the expired session.
23. **Gap crossing.** A gap of 1,000,000 ids that are irrelevant to the host, followed by one directed message, is delivered within one wake, with no keepalive tick. Another host's request is answered during the crossing.

**Daemon (`test/host/host.test.ts`, `local-ipc.test.ts`, `fencing.test.ts`)**

24. **Batch commit.** One chunk of k frames gives one transaction (counted with a commit hook) and one receipt with `through =` the last position. A second SQLite handle sees the rows and `bookmark` before the stub coordinator receives the receipt.
25. **Fan-out.** A host with s1 (sender), s2, and s3 (start ≥ seq): the copy goes to s2 only. Its bytes and hash equal the coordinator's, and `recipient` is `'*'`. A broadcast with no eligible member stores nothing and still advances `D`.
26. **Late member.** A `member{s, start=99}` batch, then broadcast 100, then directed 101: `pending(s)` is `[100, 101]` in order. With no `member` for s, directed 101 is a violation.
27. **Replay skip.** Frames `≤ D` write nothing, even when the local row was swept. `resume > D` raises `D` in the hello commit.
28. **Violations.** Each case aborts with no commit, no receipt and `D` unchanged, then reconnects with backoff:
    - a position `seq` not above the previous position or `hello.resume`;
    - invalid JSON;
    - an unknown type;
    - an oversized event;
    - a directed message to a non-member;
    - a hash conflict on a dedup key;
    - `hello.host` mismatch;
    - `hello.stream` mismatch.
29. **Receipt poster.**
    - At most one in flight, coalesced to the latest.
    - Retries on 503 or a transport error.
    - Aborts the connection on 400 or 409.
    - Ignores replies after its connection ends.
    - With `resume > C` and nothing pending, a chunk that holds only `member` frames posts nothing, and the stream stays open across a keepalive interval.
30. **Lifetime registration.**
    - With `presenceActive`, a heartbeat that `registeredResult` answers sends no POST.
    - Presence `unknown` clears the entry and posts exactly one registration. For a key on another host, the socket receives the `owner_conflict` registration event.
    - A `leave` clears the entry, so the next heartbeat posts.
    - Without `presenceActive` (v1 mode), heartbeats still post.
31. **Presence.** One POST per `presenceWindowMs/3` per host, whatever the session count. It lists only sessions with live leases. Above 4096 keys it sends the first 4096 in key order and reports.
32. **Settings.** Hello values drive the idle timeout (3K) and the presence interval (P/3). The sweep keeps an acknowledged row older than `HISTORY_MS` while its `sequence` is above `bookmark`, and removes it once `D` passes it.
33. **Version selection.**
    - With a NULL bookmark, a 404 runs one v1 stream request, and after it ends the next attempt tries `/v2` after the backoff.
    - A v1 coordinator answers the query-less `/v2` request with 404, not 400.
    - With a bookmark, a 404 fails closed and the daemon never runs v1.
34. **Schema.**
    - 0→4, 2→4 and 3→4 upgrades each end with `inbox_stream` present and `user_version = 4`.
    - An old daemon fails closed on 4.
    - `--stream-clients-stopped` gives 4→3. `--legacy-clients-stopped` and `--contract-clients-stopped` accept 4.
    - `--reset-bookmark` fails while a daemon holds the lock. Otherwise it clears both columns in one transaction and prints `highWater`.

**Crash tests**

35. SIGKILL the daemon before COMMIT: after restart, every owed message is stored exactly once.
36. SIGKILL after COMMIT and before the receipt: the reconnect sends `after=N`, the stream resumes at `C < N`, replayed frames are skipped, `C` reaches N, and there are no duplicate rows.
37. SIGKILL the coordinator after the `send` commit, before notify: the message is delivered after restart.
38. Force `hosts.acked` lower, simulating a lost NORMAL suffix: the reconnect replays and skips, with no loss and no duplicates.
39. SIGKILL between the cut-over commit and the daemon's hello commit: the second connect produces the same final inbox.
40. **Restore.**
    - A restored, older inbox file (`D < C`) gives 409 with no writes. `--reset-bookmark` then resumes at `C`.
    - A restored coordinator file gives 409 for each bookmarked daemon. After `--reset-bookmark` on each host and `restore-coordinator.ts --sequence-floor max(highWater)`, new messages get ids above the floor, replayed ids deduplicate, and no hash conflict occurs.
    - A floor below a host's `highWater` produces a fail-closed hash conflict that names the session and message id.

**Migration and acceptance (`test/acceptance/`, `test/installation/`)**

41. **Skewed cursors.**
    - Setup: one v1 host with 3 sessions at cursors 10, 50 and 90. The third session's rows 11–90 are acknowledged and swept locally. One session has three messages stored and acknowledged whose cursor update is rolled back (cursor forced back by k = 3). One message is offered but not stored. Session 1 sent broadcast 70.
    - Action: cut over.
    - Check, by message id: each session holds exactly its owed set; nothing at or below its cursor returns; the k re-sent rows are deduplicated; session 1 lacks its own broadcast.
42. **Mixed fleet.** Host A on v1 and host B on v2 exchange directed messages and broadcasts in both directions. Every owed message arrives exactly once. The Pi native test (`test/pi/native.test.mjs`) shows a v2 fan-out broadcast recorded with no model turn, while a directed message triggers one. Collection respects both hosts.
43. **Randomised model.** One process, real store, real handler and a real daemon. The run interleaves sends, registrations, expiries, daemon kills at named points, coordinator restarts, receipt loss and collection, with seeds reported. It checks:
    - each session's stored ids equal the oracle owed set;
    - no duplicates;
    - `D` is monotone;
    - `C ≤ D` holds at every point;
    - the window is never exceeded;
    - no owed and unstored message is ever collected.
44. **Rollback.** v2→v1 with the scripts loses nothing. A daemon left on v2 after a coordinator-only rollback cuts over again with `C0 ≤ after`, also with no loss.
45. **Interop.** The Rust client interop test passes unchanged.
46. **Performance gates** (`test/perf/`, an in-repo test that runs the real store, handler and daemon in one process through a TCP delay proxy at 30 ms each way; run by its own npm script):
    - a 100-message backlog to one session drains in at most `ceil(100/W) + 1` round trips plus local commits (today 7 s);
    - a broadcast to 100 sessions on one daemon costs 1 frame, 1 daemon commit and 1 receipt (today 659 ms);
    - 1,000 idle sessions cost 0 per-session coordinator requests and 1 presence POST per host per interval.

## Deferred (peering)

Designed for, not built:

- **Log identity.** `store_config.stream_id` is an opaque 32-hex token that names one coordinator log. The daemon keys its bookmark by it. Peering defines origin identity when it is built.
- **Position separate from identity.** Frames carry `seq`, which is the local arrival order, apart from `message`. Bookmarks, receipts, filters and collection never read `message.sequence` or parse `message.id`. Dedup keys on the opaque id string. Today the rule `seq === message.sequence` is enforced only for local origin, and peering drops it.
- **A peer is a principal.** A peer coordinator would be one more principal with a `hosts` row, and it reuses the same stream, bookmark, window and cumulative receipts. Collection then needs one more term ("unpinned by every peer's `acked`"), added beside the session term.
- **Peer configuration.** Peers are deployment facts, so they go in `coordinator.json`, not in `roger-wilco.conf`. No new table is added until peering is built.
- **Envelope change still required.** `isMessage` ties `id` to `deliveryId(namespace, sequence)` (`protocol/index.ts:417`). Origin-qualified ids need a new envelope version that relaxes this. Loop suppression (a `path` field) is added together with that version bump. v2 adds nothing that depends on that coupling.

## Revision notes

1. Stream handler spins when the window is full (major): **applied.** The wait returns at once only when `dirty` and `room` both hold, step 6 re-arms `dirty` only with room, and test 16 checks that the process stays responsive.
2. `lastReceipted` has no initial value, so a member-only batch gets a 400 loop (major): **applied.** `lastReceipted := hello.resume`, a batch with no position frame posts nothing, and test 29 covers it. The relaxed receipt rule (13) would also accept `resume`.
3. The `SCAN_SPAN` crawl delays delivery by minutes (major): **applied.** Step 6 re-arms while `scanned < tail` with room, every batch yields with `setImmediate`, `hostBatch` returns `tail`, and test 23 covers a 1,000,000-id gap.
4. Coordinator restore reissues ids and stalls hosts (major): **applied with changes.** A new §Coordinator restore adds `restore-coordinator.ts --sequence-floor` and has `--reset-bookmark` print `highWater`. No automatic quarantine was added: a conflict stays fail-closed with a manual remedy, because the core cannot choose between two messages with one id.
5. Live record lacks the last-sent membership (minor): **applied.** `sentMembers` is added, the diff runs against it, and test 22 also checks that no broadcast copy is stored for the expired session.
6. The H justification is false (minor): **applied.** The justification is rewritten, and the v2 sweep keeps rows above `bookmark`. The remaining pre-cut-over case is the same as existing v1 behaviour.
7. Registration shortcut depends on displayName, and unleased sessions are uncovered (minor): **displayName part applied.** The unleased-session part was not applied: an unleased running runtime exists only while another runtime of the same session holds the lease, or until its bind retry succeeds, and `leave` covers expired sessions.
8. The local-completion rule depends on displayName (major): **applied.** The rule now reuses `registeredResult`, and the `bare` flag and displayName clause are noted as removed with the names branch.
9. `hostEpoch` cache (major): **applied.** `hostEpoch`, `knownEpoch`, `Live.epoch` and the epoch test assertions are removed. A full-set `members` frame was not adopted, because it is unbounded in size, whereas `member`/`leave` frames are always small.
10. Reserved `path` field (major): **applied.** `path` and `MAX_STREAM_EVENT_BYTES_V2` are removed and `MAX_STREAM_EVENT_BYTES` is reused.
11. H derived from `retention` (major): **applied.** `hello.retentionMs`, `inbox_stream.retention_ms` and the H derivation are removed, and the daemon keeps the fixed `HISTORY_MS`.
12. Static peers in `roger-wilco.conf` (major): **applied.** Peers are restated as `coordinator.json` deployment facts, and the history table is removed.
13. `prev` chain (minor): **applied.** It is replaced by strictly increasing position `seq` above `hello.resume`.
14. Offered-boundary set (minor): **applied.** The coordinator accepts any value in `C < through ≤ sentThrough`. Only in-flight `msg` entries are tracked, and the non-boundary 400 and its test are removed.
15. `foreign` presence result (minor): **applied.** Presence returns `{unknown}` only, and `register`'s `owner_conflict` path does the rest.
16. `leave` and presence both clear `registered` (minor): **not applied.** Presence lists only leased sessions, so it cannot see an expired, unleased session. `leave` is the only signal that makes that session's next adapter start post at once.
17. Unused hello and health fields (minor): **applied.** `hello.window`, `hello.tail`, the `resume ≤ tail` check and `/health.protocols` are removed.
18. `StreamId` grammar (minor): **applied.** `stream_id` is 32 lowercase hex and the type name is dropped.
19. Unused `deadline` parameter (minor): **applied.** The signature is now `hostBatch(host, from, n)`.
20. Presence chunking (minor): **applied.** One POST per tick. 4096 stays a validator cap, and overflow sends the first 4096 keys and reports.
21. `initial_cursor` kept at removal (minor): **applied.** Step 8 drops `initial_cursor` and `offered`.
22. Basis outdated for claim FULL (minor): **applied.** The basis is `95ef674`, claim FULL is listed as existing, and test 13 keeps it as a regression check.
23. displayName in the registration rule (major, duplicate of 8): **applied** as in 8.
24. `lastReceipted` initial value (major, duplicate of 2): **applied** as in 2.
25. `SCAN_SPAN` crawl (major, duplicate of 3): **applied** as in 3.
26. Basis outdated (minor, duplicate of 22): **applied** as in 22.
27. Migration bound of one row per session is wrong (minor): **applied.** The bound is restated for a multi-message cursor rollback, and test 41 adds k = 3.
28. Wrong dispatch path (minor): **applied.** The path is now `src/bin/coordinator.ts:20`, and presence goes to the control handler.
29. Live record lacks the members map and per-frame bytes (minor): **applied.** `sentMembers` and `inflight: {seq, bytes}[]` are added.
30. Inbox internals not named (minor): **applied.** `forgetRegistration(key)` and `presenceActive` are specified. `reportForeign` is unnecessary because `foreign` was removed.
31. Schema 4 migration and rollback gaps (minor): **applied.** `migrateNeutralEvidence` writes 3 explicitly, 3→4 runs for every version below 4, the `--stream-clients-stopped` flag is added, and the existing flags step 4→3 first.
32. Non-idempotent `ALTER` (minor): **applied.** The `ALTER` is guarded by `table_info`, and `UPDATE … WHERE stream_id IS NULL` runs in the same transaction.
33. Hello values unbounded (minor): **applied.** `isHostFrame` enforces `SETTING_BOUNDS`, a single source shared with the settings parser, and test 1 covers it.
34. v1 fallback wording (minor): **applied.** The text now says that one v1 stream request is issued, and the next attempt tries `/v2` after the normal backoff.
35. Adapters cannot read `/health` (minor): **applied.** Settings are passed to `createControlHandler`, `CoreAccess.health()` is added, and adapters fall back to 90 s.
36. Contract edits missing (minor): **applied.** Step 10 lists the edits to lines 207, 217, 223, 398, 403, 420, 469 and 479.
37. Perf harness outside the repo, and `--reset-bookmark` unspecified (minor): **applied.** Gate 46 is an in-repo test with a TCP delay proxy. `--reset-bookmark [config.json]` takes the lock, fails if the daemon is running, clears both columns in one transaction and prints `highWater`.38. Base implementation decisions (shared foundation; recorded where the text above is silent):
    - `Settings`, `DEFAULT_SETTINGS`, `isSettingValue`, `streamIdleMs` (`3 × keepaliveMs`) and `presenceIntervalMs` (`floor(presenceWindowMs / 3)`, whole milliseconds) live in `src/protocol/index.ts` beside `SETTING_BOUNDS`, so `src/server` and `src/host` use them without importing `src/bin`. `src/bin/settings.ts` holds only the file reader and parser.
    - Settings file: a `#` starts a comment anywhere on a line; LF and CRLF lines are accepted. A repeated key fails startup. Durations require a unit (a bare number is rejected), and `window` takes no unit. Values are non-negative decimal integers. An absent file gives the defaults even when `$ROGER_WILCO_SETTINGS` names it; any other read error fails.
    - `conn` (in `hello` and `HostReceipt`) is a lowercase UUID, any version. `hello.host` is a non-empty string; equality with `config.hostId` is the daemon's check. Validators require every listed key (exact key sets), not only the absence of extra keys.
    - `Presence.sessions` and `PresenceResult.unknown` may be empty. `PresenceResult.unknown` has the same canonical, unique, at-most-4096 rule as the request.
    - Test 1's byte-boundary case ("accepted at `MAX_STREAM_EVENT_BYTES`, one byte more rejected") exercises the daemon's SSE parser, so it belongs to the daemon stage. The protocol test checks that the largest valid `msg` event fits within `MAX_STREAM_EVENT_BYTES`. Test 3's "same row" comparison through the store belongs to the coordinator stage; the protocol test compares the v1 golden frame's message with the same message in a v2 `msg` frame.

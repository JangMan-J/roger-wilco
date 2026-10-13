# Roger-Wilco Timer and Local Inbox DB Mutation/Access Inventory

Historical baseline inventory from before the push migration.
Use the [local contract](local-contract.md) for the selected design and [verification record](verification.md) for current evidence.

## Executive Summary

This inventory identifies every recurring timer, timeout, interval, and local SQLite inbox mutation/access across the codebase (`src/`, `test/`, and `deploy/`). It supports the migration proposed in `rogers.json` (replacing idle inbox and registration scan loops with a daemon-owned Unix-domain socket [UDS] notification and write channel, commit-triggered wakeups, and reconnect reconciliation) while preserving outbound HTTPS/SSE delivery and entity integer-metadata rules.

Key architectural takeaways:
1. **Three forbidden recurring scan loops** exist in the current codebase:
   - Pi pending inbox scan (`250 ms`, `setInterval` in `src/pi/index.ts:200`)
   - Host registration outbox scan (`500 ms`, `delay` in `src/host/index.ts:297`)
   - Coordinator stream replay scan (`250 ms`, `wait` in `src/server/delivery/index.ts:104`)
2. **Hidden client writers and multi-process SQLite concurrency conflict with daemon-only ownership**:
   - `src/pi/index.ts` instantiates `createHostInbox` directly inside each Pi extension session runtime (`DatabaseSync` opening SQLite with `WAL` and `busy_timeout = 5000`).
   - Pi performs direct SQLite write mutations: `bind()`, `renew()`, `release()`, `acknowledgePi()`, and `enqueueRegistration()`.
   - The `LocalHostInbox` constructor (`src/host/index.ts:28-64`) performs DDL migration (`ALTER TABLE messages ADD COLUMN acknowledged_at`) and opportunistic `sweep()` on every instantiation. When Pi processes instantiate `LocalHostInbox`, they run DDL/sweeps against the daemon database file.
   - Tests and fixtures independently instantiate `createHostInbox` or raw `DatabaseSync` to inject frames, verify state, or simulate crash windows.
3. **Timer classification**: Permitted timers (lease renewal, presence heartbeats, network deadlines, backoff, keepalives) are distinguished from forbidden polling loops.
4. **Integration test adaptations**: Existing end-to-end and lifecycle tests rely on direct DB writes or tight polling loops and will need adaptation for daemon UDS protocol fixtures.

---

## 1. Recurring Timers and Timeouts Inventory

### 1.1 Classification Criteria
- **Forbidden (Idle Polling / Scans)**: Periodic timers (`setInterval` or recursive delay loops) that query or scan a database table or queue on fixed intervals without an external event or write trigger.
- **Permitted (Lease / Presence / Heartbeat / Deadline / Backoff)**: Timers that enforce timeouts, network keepalives, exponential retry backoff, session presence reporting, or distributed lease safety bounds.

### 1.2 Inventory Table

| File Path | Qualified Function / Scope | Type & Period | Classification | Callers & Trigger Context | Purpose / Migration Note |
|---|---|---|---|---|---|
| `src/pi/index.ts:200` | `createPiDelivery().bind` | `setInterval` (250 ms `POLL_MS`) | **Forbidden** (Idle inbox polling) | Called on `bind()` after successful lease acquisition. Runs `wake() -> pass() -> inbox.pending()`. | **Eliminate**. Replace with daemon UDS push notification (`message` event) triggered by coordinator commit. |
| `src/host/index.ts:297` | `createHostDaemon().registrationLoop` | Recursive loop + `delay` (500 ms fixed) | **Forbidden** (Registration outbox scan) | Started in `createHostDaemon().start()`. Scans `inbox.registrations()` every 500 ms. | **Eliminate**. Replace with commit-triggered notification over internal channel when Pi enqueues registration via UDS. |
| `src/server/delivery/index.ts:104` | `createStreamHandler().handler` | Recursive loop + `wait` (250 ms `POLL_MS`) | **Forbidden** (Coordinator replay polling) | Active during each host SSE connection. Scans `store.pending(hostId, 500)`. | **Replace with commit trigger / condition wakeup**. When `store.send()` commits, notify active stream loop; retain initial catch-up query. |
| `src/pi/index.ts:202-212` | `createPiDelivery().bind` | `setInterval` (1,000 ms `RENEW_MS`) | **Permitted** (Lease renewal / recovery) | Called on `bind()`. Runs `ownsLease()` (`inbox.renew`) or re-attempts `inbox.bind()`. | **Preserve or adapt to UDS ping/lease**. Enforces crash detection and session lease duration (`LEASE_MS = 30s`). |
| `src/pi/index.ts:286-297` | `rogerWilco().on("session_start")` | `setInterval` (1,000 ms `RENEW_MS`) | **Permitted** (Bind retry on conflict) | Started when initial `delivery.bind()` fails with `owner_conflict`. Retries until acquired or aborted. | **Preserve or adapt to UDS lease grant events**. |
| `src/pi/index.ts:301-304` | `rogerWilco().on("session_start")` | `setInterval` (30,000 ms) | **Permitted** (Presence heartbeat) | Started on successful session startup. Enqueues `{ session }` into registration queue. | **Preserve cadence; route via UDS**. Replaces direct DB insert with UDS `register` command to daemon. |
| `src/host/index.ts:192` | `delay` | `setTimeout` (variable ms) | **Permitted** (Abortable delay / backoff) | Helper used in `registrationLoop` and `streamLoop` for retry backoff. | **Preserve**. Standard async sleep for reconnection backoff. |
| `src/host/index.ts:284, 301, 306, 343` | `registrationLoop`, `streamLoop` | Exponential backoff (250 ms to 10,000 ms) | **Permitted** (Network retry backoff) | On network/HTTP/SSE errors, delays next attempt by `backoff = Math.min(backoff * 2, RETRY_MAX_MS)`. | **Preserve**. Necessary for network resilience. |
| `src/host/index.ts:313` | `createHostDaemon().streamLoop` | `setTimeout` (`REQUEST_TIMEOUT_MS = 10s`, `STREAM_IDLE_MS = 45s`) | **Permitted** (Stream request & idle deadline) | Set before SSE request (`REQUEST_TIMEOUT_MS`) and reset on every byte received (`STREAM_IDLE_MS`). | **Preserve**. Guards against silent TCP connection drops. |
| `src/server/delivery/index.ts:20` | `wait` | `setTimeout` (variable ms) | **Permitted** (Abortable delay helper) | Used by coordinator stream handler. | **Preserve** (for timeout/keepalive waits). |
| `src/server/delivery/index.ts:26` | `write` | `AbortSignal.timeout` (`WRITE_DEADLINE_MS = 5s`) | **Permitted** (Socket drain deadline) | Waiting on `drain` event if response buffer is backpressured. | **Preserve**. Backpressure protection against slow host consumers. |
| `src/server/delivery/index.ts:99` | `createStreamHandler().handler` | Timestamp comparison (`KEEPALIVE_MS = 15s`) | **Permitted** (SSE keepalive comment) | In stream loop, sends `: keepalive\n\n` every 15s of silence. | **Preserve**. Keeps SSE connection active through middleboxes. |
| `src/server/http.ts:31` | `readBody` | `setTimeout` (`BODY_DEADLINE_MS = 10s`) | **Permitted** (HTTP body deadline) | Aborts incoming HTTP requests taking >10s to send body. | **Preserve**. Security / DoS defense. |
| `src/server/store.ts:179, 200` | `Store.maintain` | Interval gate (`MAINTENANCE_MS = 60s`) | **Permitted** (Retention sweep gate) | Checked on read/write mutations (`store.entity`, `store.inbox`, etc.) via `now < this.nextMaintenance`. | **Preserve**. Rate-limits SQLite cleanup sweeps. |
| `src/host/index.ts:66, 72` | `LocalHostInbox.sweep` | Interval gate (`SWEEP_MS = 60s`) | **Permitted** (Inbox sweep gate) | Checked in constructor, `sweep()`, and `registrations()`. | **Preserve within daemon**. Daemon-only maintenance task. |
| `test/installation/session.mjs:124` | `sessionHarness.start` | `setInterval` (100 ms) | **Test Loop** (Fixture receipt poll) | Test harness scanning session JSONL for receipt verification. | **Test utility only**. |
| `test/pi/crash-child.mjs:105` | Process keepalive | `setInterval` (1,000 ms) | **Test Utility** | Keeps dummy test child process alive until killed. | **Test utility only**. |

---

## 2. Local Inbox Database Mutations and Access Inventory

### 2.1 Schema of Host Inbox Database (`inboxPath`)
The host inbox SQLite database contains three tables:
1. `messages`: `(session_key TEXT, message_id TEXT, sequence INTEGER, message_json TEXT, receipt_entry_id TEXT, receipt_session_file TEXT, acknowledged_at INTEGER, PRIMARY KEY (session_key, message_id))`
   - Indices: `messages_due (session_key, receipt_entry_id, sequence)`, `messages_acknowledged (acknowledged_at)`
2. `leases`: `(session_key TEXT PRIMARY KEY, runtime_id TEXT, expires_at INTEGER)`
3. `registrations`: `(session_key TEXT PRIMARY KEY, registration_json TEXT)`

### 2.2 Inventory of Local Inbox Database Accesses in `src/`

| File Path | Qualified Function / Class | Access Type | SQL / Operation | Caller(s) | Role & Architectural Issue |
|---|---|---|---|---|---|
| `src/host/index.ts:28-64` | `LocalHostInbox.constructor` | **Schema Migration & DDL** | `CREATE TABLE IF NOT EXISTS messages... leases... registrations...`; `PRAGMA table_info(messages)`; `ALTER TABLE messages ADD COLUMN acknowledged_at...`; `UPDATE messages SET acknowledged_at...`; `sweep()` | `createHostInbox(path)` in `src/bin/host.ts:11` AND in `src/pi/index.ts:274` | **Conflict**: Both Daemon AND Pi run constructor migrations and sweeps directly on the database file. Pi must never execute DDL. |
| `src/host/index.ts:66-73` | `LocalHostInbox.sweep` | **Write Mutation** | `DELETE FROM messages WHERE acknowledged_at < ? AND receipt_entry_id IS NOT NULL`; `DELETE FROM leases WHERE expires_at <= ?` | Constructor, `sweep()`, `registrations()` | **Conflict**: Executed during Pi constructor and daemon loop. Should be daemon-only. |
| `src/host/index.ts:88-93` | `LocalHostInbox.receive` | **Write Mutation** | `INSERT OR IGNORE INTO messages (session_key, message_id, sequence, message_json) VALUES (?, ?, ?, ?)` | `createHostDaemon().streamLoop` (`src/host/index.ts:327`) | **Permitted Daemon Operation**. Host daemon commits incoming frame before coordinator ACK. |
| `src/host/index.ts:95-97` | `LocalHostInbox.bind` | **Write Mutation** | Delegates to `setLease(session, runtimeId, now, false)`: `INSERT INTO leases ... ON CONFLICT(session_key) DO UPDATE` | `createPiDelivery().bind` (`src/pi/index.ts:189, 207`) | **Conflict (Client Writer)**: Pi extension connects to SQLite to acquire/update lease. Must move to daemon UDS command. |
| `src/host/index.ts:99-101` | `LocalHostInbox.renew` | **Write Mutation** | Delegates to `setLease(session, runtimeId, now, true)`: `INSERT INTO leases ... ON CONFLICT(session_key) DO UPDATE` | `createPiDelivery().ownsLease` (`src/pi/index.ts:128`) | **Conflict (Client Writer)**: Pi extension renews lease directly in DB every 1s. Must move to daemon UDS command/keepalive. |
| `src/host/index.ts:120-123` | `LocalHostInbox.release` | **Write Mutation** | `DELETE FROM leases WHERE session_key = ? AND runtime_id = ?` | `createPiDelivery().stop` (`src/pi/index.ts:224`) | **Conflict (Client Writer)**: Pi extension deletes lease directly in DB on shutdown. Must move to daemon UDS command. |
| `src/host/index.ts:125-129` | `LocalHostInbox.owns` | **Read Access** | `SELECT runtime_id, expires_at FROM leases WHERE session_key = ?` | Helper in `pending()` and `acknowledgePi()` | Internal verification of lease ownership. |
| `src/host/index.ts:131-136` | `LocalHostInbox.pending` | **Read Access** | `SELECT message_json FROM messages WHERE session_key = ? AND receipt_entry_id IS NULL ORDER BY sequence ASC` | `createPiDelivery().pass` (`src/pi/index.ts:149`), `resumeAfterAbort` (`src/pi/index.ts:173`) | **Conflict (Multi-process reader)**: Currently polled every 250ms by Pi. Must be replaced by daemon push/query over UDS. |
| `src/host/index.ts:138-156` | `LocalHostInbox.acknowledgePi` | **Write Mutation** | `UPDATE messages SET receipt_entry_id = ?, receipt_session_file = ?, acknowledged_at = ? WHERE session_key = ? AND message_id = ?` | `createPiDelivery().receipt` (`src/pi/index.ts:119`) | **Conflict (Client Writer)**: Pi extension updates receipts directly in SQLite. Must move to daemon UDS `ack` command. |
| `src/host/index.ts:158-163` | `LocalHostInbox.enqueueRegistration` | **Write Mutation** | `INSERT INTO registrations (session_key, registration_json) VALUES (?, ?) ON CONFLICT(session_key) DO UPDATE` | `rogerWilco().on("session_start")` (`src/pi/index.ts:300, 303`) | **Conflict (Client Writer)**: Pi writes directly to `registrations` table. Must move to daemon UDS `register` command. |
| `src/host/index.ts:165-168` | `LocalHostInbox.registrations` | **Read Access** (+ sweep) | `SELECT session_key, registration_json FROM registrations ORDER BY session_key` | `createHostDaemon().registrationLoop` (`src/host/index.ts:287`) | **Permitted Daemon Operation**. Host daemon drains outbox. (Outbox table can be replaced or kept purely daemon-internal). |
| `src/host/index.ts:170-174` | `LocalHostInbox.completeRegistration` | **Write Mutation** | `DELETE FROM registrations WHERE session_key = ? AND registration_json = ?` | `createHostDaemon().registrationLoop` (`src/host/index.ts:294`) | **Permitted Daemon Operation**. Host daemon removes completed registration. |
| `src/host/index.ts:176-178` | `LocalHostInbox.close` | **DB Lifecycle** | `this.db.close()` | `bin/host.ts:18`, `src/pi/index.ts:261, 281, 316` | **Conflict**: Pi closing its own connection to the database file. Under UDS, Pi closes the socket client, not the SQLite DB. |

---

## 3. Hidden Client Writers and Constructor Conflict Analysis

### 3.1 Direct Client Instantiation in Pi Runtime
In `src/pi/index.ts:273-275`:
```typescript
const { createHostInbox } = await import("../host/index.ts");
const inbox = createHostInbox(config.inboxPath);
```
Every active Pi session instantiates its own `LocalHostInbox` against `config.inboxPath`.
This causes:
1. **Uncoordinated Multi-Process SQLite Connections**: Node.js `node:sqlite DatabaseSync` objects run concurrently in the daemon and in every Pi extension worker.
2. **Hidden Constructor DDL / Migrations**: Each Pi worker constructor runs `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = FULL`, `CREATE TABLE IF NOT EXISTS`, and `ALTER TABLE messages ADD COLUMN acknowledged_at`. If multiple Pi sessions start simultaneously or when the daemon starts, concurrent DDL causes `busy` errors or lock contention.
3. **Hidden Sweeps**: `sweep()` runs in the constructor of `LocalHostInbox`, causing Pi workers to delete expired messages and leases.
4. **Client Database Mutations**:
   - `bind`: Client writes to `leases`.
   - `renew`: Client writes to `leases`.
   - `release`: Client deletes from `leases`.
   - `enqueueRegistration`: Client writes to `registrations`.
   - `acknowledgePi`: Client writes to `messages` (`receipt_entry_id`, `acknowledged_at`).

### 3.2 UDS Daemon-Only Ownership Solution (per `rogers.json`)
- **Daemon Ownership**: The daemon process alone opens `DatabaseSync(config.inboxPath)` and owns all DDL, schema migrations, reads, writes, and sweeps.
- **UDS Listener**: The daemon creates and listens on a protected Unix-domain socket (e.g. at `join(dirname(config.inboxPath), "host.sock")` with `0o700` directory and `0o600` socket permissions).
- **Pi Client**: The Pi extension connects via Node net (`net.createConnection({ path: socketPath })`). It sends JSON commands over UDS:
  - `bind { session, runtimeId }`
  - `renew { session, runtimeId }`
  - `release { session, runtimeId }`
  - `register { session, displayName }`
  - `ack { session, runtimeId, proof: PiReceipt }`
  - `subscribe { session, runtimeId }` -> receives push notifications from daemon when deliveries commit.

---

## 4. Test and Fixture Database Access Inventory

Tests frequently open the inbox database directly for setup, assertion, or takeover simulation. These must be adapted when transitioning to daemon-owned writes.

| Test File | Direct SQLite / Inbox Usage | Purpose | Adaptation Required for Migration |
|---|---|---|---|
| `test/host/host.test.ts:198` | `new DatabaseSync(path)` | Inspects `acknowledged_at` column and indices directly. | Keep as low-level store/schema test, or verify via daemon if direct DB inspection is retained for unit tests. |
| `test/pi/config.test.mjs:100, 139` | `new DatabaseSync(inboxPath)` | Verifies that `~/` expands to the same path and inspects leases table. | Retain path resolution checks; update lease check to query daemon socket or read-only probe. |
| `test/pi/native.test.mjs:125, 174` | `new DatabaseSync(inboxPath, { readOnly: true })` | Asserts lease record exists with correct `runtime_id`. | If daemon owns DB, read-only assertion can pass, or test can check via UDS status probe. |
| `test/pi/native.test.mjs:137, 226` | `new DatabaseSync(inboxPath)` (read-write) | Simulates takeover by updating `leases` table directly (`UPDATE leases SET expires_at...`). | Must be simulated by starting a second runtime or sending a bind command via UDS. |
| `test/pi/crash.test.mjs:20, 27` | `new DatabaseSync(inboxPath, { readOnly: true })` | Validates lease persistence and message status across Pi crash stages. | Direct read-only inspection remains compatible with SQLite WAL mode. |
| `test/pi/tools.test.mjs` | Mocks HTTP routes (`/v1/sessions`, `/v1/claims`, `/v1/entities`) | Tests Pi tools (`roger_identity`, `roger_send`, `roger_entity`, `roger_board`). | No direct inbox DB manipulation; already updated by parent metadata work. |
| `test/acceptance/native-delivery.test.ts` | Uses `createHostInbox` and `createHostDaemon` with real Pi instances. | Tests end-to-end idle and busy delivery. | Adapt to pass UDS socket path or launch host daemon which provides UDS. |
| `test/pi/receipt.test.mjs` | Focuses on Pi session JSONL parsing and fsync (`persistedReceipt`). | Verifies Pi receipt proof generation. | No inbox DB dependency; unaffected. |
| `test/installation/rollback.mjs:20` | `new DatabaseSync(..., { readOnly: true })` | Verifies coordinator database backup during rollback. | Coordinator DB; unaffected by host inbox changes. |
| `test/installation/cross-host.mjs` | Spawns coordinator, hosts, and Pi sessions. | Full multi-process system test. | Must start host daemon before Pi starts so UDS socket is present. |

---

## 5. Integration Test Adaptations

When the host daemon assumes exclusive ownership of the inbox database and exposes a UDS channel:

1. **Host Daemon as Socket Provider**:
   - `createHostDaemon` must manage the UDS server lifecycle (bind socket on startup, clean up socket file on shutdown).
   - Tests that previously tested `createPiDelivery` in isolation without a host daemon (`test/pi/native.test.mjs`) must either:
     - Use a lightweight embedded UDS test adapter or harness daemon.
     - Or `createPiDelivery` must accept a UDS client interface rather than direct SQLite `HostInbox`.
2. **Elimination of Polling Loop Assertions**:
   - Tests checking `POLL_MS` timing (e.g. waiting for 250ms scan to pick up a message) should be updated to verify push-triggered dispatch (settling via immediate push notifications).
3. **Takeover and Conflict Testing**:
   - `test/pi/native.test.mjs` tests lease conflict and takeover by executing raw SQL `UPDATE leases SET expires_at = ?`. Under daemon-owned UDS, takeover tests can either:
     - Issue a `bind` from a secondary client with simulated clock skew.
     - Or allow testing hooks in the daemon test harness.
4. **Registration Outbox Verification**:
   - Tests asserting that registrations queue and retry (`test/host/host.test.ts`) currently verify that `inbox.enqueueRegistration` writes to SQLite. Under UDS, registration commands are received over socket, persisted by the daemon, and dispatched immediately upon network availability without waiting for the 500 ms poll.

---

## 6. Summary Matrix for Implementation Hand-Off

| Component | Current State | Required State | Immediate Action Required |
|---|---|---|---|
| **Pi Delivery Polling** | `setInterval(wake, 250)` in `src/pi/index.ts:200` | Socket-driven event stream (`daemon.on("message")`) | Remove `POLL_MS` interval; trigger `wake()` on socket event and transcript FSWatcher. |
| **Host Registration Scan** | `delay(500, signal)` in `src/host/index.ts:297` | Event-triggered outbox processing | Remove 500 ms scan; trigger dispatch on registration arrival or reconnection. |
| **Coordinator Stream Polling** | `wait(250, signal)` in `src/server/delivery/index.ts:104` | Commit-triggered event emitter / condition variable | Trigger stream wakeup on `store.send()` / `store.acknowledge()`; retain initial catch-up read. |
| **Inbox DB Concurrency** | Multi-process direct access from Pi & Daemon | Single-process daemon ownership | Move `createHostInbox` exclusively inside `HostDaemon`; replace Pi inbox with `HostClient` UDS client. |
| **Constructor Migrations** | Executed by any caller of `createHostInbox` | Daemon-only startup task | Restrict schema DDL to daemon initialization. |
| **Pi Receipts & Leases** | Pi directly writes `messages` & `leases` tables | Pi sends `ack` and `bind`/`renew` requests via UDS | Expose RPC/message handlers on UDS server; daemon executes SQLite transaction and replies. |

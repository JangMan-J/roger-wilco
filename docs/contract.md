# Roger-Wilco protocol contract v1

## Scope and runtime

This contract describes the current implementation.
The minimum Node version is `>=26.11.1`.
The pin in `.node-version` is 26.11.1.
The runtime uses `node:sqlite` and `DatabaseSync`.
Runtime code does not import a reference project.
Shared declarations and validators live in `src/protocol/index.ts`.

The coordinator owns messages, registrations, claims, and cursors.
The host daemon owns one local SQLite inbox and one outbound SSE stream for all of its sessions.
The Pi extension uses that inbox.
It also uses authenticated outbound HTTP.

There is no inbound host server.
There is no callback URL.
There is no webhook signing.
There is no research type.
Research remains in pi-webtasks.

## Identity and configuration

### Sessions and addresses

A session is `{namespace, piSessionId}`.
The namespace is lowercase.
The Pi session id is a UUID.
The address is `namespace:uuid`.
Display names are not routing keys.

A fork or a new session gets a new UUID and a new address.
Resume retains both the UUID and the address.
Message IDs are `namespace:msg:<SQLite sequence>`.
They are safe positive decimal IDs.
They are distinct from the old service.

One coordinator namespace is configured per deployment.
Cross-namespace sends are rejected.
Host ID is routing metadata, not an address.

### Host configuration

Provision each host once with `{coordinatorUrl, hostId, credential, inboxPath}`.
The Pi extension also receives `namespace`.
Configuration must have no callback URL.
Configuration must have no inbound listener.
Runtime paths come from configuration or the home directory.
Those paths must not hard-code a user's absolute path.

### Authentication and trust

The coordinator maps one bearer token to exactly one `HostPrincipal.hostId`.
It ignores caller-supplied host headers.
An unknown host is rejected as `unknown_host`.
A bad credential is rejected as `unauthorized`.
Those two failures stay distinct.

Pi tools take `sender` and the claim owner from `ctx.sessionManager.getSessionId()` and the configured namespace.
They do this on each call.
They do not use host-global sender headers.
They do not use a user-provided sender ID.

The extension is permitted to use the trusted host credential for outbound HTTP.
The server checks that the sender is registered on that authenticated host.
The server checks that the claim owner is registered on that authenticated host.
Shared credentials authorize trusted participants.
They do **not** isolate adversarial agents from each other.

A host change for a registered session returns `owner_conflict`.
Automatic cross-host migration is excluded.

### Leases

The local inbox allows one live runtime per session through a renewable lease.
A duplicate `bind` returns `owner_conflict` while that lease is live.
On that conflict, the Pi extension keeps the runtime.
It waits and retries the lease.
It does not take an unexpired lease.
A stale lease expires without dropping messages.
The implemented lease lasts 30 seconds.
The extension retries a conflicting binding every second.

A stop or a switch releases the old runtime.
Only the addressed session is permitted to consume its own inbox.

### Errors and limits

The error shape is `{ok:false,error,message}` in `src/protocol/index.ts`.

- `invalid_request` is HTTP 400.
- `unauthorized` is HTTP 401.
- `unknown_host` is HTTP 403.
- `not_found` is HTTP 404.
- `owner_conflict` is HTTP 409.
- `temporarily_unavailable` is HTTP 503.

A network failure has no response.
The client queues registration and retries it.
Local messages stay pending.
An unreachable coordinator **never** prevents Pi from starting.

Invalid credentials are never acknowledged.
Invalid envelopes are never acknowledged.
Unauthorized receipts are never acknowledged.

JSON requests are bounded to 1 MiB.
Normalized message input JSON is also bounded to 1 MiB before storage.
This limit includes `recipient`, `body`, `data`, and optional `visibility`.
It prevents compact numeric literals from expanding into an oversized stored message.
The host accepts SSE events through 1 MiB plus 1 KiB.
The additional allowance covers the bounded version-1 envelope and SSE framing.
Validators reject an invalid field on a versioned protocol record.
They reject an extra field on a versioned protocol record.
Only finite JSON values are accepted.

A message body must contain 1 to 65536 characters.
A message sequence must be a safe positive integer.
A UUID must be valid.
A namespace must be valid.

`visibility` is metadata.
The default is `private`.
The other allowed value is `public`.
`public` does not imply recipient confidentiality.

## HTTP control and SSE

### Fixed routes

All routes except `GET /health` require `Authorization: Bearer <host credential>`.
The content type for JSON writes is `application/json`.
Each successful control JSON response contains `{ok:true,value:...}`.
The coordinator HTTP entry handles these fixed paths.

| Request | Body or query | Success value or rule |
|---|---|---|
| `POST /v1/sessions/register` | `Registration` | `RegistrationResult`. The initial cursor is the current message tail, set atomically. A retry from the same host returns the existing subscription and the **unchanged** cursor. Another host receives HTTP 409. Registration is durable before the response. |
| `POST /v1/sessions/rank` | `{session: SessionAddress, rank: number}` | Updated `RegistrationResult`. Rank must be an integer from 1 through 5. Any authenticated host can change this metadata. |
| `GET /v1/sessions` | none | Registered sessions and host assignment. No active Pi session is required. |
| `GET /v1/claims` | none | Active work claims. Expired claims are removed. |
| `POST /v1/messages` | `{sender: SessionAddress, message: MessageInput}` | `Message`. The server checks the sender host assignment and the same namespace. It checks the envelope. It rejects an invalid envelope. It persists the message before success. Broadcast recipient `*` excludes the sender at delivery. |
| `GET /v1/inbox?namespace=...&piSessionId=...&since=0&limit=100` | explicit historical query | Ordered messages for the session. The host assignment is checked. A caller can query messages from before registration. Those messages do not generate historical automatic wake-ups. |
| `POST /v1/claims` | `{key,owner:SessionAddress,ttlSeconds,note?}` | `{claim,acquired}`. A positive `ttlSeconds` value expires after that many seconds. Zero means no expiry. Acquire, renew, and takeover of an expired claim are atomic. Release is owner-only. |
| `POST /v1/claims/release` | `{key,owner:SessionAddress}` | A boolean. Release is owner-only. |
| `GET /v1/hosts/stream` | `Accept: text/event-stream` | One authenticated stream per host. Each `event: message` carries a JSON `StreamFrame`. The `id` field is permitted to be the message ID for diagnostics. It **never** drives the durable resume position. Keepalives are comments. A new connection supersedes the earlier stream for that host. Disconnection or shutdown cancels the stream promptly. |
| `POST /v1/hosts/receipts` | `Receipt` | `Subscription`. The coordinator accepts a receipt only for a delivered message on that authenticated host's own subscription. The per-subscription cursor advances monotonically and contiguously. A repeated receipt is idempotent. A forged ID is rejected. An undelivered future ID is rejected. |

Registration results contain `rank` and can also contain `displayName` and `lastSeenAt`.
A registration retry refreshes presence without resetting the subscription cursor.
Presence timestamps do not prove that a Pi runtime still owns its local lease.

### Rank and Pi tools

New and migrated sessions start at rank 1, named `fleet`.
Ranks 2 through 5 have numeric labels.
Rank is user-assigned metadata, separate from the routing namespace.
It grants no authority and changes no delivery or claim permissions.
Changing rank does not refresh presence or reset a cursor.
Heartbeats preserve rank.

The coordinator has no MCP endpoint.
The Pi extension uses HTTP directly.
The coordinator and host have no third-party runtime dependencies.

Pi exposes `roger_send`, `roger_inbox`, `roger_board`, `roger_claim`, and `roger_release`.
These tools derive their actor from the invoking Pi session.
The board defaults to sessions seen within 90 seconds and includes active claims.
The model tool accepts `{all:true}` to include stale, retained registrations.
The user commands `/roger` and `/roger_board` display the board without a model turn.
They offer a session picker followed by a rank picker.
Cancellation leaves all ranks unchanged.
`/roger all` includes stale, retained sessions.
Session rows show rank, presence age, and `lastSeenAt`.

### Retention

The coordinator removes registrations unseen for more than seven days.
The 90-second board filter does not stop delivery.
Until registration expiry, offline sessions retain direct and broadcast delivery through their host.
After expiry, registration starts at the durable message sequence watermark.
It does not replay messages from the expired subscription.

Messages older than seven days are eligible for collection.
The coordinator keeps each message that a retained recipient still needs.
Unrelated low cursors do not keep old messages.
A broadcast uses one coordinator row, not one stored copy per session.
Each receiving host stores a separate local delivery for each recipient.

The latest acknowledged cursor remains retryable while its registration survives.
Older receipt retries require retained message history.
The initial registration cursor is not proof of delivery.
Maintenance runs at startup and at most once per minute during store activity.
Each sweep collects at most 5,000 messages.

### Examples

Registration request:

```json
{"session":{"namespace":"fleet","piSessionId":"550e8400-e29b-41d4-a716-446655440000"},"displayName":"Alice"}
```

Successful registration value:

```json
{"subscription":{"id":"fleet:550e8400-e29b-41d4-a716-446655440000","session":{"namespace":"fleet","piSessionId":"550e8400-e29b-41d4-a716-446655440000"},"hostId":"host-a","cursor":0},"rank":1}
```

`POST /v1/messages` body:

```json
{"sender":{"namespace":"fleet","piSessionId":"550e8400-e29b-41d4-a716-446655440001"},"message":{"recipient":"fleet:550e8400-e29b-41d4-a716-446655440000","body":"Please read the update.","visibility":"private"}}
```

SSE frame:

```json
{"type":"message","subscriptionId":"fleet:550e8400-e29b-41d4-a716-446655440000","message":{"version":1,"id":"fleet:msg:1","sequence":1,"timestamp":123,"sender":"fleet:550e8400-e29b-41d4-a716-446655440001","recipient":"fleet:550e8400-e29b-41d4-a716-446655440000","body":"Please read the update.","data":null,"visibility":"private"}}
```

### Replay and cursors

`CoordinatorStore.pending(host,limit)` is the SQLite replay source for catch-up sends and live sends.
Frames are ordered by global sequence per subscription.
The stream checks that result first.
It then waits for database changes.
The wait uses a race-free subscribe/recheck loop.

Each connection holds at most one unacknowledged in-flight event per subscription.
After the receipt, the stream reads SQLite again.
The delivery path excludes event-buffer-only delivery, repeated unacknowledged tight-loop sends, unbounded in-memory queues, and cross-host head-of-line blocking.
Writes/backpressure for a slow host stay bounded.
Cancellation for a slow host stays bounded.

The host reconnects with capped exponential backoff.
It authenticates on each connection.
`Last-Event-ID` is ignored for durable resume.
The host calls the receipt POST **only after** the local inbox `receive` transaction commits.
That transaction deduplicates the message.

The host skips malformed or unsupported message events without a receipt.
The affected subscription stays pending while other subscriptions continue on that connection.
Transport failures, invalid UTF-8, and oversized events still close the stream.

A crash after that commit and before the receipt POST replays the same message.
The replay inserts no duplicate local event.
Tests must reopen both SQLite files.

Cursor progress is independent for each subscription.
A later global ID cannot skip an earlier unacknowledged message for that subscription.
SQLite storage transactions initialize the first cursor atomically with registration.
Only lane A changes coordinator schemas.

### Host stream deadlines

The host stream client uses a 10-second deadline for response headers.
After the response headers arrive, the idle deadline is 45 seconds.
A nonempty read resets that idle deadline.
SSE comments are nonempty reads.
Keepalive comments therefore reset the idle deadline.
The parser does not emit a comment as a message.

Host JSON posts also use a 10-second deadline.
When a stream deadline expires, the host aborts that stream read.
It then reconnects with the capped exponential backoff.

## Local inbox and Pi ACK

### Local inbox

`HostInbox` methods in `src/protocol/index.ts` use one durable local SQLite database per host.
The `receive(frame)` transaction deduplicates by the immutable namespaced message ID.
It records the addressed session.

The local registration queue survives a coordinator outage.
Startup binding enqueues registration and returns without a wait for HTTP.
The host daemon drains and retries registrations.
It does not reset subscription cursors.

The host removes local message rows seven days after their Pi ACK.
It never expires an unacknowledged local message.
Legacy acknowledged rows receive a full seven-day window after migration.
Cleanup runs at startup and once per minute during daemon activity.
An old replay after cleanup checks the retained Pi JSONL receipt before submission.
Without that original JSONL record, a replay can produce another Pi entry.

SQLite reuses space after collection but does not shrink its database file.
Journal size limits reduce retained WAL files after resets.
Active transactions or readers can keep the WAL above those limits.

`pending` is restricted to the active `(session,runtimeId)` lease.
When Pi is closed, the host daemon continues to receive.
It never spawns Pi.
Lane E is permitted to call coordinator HTTP directly for session-bound tools.
When the extension is gone, the local queue also retries session registration.

### Pi receipt boundary

This is the native Pi 1.1.0 acceptance boundary.
`ExtensionAPI.sendMessage()` is `void`.
The wrapper starts an async `sendCustomMessage`.
The wrapper catches errors internally.
On a fresh session, the return can precede creation of the session JSONL.
If the session is busy, `followUp` is queued with no session entry.

The return value is not a Pi receipt.
`message_end` alone is not a Pi receipt.
An in-memory `getBranch()` entry is not a Pi receipt.
Model completion is not a Pi receipt.

For each immutable `DeliveryId`, lane E submits `{customType:'roger-wilco',content:<agent-message>,display:true,details:{deliveryId}}` with `{deliverAs:'followUp',triggerTurn:true}`.
The message retains its sender metadata without a repeated warning prefix.
The accepted boundary is an exact `custom_message` record in the current persistent session JSONL.
That file is the current `SessionManager.getSessionFile()` result.
The record matches `details.deliveryId` and `customType`.
Lane E fsyncs that file after the record is present.

Lane E must check the current session ID again before the ACK commit.
Lane E must check the runtime lease again before the ACK commit.
`HostInbox.acknowledgePi` then commits `PiReceipt`.
`PiReceipt` contains the entry ID and the session file.

After a restart, replay first scans the reopened current session file for exact IDs.
It ACKs IDs that are already in that file.
It submits only absent IDs.
A busy queued ID remains pending until it appears on disk.
One runtime must not stack duplicate queued submissions.
The runtime tracks each in-flight ID until that ID is persisted, stopped, or known failed.

Lane E does not ACK on absence.
Lane E does not ACK on a parse error.
Lane E does not ACK on a missing persistent file.
Lane E does not ACK on an I/O failure.

It reports a status or an error.
It keeps the message pending.
It retries on resume or recovery.
Production sessions require a persistent `SessionManager`.
In-memory SDK fixtures must switch to a persistent manager.
A missing file never blocks Pi startup.

Lane E must not trust an ID on another session file.
Lane E must not trust an ID on an in-memory branch.
A crash between submission and the local ACK can leave a duplicate Pi record.
The preserved ID makes that duplicate recognizable.
Pi save and the Pi ACK are **not** model completion.
The local inbox ACK stays separate from the coordinator receipt.

### Process-crash tests

Lane E and lane F must run process-crash tests before they claim at-least-once Pi delivery.
One stage is a kill before the Pi file write.
One stage is a kill after the file write and before `fsync`.
One stage is a kill after `fsync` and before the inbox ACK.
One stage is a kill after the ACK.

The test reopens the session and the inbox.
The result must be the correct retry or dedup.
No message moves across session IDs.
The same lanes must also run tests for idle wake-up, busy follow-up, resume, fork, duplicate owner, and absence of a file.

Native crash tests in `test/pi/crash.test.mjs` kill the actual extension with `SIGKILL`.
They resume that extension immediately.
Resume starts while the dead owner's lease is still live.
The new runtime waits and retries the lease.
It does not take the unexpired lease.

`SIGKILL` is not a power-loss test.
These tests do not prove that unsynced pages survive power loss.
The contract probe exercises idle persistence, busy persistence, and reopen.
It does not simulate `SIGKILL`.
It does not simulate power loss.

### Busy-abort recovery

Busy-abort recovery is required.
The signal-aware native regressions and final recovery review passed.

An aborted busy turn must leave the `DeliveryId` pending until the exact JSONL record is on disk.
Lane E must fsync that file before the ACK.
A recovery wake must pass a fresh lease check after asynchronous work.
It must pass the current session check after that same work.
It must not submit the same `DeliveryId` again.

A normal stop does not prove this recovery.
A stream that ignores abort does not prove this recovery.
A signal-aware native regression verifies this recovery.
The recovery guard uses submitted custom-message IDs that remain pending in the inbox.
It does not use Pi's user-input queue count.
A single control wake resumes the queued original without submitting that original again.
Repeated settled events do not create a wake loop.
If the recovery turn also aborts, the original stays pending for later input or restart.

## Lane ownership

### Lane A

`src/server/store.ts` exports `createCoordinatorStore(dbPath: string, namespace: string): CoordinatorStore`.
Tests live in `test/store/`.
Lane A is the one schema owner.
It uses WAL, parameterized `node:sqlite`, and synchronous transactions.
It has no release-watcher tables and no feed tables.

### Lane B

`src/server/control/index.ts` exports `createControlHandler(store: CoordinatorStore, auth: HostAuthenticator): ControlHandler`.
Tests live in `test/control/`.
The bin composes this handler with lane C.
This lane does not edit the schema.
It serves the fixed HTTP control routes.

### Lane C

`src/server/delivery/index.ts` exports `createStreamHandler(store: CoordinatorStore, auth: HostAuthenticator): ControlHandler`.
Tests live in `test/delivery/`.
It serves the stream route and the receipt route.
One host stream covers all subscribed sessions.
This lane does not edit the schema.

### Lane D

`src/host/index.ts` exports `createHostInbox(path: string): HostInbox`.
It also exports `createHostDaemon(config: HostConfig, inbox: HostInbox): {start(signal: AbortSignal): Promise<void>;stop(): Promise<void>}`.
Tests live in `test/host/`.
Lane D is the sole owner of the host schema and the durable local registration queue.

### Lane E

`src/pi/index.ts` exports the default `(pi: ExtensionAPI): void` Pi extension.
The bound runtime implements `PiDelivery`.
It loads `PiConfig`.
It opens the lane D inbox.
On session shutdown it closes that inbox and relinquishes the lease.

Tests live in `test/pi/`.
There is no second inbox implementation.
The sender and the claim owner come from the current session context.

### Lane F

`test/acceptance/` holds real coordinator, host, and native Pi session tests.
Lane F has no implementation ownership.

### Lane G

Lane G owns `src/bin/coordinator.ts`, `src/bin/host.ts`, `deploy/`, `docs/`, and the root manifest, lock, and notices.
It covers startup and teardown composition.
It covers lifecycle, health, and cutover/rollback.
The contract owner is the sole editor of `src/protocol/`.
Later shared changes return to the integrator.

### Probes

`node --test test/protocol/*.test.mjs test/protocol/*.test.ts` has **no skips**.
`PI_PACKAGE_DIR` can override the selected installed Pi package for the native probe.
The default package is `~/pi-main/packages/coding-agent` with sibling `../ai`.
No external provider calls are made.

The Node SQLite probe tests named parameters, positional parameters, numeric ID type, WAL, and reopen.
`npm run typecheck` is separate.
The native probe establishes a file-observed boundary.
It is not successful process-crash acceptance.
Completion gates remain lane-owned.

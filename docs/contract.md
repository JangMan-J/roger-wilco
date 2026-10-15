# Roger-Wilco protocol contract v1

Requirements are defined in [requirements.md](requirements.md).
This contract describes the implementation of those requirements. If the
two documents differ, [requirements.md](requirements.md) is correct and
this contract is in error.

## Scope and runtime

This contract describes the current implementation.
The minimum Node version is `>=24`.
The development pin in `.node-version` is 24.21.0.
The full check passes on both Node 24.21.0 and Node 26.11.1.
The runtime uses `node:sqlite` and `DatabaseSync`.
Runtime code does not import a reference project.
Shared declarations and validators live in `src/protocol/index.ts`.
The core is `src/protocol/`, `src/server/`, and `src/host/`.
It names no agent adapter.
Adapters are in-repo and attach through `HostInbox` and the HTTP routes.
The Pi adapter is in `src/pi/`, and the tracer agent is in `src/agent/`.

The coordinator owns messages, registrations, claims, and cursors.
The host daemon owns one local SQLite inbox and one outbound SSE stream for all of its sessions.
The Pi extension accesses that inbox through a private Unix socket.
It also uses authenticated outbound HTTP.

There is no inbound host network server.
There is no callback URL.
There is no webhook signing.
There is no research type.
Research remains in pi-webtasks.

## Identity and configuration

### Sessions and addresses

A session is `{namespace, sessionId}`.
The namespace is lowercase.
The field was named `piSessionId` before the agent-neutral contract.
That name is rejected, not translated, on every wire and socket surface.
Agent adapters translate their own session identity at their edge.
A session id is one opaque token.
There is not a second grammar for agent ids.
The token has 1 to 128 characters.
Each character is an RFC 3986 unreserved character: `A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, or `-`.
Case is preserved.
`Tracer-1` and `tracer-1` are different sessions.
An id can begin with `_` or `-`.
The id has no colon, space, or control character.
A colon would make the address split ambiguous.
These characters are URL-safe, so the inbox query cannot hide another parameter.
The 128-character limit keeps three session keys inside the stream event limit.
The address is `namespace:` plus the canonical session id.
Entity labels and call signs are not routing keys.

One legacy rule remains.
An id shaped like a UUID is canonicalized to lowercase in `sessionKey`.
Pi session IDs have this shape.
That lowercase address stays byte-identical for an existing Pi session.
This is canonicalization, not a second accepted grammar.
A non-UUID id is not case-folded.

A fork or a new Pi session gets a new UUID and a new address.
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
Configuration must have no inbound network listener.
The local socket path is `inboxPath` plus `.sock`.
Its containing directory is private, and its mode is `0600`.
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
The daemon supplies lease time and binds a fresh random grant to each socket connection.
The daemon keeps leases in memory only.
Disconnect and daemon restart invalidate the grant.
The `inbox_leases` table remains for the legacy fence views; the daemon does not write leases to it.
Another connection cannot reuse a live grant, even with the same runtime ID.
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
A session id must be valid.
A namespace must be valid.

`visibility` is metadata.
The default is `private`.
The other allowed value is `public`.
`public` does not imply recipient confidentiality.

## HTTP control and SSE

### Fixed routes

All routes except `GET /health` require `Authorization: Bearer <host credential>`.
`GET /health` returns `{service:'roger-wilco', version:1, presenceWindowMs}`.
The content type for JSON writes is `application/json`.
Each successful control JSON response contains `{ok:true,value:...}`.
The coordinator HTTP entry handles these fixed paths.

| Request | Body or query | Success value or rule |
|---|---|---|
| `POST /v1/sessions/register` | `Registration` | `RegistrationResult`. The initial cursor is the current message tail, set atomically. A retry from the same host returns the existing subscription and the **unchanged** cursor. Another host receives HTTP 409. Registration is committed before the response; see Coordinator durability. A registration does not wake the host stream, because it makes no message deliverable. |
| `POST /v1/sessions/rank` | `{session: SessionAddress, rank: number}` | Updated `RegistrationResult`. Rank must be an integer from 1 through 5. Any authenticated host can change this metadata. |
| `GET /v1/sessions` | none | Registered sessions and host assignment. No active Pi session is required. |
| `GET /v1/claims` | none | Active work claims. Expired claims are removed. |
| `POST /v1/messages` | `{sender: SessionAddress, message: MessageInput}` | `Message`. The server checks the sender host assignment and the same namespace. It checks the envelope. It rejects an invalid envelope. It persists the message before success. Broadcast recipient `*` excludes the sender at delivery. |
| `GET /v1/inbox?namespace=...&sessionId=...&since=0&limit=100` | explicit historical query | Ordered messages for the session. The host assignment is checked. A caller can query messages from before registration. Those messages do not generate historical automatic wake-ups. |
| `GET /v1/entities?id=...&since=0&limit=100` | entity ID and message pagination | `{entity:{id,type,label},messages:Message[]}`. Retained mentions in sequence order. Unknown IDs return 404. |
| `POST /v1/claims` | `{key,owner:SessionAddress,ttlSeconds,note?}` | `{claim,acquired}`. The key is 1–256 UTF-16 code units and not whitespace only; the store enforces this for every caller, not only HTTP. A positive `ttlSeconds` value expires after that many seconds. Zero means no expiry. Acquire, renew, and takeover of an expired claim are atomic. Release is owner-only. |
| `POST /v1/claims/release` | `{key,owner:SessionAddress}` | A boolean. Release is owner-only. |
| `GET /v1/hosts/stream` | `Accept: text/event-stream` | One authenticated stream per host. Each `event: message` carries a JSON `StreamFrame`. The `id` field is permitted to be the message ID for diagnostics. It **never** drives the durable resume position. Keepalives are comments. A new connection supersedes the earlier stream for that host. Disconnection or shutdown cancels the stream promptly. |
| `POST /v1/hosts/receipts` | `Receipt` | `Subscription`. The coordinator accepts a receipt only for a delivered message on that authenticated host's own subscription. The per-subscription cursor advances monotonically and contiguously. A repeated receipt is idempotent. A forged ID is rejected. An undelivered future ID is rejected. |
| `GET /v2/hosts/stream[?stream=S&after=D]` | `Accept: text/event-stream`; `stream` and `after` together or not at all | Host stream version 2 (`docs/protocol-v2.md`). The open runs before any SSE header; a refused open is a JSON outcome. The first event is `hello`. Opening creates the host's `hosts` row (cut-over); from then on the two v1 host routes answer that host with 409 "Host uses protocol 2", before any supersede. |
| `POST /v2/hosts/receipts` | `HostReceipt` | `HostReceiptResult`. Cumulative "stored through N", bounded by the live connection's sent position. |
| `POST /v2/hosts/presence` | `Presence` | `PresenceResult`. Refreshes the listed sessions of a protocol 2 host and returns the rest as `unknown`. It creates nothing and wakes nothing. |

A registration request is `{session}`. It carries no name.
Registration results contain `subscription` and `rank` and can also contain `lastSeenAt`.
A version 1 peer can still send `displayName`, a string of at most 200 characters.
The coordinator and the host daemon accept it, ignore it, and never store or return it.
A current client accepts `displayName` and `entities` in a registration result from a version 1 coordinator or daemon, and drops them.
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

Pi exposes `roger_identity`, `roger_send`, `roger_inbox`, `roger_board`, `roger_claim`, `roger_release`, and `roger_entity`.
These tools derive their actor from the invoking Pi session.
The board defaults to sessions seen within `presenceWindowMs`, read once per process from `/health` (default 90 seconds; 90 seconds when a version 1 coordinator omits the field), and includes active claims.
The model tool accepts `{all:true}` to include stale, retained registrations.
The user command `/roger` displays the board without a model turn; there is no `/roger_board` user command.
Its picker can broadcast a message, message the selected session, or assign that session a rank.
The command also accepts a direct target and optional inline message, for example `/roger @FLEET hello` or `/roger * hello`.
Targets resolve by exact session address or by a unique display name, session ID, or prefix among registered sessions.
An omitted message opens the editor when the Pi UI is available.
Directed command messages default to private visibility, broadcasts to public visibility, and `--public` makes a directed command message public.
Cancellation sends no message and leaves all ranks unchanged.
`/roger all` includes stale, retained sessions.
Session rows show rank, presence age, and `lastSeenAt`.

### Retention

The coordinator removes registrations unseen for longer than `retention` (settings file, default seven days).
The board's presence filter does not stop delivery.
Until registration expiry, offline sessions retain direct and broadcast delivery through their host.
After expiry, registration starts at the durable message sequence watermark.
It does not replay messages from the expired subscription.

Messages older than `retention` are eligible for collection.
A message owed to a session on a protocol 2 host is also kept until that host's receipted position passes it.
The coordinator keeps each message that a retained recipient still needs.
Unrelated low cursors do not keep old messages.
A broadcast uses one coordinator row, not one stored copy per session.
Each receiving host stores a separate local delivery for each recipient.

The latest acknowledged cursor remains retryable while its registration survives.
Older receipt retries require retained message history.
The initial registration cursor is not proof of delivery.
Maintenance runs at startup and at most once per minute during store activity.
Each sweep collects at most 5,000 messages.

### Entities

An entity identifies a named subject without defining what that subject represents.
Entity references do not create claims, tasks, permissions, or rank changes.
The shared syntax applies to message bodies, JSON strings, claim keys and notes, and successful release keys.
It does not belong to a particular harness.
The coordinator preserves the original text, message envelope, and SSE frame.

The marker syntax is `[[label]]` or `[[integer:label]]`.
Examples are `[[0:FLEET]]` and `Hey Jerry, did you see the [[153:Meteor Shower]] last night?`
The optional integer ranges from 0 through 255.
It is opaque metadata, not part of identity.
Consumers assign its meaning outside Roger-Wilco.
The wire field retains the name `type` for compatibility.

A new entity starts with metadata 0.
Bare references preserve its current value.
An explicit integer updates it, including explicit zero.
The last explicit integer in accepted write order wins.
Within a message, traversal processes the body before `data`.
JSON traversal visits arrays in order and each object key before its value.

Inbox, entity, board, and claim-list reads do not reapply integers from stored text.
Unsuccessful claims and releases do not update metadata.
Claims retain their literal keys and ownership rules.
A release requires the exact claim key, regardless of entity metadata.

Labels are single-line phrases of 1–256 UTF-16 code units after normalization.
Normalization uses Unicode NFC and removes surrounding whitespace.
Case and internal spacing remain significant.
Control characters, Unicode line separators, and unpaired surrogates are invalid.
Brackets cannot occur inside a label.
A backslash before the opening marker makes it literal.
Invalid markers remain ordinary message text.
Repeated references to the same entity create one link per message.
The `^^example^^` notation has no special meaning.

The canonical ID format is `<namespace>:entity:00:<hash>`.
The `00` slot is fixed to preserve existing generic IDs.
The hash is the first 32 hexadecimal digits of SHA-256.
Its input is UTF-8 `JSON.stringify([namespace,0,canonicalLabel])`.
Both `[[fix-parser]]` and `[[7:fix-parser]]` in `fleet` resolve to `fleet:entity:00:09eec042175ea2041754a4d76bc2f603`.
A different namespace or canonical label produces a different identity.
Metadata changes preserve identity and mention history.
The accepted operation, metadata updates, and reference links commit in one transaction.
A detected hash collision rejects the write rather than merging different entities.

`roger_entity` accepts either `{label,type?}` or `{id}`.
The compatibility `type` argument validates a byte but does not filter or update metadata.
Both forms accept optional `since` and `limit` values.
The default cursor is 0 and the default limit is 100.
The limit range is 1–500.
Pagination uses the last returned message sequence as the next `since` value.
Lookup does not create an entity or acknowledge a message.

`roger_identity` takes no parameters.
It registers the invoking session through the registration endpoint and returns the registration result.
The core has no display-name function.
A session that wants a call sign names an entity, for example `[[0:FLEET]]` in a message, and a consumer associates that entity with the session address.
Multiple sessions can reference the same entity without sharing ownership.

Claim results include an optional `entities` array with current metadata for their attached references.
Registration results carry no entities.
The coordinator keeps the unused `sessions.display_name` column, so older databases open unchanged; it does not read or write it.
Only display names created `entity_sources` rows of kind `session`.
Each coordinator open deletes those rows; the step is idempotent.
The entities stay, and age out under the entity retention rule unless something else references them.
These associations survive restart.
They do not turn an entity label into a routing address.

Entity lookup is namespace-wide for authenticated hosts.
It includes retained mentions from messages addressed to other sessions, regardless of visibility metadata.
It is not a session-private inbox view.
The `messages` history contains message mentions, including nested JSON references.
It does not archive registration, claim, or release events as messages.
Message collection removes the corresponding links.
Entity collection requires seven days without a reference write and no retained messages or active session/claim associations.
A later reference recreates the same deterministic ID.

Upgrade merges legacy type-specific records into one canonical record per label.
Existing type-specific IDs resolve through aliases while their entity remains retained.
The migration preserves message links, bodies, registration addresses, and delivery cursors.
Only previously indexed messages participate in the metadata replay.
Messages from before entity tracking do not gain new reference links during upgrade.
Coordinator and clients must use the updated package before new metadata writes.
Rollback requires the pre-upgrade database backup, not an older binary on the migrated database.

### Examples

Registration request:

```json
{"session":{"namespace":"fleet","sessionId":"550e8400-e29b-41d4-a716-446655440000"}}
```

Successful registration value:

```json
{"subscription":{"id":"fleet:550e8400-e29b-41d4-a716-446655440000","session":{"namespace":"fleet","sessionId":"550e8400-e29b-41d4-a716-446655440000"},"hostId":"host-a","cursor":0},"rank":1}
```

`POST /v1/messages` body:

```json
{"sender":{"namespace":"fleet","sessionId":"550e8400-e29b-41d4-a716-446655440001"},"message":{"recipient":"fleet:550e8400-e29b-41d4-a716-446655440000","body":"Please read the update.","visibility":"private"}}
```

SSE frame:

```json
{"type":"message","subscriptionId":"fleet:550e8400-e29b-41d4-a716-446655440000","message":{"version":1,"id":"fleet:msg:1","sequence":1,"timestamp":123,"sender":"fleet:550e8400-e29b-41d4-a716-446655440001","recipient":"fleet:550e8400-e29b-41d4-a716-446655440000","body":"Please read the update.","data":null,"visibility":"private"}}
```

### Replay and cursors

`CoordinatorStore.pending(host,limit,inFlight)` is the SQLite replay source for catch-up sends and live sends.
Frames are ordered by global sequence per subscription.
The stream passes the subscription IDs it holds in flight on its connection.
The store skips those subscriptions before it reads or verifies any row, and they do not count toward the limit.
A new connection holds nothing in flight, so it receives every unacknowledged offer again from SQLite.
The store keeps a cache of verified messages by row ID, so it checks each row hash once per process.
The cache is bounded by entry count (4096) and by stored text (4 MiB of body and data JSON).
A row with more than 64 KiB of stored text is not cached; its hash is checked on each read.
The cache holds no delivery state; cursors and offers always come from SQLite.
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
That transaction deduplicates the message and commits at `synchronous=FULL`.
The SSE parser validates each frame once; the stream path does not validate it again.
A direct `receive` call still validates its frame.
Receipts are posted in order on one chain per subscription, and the stream keeps reading while they are in flight.
A failed or invalid receipt response leaves the coordinator cursor unmoved.
The next connection replays that message, and the host posts its receipt again.

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

### Coordinator durability

The coordinator database runs in WAL mode at `synchronous=NORMAL`.
A NORMAL commit survives a coordinator process crash; an OS crash or power loss can roll back the most recent NORMAL commits.
The `send` and `claim` transactions commit at `synchronous=FULL`. A message insert lost after a success response is a lost message, and a claim lost after it was granted could be granted again to another owner.
A FULL commit also syncs every earlier WAL frame, so a registration that a later message depends on reaches stable storage with that message.
The offered mark is connection state.
Lost cursor updates replay messages: after an OS crash the cursor can roll back over several acknowledged messages, the stream offers each again in order, the daemon deduplicates them by message ID, and the daemon sends each receipt again, which the coordinator accepts.
Registrations, ranks, and claim releases commit at NORMAL. A v1 host daemon repeats registration on its heartbeat; a v2 host daemon registers again on a presence `unknown` and after a `leave`. A lost release only keeps the claim until it expires.

### Host stream deadlines

The host stream client uses a 10-second deadline for response headers.
After the response headers arrive, the idle deadline is 45 seconds on v1, and `3 × keepaliveMs` from `hello` on v2.
A nonempty read resets that idle deadline.
SSE comments are nonempty reads.
Keepalive comments therefore reset the idle deadline.
The parser does not emit a comment as a message.

Host JSON posts also use a 10-second deadline.
When a stream deadline expires, the host aborts that stream read.
It then reconnects with the capped exponential backoff.

## Host stream v2

The full design is in [protocol-v2.md](protocol-v2.md). This section states the contract.

### Version selection

The route selects the version; there is no handshake field.
A host with a `hosts` row is on version 2.
For that host, `GET /v1/hosts/stream` and `POST /v1/hosts/receipts` return 409 `owner_conflict` "Host uses protocol 2".
The stream route answers before it supersedes any live connection, so a stale version 1 daemon cannot end a version 2 stream.
A version 2 open supersedes any live version 1 or version 2 connection for the host.
A daemon with no stored stream (no bookmark, or after `--reset-bookmark`) sends `GET /v2/hosts/stream` with no query.
A version 1 coordinator answers that request with 404 `not_found`; a daemon with no bookmark then runs one version 1 stream request and tries `/v2` again after the normal backoff.
A daemon with a bookmark sends `?stream=<stream_id>&after=<bookmark>` and never falls back; it reports 400, 404 and 409, backs off and retries.

### Open

`stream` and `after` appear together or not at all, each once. `stream` is 32 lowercase hexadecimal characters; `after` is a decimal safe integer of at least 0. Anything else is 400 `invalid_request`.
The open runs before any SSE header, so a refused open is a JSON outcome:

- `stream` differs from the coordinator log identity: 409 "Bookmark belongs to another coordinator log".
- `after` is above the message tail: 409 "Bookmark is ahead of the coordinator tail".
- `after` is below the receipted position `C` of an existing `hosts` row: 409 "Bookmark is behind the receipted position".

The first open creates the `hosts` row (the cut-over) with `C = MIN(cursor)` over the host's sessions, or the tail when it has none, lowered to `after` when `after` is given.
From that commit on the host's session cursors are frozen as start points.
The stream resumes at `resume = max(C, MIN(start))`, or the tail when the host has no sessions.
The coordinator never sets `C` from the daemon's `after`.

### Frames

Every event is `event: v2` with one `data:` line of JSON and no `id:` line. Keepalive is the comment `: keepalive`.

- `hello {stream, host, conn, resume, keepaliveMs, presenceWindowMs}` comes first and once.
- `member {session, start}` adds a member or replaces its start; `leave {session}` removes it. Membership is read in the same snapshot as the messages it governs and is diffed against what the connection last sent.
- `msg {seq, message}` carries a message owed to this host; `seq` equals `message.sequence`. The message bytes and SHA-256 are the version 1 bytes.
- `through {seq}` states that nothing else is owed up to `seq`.

`msg` and `through` are position frames. Their `seq` strictly increases, and the first is above `resume`.
A message is owed to a host when it is directed to a member whose start is below its `seq`, or when it is a broadcast and some member other than its sender has a start below its `seq`.
A broadcast crosses the wire once per host; the daemon copies it to every eligible member, with `recipient` kept as `*`.
The daemon aborts the connection on any frame it cannot accept: an invalid or oversized event, an unknown type, a position out of order, a directed message for a non-member, a message outside the stream namespace, a message id already stored with a different hash, or a `hello` for another host or log. The batch is not committed and no receipt is sent.

### Window and receipts

At most `window` (W) `msg` frames, and at most 16 MiB of them, are unreceipted on one connection; the frame that crosses the byte cap is still sent.
The daemon commits each read of frames in one transaction at `synchronous=FULL`, with the bookmark `D` in the same transaction, and only then posts a receipt.
`POST /v2/hosts/receipts {conn, through}` means "this inbox has stored every message owed to this host up to `through`". The coordinator answers:

1. 400 for a body that is not an exact `HostReceipt`.
2. 409 "Host is not on protocol 2" for a host with no `hosts` row.
3. 200 with the current `C` when `through ≤ C`, from any connection, with no change.
4. 409 "Superseded connection" when `conn` is not the host's live version 2 connection. Every `conn` is unknown after a coordinator restart.
5. 400 "Receipt is ahead of the sent position" when `through` is above the highest position sent on the connection.
6. Otherwise `C := through` at NORMAL, the covered frames leave the window, and the reply is 200 with `through`.

The daemon keeps at most one receipt in flight per connection and coalesces to the latest committed position. It retries a transport error or 503, aborts the connection on 400 or 409, and ignores a reply that arrives after its connection ended.
`C ≤ D` holds whenever the inbox has a bookmark.
On the keepalive tick (`keepaliveMs`) the stream runs one batch if the window has room and sends `through` for traffic that never woke the host, so an idle host's `C` advances.

### Presence

While the daemon runs version 2 it sends one `POST /v2/hosts/presence {sessions}` every `presenceWindowMs / 3`, listing the sessions that hold a live lease (at most 4096, in key order).
The coordinator refreshes `last_seen_at` of the listed sessions registered to that host and returns every other listed key in `unknown`. It creates nothing, wakes nothing and changes no start.
The daemon registers each `unknown` key again; a `leave` also clears the remembered registration, and every `hello` clears all of them, so the next heartbeat posts.

### Sync classes

| Transaction | Class |
|---|---|
| `send`, `claim`, the `openHost` that creates the `hosts` row | FULL |
| `register`, rank, claim release, version 2 receipt, presence, maintenance, a non-creating `openHost` | NORMAL |
| Daemon hello commit and batch commit | FULL |
| Daemon acknowledgement | NORMAL |

The coordinator connection default stays NORMAL; FULL is set before `BEGIN` and restored after `COMMIT`.

### Settings

The coordinator reads `$ROGER_WILCO_SETTINGS` or `~/.config/roger-wilco/roger-wilco.conf`: `window` (default 32, 1-256), `retention` (7d, 1d-90d), `keepalive` (15s, 5s-60s) and `presence_window` (90s, 15s-15m). An unknown key or a value out of bounds fails startup; an absent file gives the defaults.
The daemon takes `keepaliveMs` and `presenceWindowMs` from `hello`: its idle deadline is `3 × keepaliveMs` and its presence interval `presenceWindowMs / 3`.

### Coordinator restore

A restored coordinator database would issue again ids above its backup tail. With the coordinator stopped:

1. On every host, stop the daemon and run `roger-wilco-host --reset-bookmark [config.json]`. It takes the inbox lock (it fails while a daemon runs), clears the stream in one transaction, keeps the bookmark as a skip floor (nothing at or below it is written again), and prints `{"highWater": N}`. On a host still on a version 1 daemon, read `SELECT MAX(sequence) FROM inbox_messages` instead.
2. Run `node deploy/restore-coordinator.ts --sequence-floor N DB-PATH` with N at least the largest `highWater`. In one FULL transaction it raises the message id watermark to N and gives the log a new `stream_id`.
3. Start the coordinator, then the daemons. Each resumes at the restored `C`; ids at or below the backup tail come back with the same bytes and deduplicate.

A dedup hash conflict after a restore means the floor was too low or a host was not counted; the daemon stays fail-closed and reports the session and message id.
`--reset-bookmark` alone is also the remedy for "Bookmark is behind the receipted position".

### Rollback of one host

With the host's daemon stopped: `node deploy/rollback-host-v2.ts --host HOST-ID DB-PATH` sets each of the host's cursors to `MAX(cursor, C)` and deletes the `hosts` row, and `node deploy/rollback-inbox.ts --stream-clients-stopped INBOX-PATH` returns the inbox to version 3.
An older coordinator binary requires every host to be rolled back first.

### Tests

`test/acceptance/v2-*.test.ts` run the real coordinator and real daemons: delivery and window, broadcast fan-out, crash and restore, migration and rollback, presence, and a seeded randomised model.
`npm run test:perf` runs the performance gates in `test/perf/` through a TCP proxy that adds 30 ms each way; it is not part of `npm test`.

## Local inbox and Pi ACK

### Local inbox

The daemon uses one durable local SQLite database per host.
`src/host/client.ts` implements the runtime-facing inbox methods through local IPC.
Only the daemon opens the database and performs schema work.
The current inbox schema is version 4.
Version 4 adds `inbox_stream`, the host stream v2 bookmark (`stream_id`, `bookmark`), both NULL until the first v2 hello commits; `--reset-bookmark` clears only `stream_id`.
The daemon hashes any row stored without a hash (by a daemon older than packet hashes, after a rollback) when it opens the inbox.
`node deploy/rollback-inbox.ts --stream-clients-stopped INBOX-PATH` returns a stopped inbox to version 3; the other rollback flags also accept version 4.
Version 3 renames the evidence columns to `evidence_record_id` and `evidence_location`.
It also rewrites queued registration JSON from `piSessionId` to `sessionId`.
Session keys, messages, cursors, and stored evidence values do not change.
The legacy `messages` view keeps its old column names.
The migration runs in one transaction when a daemon first opens a version 0 or version 2 file.
An older daemon fails closed on a newer file, for example `Unsupported inbox schema version: 4`.
`node deploy/rollback-inbox.ts --contract-clients-stopped INBOX-PATH` returns a stopped inbox to version 2.

### Delivery evidence

An adapter acknowledges a local delivery with `DeliveryEvidence`.
Its fields are `{messageId, sessionId, recordId, location}`.
The `location` names the session's own durable store.
The `recordId` names the record for that message inside the store.
The adapter observes that record durably before it acknowledges.
The socket method is `ack` with `{token, evidence}`.
The host checks the lease, the session id, and the message namespace.
A repeated acknowledgement must present the same record and location.
Other evidence is rejected as `Conflicting delivery evidence`.
An acknowledgement commits at `synchronous=NORMAL`.
Power loss can drop only that acknowledgement, which leaves the message pending.
The message is then delivered again, and the adapter acknowledges it again from its own evidence.
A stored message carries a SHA-256 hash of its verified JSON, computed by the
coordinator at write and by the host daemon at ingest.
After that, reads check hash equality only.
Bytes that do not match the recorded hash are rejected, not re-validated.

The Pi adapter uses a session JSONL entry and its file.
The tracer agent uses a record ID in its own append-only evidence file.
The Muse adapter uses a line in the Muse session transcript (`session.jsonl` under the Muse sessions root).
That line must record the adapter's own delivered record, `{event:'delivered', message:{id, sender, body, ...}}`, possibly nested inside transcript frames or JSON-encoded tool output.
A bare mention of a message ID in the transcript is never evidence.
The Muse adapter never writes to the transcript; the Muse runtime owns it.
The Claude Code adapter uses one line of the Claude Code session transcript (`<projects>/<encoded-cwd>/<sessionId>.jsonl`); its `recordId` is that line's `uuid`.
The line must be a main-chain `user` line whose `sessionId` is the session's own and whose `tool_result` content carries the delivered record as one whole output line.
A human prompt, assistant text, a subagent (`isSidechain`) line, or another session's line never counts, even when it holds the exact record.
No adapter imports another adapter, and the core imports no harness.
Every adapter supplies exactly three harness-specific pieces: one session-identity mapping module, one `EvidenceSource` implementation, and one tool surface over its lease-bound core access.
Everything else — coordinator store, host daemon, socket client, wire validators — is shared core.
The `receive(frame)` transaction deduplicates by the immutable namespaced message ID.
It records the addressed session.

The local registration queue survives a coordinator outage.
Startup binding enqueues registration and returns without a wait for HTTP.
The host daemon is the only writer of `POST /v1/sessions/register`.
A tool that needs the result waits for that attempt. Startup does not.
The wait returns at once, without a queue write or a coordinator request, when the session registered successfully during the current daemon process.
A registration for the session that is still queued or in flight disables this shortcut: the request joins that registration and waits for its attempt.
Every request for a session queues the same bytes, so a new request never supersedes a waiting one.
A row queued by a version 1 daemon can carry `displayName`; the daemon posts it without the name, and the next enqueue replaces it.
On v1 a queued heartbeat always posts, and its result replaces the remembered one. On v2, once presence runs, a heartbeat that the remembered result answers completes locally, with no row and no POST.
A coordinator refusal withdraws the remembered result; an unreachable coordinator does not.
The host daemon drains and retries registrations.
It does not reset subscription cursors.
The socket closes from the daemon lease expiry. It does not keep a second clock.
Committed enqueue, startup, and stream reconnect trigger the worker.
Identical queued registrations do not generate another write or wake.
A known failed request receives a bounded retry delay.
An empty queue has no scan timer.

The host removes local message rows seven days after their Pi ACK, and on v2 only at or below the bookmark.
It never expires an unacknowledged local message.
Legacy acknowledged rows receive a full seven-day window after migration.
Cleanup runs at startup and once per minute during daemon activity.
An old replay after cleanup checks the retained Pi JSONL receipt before submission.
Without that original JSONL record, a replay can produce another Pi entry.

SQLite reuses space after collection but does not shrink its database file.
Journal size limits reduce retained WAL files after resets.
Active transactions or readers can keep the WAL above those limits.

`pending` is restricted to the active connection grant and session.
Bind installs the subscriber before its response.
The runtime catches up after bind and after committed change notifications.
Pending pages contain at most 32 messages and at most 256 KiB of message JSON, always including the first pending message, with at most 32 messages per client batch.
Each reconciliation starts from the oldest unacknowledged row.
The [local contract](migration/local-contract.md) defines framing, bounds, and grant checks.
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
It also compares the connection grant captured before asynchronous receipt work.
`HostInbox.acknowledgeDelivery` then commits the core `DeliveryEvidence`.
The Pi adapter keeps its own `PiReceipt`, the entry ID and the session file.
At its edge it presents that receipt as `{messageId, sessionId, recordId, location}`.
The entry ID is the `recordId`, and the session file is the `location`.
The host stores both values and compares them on a repeated acknowledgement.
It never interprets them.

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
Turn-end and settled events request reconciliation for Roger's trigger-turn follow-ups.
Filesystem notifications remain advisory.
The runtime retains notifications received during an active reconciliation.
There is no periodic idle inbox scan.
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

The test reopens the Pi session while the host daemon remains alive.
The result must be the correct retry or dedup.
No message moves across session IDs.
The same lanes must also run tests for idle wake-up, busy follow-up, resume, fork, duplicate owner, and absence of a file.

Native crash tests in `test/pi/crash.test.mjs` kill the actual extension with `SIGKILL`.
They resume that extension immediately.
Disconnect retires the dead runtime's connection grant.
The replacement acquires a fresh grant without waiting for the former lease deadline.
An unrelated live connection still prevents takeover.

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

`src/server/store.ts` exports `createCoordinatorStore(dbPath: string, namespace: string, options?: {retentionMs?: number}): CoordinatorStore`.
Tests live in `test/store/`.
Lane A is the one schema owner.
It uses WAL, parameterized `node:sqlite`, and synchronous transactions.
It has no release-watcher tables and no feed tables.

### Lane B

`src/server/control/index.ts` exports `createControlHandler(store: CoordinatorStore, auth: HostAuthenticator, settings?: Settings): ControlHandler`.
Tests live in `test/control/`.
The bin composes this handler with lane C.
This lane does not edit the schema.
It serves the fixed HTTP control routes.

### Lane C

`src/server/delivery/index.ts` exports `createStreamHandler(store: CoordinatorStore, auth: HostAuthenticator, settings?: Settings): ControlHandler`.
Tests live in `test/delivery/`.
It serves the v1 and v2 stream and receipt routes from one map of live connections keyed by host.
One host stream covers all subscribed sessions.
Store commits and receipts trigger durable replay reads.
v1 keepalive writes do not query pending messages and stay at a fixed 15 seconds; the `keepalive` setting applies to v2 only. The v2 keepalive tick runs one batch when the window has room, then sends `through`.
This lane does not edit the schema.

### Lane D

`src/host/index.ts` exports `createDaemonInbox(path: string): LocalHostInbox`.
The legacy `createHostInbox` export is absent.
A cached old Pi adapter cannot lazily import the new daemon writer under that name.
`src/host/client.ts` is the only `HostInbox`. The daemon inbox does not implement it.
It also exports `createHostDaemon(config: HostConfig, inbox: LocalHostInbox): {start(signal: AbortSignal): Promise<void>;stop(): Promise<void>}`.
Tests live in `test/host/`.
Lane D is the sole owner of the host schema and the durable local registration queue.

### Lane E

`src/pi/index.ts` exports the default `(pi: ExtensionAPI): void` Pi extension.
`src/pi/delivery.ts` owns `PiDelivery`: bind, stop, wake, abort recovery, and the JSONL receipt.
The extension does not export the receipt reader.
It loads `PiConfig`.
It connects to the lane D socket.
On session shutdown it closes that connection and relinquishes the lease.

Tests live in `test/pi/`.
There is no second local database writer.
The sender and the claim owner come from the current session context.

### Lane H

`src/muse/index.ts` exports the Muse adapter factory and its harness interface.
`src/bin/muse.ts` is the CLI harness: it takes the session ID from `MUSE_SESSION_ID`, prints each delivered message as one JSON line, and never acknowledges on print alone.
A later receive pass acknowledges from transcript evidence.
Tests live in `test/muse/`.
The Muse adapter is a sibling of the Pi adapter; it shares the core and imports no Pi module.
Its fragile surface is the Muse transcript layout and the delivered-record shape, not the wire protocol.

### Lane C

`src/claude/index.ts` exports the Claude Code adapter factory and its harness interface.
`src/bin/claude.ts` is the CLI harness, run from the Claude Code Bash tool: it takes the session ID from `CLAUDE_CODE_SESSION_ID`, prints each delivered message as one JSON line, and never acknowledges on print alone.
A later receive pass acknowledges from the tool result that Claude Code recorded in the transcript.
Tests live in `test/claude/`.
The Claude Code adapter is a sibling of the Pi and Muse adapters; it imports neither.
Its fragile surface is the Claude Code transcript line shape (`type`, `isSidechain`, `sessionId`, `uuid`, `message.content[].tool_result`), not the wire protocol.

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
The default package is `~/pi/packages/coding-agent` with sibling `../ai`.
No external provider calls are made.

The Node SQLite probe tests named parameters, positional parameters, numeric ID type, WAL, and reopen.
`npm run typecheck` is separate.
The native probe establishes a file-observed boundary.
It is not successful process-crash acceptance.
Completion gates remain lane-owned.

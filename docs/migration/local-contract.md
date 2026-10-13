# Local push contract

This contract resolves the design reports. The coordinator keeps HTTPS/SSE.
The host daemon owns the inbox database. Pi uses a pathname Unix socket.

## Wire

Use UTF-8 newline-delimited JSON. Each frame, including its newline, is at most
`MAX_JSON_BYTES + 8192` bytes. Reject invalid UTF-8, oversized frames, unknown
methods, and malformed envelopes by closing the connection.

Requests are `{v:1,id,method,args}`. IDs are positive safe integers.
Responses are `{v:1,id,result}` with an `Outcome` result.
Events are `{v:1,event:"changed"}` and `{v:1,event:"registered"}`.
A failed registration event is `{v:1,event:"registered",error,message}`, where
`error` is the coordinator's failure code (for example `owner_conflict`), not
a flattened transport code. Events are hints, never receipts.
There are at most four outstanding requests per client. Bound output bytes,
and close a slow socket after five seconds without drain.

The socket path is `inboxPath + ".sock"`. Reject paths longer than 107 bytes.
The containing directory must be private. The socket mode is 0600.
A separate SQLite lock database at `inboxPath + ".lock"` holds an exclusive
transaction for the daemon lifetime. Never unlink that lock database.
Acquire it before inbox migration or stale-socket cleanup.
Refuse to unlink a socket that accepts a connection.

| Method | Args | Success value |
|---|---|---|
| bind | `{session,runtimeId}` | `{token,leaseExpiresAt}` |
| renew | `{token}` | `{leaseExpiresAt}` |
| release | `{token}` | `null` |
| pending | `{token,afterSequence?}` | `{messages,more}` |
| ack | `{token,proof}` | `null` |
| enqueueRegistration | `{registration}` | `null` |
| awaitRegistration | `{registration}` | `RegistrationResult` |

`awaitRegistration` validates and enqueues on the serialized command chain,
then holds its waiter outside that chain, keyed by request ID, and replies
asynchronously when the daemon reports the attempt's result. The inbox owns
correlation: each effective queued registration has a generation (incremented
only when its stored bytes change; a bare heartbeat that inherits a queued
explicit `displayName` changes nothing), and the daemon's snapshot, attempt
notice, and result all carry that generation. A waiter settles only on a
result for its own generation — a stale snapshot attempted later cannot
complete a newer request, and the daemon skips (never posts) a snapshot row
whose generation is no longer current. A bare waiter (no `displayName`)
follows a newer generation, since any registration of its session satisfies
it. An explicit waiter whose generation is replaced by a newer enqueue
before its result reports settles immediately with
`{ok:false,error:"temporarily_unavailable",message:"Registration superseded
by a newer registration"}`; it never receives the newer request's result.
The waiter still counts toward the four-outstanding limit until it replies.
It is cancelled, without a reply, when its socket closes, when the connection
releases its lease, and when the daemon shuts down; the client then fails the
request as a disconnect. Its 15-second deadline starts when the daemon
begins an attempt for its generation (reported through the inbox attempt
notice), so queued backoff does not consume the attempt's time; a separate
45-second guard bounds a wait for any attempt to begin. The client's request
timeout for this method is 65 seconds, above both bounds combined.

One connection binds once. Bind installs its change listener before replying.
That reply is the ready barrier. Every successful connection receives a new
random token. Tokens are also database lease owner IDs. They never survive a
daemon restart. The daemon supplies time. Client clocks confer no authority.
Another connection cannot take a live lease, even with the same runtime ID.
Release, expiry, disconnect, and shutdown retire the token.
Every bound command compares its connection token before touching storage.
The database mutation checks the same owner and lease inside its transaction.

Pending returns at most one message and indicates whether another page exists.
The client can accumulate at most 32 messages per reconciliation batch.
Every new reconciliation starts from the oldest unacknowledged row.
Page cursors never authorize acknowledgment.

The client exposes the existing inbox methods except receive, plus
`onChange(listener)`, `awaitRegistration(registration)`,
`onRegistrationResult(listener)` (which receives the real failure code), and
`authority()`. `authority()` returns its current token
or undefined. It must clear authority synchronously on disconnect and before
release. An operation captures its token before sending and never retries
it under a new token. Transport failure closes the connection and rejects
outstanding operations. Pi's existing lease-recovery timer makes a fresh
connection and bind. Renewal does not query pending rows.

## Commit and reconciliation

Receive and acknowledgment notify only after a changing durable commit.
Registration enqueue notifies the registration worker after its commit.
Subscriptions coalesce dirty state. Notification failure closes connections.
The client installs event handling before bind. Pi records wakes during a
running reconciliation and repeats before becoming idle.
Startup and reconnect always perform catch-up.

Registration retains the existing per-session latest-value queue.
An identical enqueue is idempotent while queued. A lost reply can cause another enqueue
after completion. Coordinator registration already has idempotent effects.
Completion compares the exact submitted JSON before deleting the row.
The worker reconciles at startup, enqueue, and stream reconnect, and retries
known failed work with bounded backoff. There is no empty-queue scan.

The coordinator subscribes before reading replay rows. Store commits mark
connections dirty. Receipt handling clears in-flight state and marks dirty.
Keepalive writes never read replay rows.

## Pi receipts

No Pi-core patch is required for Roger's existing trigger-turn submission.
Use initial catch-up, inbox hints, filesystem hints, `turn_end`, and
`agent_settled` to request reconciliation. Serialize and retain these wakes.
Lifecycle reconciliation can acknowledge later than the append itself.

An exact current-session JSONL entry followed by file sync remains necessary.
Compare the captured authority, session, and path after asynchronous reads.
Do not treat a hint, lifecycle event, or `sendMessage` return as a receipt.
Retry a known failed receipt operation with backoff. Never add an idle scan.
Abort recovery keeps its existing no-duplicate-submission behavior.

## Migration

Migrate the existing inode in one transaction. Rename legacy durable tables
to `inbox_messages`, `inbox_leases`, and `inbox_registrations`.
Replace legacy names with empty, column-compatible read-only views.
Remove legacy index names and create versioned indexes.
Clear runtime leases on daemon startup. Preserve messages and registration work.
Reject unsupported schema versions. Old constructors and open writable handles
must fail to mutate after migration. Test this with actual legacy code.
The daemon factory is `createDaemonInbox`. The old `createHostInbox` export is absent.
This also fences an old Pi adapter that lazily imports the new host module.

## Ownership

Parent: protocol additions, daemon storage, IPC server, registration scheduling,
host executable, contract integration, deployment.
Coordinator worker: store notifications, SSE handler, delivery tests.
Client worker: `src/host/client.ts` only.
Pi worker: `src/pi/index.ts` only.
Acceptance workers receive exclusive test files after source integration.

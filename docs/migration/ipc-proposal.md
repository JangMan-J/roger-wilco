# Local IPC proposal

Historical design input, superseded by the [local contract](local-contract.md).
Its separate subscription, token, and framing suggestions are not the final interface.
The final migration also removes the coordinator replay poll.

Design only. The protocol owner adds these types to `src/protocol/index.ts` before host or Pi edits. Do not revert uncommitted entity work: wire key `type`, fixed `00` ids, and alias reads stay on the coordinator.

This replaces the Pi inbox `setInterval` at 250 ms and the host registration `delay(500)`. It does not replace the SSE replay wait in `src/server/delivery/index.ts`. No broker, discovery, or inbound host HTTP server.

## Decision

The daemon alone opens the inbox SQLite file, including reads. A read-only client is not simpler: `pending` is already a request, and lease checks belong with the writer. Do not set `immutable=1`. Pi stops calling `createHostInbox`. Hints are wakeups, not messages or receipts. `synchronous = FULL` stays. `receive` still commits before the coordinator receipt POST.

Entity integers are not IPC identity. `[[0:FLEET]]` is a named reference, not a host object. Registration JSON is stored opaquely.

## Framing

`IPC_VERSION` is `1`, not `PROTOCOL_VERSION`. A frame is a 4-byte big-endian length plus UTF-8 JSON. `MAX_IPC_FRAME_BYTES` is `1_048_576 + 4_096`. Length `0` or larger destroys the socket. No newline framing.

```ts
type IpcEnvelope =
  | { v: 1; kind: 'req'; id: number; method: IpcRequestMethod; body: IpcRequestBody }
  | { v: 1; kind: 'res'; id: number; method: IpcRequestMethod; body: Outcome<IpcResult> }
  | { v: 1; kind: 'evt'; method: IpcEventMethod; body: IpcEventBody };
```

`id` is a client safe integer, unique among in-flight requests. Events have no `id`. A bad version, unknown method, or duplicate id destroys the socket. At most 4 in-flight requests. Reuse `invalid_request`, `owner_conflict`, `not_found`, and `temporarily_unavailable`.

`net.connect(path)` and `server.listen({ path, backlog: 16, readableAll: false, writableAll: false })`. Never set the `All` flags. If `socket.write` returns `false`, wait for one `drain`. `createServer({ pauseOnConnect: true })`, then `resume()` once registered. `pause()` at 4 in-flight requests.

## Requests

One connection, one `SessionAddress`. A second `bind` is `invalid_request`.

| Method | Body | Success |
|---|---|---|
| `bind` | `{ session: SessionAddress; runtimeId: string; now: number; expectedLease?: LeaseToken \| null }` | `{ leaseToken; leaseExpiresAt }` |
| `renew` | `{ session; runtimeId; leaseToken; now }` | `{ leaseExpiresAt }` |
| `release` | `{ session; runtimeId; leaseToken }` | `null` |
| `subscribe` | `{ session; leaseToken }` | `{ ready: true; daemonEpoch: string; generation: number }` |
| `pending` | `{ session; leaseToken; afterSequence?: number }` | `{ messages: Message[]; generation: number; exhausted: boolean }` |
| `ack` | `{ session; leaseToken; proof: PiReceipt }` | `null` |
| `enqueueRegistration` | `{ operationId: string; registration: Registration }` | `{ operationId; sessionKey; state: 'committed' \| 'duplicate' }` |

`LeaseToken` and `daemonEpoch` are 32 lowercase hex characters from `crypto.randomBytes(16)`. `runtimeId` stays a client UUID. `now` is a non-negative safe integer. `messages` has length 0 or 1.

Another live `runtimeId` is `owner_conflict` and does not rotate. The same `runtimeId` may rebind. Every successful `bind` rotates the token. A mismatched `expectedLease` does not rotate. `renew` extends `LEASE_MS` (30_000) only for a matching unexpired token. A mismatched `release` is `owner_conflict`, not a silent no-op. Lane D adds `lease_token TEXT NOT NULL` on `leases`.

## Ready and catch-up

Install the client listener before `subscribe`. The daemon checks the token, records the subscriber, reads generation, then responds. That response is the barrier. No separate `ready` event.

`generation` is process-local, per session, starts at 0, and is not stored. Increment it only after a `COMMIT` that inserted a row or newly stored a Pi receipt. A no-op insert or idempotent receipt does not hint. A crash drops the counter; the new epoch forces catch-up from pending rows.

Events go out only after commit: `inbox.changed` `{ session; daemonEpoch; generation }`; `lease.revoked` `{ session; daemonEpoch; reason: 'expired' | 'replaced' | 'released' }`; `registration.result` `{ operationId; sessionKey; daemonEpoch; ok; error?; message? }`; `resync.required` `{ daemonEpoch; reason: 'overflow' | 'notifier' }`. Generation does not authorize an ack.

Catch-up, listener armed:

1. `latest = ready.generation`. A matching-epoch hint raises it. A different epoch reconnects from `bind`.
2. `pending` omits `afterSequence`. One short read copies at most one `Message`, records that snapshot generation, ends the read, then writes. `exhausted` means no further pending row.
3. Page with `afterSequence = message.sequence` only while not exhausted and no newer hint arrived. Not an ack watermark. Pending means `receipt_entry_id IS NULL`, including older rows.
4. A newer hint abandons the page and repeats step 2.
5. Idle only after a full read at or above `latest`, with no newer hint before idle.

Stale `pending` or `ack` returns `owner_conflict`, not today's empty array. `ok: true` and no messages means no work. Do not hold a read transaction across a socket wait.

## Ack and registration

The daemon does not read JSONL. Lane E still requires the exact `custom_message` (`customType: 'roger-wilco'`, `details.deliveryId`) in the current `getSessionFile()`, then `fsync`, then a session and file recheck, then `ack` with that `PiReceipt`. The daemon checks the token and keeps today's idempotent and conflicting-receipt rules. Directory `fs.watch` may trigger that check. It is not an inbox poll. A missed event leaves the row pending. Neither `sendMessage` nor the hint is a receipt.

Invalid enqueue is `invalid_request`, not a thrown `TypeError`. Lane D adds `operation_id TEXT NOT NULL`; session key stays primary. Same id and JSON returns `duplicate` and kicks a still-queued row. Same id and different JSON is `invalid_request`. A new id replaces the row. `completeRegistration` still deletes only matching JSON. After commit, respond, then `POST /v1/sessions/register`. No scan. Startup reconciles existing rows once. Failure keeps the 250 ms to 10 s backoff for that row. Exhaustion emits `registration.result` with `ok: false` and waits for a new enqueue or stream reconnect. The direct session-start POST stays and must not open SQLite. The 30 s presence refresh uses this enqueue.

## Notifier, path, singleton

After commit, before the event loop, set generation and one dirty mark. A newer generation replaces an unsent hint. Commits do not wait for `drain`. If the mark cannot be queued, or `drain` exceeds `IPC_WRITE_DEADLINE_MS` (5_000), destroy that subscription. If the mark throws, abort the daemon. Do not hide a skipped notifier with an inbox timer. Destroy a socket whose `writableLength` exceeds the frame maximum. Other sessions keep committing.

Request deadline is `IPC_REQUEST_DEADLINE_MS` (10_000). The client renews a known lease every 1_000 ms. The daemon arms one timer at `expires_at`, then emits `lease.revoked`. No lease-table scan. The 60 s retention sweep may stay and must not wake runtimes. An unsubscribed socket dies after 10 s. Timeouts do not query SQLite. EOF, error, epoch change, or `resync.required` restarts at `bind`.

```ts
export function ipcSocketPath(inboxPath: string, socketPath?: string): string
```

Default is `${inboxPath}.sock`, resolved like `inboxPath`. Optional `HostConfig.socketPath` is only for an overlong path or an operator override. `Buffer.byteLength` must be at most 107. Fail startup if longer. No abstract socket. No other config key.

`mkdir` the parent at `0o700` before listen. On `listening`, `chmod` the socket to `0o600`. This Node creates mode `0755` first, so the directory mode is the boundary. No public peer-credential method; do not read private handles. Same-user directory access is the trust boundary. The token still fences runtimes.

1. `fs.open(inboxPath + '.lock', 'wx')`, write `` `${pid}\n` ``, `fsync`, hold the fd. On `EEXIST`, `process.kill(pid, 0)`. Alive: refuse and do not unlink the socket. `ESRCH`: unlink the lock and retry once. `EPERM`: fail closed.
2. `net.connect` with a 200 ms deadline. Connected: refuse. `ENOENT`: listen. `ECONNREFUSED`: unlink, then listen.
3. On `EADDRINUSE`, probe again. Connected: refuse. Never unlink a socket that accepted, or one not just observed as refused.
4. Clean stop closes the server and unlinks socket and lock. A crash leaves both for the next pid check and connect probe.

## Lane cuts

Protocol gate first: versions, frame maximum, `ipcSocketPath`, envelopes, optional `socketPath`. Do not change `Message`, `EntityRef`, routes, or coordinator schemas.

Lane D listens inside `createHostDaemon` and is the only `createHostInbox` caller. Hint after a changing `receive` or `acknowledgePi` commit. Replace the empty registration scan with a direct kick. Add the two columns. Export no client database handle.

Lane E uses one `net.Socket`. Delete the 250 ms interval and the direct inbox open. `createPiDelivery` takes the client. `wake` runs on `inbox.changed` and the advisory JSONL watcher. `stop` releases, then destroys the socket. The 1 s owner-conflict retry must not call `pending` while unbound.

Lane G allows optional `socketPath`. Only the host listens. Lanes A and C stay unchanged. Lane F later checks quiet idle: no recurring inbox or registration SQL. No model calls.

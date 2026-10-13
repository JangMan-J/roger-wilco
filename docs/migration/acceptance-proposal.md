# Acceptance proposal

Historical test-design input, superseded by the [local contract](local-contract.md) and [verification record](verification.md).
The final scope includes removal of the coordinator replay poll.
This proposal does not report executed acceptance results.

The protocol contract gate runs first. This file is the behavioral proof after that gate names frames, generations, epochs, and fencing. It does not restate the wire schema, `docs/contract.md`, or the requirements table in `docs/validation.md`.

No paid provider calls. Native Pi rows keep the synthetic stream in `test/acceptance/native-pi.ts` (`installSyntheticModel`). New daemon rows use a fake runtime adapter, synthetic messages, the real inbox, and a real pathname socket. Installation scripts are deploy evidence, not this proof. `test/installation/bin.test.ts` proves packaged SIGTERM exit, not socket recovery.

## Oracles already in tree

Do not copy these matrices.

| Concern | Existing proof | Gap left |
|---|---|---|
| Inbox fencing, dedup, reopen | `test/host/host.test.ts`, durable per-recipient dedup | Stale socket commands |
| Lost coordinator receipt | same file, host commits before receipt; `test/acceptance/native-delivery.test.ts`, host restart after local commit but lost receipt | Crash before a local hint |
| Registration HTTP loss and cursor | `test/acceptance/coordinator.test.ts`, registration response loss; `test/host/host.test.ts`, registration queue survives reopen | IPC reply loss after daemon enqueue |
| SSE backpressure | `test/acceptance/coordinator.test.ts`, backpressured real TCP; `test/delivery/delivery.test.ts`, one stalled subscription | Slow local subscriber |
| Exact JSONL receipt | `test/pi/receipt.test.mjs`; `test/protocol/pi-ack-probe.test.mjs` | 250 ms wake interval removed |
| Idle, busy, abort, resume, lease | `test/pi/native.test.mjs`; `test/pi/config.test.mjs`; `test/pi/crash.test.mjs` | No second transcript file |
| Fork and switch | `test/acceptance/native-delivery.test.ts`, real Pi fork and switch, through `nativePiRuntime` | No new fork file |
| Entity metadata | `test/protocol/contract.test.ts`; store tests below | No copied metadata matrix |

Reuse, do not duplicate: `fixture`, `message`, `frame`, `waitFor` in `test/host/host.test.ts`; `rig`, `nativePi`, `nativePiRuntime`, `persistedMessages` in `test/acceptance/native-delivery.test.ts` and `test/acceptance/native-pi.ts`; `fileStore`, `register`, `ok` in `test/store/store.test.ts`. SIGKILL stages stay in `test/pi/crash.test.mjs` with `test/pi/crash-child.mjs`: `before-write`, `after-write-before-fsync`, `after-fsync-before-ack`, `after-ack`.

## Scans this proof removes

`src/pi/index.ts` wakes from `setInterval` every 250 ms. `src/host/index.ts` `registrationLoop` reads the registration table, then waits 500 ms. Those are the idle scans. The coordinator SSE poll in `src/server/delivery/index.ts` stays.

Classify apart from inbox and outbox scans: lease renewal at 1 s, presence heartbeat at 30 s, stream idle deadline at 45 s, and host sweep at 60 s. A retry timer may arm a known failed operation. It may not ask whether new work exists.

`createHostDaemon` accepts only the concrete local inbox, so a wrapper cannot count queries. Count `DatabaseSync.prototype.prepare` text in-process. Quiet idle fails if Pi still opens the shared inbox and scans it.

## New file

Add `test/host/local-ipc.test.ts` only.

| Id | Action | Required observation |
|---|---|---|
| quiet-idle | Bind, subscribe, ready catch-up, then both queues empty for longer than 500 ms | No repeated pending-inbox or registration-table reads. Classified timers may run. Startup reconciliation may run once |
| subscribe-commit | Commit before subscribe, between ready and snapshot, and during drain before idle | Every still-pending id is found. Hints collapse on delivery id. No idle while a newer generation is dirty. Query pending state, not rows above the last sequence. Do not hold the read transaction across the wait |
| crash-after-commit | Kill the daemon after inbox or registration commit, before any hint byte | Restart snapshot or one startup reconciliation finds the row. No coordinator redelivery. Nothing is acknowledged |
| notifier-fault | Throw from the post-commit notifier after a successful commit | Subscription closes or daemon exits. A live miss fails. An idle reread is not the detector. Test-only injection, not a production timer |
| stale-socket | Leave a pathname socket, restart, and start a second daemon | Replace a dead socket only. Do not unlink a live socket. New epoch. Old subscriber rebinds. A generation is not an ack watermark |
| slow-consumer | Stop one session’s reads past the byte bound; let another continue | Coalesce or fail visibly. Do not drop the last wakeup silently. Other session still commits and can acknowledge. Queued bytes stay bounded |
| stale-lease | Replace the lease, then send old bind, renew, and ack | Commands fail atomically. Old runtime cannot acknowledge the current delivery |
| registration-reply-loss | Disconnect after enqueue commit, before the response; resubmit the same operation id | Same durable row. Worker runs without a queue scan. No invisible stranded row |

Fencing stays in `host.test.ts`. Delayed JSONL takeover stays in `config.test.mjs`. Cursor atomicity stays in `coordinator.test.ts`. Oversized-event and 1 MiB tests cover a fast source.

## Transcript states

No new transcript file. After the 250 ms interval is gone, existing tests still pass on the synthetic model only.

Receipt rule unchanged: exact custom-message id in the current session JSONL, file sync, then current lease. `sendMessage` return and a socket hint are not receipts.

| State | Existing proof | Added observation |
|---|---|---|
| idle | `native.test.mjs`; `native-delivery.test.ts` idle and busy; `pi-ack-probe.test.mjs` idle send | Ack only after the receipt rule. Catch-up or an explicit trigger may read. A timer may not |
| busy | `native.test.mjs` queued follow-up; `pi-ack-probe.test.mjs` busy followUp | Stays pending until the queued turn persists |
| abort | `native.test.mjs` aborted provider and interrupted abort | Abort does not acknowledge. One recovery wake does not resubmit the id |
| fork | `native-delivery.test.ts` real Pi fork and switch | New address. Does not consume the original pending id |
| resume | `native.test.mjs` resume does not resend; closed-owner case in `native-delivery.test.ts`; four SIGKILL stages in `crash.test.mjs` | Inspect existing transcript evidence before reinjecting. Missing trigger leaves the row pending |

Watch coalescing, inode replacement, and overflow stay an adapter limit until an append-path hook exists. A probe may sit beside `pi-ack-probe.test.mjs`. It must not invent a receipt.

## Metadata

Keep the uncommitted tests. They are the metadata matrix.

- `test/protocol/contract.test.ts`: integer is not identity. Bare mention is absent metadata. Explicit `0` is a write. Last explicit value wins.
- `test/store/store.test.ts`, entity metadata changes preserve identity: routing, history, and reopen ignore the integer as identity.
- same file, legacy type-keyed entities merge atomically: generic slot stays `00`, old typed ids are aliases, label collision rolls back the version marker, history survives reopen. `type` wire key stays.
- same file, session names, JSON strings, claims, and releases: `[[0:FLEET]]` is a display-name reference, not a host or coordinator object, and does not change the session address. `[[153:Meteor Shower]]` is an arbitrary label across body, nested JSON strings, claim key, claim note, and a successful release key. Failed claim or release does not change metadata. List, inbox, and entity reads do not replay.

One added assertion on that legacy store test, not a new file: unsupported `entity_metadata_version` fails closed and does not write version 1.

Migration replays indexed message bodies only. Recover a later integer that lives only in `data`, a display name, or claim text only when that source was already indexed. New writes stay on the live-path test. Readers do not replay.

`test/control/control.test.ts` entity lookup and the `[[0:FLEET]]` call in `test/pi/tools.test.mjs` stay their own proofs.

## Order

1. Contract gate.
2. `npm run test:contract` and the named store entity tests.
3. `test/host/local-ipc.test.ts`.
4. Existing Pi and acceptance files with the inbox poll removed, synthetic model only.

Pass means those rows hold. It does not mean power-loss durability, timely watch delivery, or a deployed cross-host run.

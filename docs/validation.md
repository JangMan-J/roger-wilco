# Extraction verification

The extraction and cutover checks passed.
The current source revision also passes all 64 tests, without failures or skips.
The recorded 52-test runs preceded the slash-command follow-up, which brought the baseline to 53 tests.

## Feedback revision — 2026-10-12

The baseline is commit `5282762`.
The revised source passes `npm run check`, including type-checking and all four native SIGKILL checkpoints.
The suite contains 64 tests.
The packaged coordinator and host start after a production install with no third-party runtime dependencies.
Deployment verification for this revision is pending.

- Invalid and unsupported SSE events no longer reconnect the entire host.
  A regression proves that another subscription continues on the same connection.
- Coordinator retention preserves needed deliveries, latest receipt retries, and the durable sequence watermark.
  Expired registrations no longer receive future broadcasts.
- Host retention removes old Pi-acknowledged rows and preserves pending deliveries.
- Receipt caching preserves file identity, fsync, lease, and session checks.
  A 6,864,063-byte fixture used one full read for 100 pending lookups.
  The full-suite run took 33.72 ms for those lookups.
- Native command tests cover recent presence, stale-session inspection, picker cancellation, and cross-host rank assignment without a model turn.
  Rank survives SQLite reopen without changing presence, ownership, or cursors.
- The coordinator has no MCP route.
  The shared HTTP reader retains byte limits, deadlines, and interruption cleanup.
- Independent source review found no new receipt-cache or rank-picker defect.
  Host deduplication after history cleanup requires the original Pi JSONL receipt.

The remaining sections record the original extraction and deployment evidence.

## Scope

The supported runtime is Node 26.11.1 and the installed Pi 1.1.0 SDK.
Native Pi tests use persistent session files and a synthetic model stream.
They test message delivery and wake-up without paid provider requests.
They do not prove model completion or power-loss durability.

## Requirements

| Requirement | Evidence |
|---|---|
| Node SQLite port | `test/protocol/sqlite-probe.test.mjs`: named and positional parameters, numeric IDs, WAL, and file reopen |
| Versioned envelopes and identity | `test/protocol/contract.test.ts`, shared validators in `src/protocol/index.ts` |
| Durable registration and initial cursor | `test/store/store.test.ts`, `test/acceptance/coordinator.test.ts` |
| Lost registration response | Real HTTP response-loss test in `test/acceptance/coordinator.test.ts` |
| Session-bound authorization | `test/control/control.test.ts`, `test/pi/tools.test.mjs` |
| Presence and claims | Store and control tests cover presence updates, claim competition, renewal, expiry, and owner-only release |
| Directed routing and broadcasts | Store and acceptance tests verify host boundaries and broadcast exclusion of the sender |
| Separate durable acknowledgements | Host replay tests plus native Pi crash tests |
| No catch-up race | Delivery and acceptance tests send messages after an empty replay check |
| Slow and disconnected hosts | Real TCP backpressure and independent-host tests in `test/acceptance/coordinator.test.ts` |
| Silent stream recovery | Real HTTP tests exercise the 10-second header deadline and 45-second idle deadline |
| One active Pi owner | Native duplicate-open, stale-read, and lease-expiry tests |
| Resume after SIGKILL | Four subprocess checkpoints restart immediately and wait for the real lease to expire |
| Idle wake and busy follow-up | Native Pi tests and physical-host package tests |
| Aborted busy turn | Signal-aware native regression and four distinguishing mutation probes verify the recovery guards |
| Fork and switch | Actual Pi runtime `fork()` and `switchSession()` calls in acceptance tests |
| Pi closed | The host retains the message, and a resumed persistent session consumes it |
| Coordinator unavailable | Pi starts, queues registration locally, and the daemon retries |
| Invalid credentials and envelopes | Control, delivery, and host tests reject them without delivery or acknowledgement |
| Maximum-size message delivery | Real HTTP requests at 1 MiB reach durable host storage and ACK, survive reopen, and permit the next delivery |
| Research coexistence | Native Pi loads both extensions and dispatches research through a synthetic HTTP provider |
| Package startup and shutdown | `test/installation/bin.test.ts`, systemd service checks on both physical hosts |
| Independent runtime package | The tarball contains the implementation and pinned dependency lock, without either reference project |
| Migration and rollback | Private snapshots, the directed old-MCP rehearsal, and retained old databases |
| License provenance | `SOURCE-MANIFEST.md`, `LICENSE`, and `THIRD-PARTY-NOTICES` |

## Recorded extraction checks

- Final `npm run check`: 52 tests passed locally, with no failures or skips.
- After the portable backpressure fixture change, its local rerun and type-check passed.
- The final remote `npm run check` passed all 52 tests without skips.
- The remote command set `WEBTASKS_PACKAGE_DIR` to its installed research-only checkout.
- The backpressure fixture fills the actual TCP buffers instead of assuming the same capacity on both hosts.
- `npm audit --omit=dev`: no reported vulnerabilities.
- Production-only `npm ci` succeeded from the packaged lock on both hosts.
- The package passed directed messaging between `decent-spider` and `agent-jail`.
- The second session on `decent-spider` remained idle during directed messages.
- The final physical-host rerun used message IDs `fleet:msg:13`, `fleet:msg:14`, and `fleet:msg:15`.
- The rollback rehearsal delivered old message 11 to an isolated test identity.
- The rehearsal retained all nine new coordinator messages.

Private installation evidence is under `~/.local/share/roger-wilco/installation-check/`.
It includes `result.json`, persistent Pi sessions, `rollback.json`, and `research.json`.
The old source, credentials, configuration, and inbox snapshots remain under `~/.local/share/roger-wilco/before-cutover-20261012/`.
Both physical hosts retain their own snapshot.
The coordinator snapshot includes all old messages and an empty active-claims list.
The destination inventory records no pending deliveries for either old recipient.

Both hosts now load the Roger-Wilco package from `~/.local/share/roger-wilco/package`.
The old coordinator is stopped, and its global MCP declarations are removed.
The research receivers remain active.
The old agent-message route returns 404.
A signed replay on the research route returns 200 without changing the six stored research events on either host.
A signed agent-message event on the research route returns 400.

The old receiver environments contained empty research signing keys.
Cutover created separate private research signing keys instead of stopping the receivers.
It retained the provider API credentials and research storage.
The active receiver configurations no longer contain the old agent-message signing keys.

The maintained research checkout and both installed checkouts pass type-checking and all 209 tests without skips.
Its 45 TypeScript source and test files match both installed checkouts byte for byte.
The installed test environments link the selected Pi SDK for direct test imports.
The local installed checkout uses compiler and Node type links from the maintained development checkout.
These links are test prerequisites, not new production dependencies.

The configured LSP server commands were unavailable.
TypeScript checks passed with `tsc --noEmit`.
Installing the configured server or correcting `pi-lsp.json` restores LSP diagnostics.

## Review and limits

The first delivery review found stale-lease, aborted-follow-up, and silent-stream defects.
The fixes added native recovery tests and real HTTP timeout tests.
`docs/delivery-review.md` records the original findings.
The recovery review verified the lease and stream fixes.
Its final resolution verifies both signal-aware abort fixes and the pending-ID and ownership guards.
All four targeted mutation probes distinguished those guards.
The review reports preserve the original failures and their resolution.

The tests execute all native Pi prerequisites.
A missing SDK does not produce a successful skip.
SIGKILL tests verify process-crash recovery, not sudden power loss.
The model stream is synthetic, so the evidence proves wake-up and persistence rather than a real model's response.

## Final package reconciliation — 2026-10-12

The final audit found that the installed Pi adapter preceded the last abort-recovery fixes.
Both hosts now have the corrected package.
Production dependency installation passed again from the packaged lock on each host.
All runtime source files match the maintained source byte for byte.
The remote verification checkout also matches all current source and test files.

After the payload fix, the packaged extension passed the final physical-host rerun at 19:59 UTC.
That run verified automatic registration, directed round trips, idle wake-up, busy follow-up, and same-host isolation.
Both research receivers passed signed replay and retired-route checks again.
Each research database retained its six existing events.

The new services remain enabled and active.
The coordinator health endpoint returns `{"ok":true,"value":{"service":"roger-wilco","version":1}}`.
Both hosts have no old global MCP declaration.
The old callback destination list is empty.
The old coordinator remains stopped.

Already-open Pi sessions retain their loaded extensions until `/reload`.
New and reloaded sessions load the corrected package automatically.
The isolated acceptance sessions loaded that package directly.

## Payload-boundary audit

The independent audit found one additional delivery defect.
An accepted 1 MiB request exceeded the host parser limit after envelope and SSE framing.
Replay then blocked that subscription behind the same undeliverable message.

The regression reproduced that failure through real HTTP and the host daemon.
The fix shares the 1 MiB request limit and adds a normalized-input limit before storage.
The host parser allows another 1 KiB for bounded envelope metadata and framing.
The maximum-metadata test covers both directed and broadcast envelopes.

The original integrated regression covered HTTP and MCP input, durable host receipt, coordinator ACK, and both SQLite reopens.
The current regression retains the HTTP path after removal of the unused MCP endpoint.
It also rejects raw oversize requests and compact numeric input that expands beyond the normalized limit.
Rejected requests leave no poison records.
The next valid message reaches the host and advances its cursor.
The host regression still rejects oversized SSE events without acknowledgement.

Both hosts passed all 52 tests after this correction.
The existing coordinator records also passed the normalized-input validator before upgrade.
The final package contains the correction on both hosts.

## User command follow-up

`/roger` and its `/roger_board` alias display the session address, host, registered sessions, and active claims.
The agent-facing tool remains unchanged.
The native Pi command test invokes both slash commands through `session.prompt()`.
It verifies readable board output, coordinator and binding errors, and zero model calls.
The focused command and binding suite passes all six tests, together with type-checking.

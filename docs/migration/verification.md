# Push migration verification

Status: local verification passed. Production deployment is not complete.
The metadata baseline is commit `edfe56e`.
The rollout record will identify the deployed source revision.

## Completed checks

| Check | Result |
|---|---|
| Metadata baseline `npm run check` | 74 passed, no failures or skips |
| Complete push migration `npm run check` | 88 passed, no failures or skips, 57.35 seconds |
| Current TypeScript check | `npm run typecheck` passed |
| Host storage and transport recovery | `test/host/host.test.ts`: 7 passed |
| Legacy writer fencing and lossless rollback | `test/host/fencing.test.ts`: 3 passed |
| Native Pi SIGKILL and receipt ordering | Crash and push-receipt probe files: 7 passed |
| Pi socket integration | Config, native lifecycle, and tool files: 8 passed |
| Coordinator and native acceptance | Delivery and acceptance files: 18 passed |
| Local IPC acceptance | 7 passed, including daemon SIGKILL before notification |
| Retry and registration regressions | Focused config and IPC run: 11 passed |

The native process tests cover these checkpoints:

- Before the Pi file write.
- After the write and before file sync.
- After file sync and before the local acknowledgment.
- After the acknowledgment commit, before its response reaches Pi.

The host daemon remains alive across each Pi process death.
Disconnect retires the old connection grant.
Each restarted session preserves one exact delivery entry.
These tests do not simulate power loss.

The receipt-order probe uses Pi 1.1.0 with a synthetic provider.
Run-path message-end events precede insertion into the session tree.
Later turn-end and settled events observe consumed follow-ups.
The probe proves event ordering, not disk durability.
The separate crash tests use persistent JSONL and real file sync.

The fence test loads the actual pre-migration host implementation from Git.
It retains an open handle, prepared statements, and an active WAL snapshot across migration.
Old writes fail, and new inbox rows remain intact.
The rollback test preserves post-cutover pending messages, receipts, and queued registrations.

The daemon crash test kills the process immediately after SQLite commits.
Its runtime receives no change notification before the kill.
A replacement daemon recovers the stale socket and the pending row without coordinator redelivery.

The pressure fixture initially exceeded the body-specific length limit.
The corrected fixture puts its large payload in the supported `data` field.
The resulting test reaches socket backpressure and passes.

## Review corrections

A bounded source review found two defects:

- Startup retry callbacks accumulated behind a stalled IPC request.
- Identical queued registrations generated redundant writes and notifications.

The retry path now permits one scheduled operation.
Registration updates now require changed JSON.
Targeted runtime regressions pass.
The complete suite passes all 88 tests.

## Remaining gates

- Installed-package executable checks.
- Private backups and retained rollback package on both machines.
- Installed two-host messaging, metadata, isolation, busy follow-up, and research coexistence.
- Final source revision and rollout evidence.

Configured LSP diagnostics skipped unavailable default servers.
TypeScript provides the current compiler check.
Installing the configured server or correcting its command in `pi-lsp.json` enables LSP diagnostics.

## Pre-cutover observations

Both machines run Node 26.11.1.
The local coordinator and both host daemons report active.
Both inbox directories currently have mode `0755`.
Cutover must change those directories to `0700` before the new socket starts.

Local Pi settings load the source project.
Remote Pi settings load `~/.local/share/roger-wilco/package`.
The installed acceptance fixture must explicitly select the deployed package on both hosts.

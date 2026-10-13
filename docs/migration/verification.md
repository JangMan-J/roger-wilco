# Push migration verification

Status: all 89 local tests passed. Both hosts passed installed-package acceptance.
The metadata baseline is commit `edfe56e`.
The deployed runtime revision is `000957accc41329dc380ab2417443495a55e44b2`.

## Completed checks

| Check | Result |
|---|---|
| Metadata baseline `npm run check` | 74 passed, no failures or skips |
| Complete push migration `npm run check` | 88 passed, no failures or skips, 57.35 seconds |
| Final mixed-version guard `npm run check` | 89 passed, no failures or skips, 57.34 seconds |
| Current TypeScript check | `npm run typecheck` passed |
| Host storage and transport recovery | `test/host/host.test.ts`: 7 passed |
| Legacy writer fencing and lossless rollback | `test/host/fencing.test.ts`: 4 passed in the final suite |
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
The complete suite passed all 88 tests before the final mixed-version compatibility guard.
The expanded suite passes all 89 tests.

A final rollout audit found a mixed-version import hazard.
A cached old adapter can import the newly installed writer under the old factory name.
The daemon factory now uses `createDaemonInbox`, and the legacy export is absent.
A regression runs the actual old Pi adapter against the new module.
It confirms that the adapter neither opens SQLite nor acquires the daemon lock.
The expanded fence and executable checks pass all six focused tests.

## Installed acceptance

The rollout completed on 2026-10-13.
The coordinator restarted at `05:14:24Z`, the local daemon at `05:14:25Z`, and the remote daemon at `05:14:26Z`.
All three services reported `active`.
The coordinator health response was `{"ok":true,"value":{"service":"roger-wilco","version":1}}`.

Both installed packages contain identical source hashes for all 12 runtime files.
The local installed source also matches the source checkout.
Both installed executable help commands passed on each host.
Each socket has mode `0600`, and each inbox directory has mode `0700`.

The retained release directory on each machine is:
`~/.local/share/roger-wilco/releases/000957a-20261013/`.

| Artifact | SHA-256 |
|---|---|
| `roger-wilco-0.1.0.tgz` | `1a4716ec43a2121df5f0d80fe6766e3b28dbf5d6fa794f94faae352b8e632913` |
| `rollback-edfe56e.tar.gz` | `1d23895bee60b0816ba2482b2a0e32c2f6b4403bc1752bb478b278f729403033` |

Each machine retains a private snapshot at `~/.local/share/roger-wilco/before-push-20261013-89315bc/`.
The snapshot name predates the final release revision.
It contains the previous package, configuration, Pi settings, service units, and consistent database copies.
The release directory also retains the previous installed package.
The [rollback procedure](../operations.md#transport-rollback) preserves post-cutover state instead of restoring stale databases.

### State preservation

The post-upgrade checks reported schema 2, database integrity `ok`, and empty legacy views on both hosts.

| Host | Snapshot rows | Pending at snapshot | Preserved rows |
|---|---:|---:|---:|
| `decent-spider` | 292 | 95 | 292 |
| `agent-jail` | 89 | 67 | 89 |

The checks compared message content, sequence, and existing receipt proofs.
They did not require offline sessions to consume their pending rows.
A separate coordinator comparison preserved all 201 snapshot messages and 77 session bindings.
All session cursors remained monotonic.
The coordinator snapshot contained zero active claims.

### Physical-host behavior

At `05:15:01Z`, `test/installation/cross-host.mjs` passed against the installed packages on both hosts.
It used real Pi sessions with a synthetic offline model stream, without paid inference calls.

- Automatic registration and cross-host rank updates passed.
- Directed messages `fleet:msg:202` through `fleet:msg:204` reached the intended sessions.
- Idle delivery woke its session.
- Busy delivery entered the follow-up queue before the held turn ended.
- The other local session stayed idle.
- Metadata changed from 255 to 153 without changing the entity ID.
- Lookup with the previous integer preserved the current metadata.
- All three child processes completed bounded shutdown.

Later SQLite checks confirmed runtime receipt proofs for all three messages.
The local host held the receipt for message 202.
The remote host held the receipts for messages 203 and 204.

The installed research check returned HTTP 200 for an existing signed callback.
The retired agent route returned 404, and the research route rejected an agent event with 400.
All seven stored research events remained unchanged.
The native acceptance suite also loaded the research-only extension beside Roger.

Machine-readable evidence is under `~/.local/share/roger-wilco/installation-check/`:

- `result.json`: physical-host acceptance on the coordinator machine.
- `research.json`: signed callback and unchanged research state.
- `upgrade.json`: database preservation on each host.

### Existing sessions and limits

Existing Pi processes retain their loaded code until `/reload`.
The migration deliberately fences those old clients instead of terminating user sessions.
The actual legacy-client tests prove that old handles cannot mutate the new tables.
The mixed-version import test proves that the old factory name cannot acquire the new daemon writer.
Fresh installed sessions passed acceptance, but this rollout does not claim that every existing session reloaded.

The remote environment emitted a missing extra-CA-file warning.
Authenticated HTTPS delivery still passed under that environment.
No credential, certificate, provider, or research configuration changed.

Configured LSP diagnostics skipped unavailable default servers.
TypeScript provides the current compiler check.
Installing the configured server or correcting its command in `pi-lsp.json` enables LSP diagnostics.

## Pre-cutover observations

Both machines run Node 26.11.1.
The local coordinator and both host daemons report active.
Both inbox directories had mode `0755` before cutover.
The rollout changed those directories to `0700` before the new sockets started.

Local Pi settings load the source project.
Remote Pi settings load `~/.local/share/roger-wilco/package`.
The installed acceptance fixture must explicitly select the deployed package on both hosts.

### Early local schema migration

The local inbox reached schema 2 before the intended service cutover.
The old daemon first reported rejected writes at `2026-10-13T04:16:17Z`.
Its process still used the previous installed package.
The live source-package configuration exposed the mixed-version constructor path.
The triggering process was not identified.

The private snapshot at `2026-10-13T04:59:53Z` contains 292 local inbox rows.
The preservation check at `2026-10-13T05:10:10Z` confirms all 292 rows, including 95 pending rows.
The coordinator snapshot has integrity `ok` and zero active claims.
Rollout preserves the current database and replaces the old daemon.
It does not restore a pre-migration database over later state.
The replacement daemon started at `05:14:25Z`.
The installed cross-host and receipt checks then passed.

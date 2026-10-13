# Push delivery and entity metadata migration

Status: all 89 tests passed, including the final mixed-version guard. Deployment is in progress.

## Objective

The authorized scope includes the plan, implementation, verification, and deployment on both existing hosts.
The research input is `rogers.json`, field `output.content`.
This plan uses its recommendation to retain outbound HTTPS/SSE and add host-local IPC.
Source inspection also found a coordinator replay scan that the research baseline did not identify.

Roger remains a coordination module that consumers can use with different harnesses.
Pi is the current adapter, not the definition of an entity or a delivery receipt.
This migration does not add a broker, discovery platform, workflow engine, or new fleet-specific model.

## Requirements

| Requirement | Implementation target | Acceptance evidence |
|---|---|---|
| Stable entity identity | Namespace and canonical label determine the ID. The integer is opaque metadata. | Identity-change regression and legacy-record migration |
| Shared reference syntax | Session names, messages, JSON strings, claim text, and successful release keys use one parser. | Store and real HTTP integration tests |
| Read-only metadata observation | Inbox, lookup, and board reads preserve the current integer. | Old-text replay regression |
| Harness-independent direction | New local transport separates delivery acceptance from the adapter's proof mechanism. | Contract and implementation review |
| One local writer | The daemon owns inbox schema, storage, leases, registration queue, and receipts. | Client source checks and old-writer fence tests |
| Commit-triggered local delivery | Bounded Unix socket notifications trigger catch-up after durable commit. | Subscribe/commit race and notifier-failure tests |
| Event-driven registration | Committed enqueue, startup, and reconnect trigger pending work. | Response-loss and quiet-idle tests |
| Event-driven coordinator delivery | Store commits and receipt changes wake SSE replay. | No recurring idle replay queries |
| Separate delivery receipts | Host storage precedes coordinator receipt. Adapter evidence precedes runtime receipt. | Real storage and native Pi tests |
| Crash recovery | Pending rows survive daemon/runtime failure and reconnect. | Crash-window and offline-runtime tests |
| Bounded pressure | Slow clients cannot stall durable reception or other sessions. | Socket pressure, frame limits, and shutdown tests |
| Existing behavior | Isolation, busy follow-up, resume, fork, abort recovery, and research coexistence remain valid. | Existing acceptance suite through the new transport |
| Coordinated rollout | Both hosts use the verified package, with retained backups and a rollback procedure. | Installed-package and physical-host checks |

Lease renewals, presence heartbeats, transport keepalives, deadlines, and retries of known operations remain permitted.
They must not conceal recurring inbox or registration scans.
An idle transport heartbeat must not trigger a database catch-up query.

## Current state

The original source baseline is commit `0c20ef3b5514175a612a9ef22cf1f5dfac1c4486`.
Commit `edfe56e` contains the metadata correction.
Its complete `npm run check` passed 74 tests with no failures or skips.
The final migration passes all 89 tests, including the old-adapter import regression.
These results do not establish transport migration or production acceptance.

The source baseline contained three ordinary scan loops:

- Pi reads pending inbox rows every 250 milliseconds.
- The host scans queued registrations every 500 milliseconds.
- Each coordinator SSE connection scans pending deliveries every 250 milliseconds.

Each baseline Pi runtime also opened the host SQLite file and performed schema work and mutations.
Removing only the polling timers leaves that multi-writer model unchanged.

Live observations show an active coordinator and host daemon on `decent-spider`.
The `agent-jail` host daemon is also active.
Both machines report Node 26.11.1.
Installed code remains under `~/.local/share/roger-wilco/package`.
The coordinator retains its current HTTPS endpoint and port.

## Metadata contract

`[[0:FLEET]]` can appear in an agent's registered display name.
FLEET is an ordinary named entity, not a machine or coordinator identity.
The session's routing address, ownership, and rank remain separate.

`Hey Jerry, did you see the [[153:Meteor Shower]] last night?` preserves its exact text.
The integer changes metadata for the same Meteor Shower entity.
Its meaning belongs to external consumers.

Bare references preserve existing metadata.
An explicit integer applies in accepted write order.
An explicit display-name write can reassert its integer.
Heartbeat registrations omit the display name.
Claim keys remain literal and releases require the exact key.
Losing claims, unsuccessful releases, and reads do not apply metadata.

Existing generic IDs remain stable.
The migration merges old type-specific records and retains their IDs as aliases.
The wire field name `type` remains for compatibility, but no longer participates in identity.
Message history remains message history.
The migration does not manufacture registration or claim events as messages.

## Contract gate

The [local contract](local-contract.md) resolves these decisions:

1. Local framing, size limits, request IDs, errors, and socket lifecycle.
2. Subscription readiness, catch-up, dirty notifications, and reconnect behavior.
3. Lease fencing across connection replacement, runtime replacement, and daemon restart.
4. Daemon-owned registration enqueue, retry scheduling, and response-loss handling.
5. Generic delivery acknowledgment and Pi's exact persistence evidence.
6. Safe migration of old inbox writers that still hold SQLite connections.
7. Coordinator commit notifications and SSE catch-up ordering.

The old-client constraint requires explicit handling.
Replacing the installed package does not replace modules already loaded in live Pi processes.
If an old constructor can recreate the table, a rename alone is insufficient.

## Parallel design lanes

| Lane | Responsibility | Report |
|---|---|---|
| Receipt feasibility | Verify current SDK append signals and exact persistence ordering | [Receipt feasibility](receipt-feasibility.md) |
| Lease contract | Define ownership, fencing, restart, and legacy-client rules | [Lease contract review](lease-contract-review.md) |
| Local IPC | Define bounded framing, socket lifecycle, and subscription protocol | [IPC proposal](ipc-proposal.md) |
| Registration worker | Replace recurring scans with committed-work scheduling | [Registration proposal](registration-proposal.md) |
| Coordinator push | Replace recurring replay reads with commit notifications | [Coordinator proposal](coordinator-push-proposal.md) |
| Timer inventory | Identify every scan and local writer | [Timer and writer inventory](timer-writer-inventory.md) |
| Deployment inventory | Identify package, configuration, service, and fixture changes | [Deployment inventory](deployment-inventory.md) |
| Acceptance design | Reuse fixtures and define distinguishing failure tests | [Acceptance proposal](acceptance-proposal.md) |

These reports are inputs, not independent normative contracts.
The integrated contract and this plan resolve disagreements before code ownership splits.

## Implementation phases

1. Complete the metadata baseline and freeze the local protocol contract.
2. Assign exclusive source and test files to implementation workers.
3. Implement daemon storage/IPC, reconnecting client, event-driven registration, and coordinator notifications.
4. Integrate Pi receipt notifications without weakening exact evidence checks.
5. Adapt the existing native tests to the real local transport.
6. Pass quiet-idle, races, failure injection, fencing, retention, and package checks.
7. Review the combined changes for contract compliance and migration safety.
8. Back up the installed packages, configuration, and databases on both hosts.
9. Fence old writers and deploy the coordinator, daemons, and adapters in the verified order.
10. Verify both installed hosts, directed messaging, busy follow-up, metadata, and research coexistence.
11. Record source revision, evidence, reload requirements, and rollback boundaries.

## Deployment gates

No production upgrade precedes local acceptance and a tested schema migration.
The rollback package and consistent database backups remain available before any service restart.
One operator owns the cutover.
Pending delivery, registration, and work-claim state must survive the transition.
Rollback must reconcile post-cutover writes rather than discard them.

Current service paths and configuration are facts to verify again at cutover.
Private credentials never enter reports, test fixtures, or Git.
Existing sessions require a safe reload or an explicit compatibility fence.
The final evidence must distinguish new-session acceptance from still-loaded old clients.

## Selected decisions

- Connection-bound random grants fence local commands. Client clocks confer no authority.
- A separate SQLite lock provides daemon singleton ownership without stale PID-file races.
- Empty read-only views fence old writers on the existing database inode.
- Bind installs the subscription before its ready response. Pending pages remain bounded.
- Pi uses lifecycle reconciliation for its existing trigger-turn path, with exact JSONL and fsync evidence.
- Receipt timing can follow a later lifecycle boundary. The design does not promise a universal Pi append hook.
- The local contract assigns exclusive implementation files.

## Cutover order

1. Complete source tests, real IPC acceptance, and the legacy-writer regression.
2. Save consistent database backups, configuration, and installed packages on both hosts.
3. Install the tested package while the existing processes retain their current code.
4. Restart the coordinator with the same configuration and database.
5. Restart each host daemon. Its transaction fences old local writers before the socket becomes ready.
6. Load the new Pi adapter in installation fixtures on both machines.
7. Verify directed delivery, isolation, busy follow-up, metadata, and research coexistence.
8. Record the deployed revision and the reload requirement for existing Pi processes.

Rollback must retain the post-cutover inbox and coordinator state.
Old code cannot open the fenced inbox directly.
The rollback procedure must reconcile new rows and receipts before restoring a legacy schema.

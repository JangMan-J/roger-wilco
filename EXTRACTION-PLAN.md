# Roger-Wilco extraction plan

Status: complete, verified, and installed on both hosts.
All source, package, and cutover verification gates pass.
SSE + HTTP is the selected transport.

## Execution record

The sections after this record preserve the agreed extraction scope.
The [verification ledger](docs/validation.md) maps requirements to tests and deployment evidence.

- [x] Freeze the protocol and run the native Pi and SQLite prerequisite probes.
- [x] Implement coordinator storage, HTTP control, MCP, and SSE delivery.
- [x] Implement the outbound host daemon, durable inbox, registration retry, and session leases.
- [x] Implement automatic Pi binding, session-bound tools, fork, switch, resume, and file-observed acknowledgement.
- [x] Complete the signal-aware busy-abort regressions and final recovery review.
- [x] Install the package and services on `decent-spider` and `agent-jail`.
- [x] Verify directed messages and wake-up between the physical hosts.
- [x] Rehearse rollback and preserve the old databases, configuration, source, and credentials.
- [x] Retire the old coordinator, MCP declarations, agent-message routes, and signing configuration.
- [x] Remove the agent-message branch from pi-webtasks while preserving research.
- [x] Fix the audited payload-limit mismatch and verify boundary-size delivery.
- [x] Complete the final requirement audit and record all verification results.

Final checks passed 52 Roger-Wilco tests on each host and 209 research tests in each maintained or installed checkout.
The final package passed physical-host acceptance at 19:59 UTC on 2026-10-12.
Existing Pi sessions load the new extension after `/reload`.

The package entry points are `src/bin/coordinator.ts`, `src/bin/host.ts`, and `src/pi/index.ts`.
Node is pinned in `.node-version`.
Dependencies are pinned in `package-lock.json`.
The package carries an exact copy of that lock for installation.
The [operations guide](docs/operations.md) covers installation, cutover, and rollback.

## Selected transport

Each host opens an authenticated SSE connection to the coordinator.
Ordinary authenticated HTTP requests carry commands and acknowledgements.
The implementation does not include IRC, WebSocket, or an alternative transport framework.

This design removes inbound host callbacks, callback URL provisioning, and webhook signing from the communication path.
The coordinator retains durable messages and delivery progress.
The host retains its local inbox and session routing.
The transport lanes own the stream server and reconnecting host client.
Webhook code supplies behavior and test evidence, rather than an implementation that must survive extraction.

SSE event receipt alone does not acknowledge durable storage.
Resume must use the durable receipt position, not an automatically advanced in-memory event ID.

The Pi review found one prerequisite for the delivery contract.
The current Pi `sendMessage` wrapper does not await asynchronous message submission.
The old immediate inbox acknowledgement therefore does not establish durable Pi acceptance.
Contract freeze requires a small Pi experiment that establishes an observable acknowledgement boundary and recovery behavior.
The implementation must not claim at-least-once Pi delivery until that experiment and crash tests pass.
If the installed Pi interface lacks the required signal, the plan must identify that gap before implementation.

## Goal

Roger-Wilco will own agent coordination, durable messaging, and automatic Pi session wake-up.
It will extract useful implementation from both reference projects.
It will have no runtime dependency on either project.
Research submission and research results remain in `pi-webtasks`.

The implementation will begin with one short contract phase.
Seven implementation lanes can then proceed in parallel, with exclusive file ownership.

## Source baseline

| Reference | Commit |
|---|---|
| `reference/agent-bus` | `5c373b269e3afb60b7f52d2b0fd00408f2ee4142` |
| `reference/pi-webtasks` | `7239e524daeb0ce6f126f538ba09d0010c85ed72` |

Both reference worktrees were clean during planning.
The existing graph indexes predate these changes.
Direct source inspection supplies the extraction evidence.
The reference directories are inputs, not production dependencies or editable implementation targets.

## Proposed structure

One TypeScript package contains three runtime entry points:

- The coordinator stores presence, messages, claims, registrations, and outbound delivery progress.
- The host daemon receives the stream and stores messages durably, even while Pi is closed.
- The Pi extension binds the current session, exposes communication tools, and delivers messages into Pi.

The coordinator and host daemon remain separate processes because they run on different hosts.
One project owns their protocol and lifecycle.

The selected runtime is Node for all three entry points.
This choice preserves the TypeScript implementation and Pi integration.
It requires a bounded port of the coordinator's Bun SQLite calls and tests.
The contract phase will pin the Node version and dependency versions.
A Rust rewrite is outside this extraction.

Proposed layout:

```text
src/
  protocol/             # Shared records, validation, interfaces
  server/
    store.ts            # All coordinator schema and persistence
    control/            # HTTP, auth, MCP, host/session registration
    delivery/           # SSE subscriptions, replay, receipt ACKs
  host/                 # Stream client, local inbox, delivery ownership
  pi/                   # Session lifecycle, bound tools, Pi notifications
  bin/                  # Coordinator and host daemon entry points
test/
  protocol/
  store/
  control/
  delivery/
  host/
  pi/
  acceptance/
deploy/
docs/
```

## Extraction boundaries

Paths in the first column are relative to the named reference.

| Source | Destination | Treatment |
|---|---|---|
| agent-bus `src/db.ts` | `src/server/store.ts` | Extract presence, mailbox, claims, row conversion, and delivery cursors. Port the SQLite adapter. |
| agent-bus `src/server.ts` | `src/server/control/` | Extract coordinator operations and MCP tools. Replace identity plumbing and project metadata. |
| agent-bus `src/index.ts`, `src/config.ts` | `src/server/control/`, `src/bin/` | Extract auth, health, routing, configuration, and shutdown. Recompose startup. |
| agent-bus `src/webhooks.ts` | `src/server/delivery/` | Reuse cursor advancement and independent delivery behavior. Replace webhook sending with SSE. |
| pi-webtasks `src/agent-bus.ts` | `src/protocol/` | Extract message validation. Define Roger-Wilco names and versioned envelopes once. |
| pi-webtasks `src/webhook.ts` | None | Exclude signature verification. Authenticated TLS connections replace signed webhook receipt. |
| pi-webtasks `src/webhook-inbox.ts` | `src/host/` | Extract durable receipt, deduplication, ownership, and acknowledgement. Remove research records. |
| pi-webtasks `src/webhook-receiver.ts` | `src/host/` | Reuse persist-before-acknowledgement behavior. Replace the HTTP receiver with an outbound stream client. |
| pi-webtasks `src/webhook-worker.ts`, `src/webhook-consumer.ts` | `src/host/` | Extract only agent-message routing and delivery. Collapse research-specific processing stages where unnecessary. |
| pi-webtasks `src/index.ts` | `src/pi/` | Extract serialized recovery, inbox watching, session teardown, and Pi follow-up delivery. Add automatic registration. |

The first release excludes release watching, public feeds, landing-page visualization, and research-provider code.
It also excludes inbound host servers, webhook credentials, and callback destination registration.
Message visibility remains explicit metadata. It does not imply recipient-only confidentiality.
Old database import is outside the first release.
The migration procedure preserves old databases and drains pending delivery before retirement.

## Contract phase: the only initial implementation gate

One owner freezes the contracts before the implementation lanes start.
The result includes `src/protocol/`, example payloads, contract tests, and entry-point signatures.
This phase also establishes package manifests, the lockfile, and the test commands.

The contract phase freezes these decisions together:

1. Identity: The coordinator namespace and Pi session ID identify a session. Host IDs supply routing metadata. Display names are separate.
2. Resume: A resumed session keeps its address. A new or forked session gets its own address.
3. Ownership: One active runtime owns a session binding. Duplicate activation reports a conflict. Stale ownership expires without discarding pending messages.
4. Host provisioning: Host configuration contains the coordinator URL, host ID, and credentials. It contains no callback URL.
5. Session registration: Startup binds the session to its host and creates its subscription automatically.
6. Registration retry: Repeated registration resumes the existing subscription. It never resets delivery progress.
7. First delivery: Initial subscription creation establishes its starting position atomically, before registration returns success.
8. Envelope: One versioned message record defines IDs, sender, recipient, payload, visibility, and delivery identity.
9. Acknowledgements: Coordinator-to-host receipt and host-to-Pi delivery are separate durable acknowledgements.
10. Tools: Pi tools derive the sender and claim owner from the current session. They do not use host-global MCP headers.
11. Trust: Shared credentials authorize trusted participants. They do not establish adversarial isolation between agent identities.
12. Errors: The contract defines outcomes for invalid credentials, unknown hosts, ownership conflicts, and temporary network failures.

The frozen interfaces cover coordinator storage, control requests, stream subscriptions, and local inbox delivery.
They define input validation, ordering, cursor initialization, retries, and shutdown behavior.
They contain no research types or imports.

The host requires one-time provisioning.
Ordinary session starts require no destination-file edits, manual inbox binding, or manual presence registration.
An unavailable coordinator must not prevent Pi from starting.
Reconnect behavior must preserve pending messages and expose registration failures.

One host stream carries messages for its registered sessions.
Each session retains its own durable subscription cursor.
Stream reconnection does not create new subscriptions or reset cursors.
The stream reads only subscriptions assigned to the authenticated host.
The host acknowledges a message only after its local inbox transaction commits.
The coordinator accepts acknowledgements only for the authenticated host's assigned subscriptions.
Session registration establishes its initial cursor atomically.
Messages before first registration remain available through explicit inbox queries, but do not automatically trigger historical wake-ups.

Catch-up and live delivery must have no subscription race.
A slow host cannot block other hosts or create an unbounded memory queue.
The stream sends keepalives and detects disconnection.
The host reconnects with bounded backoff.
The replay source remains SQLite, not a process-local event buffer.
Moving an existing session to another host must reconcile pending local receipts before changing its delivery assignment.
Automatic cross-host session migration is outside the first release.

## Parallel implementation lanes

Each lane owns its tests beside its implementation.
The acceptance lane owns only cross-module tests.
The integrator owns shared contracts and root files throughout.

| Lane | Exclusive outputs | Reusable input | Completion gate |
|---|---|---|---|
| A: coordinator storage | `src/server/store.ts`, `test/store/` | `BusStore`, `test/bus.test.ts` | Durable reopen, routing, claims, registrations, and monotonic cursors pass against real SQLite. |
| B: control interface | `src/server/control/`, `test/control/` | MCP handlers, HTTP/auth factory, config | Auth, session-bound operations, host/session registration, and input validation pass through real HTTP. |
| C: stream delivery | `src/server/delivery/`, `test/delivery/` | Cursor/retry behavior in `src/webhooks.ts`, `test/webhooks.test.ts` | Authenticated SSE, replay, receipt ACKs, restart, and cancellation preserve delivery progress. |
| D: host client and inbox | `src/host/`, `test/host/` | Inbox, receipt persistence, agent consumer | Outbound connection, durable receipt, reconnect, replay handling, and ownership pass without research dependencies. |
| E: Pi adapter | `src/pi/`, `test/pi/` | Extension lifecycle, agent-message SDK tests | Automatic binding, session-specific tools, idle wake-up, busy follow-up, and resume pass. |
| F: acceptance tests | `test/acceptance/` | Existing HTTP and native Pi SDK fixtures | End-to-end scenarios pass through actual module entry points. |
| G: assembly and operations | `src/bin/`, `deploy/`, `docs/`, root manifests and notices | Existing process supervision, container deployment, package metadata | Install, startup, shutdown, health, migration rehearsal, and rollback checks pass. |

Lane A owns every coordinator schema change, including subscription storage.
Lane D owns every host schema change.
Lanes B and C consume their interfaces without editing their schemas.
Lane E consumes the local inbox interface without implementing a second inbox.
Lane G remains the sole owner of dependency and manifest changes.

Lanes A through E can develop against the frozen contracts independently.
Lane F can author acceptance scenarios immediately, but final verification waits for the actual modules.
Lane G can prepare packaging immediately, but runtime assembly waits for the entry points.
Temporary test doubles do not replace integrated acceptance tests.

### Agent assignments

Gemini 3.8 Flash handles bounded discovery, source tracing, and test inventories.
Grok 4.7 handles routine implementation and code review.
Astra, Sol 6.1, or Claude Opus 5.5 handles critical contracts, Pi acknowledgement design, and final integration review.
Model availability must match the configured runner before assignment.

The integrator assigns each unresolved task to one executor.
Each task includes its source evidence, exclusive output paths, contract version, and acceptance gate.
Finished agents close after their findings enter the plan or implementation.
Additional agents receive independent tasks rather than competing ownership of shared files.

## Integration order

```text
Contract phase
  ├── A: storage ──────┐
  ├── B: control ──────┼── coordinator checkpoint
  ├── C: stream ───────┘             │
  ├── D: host client ────────────────┼── durable delivery checkpoint
  ├── E: Pi adapter ─────────────────┼── session wake-up checkpoint
  ├── F: acceptance scenarios ───────┴── integrated acceptance
  └── G: packaging and migration ─────── isolated installation and cutover
```

The coordinator checkpoint exercises control operations against real storage.
The durable delivery checkpoint crosses a real SSE connection and reopens both databases.
The session checkpoint runs the packaged extension inside the Pi SDK.
The final installation check uses separate hosts before the old path retires.

A contract change returns to the integrator before dependent code changes.
Workers report required changes rather than editing shared files independently.
No worker modifies the reference repositories or live deployment during extraction.

## Required behavior and acceptance

The extraction preserves useful behavior, not every incidental implementation detail.
Claim expiry, broadcast routing, cursor progression, and session ownership require explicit tests.

| Scenario | Required result |
|---|---|
| Two Pi sessions on one host | Distinct addresses. Each directed message wakes only its intended session. |
| A second host sends a message | Startup registration supplies the route without per-session operator work. |
| A session resumes | The address and pending inbox survive. Already acknowledged messages do not replay normally. |
| A session switches or forks | The old owner stops delivery. The new session cannot consume the old session's inbox. |
| Duplicate live opening of one session | One owner delivers. The other runtime receives an explicit conflict or waits for ownership. |
| Pi is idle | A received message triggers a model turn. |
| Pi is busy | A received message queues as follow-up without interrupting current work. |
| Pi is closed | The host persists messages. Resume delivers pending messages without spawning Pi. |
| Host crashes after persistence but before receipt ACK | Coordinator replay does not create a second inbox event. |
| Coordinator restarts | Subscriptions retain progress. Pending messages remain deliverable. |
| Registration retries after a lost response | The same subscription resumes without a new tail checkpoint. |
| Invalid credentials or envelope | The connection or event is rejected without acknowledgement or Pi delivery. |
| Slow or disconnected host | Other hosts continue receiving messages. Reconnect and shutdown remain bounded. |
| Message arrives during catch-up | The message reaches the inbox without a replay/live subscription gap. |
| Competing work claims | One owner acquires the claim. Renewal, expiry, and owner-only release retain defined semantics. |
| Research extension runs beside Roger-Wilco | Research still works. Only Roger-Wilco handles agent messages after cutover. |

The target is at-least-once delivery, subject to the Pi acknowledgement gate in [Selected transport](#selected-transport).
A crash between Pi message submission and durable acknowledgement can produce a duplicate.
The implementation must preserve message identity so duplicate delivery remains recognizable.
Neither an HTTP success response nor Pi submission proves model completion.

## Migration and rollback

The new project starts with separate configuration, databases, credentials, and coordinator endpoints.
Host daemons need no exposed network listeners.
Old data directories remain untouched.
An isolated test identity proves the new path before production sessions switch.

The cutover procedure has one owner:

1. Record pending old messages and active claims.
2. Pause new sends through the old route.
3. Drain old delivery or record explicit unresolved messages before switching.
4. Provision the Roger-Wilco host and coordinator configuration.
5. Enable Roger-Wilco for the selected sessions.
6. Verify both directed messaging and wake-up across hosts.
7. Disable the old agent-message path for those sessions.
8. Keep the old databases and configuration for rollback.

The new host daemon does not accept old webhook callbacks.
Namespaced message IDs prevent accidental collisions with old records.
The procedure defines claim handover before retiring the old coordinator.
It does not silently copy active ownership into a fresh database.

Rollback disables the new communication path before restoring the old path.
Rollback preserves new pending messages for reconciliation.
It does not run both wake-up consumers for the same session.

Removal from `pi-webtasks` follows successful cutover.
Only its agent-message branch and associated configuration are removed.
Research routes, credentials, storage, and tests remain intact.

## Packaging and provenance

The package must not import files from `reference/`.
Dependency versions come from reviewed locks rather than unbounded `latest` resolution.
The installation includes separate coordinator and host entry points plus the Pi extension.
The coordinator SQLite port preserves synchronous claim updates, parameterized SQL, WAL, and durable cursor advancement.
Its initial probe covers named parameters, positional parameters, numeric IDs, and file reopen.
The test port uses `node:test` and `node:assert/strict`.
Type-checking remains separate from test execution.
Runtime paths derive from configuration or the user home.

Extracted agent-bus code retains the Pinax Network MIT notice.
The source manifest records both source commits and the files extracted.
The pi-webtasks reference has no tracked license file.
The new project records ownership and license terms before distribution.

## Source checks and planning verification

The coordinator review inspected `BusStore`, MCP handlers, HTTP routing, and the existing coordinator tests.
The delivery review inspected the sender, durable inbox, receiver, worker, and consumer.
The Pi review inspected the extension lifecycle and the installed Pi implementation.
It confirmed that new and forked sessions receive new IDs, while resume restores the existing session ID.
It also identified the asynchronous acknowledgement gap in [Selected transport](#selected-transport).

Reusable test sources include:

- `reference/agent-bus/test/bus.test.ts`: presence, routing, visibility, and claims.
- `reference/agent-bus/test/webhooks.test.ts`: delivery retry, restart, and cursor behavior.
- `reference/pi-webtasks/tests/agent-bus.test.ts`: session isolation, replay, and native Pi wake-up.
- `reference/pi-webtasks/tests/helpers/test-host.ts`: extension-loader fixtures.

The native Pi test must execute against the selected supported Pi build.
A missing SDK capability is a failed prerequisite, not a successful skipped test.

The original planning reviews did not rerun the reference suites.
Current implementation and deployment evidence is in [the verification ledger](docs/validation.md).
The reference worktrees remain unchanged.

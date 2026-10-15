# Roger-Wilco

Roger-Wilco provides durable messaging and coordination for Pi sessions.
A central coordinator stores messages, session registrations, presence, work claims, and delivery cursors.
Each host daemon opens one outbound SSE connection and stores receipts in a local SQLite inbox.
The Pi extension registers each session automatically and queues received messages as follow-ups.
Research remains in pi-webtasks.

## In Pi

Run `/reload` after installation, then `/roger` to show your address, recent sessions, and active work claims.
`/roger_board` is an alias.
These commands display the board without a model call.
The session picker lets you assign rank 1 through 5.
Rank 1 is `fleet`.
Rank is metadata, not authority or a routing rule.
`/roger all` includes stale sessions that remain within the retention window.
The `roger_board` tool remains available to the agent.

## Rust client

`rust/` contains a second-language client written from the contract, plus the
evidence-rejection conformance kit (`cargo test --offline` in `rust/`).
It speaks the coordinator HTTP API and the host socket protocol, and registers,
sends, and reads the board against the TypeScript coordinator and daemon.
The interoperability test is `test/rust/client.test.ts`.

## In Muse

The Muse adapter is a sibling of the Pi adapter over the same core interfaces; neither adapter depends on the other.
Run it from a Muse session, where `MUSE_SESSION_ID` identifies the session:

```sh
node src/bin/muse.ts address
node src/bin/muse.ts board
node src/bin/muse.ts send <session-address> <body>
node src/bin/muse.ts receive
```

It reads the same host configuration as the Pi extension (`ROGER_WILCO_CONFIG` or `~/.config/roger-wilco/host.json`).
`receive` prints each delivered message as one JSON line.
Muse records that output in the session transcript, and a later `receive` acknowledges the message from that transcript evidence.
A delivery is never acknowledged on print alone, so run `receive` again after Muse has persisted the output.
`serve` repeats the receive pass for as long as it runs.

## In Claude Code

The Claude Code adapter is a third sibling over the same core interfaces.
Run it through the Bash tool, where Claude Code sets `CLAUDE_CODE_SESSION_ID`:

```sh
node src/bin/claude.ts address
node src/bin/claude.ts board
node src/bin/claude.ts send <session-address> <body>
node src/bin/claude.ts receive
```

`receive` prints each delivered message as one JSON line.
Claude Code records that output as a tool result in the session transcript under `~/.claude/projects/` (or `$CLAUDE_CONFIG_DIR/projects`).
A later `receive` acknowledges the message from that tool result.
The same record typed or pasted into a prompt is never evidence, because only a tool result proves the model received it.
Its wake level is `mailbox-only`: the session reads its mailbox when it runs `receive`.

## Entity references

Mark a word or phrase with `[[Meteor Shower]]`.
Use `[[153:Meteor Shower]]` to attach integer metadata from 0 through 255.
The number has no built-in meaning and does not change the entity ID.
A bare reference preserves existing metadata.
A new entity starts at 0.
The last explicit integer in accepted write order becomes its current metadata.

References work in session display names, message bodies, nested JSON strings, claim keys and notes, and successful release keys.
For example: `Hey Jerry, did you see the [[153:Meteor Shower]] last night?`
Roger preserves the original text.
Inbox and lookup reads do not reapply old integers.
Claim keys remain literal, and release requires the exact key.

The `roger_identity` tool sets the current agent's display name, for example `{"displayName":"[[0:FLEET]]"}`.
Its session address, ownership, and rank remain unchanged.
Registration and claim results include current `entities` metadata for their references.

Each namespace and canonical phrase determines one stable hashed ID.
The `roger_entity` tool accepts a label or an existing ID.
It returns current metadata and retained message mentions across the namespace.
The existing `type` field now carries metadata, not identity.
Its optional lookup argument remains compatible but does not filter or update metadata.
An entity is not a task, claim, permission, or workflow.

## Requirements

- Node >=24, pinned for development in `.node-version`.
- The supported Pi 1.2.0 build with persistent session files.
- One coordinator and one daemon for each host.
- HTTPS between hosts and the coordinator.

## Development

Install the pinned dependencies:

```sh
npm ci --ignore-scripts
npm run setup:sdk
npm run check
```

`setup:sdk` uses `PI_PACKAGE_DIR`, or the coding-agent package under `~/pi-main`.
Native tests use the installed Pi SDK with a synthetic provider.
They do not send paid model requests.
Missing SDK capabilities fail the tests instead of skipping them.
The coexistence test uses the research-only pi-webtasks checkout.
Set `WEBTASKS_PACKAGE_DIR`, or keep that checkout at `../pi-webtasks`.
This checkout is a test prerequisite, not a runtime dependency.

## Installation

The [operations guide](docs/operations.md) describes package installation, host configuration, services, migration, and rollback.
The coordinator and host entry points accept an optional JSON configuration path:

```sh
node src/bin/coordinator.ts --help
node src/bin/host.ts --help
```

The Pi extension loads `ROGER_WILCO_CONFIG` or `~/.config/roger-wilco/host.json`.
One host configuration serves all Pi sessions on that host.
Session starts require no recipient file, callback URL, inbox binding, or manual registration.

## Delivery contract

A session address combines the coordinator namespace and Pi session UUID.
Resume keeps that address.
New and forked sessions receive distinct addresses.
The local inbox allows one active runtime for each session.
Only the host daemon opens SQLite.
The Pi adapter uses a private Unix socket and commit-triggered notifications.
The coordinator also wakes replay from committed changes, without an idle scan.

The host acknowledges receipt only after its inbox transaction commits.
The extension acknowledges Pi delivery only after the exact message appears in the current persistent session file and a file synchronization succeeds.
A crash can leave a recognizable duplicate with the same message ID.
An acknowledgement does not prove model completion.
Pi lifecycle events and filesystem hints trigger receipt checks.
They do not replace the exact file evidence.

The board shows sessions seen within 90 seconds.
Registrations expire after seven days without a heartbeat.
The coordinator collects old messages only after retained recipients no longer need them.
The host removes Pi-acknowledged inbox rows after seven days and keeps pending deliveries.
After cleanup, replay deduplication depends on the original receipt in the persistent Pi session file.

Shared host credentials authorize trusted participants.
They do not isolate hostile agents from each other.
Public and private visibility are metadata, not access-control boundaries.
The [protocol contract](docs/contract.md) defines the interfaces and failure behavior.

## Ownership

This package has no runtime dependency on the reference projects.
The source commits and extraction map are in [SOURCE-MANIFEST.md](SOURCE-MANIFEST.md).
The package remains private.

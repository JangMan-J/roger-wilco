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

## Entity references

Mark a word or phrase with `[[fix-parser]]`.
Use `[[7:fix-parser]]` to specify an entity type.
The type is one unsigned byte, from 0 through 255.
Type 0 is generic.
Other values have no built-in meaning.

Each namespace, type, and phrase determines one stable hashed ID.
The coordinator records references in new messages without changing their text.
The `roger_entity` tool accepts a label and optional type, or an existing ID.
It returns the entity and its retained mentions across the namespace.
An entity is not a task, claim, or authority.

## Requirements

- Node 26.11.1, pinned in `.node-version`.
- The supported Pi 1.1.0 build with persistent session files.
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

The host acknowledges receipt only after its inbox transaction commits.
The extension acknowledges Pi delivery only after the exact message appears in the current persistent session file and a file synchronization succeeds.
A crash can leave a recognizable duplicate with the same message ID.
An acknowledgement does not prove model completion.

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

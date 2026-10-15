# Transmission review: coordinator to daemon to storage to socket

Status: measured review, 2026-10-15. It feeds TODO item 2 and work-order
phase 6. Nothing here is implemented yet.

## Method

The benchmarks run the repository's own coordinator store, stream handler,
control handler, and host daemon in one process, on NVMe with btrfs. A TCP
proxy between daemon and coordinator adds 30 ms each way, which matches the
measured 60 ms round trip to the live coordinator over Tailscale. The
scripts are kept outside the repository; they import `src/` directly.

## Measurements

| Scenario | Result |
|---|---|
| 100-message backlog to one session, 60 ms RTT | 70 ms per message |
| 100 messages spread over 100 sessions, 60 ms RTT | 3.8 ms per message |
| 400 messages over 4 daemons x 25 sessions, 60 ms RTT | 2.6 s |
| 400 messages over 1 daemon x 100 sessions, 60 ms RTT | 2.8 s |
| Same 4-daemon run, coordinator `synchronous=NORMAL` | 1.1 s |
| One broadcast to 100 sessions on 1 daemon, 60 ms RTT | 659 ms |
| One broadcast to 400 sessions on 1 daemon, loopback | 689 ms |
| Coordinator `pending()` per wake, 1,000 sessions pending | 3.15 ms |
| Daemon inbox insert, `synchronous=FULL` | 1.24 ms |
| Daemon lease renew | 1.16 ms |
| Loopback backlog, both sides FULL / both NORMAL | 7.2 / 1.1 ms per message |

## What works as designed

Receipts are posted on per-subscription chains while the stream keeps
reading. Across sessions this pipelines well: the same messages spread
over 100 sessions deliver 18 times faster than to one session at 60 ms
RTT. The design is sound; the findings below are about what bounds it.

## Findings, by return on effort

### 1. The coordinator serializes every daemon on two fsyncs per delivery

Each delivered message costs two coordinator commits at the SQLite default
`synchronous=FULL`: the `offered` mark inside `pending()`, and the cursor
update in `acknowledge()`. All daemons share one writer, so adding daemons
does not add throughput: four daemons deliver the same 400 messages about
as fast as one. Setting the coordinator to `NORMAL` alone makes the
four-daemon run 2.5 times faster.

Only the message insert in `send()` needs a full sync, because a lost
insert is a lost message after the sender was told it succeeded. The
`offered` mark is connection state; the stream's in-memory `inFlight` map
already shadows it. A lost cursor update replays the message, and the
daemon's `INSERT OR IGNORE` on the message ID absorbs the duplicate and
receipts it again.

Change: run the coordinator at `NORMAL` and switch to `FULL` for the send
transaction. A `FULL` commit also syncs every earlier WAL frame. Effort:
a few lines.

### 2. Wakes that find nothing still scan every session and hash every row

Every receipt and every registration calls `notify(host)`, and each wake
runs `pending()` across all of that host's sessions. The scan computes the
SHA-256 of every candidate row, including rows for subscriptions already
in flight, which the stream then skips. At 1,000 sessions a wake costs
3.15 ms, and heartbeats alone cause one wake per session every 30 seconds.

A registration never makes new work deliverable: a new session starts its
cursor at the tail, and a known session keeps its cursor. Change: drop the
`notify` in `register()`, and exclude in-flight subscriptions before
materializing rows. Effort: a few lines, plus a test for each.

### 3. Idle sessions cost fsyncs and network requests

Per session, the daemon renews the lease every 10 seconds with a fully
synced SQLite write. Leases are deleted at every daemon start, so they are
never durable state. Each 30-second heartbeat writes a durable
registration row, posts it over the network, commits at the coordinator,
wakes the stream scan, and deletes the row again. Fifty idle sessions cost
roughly 500 daemon fsyncs and 100 coordinator requests a minute.

Change: keep leases in memory, since a socket close already releases
them. Send presence as one request per host every 30 seconds, listing the
sessions that hold leases. The registration cache (20 seconds) is shorter
than the heartbeat (30 seconds), so about a third of tool calls also pay
an extra registration round trip before their real request. Remembering
"registered during this daemon's lifetime" removes that. Effort: moderate.

### 4. One message in flight per session

The coordinator offers the next message to a subscription only after the
previous receipt arrives. A single session's backlog therefore drains at
one message per round trip: 70 ms each at 60 ms RTT, so 100 queued
messages take 7 seconds. Two busy agents talking, or a session catching up
after being offline, hit this directly.

Change: let each subscription have up to W messages in flight, and make
the receipt cumulative, "delivered through sequence N". The daemon already
commits per subscription in order, so it can send only its latest receipt
per subscription. The coordinator records the highest offered sequence
instead of one offered ID. Expected drain time becomes one round trip plus
local processing. Effort: moderate; it changes the meaning of a receipt,
so it needs protocol version 2 beside version 1.

### 5. Broadcasts fan out per session over the network

One broadcast to S sessions on a host becomes S stream frames, S daemon
fsyncs, S receipt requests, 2S coordinator commits, and S scans. A
broadcast to 100 sessions on one daemon takes 659 ms at 60 ms RTT.

Change: send one frame per host and let the daemon copy it to its local
sessions in one transaction. This needs a per-host broadcast cursor. It
fits naturally with finding 7. Effort: moderate.

### 6. The daemon commits each frame separately

Frames are ingested one at a time with one full sync each, 1.24 ms per
message. Once finding 4 lets several frames arrive together, the daemon
can commit a batch in one transaction and send the receipts after that
commit. The rule that a receipt follows a durable commit still holds.
Effort: small, after finding 4.

### 7. Structural alternative: one replication cursor per host

Findings 2, 4, 5, and 6 share a cause: delivery state is kept per session
at the coordinator. Messages already carry one global sequence. A daemon
could instead keep one cursor for its host and stream everything relevant
after it, in sequence order: directed messages for its sessions, and each
broadcast once. The coordinator then runs one indexed range scan per batch,
no matter how many sessions the host has. Per-session start points, which
skip history from before a session registered, move to the daemon, which
learns them from the registration response.

This is the largest change and the largest structural gain. It also makes
message collection depend on the slowest host cursor rather than on every
session cursor. Effort: large; protocol version 2.

### 8. The socket leg makes extra round trips

The daemon's `changed` hint carries no data, so each hint costs a
`pending` round trip. Each acknowledgement is its own round trip and its
own full sync, and its commit emits another `changed`, which triggers
another `pending` pass. The client then validates every message deeply,
although the daemon has already checked its hash.

Change: acknowledge in batches, write acknowledgements at `NORMAL` (a lost
one is re-acknowledged from the adapter's evidence), and return the next
batch in the acknowledgement reply. The `rw recv <token>` proposal in
`agent-edge.md` has this shape. Effort: small; gain is local and modest.

### 9. Repeated validation and hashing

The coordinator hashes rows on every read. The daemon validates each
stream frame twice, once in the parser and once in `receive()`, and
re-serializes the message to hash it. The socket client validates again.
This is CPU work, small at current message sizes. Effort: small; lowest
priority.

## Not recommended

- **Replacing SSE plus POST with WebSocket or HTTP/2.** With cumulative
  receipts the number of receipt requests falls to about one per batch,
  so a bidirectional transport would save little.
- **Replacing SQLite.** Every measured cost comes from how often it
  commits and syncs, not from SQLite itself.

## Order

1. Coordinator durability classes (finding 1), and no wake on
   registration plus in-flight filtering (finding 2). Small changes with
   measured gains; they need no protocol change.
2. In-memory leases and per-host presence (finding 3).
3. Protocol version 2: windowed, cumulative receipts with batched daemon
   commits (findings 4 and 6), then decide whether to go on to per-host
   replication (findings 5 and 7).
4. Socket batching (finding 8) together with the `rw` edge.

┌───────────────────────────────────────────────┬───────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────┐
│                   Behavior                    │          Bucket           │                                         Why                                          │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Stream wake on registration                   │ Deleted                   │ It never finds deliverable work                                                      │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Leases stored in SQLite                       │ Deleted                   │ Wiped at every daemon start, so they never needed to be durable; keep them in memory │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Durable queue for heartbeats                  │ Deleted                   │ A stale heartbeat is worthless after a restart                                       │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Per-session heartbeat requests                │ Deleted                   │ One presence report per host does the job                                            │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Registration check before every tool call     │ Deleted                   │ Registered once per daemon lifetime is enough                                        │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ The registration cache                        │ Deleted                   │ It only exists to soften that check                                                  │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ offered mark synced to disk                   │ Deleted                   │ The stream's in-memory in-flight map already holds it                                │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Re-hashing rows on every read                 │ Deleted                   │ Check once at write, then use the verified-message cache                             │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Validating each stream frame twice            │ Deleted                   │ The parser already validated it                                                      │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Registration display-name field               │ Deleted                   │ Already scheduled in work-order phase 0; entities replace it                         │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ In-flight window per session                  │ .conf                     │ Best value depends on round-trip time                                                │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Retention at coordinator and daemon           │ .conf                     │ Depends on disk and how long hosts stay offline                                      │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Keepalive interval                            │ .conf                     │ Depends on proxies and NAT timeouts; idle timeout derives from it                    │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Presence window                               │ .conf                     │ How stale the board may be is a fleet choice; heartbeat derives from it              │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Peers, home coordinator, credentials          │ .conf                     │ Already deployment facts in host.json                                                │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Which writes get a full disk sync             │ Fixed                     │ Message insert and daemon inbox insert always need it; correctness, not tuning       │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Message size limits                           │ Fixed                     │ Every client must agree, so they belong in the wire contract                         │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ Contiguous cursor, ordering, hash algorithm   │ Fixed                     │ They are the contract                                                                │
├───────────────────────────────────────────────┼───────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
│ History window kept longer than replay cutoff │ Fixed rule, derived value │ Computed from retention, never set by hand                                           │
└───────────────────────────────────────────────┴───────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────┘

Under this rule, the .conf bucket stays to about five settings, all describing the environment. Everything that describes behavior is either in the contract or deleted.

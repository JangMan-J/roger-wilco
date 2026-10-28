# Roger-Wilco

Roger-Wilco gives a set of long-running sessions on your own machines three things: durable
mail between them, a shared Board of who is registered, and exclusive, expiring claims on
keys. A session can be an AI agent, a script, or a person behind some front end; the core
knows sessions, not the programs behind them. It routes and stores. It does not schedule work,
assign tasks, or interpret what it carries.

The core is one Rust binary, `roger`, with no external crates. One coordinator holds the
durable state, one host daemon per machine serves local sessions on a Unix socket, and
anything else (a command-line client, a test session, an adapter for a particular agent
program) sits on top of that socket.

The project's purpose and the properties it must prove are fixed in [KERNEL.md](KERNEL.md)
and [INVARIANTS.md](INVARIANTS.md); terms such as Mail, Ack, Store and Board are defined in
[GLOSSARY.md](GLOSSARY.md). Those three files are frozen: read them before changing anything.

| Module | Doc |
| --- | --- |
| Core contract: commands, delivery, claims, checks | this file |
| Deploy: install, services, host config, backups, operations | [deploy/README.md](deploy/README.md) |
| Shims: one adapter per agent program, each installed separately | `shims/<name>/README.md` |
| The gate and the review rules | [AGENTS.md § Coding standards](AGENTS.md#coding-standards) |

## Requirements

- Linux (Unix sockets; deploy uses systemd user units)
- Rust 1.89 or later (the code uses `File::try_lock`); no crates are fetched
- For a fleet of several hosts: SSH between them and a private network the coordinator can
  listen on (see [deploy/README.md](deploy/README.md))

## Build and test

```sh
cargo test --offline --features test-hooks   # unit seams + process seam (real coordinator, daemon, session)
cargo build --release --offline              # no test hooks; what deploy ships
```

The `test-hooks` feature adds `--hold-before-ack <marker>` to `session` and `adapter`: park
between the Store write and the Ack, so a test can SIGKILL a session in that window. Only
tests and `scripts/acceptance` build with it; deploy refuses a binary that carries it.

## Try it on one machine

A coordinator and a host daemon on loopback, two Tracer sessions, one Mail, one claim:

```sh
roger=target/release/roger
d=$(mktemp -d)
export ROGER_TOKEN=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
$roger coordinator --bind 127.0.0.1:7477 --data $d/coordinator.log &
$roger daemon --coordinator 127.0.0.1:7477 --host-id local --socket $d/daemon.sock --data $d/daemon.log &
$roger session --socket $d/daemon.sock --store $d/a.store --label a &   # prints READY session=<address a>
$roger session --socket $d/daemon.sock --store $d/b.store --label b &   # prints READY session=<address b>

$roger send --socket $d/daemon.sock --from <address a> --to <address b> --mail m1 --body hello
$roger receipt --socket $d/daemon.sock --mail m1     # RECEIPT ... record=r-1 ... latency_ms=<n>
$roger claim --socket $d/daemon.sock --owner <address a> --key job-1 --ttl-ms 60000
$roger board --socket $d/daemon.sock
```

Session b prints `STORED mail=m1 record=r-1 wake=true`, then `DELIVERED mail=m1 record=r-1`
once its Ack is recorded. Stop the processes with `kill %1 %2 %3 %4`.

## Commands

`roger <subcommand> [flags]`. No command has a default socket, data or log path: each comes
from its flags. `coordinator` and `daemon` read the fleet token from `ROGER_TOKEN` in their
environment, never from argv, which `ps` shows to every user.

| Subcommand | Flags | Does |
| --- | --- | --- |
| `coordinator` | `--bind <addr:port> --data <log> [--port-file <file>] [--unregister-after-ms <n>]` | central durable store; `--port-file` receives the bound port (useful with port 0) |
| `daemon` | `--coordinator <addr:port> --host-id <id> --socket <path> --data <log>` | one per host; local inbox; serves sessions on the socket |
| `session` | `--socket --store <file> [--id <address>] [--label <text>] [--declare <text>]` | the Tracer: a session with no harness that reports on stdout |
| `adapter` | same as `session` | the same session, fronted by a stdin/stdout line stream for a shim |
| `send` | `--socket --from <address> --to <address or '*'> --body <text> [--mail <id>]` | prints `ACCEPTED mail=<id>`; `--mail` defaults to a fresh UUID |
| `receipt` | `--socket --mail <id>` | one Mail's state (see Receipts and latency) |
| `label` | `--socket --from <address> --label <text>` | relabel a registered session |
| `claim`, `renew` | `--socket --owner <address> --key <text> --ttl-ms <n>` | grant or extend a Claim; `--ttl-ms` must be positive |
| `release` | `--socket --owner <address> --key <text>` | give a Claim up |
| `board` | `--socket [--from <address>]` | one Board read; `--from` counts as that session's activity |

A session's Address is `--id`, or a fresh UUID. An Address is an opaque string: a UUID, or the
whole composite `[[<token>:<label>:<token>]]<uuid>` (the label may hold spaces; a token is lowercase base 36, `0-9a-z`, no leading zeros). Every
command takes it whole and every record keeps it byte-identical; the core never splits it, and
the tokens inside mean something only to the consumer that composed them. A relabel changes the
Board's display label and never the Address, inner label included. The Tracer prints `READY session=<address>`
after it registers, `STORED mail= record= wake=` after each Store write, and
`DELIVERED mail= record=` when its Ack is recorded.

`claim`, `renew` and `release` print `CLAIMED`, `RENEWED`, `RELEASED` or `REFUSED` with the
current holder, and exit 3 when refused. `--owner` must be a registered session Address;
anything else is refused with a reason.

`board` prints one `SESSION address= host= connected= last_seen_ms= label=` line per
registered session, followed by a `DECLARED address= declared=` line if that session declared
text, then one `CLAIM holder= remaining_ms= key=` line per live claim, then
`BOARD sessions= claims=`. `last_seen_ms` is the read time while connected, else the
session's disconnect or last own activity, and 0 when it has not been seen since the
coordinator started. Which sessions count as present is the reader's choice.

Labels are stored and returned verbatim and never change an Address or its mailbox. So is
`--declare` text: whatever a session states about itself at Register.

`send --to '*'` is a Broadcast: the coordinator commits it once and fans it out to every
session registered at that moment, one Mail per recipient with id `<mail>/<address>`, each
delivered and acked like directed mail. A Mail id containing `/` is refused, so no directed
id can equal a copy id. Every delivery carries `wake`: true for directed mail, false for a
broadcast copy.

## Receipts and latency

The coordinator stamps each Mail twice on its own clock (milliseconds since the Epoch):
`accepted_at_ms` when it commits the send and answers Accepted, and `receipted_at_ms` when it
records the Receipt. Both stamps come from one clock, so their difference is end-to-end
latency with no skew between hosts. Broadcast copies carry the broadcast's `accepted_at_ms`
and their own `receipted_at_ms`. `receipt --mail <id>` (a copy by its `<mail>/<address>` id)
prints `RECEIPT mail= to= record= accepted_at_ms= receipted_at_ms= latency_ms=` once the
recipient has acked, else `PENDING mail= to= accepted_at_ms=`; an unknown id exits nonzero.
A Mail or Receipt committed before the stamps existed replays unchanged and prints `absent`,
never 0. The core reports these numbers and does nothing else with them.

## Registration

A Register adds an Address to the Board and to every later Broadcast. The registration ends
in one of two ways, both committed to the coordinator log as an `unregister` event:

- the session says so: `{"type":"unregister"}` on its daemon connection (the adapter sends
  it when its stdin closes), at once;
- the session stays disconnected (its connection or its host daemon's link closed, or it
  never re-registered after a coordinator restart) for `--unregister-after-ms` (default
  300000), read like a claim deadline at the next request that reads the registered set
  (Broadcast, Board, claim, relabel). No timer scans for it.

Mail already accepted for an unregistered Address stays pending and is delivered if that
Address registers again; a connected session never expires.

## Claims

A claim's deadline is `now + ttl_ms` on the coordinator's wall clock, in milliseconds since
the Epoch, and is stored absolute so it survives a restart; a monotonic clock alone cannot do
that. The coordinator therefore trusts its own clock: a forward step (NTP, suspend and
resume) frees held keys early and a backward step extends them. The holder may repeat either
grant and land in the same state: `claim` by the current holder is a retry-safe refresh that
resets the deadline, and `renew` sets the deadline to `now + ttl_ms` even when that is sooner
than before. Only the live holder can renew or release; a session that lost a key to expiry
cannot renew it back. One failed log append stops the coordinator rather than letting memory
and disk diverge; a restart replays the log.

## Durability and locks

Every durable log (a Store, the daemon and coordinator logs) is append-only and fsynced per
append, and replay drops a tail a crash left half-written. Each is held under an exclusive OS
lock while open: a second process on the same file fails at start, naming the file, and exits
nonzero. A host daemon likewise refuses a `--socket` path that a live process still answers
on, naming the path, and replaces only a stale socket file; a coordinator refuses a `--bind`
address already in use.

## Adapters

Anything that fronts an agent program sits on top of the core as an adapter-fronted shim
under `shims/<name>/`, each with its own README, install script and `check`. A shim starts
`roger adapter` and talks to it in JSON lines (the request and reply shapes are documented at
`adapter` in `src/session.rs`); the core never learns which program is there. When the
adapter's stdin reaches EOF it unregisters its Address (see Registration) and exits; a shim
ends its adapter by closing that stdin.

Delivery to a model is best effort and separate from the core's guarantee. The adapter writes
the Mail to its Store (durable), then tells the shim, then sends the Ack; the Ack stays the
adapter's responsibility and a shim never sends it. What a shim has shown its program is not
durable. If the adapter died before its Ack, the core redelivers and the Store reuses the same
record, so a restarted shim is told again; Mail already acked is not replayed to a model.

Wake is the shim's job; the core carries no busy or idle state. A shim that can start a turn
declares `wake-latency-ms=<n>` through `--declare` and acts on each delivery's `wake` flag;
the Store write and the Ack wait for neither.

## Checks

Every script accepts `--help` (or `-h`) and prints usage without running its work.
`scripts/acceptance --help` also lists the required cross-host environment variables.

Nothing lands on main until `scripts/check-all` exits 0; what each gate runs and the review
rules are in [AGENTS.md § Coding standards](AGENTS.md#coding-standards).

```sh
scripts/check                     # the core gate
scripts/check-all                 # the core gate, then each shim's own check
scripts/check-harness-free        # invariant: no harness identity anywhere in the core
scripts/acceptance cross-host     # proof 1: mail survives a recipient SIGKILL across hosts
scripts/acceptance no-poll        # invariant 2: mail, receipts, claim expiry, unregister on committed events; loopback
scripts/acceptance independence   # proof 2: sessions from two harnesses (plus a Tracer) exchange mail; loopback
scripts/acceptance broadcast      # proof 3: one broadcast reaches every registered session once, no wake
scripts/acceptance claims         # proof 4: claims are exclusive, renew, expire; board on both hosts
scripts/acceptance wake           # proof 5: directed mail wakes an idle session; busy queues to the turn boundary
scripts/acceptance references     # proof 6: references keep both tokens verbatim; relabel keeps the Address
```

`cross-host`, `broadcast` and `claims` run on two hosts. Set `ROGER_REMOTE_HOST` to the
remote SSH destination (no default) and `ROGER_ACCEPT_BIND` to this host's IPv4 address
reachable from that host. The coordinator binds that address, with one host daemon here
and one on the remote host, reached with `ssh` and `scp`. For example:
`ROGER_REMOTE_HOST=<ssh-destination> ROGER_ACCEPT_BIND=<local-ipv4> scripts/acceptance cross-host`.
They build a
binary with the test hooks and remove every process and directory they created. The other
checks run on loopback and need `jq`; what a proof needs from a harness is in that shim's
README.

What `scripts/check-harness-free` counts as the core, and what it exempts, is review rule 4
in [AGENTS.md § Coding standards](AGENTS.md#coding-standards).

`scripts/acceptance no-poll` runs the real binaries with no sweep (the core has none; a sweep
added later needs a switch that this check turns off). Each phase leaves one event trigger as
the only path to its result, so removing any trigger fails it, and its idle windows fail if any
core log grows while nobody asks.

`scripts/acceptance references` reads the Board through `roger board` and also searches
`src/` for any code that parses a Reference or inspects a label (invariant 3).

## Layout

| Path | Role |
| --- | --- |
| `KERNEL.md`, `INVARIANTS.md`, `GLOSSARY.md` | the frozen purpose, proofs, invariants and terms |
| `AGENTS.md` | directives, coding standards and review rules |
| `src/main.rs` | subcommand dispatch and flags |
| `src/coordinator.rs` | central durable store; pushes committed mail |
| `src/claims.rs` | claim decisions; expiry read from each committed deadline, never swept |
| `src/daemon.rs` | one per host; local inbox; serves sessions on a Unix socket |
| `src/session.rs` | contract-shaped agent session (Tracer), adapter, and the clients |
| `src/durlog.rs` | append-only fsynced log; replay drops a crash-truncated tail |
| `src/json.rs`, `src/net.rs` | JSON codec; one-object-per-line framing and message helpers |
| `tests/process.rs` | process seam, including the SIGKILL window |
| `scripts/` | gates (`check`, `check-all`, `check-*`), `acceptance`, `deploy` |
| `deploy/` | systemd user units and [deploy/README.md](deploy/README.md) |
| `shims/<name>/` | one adapter-fronted shim per harness: README, install, check |
| `VECTORS/` | the record of what was tried |
| `docs/reviews/` | historical review evidence |

Licensed under the [MIT License](LICENSE).

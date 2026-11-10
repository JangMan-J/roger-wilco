# Roger-Wilco

Roger-Wilco gives a set of long-running sessions on your own machines three things: durable
mail between them, a shared Board of who is registered, and exclusive, expiring claims on
keys. A session can be an application, a script, or a person behind some front end; the core
knows sessions, not the programs behind them. It routes and stores. It does not schedule work,
assign tasks, or interpret what it carries.

The core is one Rust binary, `roger`, with no external crates. One coordinator holds the
durable state, one host daemon per machine serves local sessions on a Unix socket, and
anything else (a command-line client, a test session, an adapter for a particular agent
program) sits on top of that socket.

This README describes the core contract, commands and checks. For development, follow the
coding standards and review rules in [AGENTS.md](AGENTS.md#coding-standards); for installation
and operations, see [deploy/README.md](deploy/README.md). Run `scripts/check-all` before
landing changes on main (see [Checks](#checks)).

| Module | Doc |
| --- | --- |
| Core contract: commands, delivery, claims, checks | this file |
| Deploy: install, services, host config, backups, operations | [deploy/README.md](deploy/README.md) |
| The gate and the review rules | [AGENTS.md § Coding standards](AGENTS.md#coding-standards) |
| A later direction: what a Usenet-like, peered core will need | [docs/USENET-MODEL.md](docs/USENET-MODEL.md) |

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

The `test-hooks` feature adds `--hold-before-ack <marker>` to `session`, `adapter` and `mcp`: park
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

Session b prints `STORED mail=m1 record=r-1 kind=directed`, then `DELIVERED mail=m1 record=r-1`
once its Ack is recorded. Stop the processes with `kill %1 %2 %3 %4`.

## Commands

`roger <subcommand> [flags]`. No command has a default socket, data or log path: each comes
from its flags. `coordinator` and `daemon` read the fleet token from `ROGER_TOKEN` in their
environment, never from argv, which `ps` shows to every user.

| Subcommand | Flags | Does |
| --- | --- | --- |
| `coordinator` | `--bind <addr:port> --data <log> [--port-file <file>] [--unregister-after-ms <n>]` | central durable store; `--port-file` receives the bound port (useful with port 0); the file appears whole, written aside and renamed into place, so a reader that sees it can use it |
| `daemon` | `--coordinator <addr:port> --host-id <id> --socket <path> --data <log>` | one per host; local inbox; serves sessions on the socket |
| `session` | `--socket --store <file> [--id <address>] [--label <text>] [--declare <text>]` | the Tracer: a session with no harness that reports on stdout |
| `adapter` | same as `session`, plus `[--registered] [--superseded]` (also send `{"type":"registered"}`, see Faces) | the same session, fronted by a stdin/stdout line stream for a client |
| `send` | `--socket --from <address> --to <address or '*'> --body <text> [--mail <id>] [--header <key=value>]...` | prints `ACCEPTED mail=<id>`; `--mail` defaults to a fresh UUID; `--header` repeats, split at the first `=` (see Kind and headers) |
| `mcp` | `[--socket <path>] [--store <file>] [--id <address> \| --id-env <NAME>] [--label <text>] [--declare <text>]` | the same session as an MCP server on stdio (see Faces) |
| `receipt` | `--socket --mail <id>` | one Mail's state (see Receipts and latency) |
| `audit` | `[--coordinator-log <file>] [--daemon-log <file>]... [--store <file>]... [--address <address>] [--bodies] [--json]` | read-only history and findings from records (see Audit) |
| `label` | `--socket --from <address> --label <text>` | relabel a registered session |
| `claim`, `renew` | `--socket --owner <address> --key <text> --ttl-ms <n>` | grant or extend a Claim; `--ttl-ms` must be positive |
| `release` | `--socket --owner <address> --key <text>` | give a Claim up |
| `board` | `--socket [--from <address>]` | one Board read; `--from` counts as that session's activity |

A session's Address is `--id`, or a fresh UUID. An Address is an opaque string: a UUID, or the
whole composite `[[<token>:<label>:<token>]]<uuid>` (the label may hold spaces; a token is lowercase base 36, `0-9a-z`, no leading zeros). Every
command takes it whole and every record keeps it byte-identical; the core never splits it, and
the tokens inside mean something only to the consumer that composed them. A relabel changes the
Board's display label and never the Address, inner label included. The Tracer prints `READY session=<address>`
after it registers, `STORED mail= record= kind=` after each Store write (followed by a
`HEADERS mail= <json>` line when the Mail has headers), and `DELIVERED mail= record=` when
its Ack is recorded.

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
id can equal a copy id.

A second `send` under a Mail id is a resend only when it repeats the first: the same sender,
the same recipient and the same body. A resend prints `ACCEPTED` again and commits nothing, so
a client that lost the answer can repeat its send. A send that differs in any of the three is
refused with a reason that names what differs (for example `mail m1: expected a resend to
repeat the Mail already accepted under this id, saw a different body`) and commits nothing:
were it accepted, it would never be delivered. A Broadcast id is held to its sender and body
the same way. Headers are not compared (see Kind and headers); the comparison still holds
after the Mail is receipted and after a coordinator restart.

## Kind and headers

Every delivery carries a `kind`: `directed` for a Mail sent to one Address, `broadcast` for a
copy of a Broadcast. It says how the Mail reached the recipient and nothing about what the
recipient does with it. The same hop carries `headers`: a JSON object of string keys to
string values that the sender set with `send` (`--header key=value`, repeatable, or
`"headers"` on the adapter's `send`); `{}` when there are none. The core never reads, compares
or ranks a key or a value. They are carried verbatim, key order included, through the
coordinator log, the host daemon log, the delivery, the session's Store entry (which gains
`kind` and `headers`) and the receipt reply. A Broadcast's headers go on every copy; resending
an existing Mail id commits nothing new and does not update them.

The coordinator bounds them once, before anything is committed: at most 32 headers; each key 1
to 128 bytes; each value 0 to 4096 bytes; keys unique; values strings. A set outside the
bounds is refused with an error whose reason says what was expected and what was seen (for
example `headers: expected at most 32 headers, saw 33`), and no Mail, log line or receipt
exists for it. A key that contains `=` cannot be given to `--header`; send it through the
adapter or the socket. Logs and Stores written before `kind` and `headers` existed replay
unchanged: such a Mail is directed, or a Broadcast copy where its log line says so, with no
headers.

## Receipts and latency

The coordinator stamps each Mail twice on its own clock (milliseconds since the Epoch):
`accepted_at_ms` when it commits the send and answers Accepted, and `receipted_at_ms` when it
records the Receipt. Both stamps come from one clock, so their difference is end-to-end
latency with no skew between hosts. Broadcast copies carry the broadcast's `accepted_at_ms`
and their own `receipted_at_ms`. `receipt --mail <id>` (a copy by its `<mail>/<address>` id)
prints `RECEIPT mail= to= record= accepted_at_ms= receipted_at_ms= latency_ms= kind=` once the
recipient has acked, else `PENDING mail= to= accepted_at_ms= kind=`, and a following
`HEADERS mail= <json>` line when the Mail has headers; an unknown id exits nonzero.
A Mail or Receipt committed before the stamps existed replays unchanged and prints `absent`,
never 0. The receipt reply on the socket (and so `roger_receipt`) also carries `from`, the
sender Address as the `mail` or `broadcast` line holds it, right after `to`; the printed
lines do not show it. The core reports these numbers and does nothing else with them.

## Audit

```
roger audit [--coordinator-log <file>] [--daemon-log <file>]... [--store <file>]...
            [--address <address>] [--bodies] [--json]
```

At least one input is required. Each is read as a plain file: no lock, no write, no
truncation, safe on a live log or Store even while its writer holds the lock. Files
are read separately, not as an atomic snapshot of the fleet. A trailing fragment
without a newline is reported and ignored; non-object or invalid JSON lines are
reported and ignored. Findings do not fail the command; unreadable inputs do.

The default output is one `WORD key=value ...` record per line, free text last.
`--json` gives one object per line with `type` equal to WORD and the same fields;
`line` and counts are integers, `absent` is `null`. Use JSON for unambiguous text
with spaces or newlines. Header and body tails become `headers` and `body` fields;
`mails` on a duplicate finding is an array rather than comma-separated text.

Sources appear in argument order, followed by the coordinator timeline, each daemon
log, each Store, then findings and the summary. Timeline order is file order, never
a sort by timestamp. `line` is the original 1-based complete-line number.

| WORD | Fields, in text order |
| --- | --- |
| `SOURCE` | `kind=coordinator\|daemon\|store path= lines=` (complete lines, skipped included) |
| `TORN` | `path= bytes=` (trailing fragment length) |
| `SKIPPED` | `path= line=` (not a JSON object) |
| `REGISTER` | `address= host= at_ms= line=` |
| `UNREGISTER` | `address= at_ms= line=` |
| `LABEL` | `address= at_ms= line= label=` |
| `DECLARED` | `address= at_ms= line= declared=` |
| `UNDECLARED` | `address= at_ms= line=` (declared text cleared) |
| `MAIL` | `mail= kind= from= to= accepted_at_ms= receipted_at_ms= latency_ms= record= receipt_line= body_bytes= line=` |
| `HEADERS` | `mail= <json object or absent>`, immediately after its Mail, delivery or Store entry |
| `BODY` | `mail= <JSON string>`, after headers, only with `--bodies` (Mail and Store) |
| `CLAIM` | `key= owner= expires_ms= line=` |
| `RELEASE` | `key= line=` |
| `STRAY_RECEIPT` | `mail= session= record= line=` (no earlier acceptance) |
| `DAEMON_DELIVER` | `path= mail= kind= from= to= line=` |
| `DAEMON_RECEIPT` | `path= mail= session= record= line=` |
| `STORE` | `path= record= mail= from= kind= line=` |
| `UNRECEIPTED` | `mail= kind= from= to= accepted_at_ms= line=` |
| `DUPLICATE_RECORD` | `path= record= mails=` (distinct Mail ids, comma-separated) |
| `RECORD_MISMATCH` | `mail= coordinator= daemon= store=` (record ids, or absent) |
| `AUDIT` | `sources= registers= unregisters= mails= receipted= unreceipted= duplicates= mismatches= torn= skipped=` |

Each Broadcast yields one `MAIL` per recipient with id `<mail>/<address>` and kind
`broadcast`. Its acceptance stamp is the Broadcast's; the first Receipt for each id
supplies its record, receipt line and receipt stamp. Unreceipted Mail has
`record=pending`. Latency is the difference of the two stamps; `body_bytes` is UTF-8
length. Old missing stamps and session `at_ms` read `absent`, never zero. Old delivery
kinds use the existing compatibility reader; a Store without either kind field reads
`absent`. Missing header sets read `HEADERS mail=... absent` (JSON `headers:null`);
an explicitly empty set emits no HEADERS line. Nonempty sets retain key order.

`--address` matches whole Addresses: session events, sender or recipient of Mail and
delivery, each Broadcast copy, receipt sessions, and claim owners. Receipt-only evidence
is also kept when supplied Mail, delivery or Store records identify that sender or
recipient. A release follows the key's last claim owner. Stores have no recipient
Address and are always listed in full: pass the desired Address's own Store. Counts
cover the listed records. Duplicate records are scoped to one input, and to a session
within coordinator and daemon logs. Mismatches compare Record ids for the same Mail
across supplied inputs, preserving every distinct id from repeated daemon receipts and
Store entries; the coordinator MAIL join still uses its first Receipt. A mismatch requires evidence
from at least two inputs and produces one finding per Mail. When a kind has several
Record ids, the finding shows a conflicting representative; the timeline retains every
source's evidence.

Audit verifies nothing: it compares no token, header, label or declared text, and
interprets no body. A consumer's own checks run over its output. See
[consumer conventions](docs/CONVENTIONS.md) for what a consumer may put in these fields
and how it verifies them; the core checks none of it.

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

Every Register, unregister, relabel and change of declared text is one line on the
coordinator log (`session`, `unregister`, `label`, `declared`), stamped `at_ms` on the
coordinator's clock, the clock `accepted_at_ms` is read on: a Register commits its `session`
line whether or not the Address was already registered on that host, then the `label` line
it carries, then a `declared` line when its text differs from what the coordinator holds.
Lines written before the stamp existed have none and replay unchanged; the coordinator reads
nothing from `at_ms`. The Board's `last_seen_ms` is unchanged: it is memory, not the log, so
a replayed session that has not re-registered still reads 0.

### Replacement registration

Newest Register always wins, including on the same host. There is no secret, refusal or
recovery barrier. Replacing a connected Address commits a `takeover` event with `session`
(the full Address), `old_host`, `host` (the new host), `at_ms` (coordinator clock), and an
internal `registration` correlation id. `roger audit` lists `TAKEOVER` followed by the
`ADDRESS_TAKEOVER` finding with `address`, both hosts, `at_ms`, and source `line`, in text
and JSON; Address filtering applies to both. Older logs are not used to infer takeovers.

A client receiving `superseded` should stop sending as that Address, tell its supervisor/user,
and not automatically re-register. This is consumer action, not core policy: the old
connection stays open, sends and in-flight Acks remain allowed, and Mail handling is unchanged.
The notice itself triggers no Register. Existing daemon reconnect behavior is retained:
it re-announces each latest still-open local connection, even one previously superseded
on another host. Such a reconnect can take the Address back; clients must act themselves.
Restoring a disconnected registration alone is not a takeover.

Daemon-generated registration ids correlate confirmations, replacements and releases to
connections; they are not Address tokens or credentials. Local replacements awaiting upstream
confirmation retain replacement evidence in memory. After reconnect only the latest live
local connection is re-registered; obsolete replacements are reported as evidence only,
without restoring their routing or presence, even if the newest connection has closed.
The coordinator advertises `registration_notices:true` in `hello_ok` before a daemon uses
these internal evidence-only messages; older coordinators keep the original restore path.
Notification is best-effort, once per local registration: transport
failure or daemon/process death can lose an observer notice. It is not Mail, has no Ack or
Store durability guarantee, and the committed audit event does not prove receipt. A delayed
notice's stamp is when the coordinator recorded it, not the local replacement time; stamps
are not unique event ids. New notification requires upgraded daemon and coordinator.

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

Opening a log replays it one line at a time, never the whole file at once. The coordinator
keeps a Mail's body in memory until its Receipt and a host daemon until its Ack; after that
each keeps the Mail's id and record and the body stays only on the log. Nothing is removed
from a log.

## Links and peers

The coordinator and each host daemon write to a peer through that peer's own queue, with one
thread per peer doing the writing. A session that stops reading its connection, or a host
daemon that stops reading its link, delays only its own lines: the sessions beside it and the
other hosts keep sending, receiving and reading the Board, and the Mail it has not taken stays
pending, unacked, for its next Register. The queue has no bound of its own: what it holds for
a peer that is not reading is Mail the process already keeps for that peer until its Ack, plus
the answers to that peer's own requests. A write that fails ends that connection. When a
connection ends, what is already queued for the peer is written first, unless the peer was
given up as silent.

A line is written whole, in one write, on a link with `TCP_NODELAY` set, so no hop waits for
more to send.

The link between a host daemon and the coordinator carries its own liveness. The coordinator
says `"ping":true` in `hello_ok`; a host daemon that reads it sends `{"type":"ping"}` every
10 s and is answered `{"type":"pong"}`. From its first ping, the coordinator gives a host
daemon up once the link has been silent for 30 s: it drops the link, so that host's sessions
read `connected=false` with `last_seen_ms` at that moment, and from then on they are
disconnected sessions like any other (see Registration). A host daemon that is frozen, or
whose host went away without closing the connection, is noticed this way; one that is only
slow for less than that is not dropped. When it runs again it finds the link closed and
reconnects.

The host daemon sets no such bound on the coordinator. A coordinator that is stopped and not
gone still holds the link and every line on it, and a second link dialed meanwhile would
have its lines applied before the first one's leftovers; the daemon stays on its link, as it
always did, and its pings make a coordinator that really is gone show as a failed write,
which ends the link and starts the reconnect. A ping carries no Mail and commits nothing.
Either side keeps a link to an older peer as before: a host daemon sends no ping to a
coordinator that did not offer it, and the coordinator holds a host daemon to the bound only
after its first ping.

A connection that has not yet shown the fleet token can end only itself: its hello line may
be at most 65536 bytes. On every connection and in every log, a JSON value nested more than
64 levels deep is refused as malformed. Either ends that connection with a reason that says
what was expected and what was seen; the process keeps serving.

## Adapters

A client written for a program connects through a face, in its own project; the core ships
none. A client that wants a line stream starts
`roger adapter` and talks to it in JSON lines (the request and reply shapes are documented at
`adapter` in `src/session.rs`); the core never learns which program is there. When the
adapter's stdin reaches EOF it unregisters its Address (see Registration) and exits; a client
ends its adapter by closing that stdin.

A delivery reaches the client after the adapter has written it to its Store, as one `mail` line
that carries the Mail's `kind` and `headers`. What a client does when a delivery arrives is the
client's own: the core carries no state about it, and the Store write and the Ack wait for
nothing the client does. The adapter writes the Mail to its Store (durable), then tells the client,
then sends the Ack; the Ack stays the adapter's responsibility and a client never sends it. What
a client has shown its program is not durable. If the adapter died before its Ack, the core
redelivers and the Store reuses the same record, so a restarted client is told again; Mail
already acked is not replayed.

`--declare` is opaque text a session states about itself at Register; the core stores it and
returns it on the Board, and reads none of it.

## Faces

A Face is one complete way a client reaches the core as a session. All three give a session the
same Mail, Store, Ack, Board, Claim and Receipt semantics and add none of their own; a client
that speaks one needs nothing more from the core.

- **The socket protocol.** Line-delimited JSON over the host daemon's Unix socket: `register`,
  `deliver`/`ack`, `send`, `board`, `claim`, `relabel`, `receipt`, `unregister`. The `session`
  Tracer and the one-shot subcommands above are clients of it. A `register` line may carry
  `"confirm_register":true`; the host daemon then sends `{"type":"registered","session":"<address>"}`
  once, when the coordinator holds that Register. Without the field no such line is ever sent.
  Separately, `"notify_superseded":true` opts in to exactly
  `{"type":"superseded","session":"<full Address>","host":"<new host>","at_ms":<integer>}`.
  Without that opt-in no unsolicited replacement line is sent.
- **The `roger adapter` stream.** The same session fronted by one JSON line stream on stdin and
  stdout, for a client (see Adapters).
- **`roger mcp`.** The same session speaking the Model Context Protocol over stdio, so a client
  that can register an MCP server needs no client code.

Replacement notices on the other faces use the same payload:
- The adapter always requests socket notices and prints an actionable `SUPERSEDED` warning
  on stderr. Only `--superseded` also emits the JSON notice on stdout. `--registered` alone
  does **not** enable it; existing reply-order clients keep their stdout contract and lifetime.
- The Tracer prints `SUPERSEDED <JSON notice>` on stdout and an actionable warning on stderr.
- MCP prints the warning only on stderr; stdout remains JSON-RPC responses. `roger_registered`
  returns `{"registered":true|false,"superseded":null|<notice>}`. The notice is sticky for this
  process, makes `registered` false, and ends a registration wait immediately. A late
  confirmation cannot clear it. Other tools remain usable; no new tool or unsolicited RPC
  notification is added.

Two signals, in order. `READY` (the Tracer), `ready` (the adapter) mean the Register was written to
the host daemon. `REGISTERED session=<address>` (the Tracer), `{"type":"registered","session":"<address>"}`
(the adapter, only when started with `--registered`, written after `ready`) and the `roger_registered`
tool (`roger mcp`) mean the coordinator holds the Register. A client that needs its registration visible
to others (Board, Claim, relabel) waits for `registered` first. During a coordinator outage it simply
arrives later, after the link is back; each face reports it once per registration. An old host daemon
or coordinator never sends it, so a client that waits for it bounds the wait. The socket's Register
field is `confirm_register`; the host daemon does not forward it upstream.

### `roger mcp`

```
roger mcp [--socket <path>] [--store <file>] [--id <address> | --id-env <NAME>]
          [--label <text>] [--declare <text>]
```

To register it with an MCP client, give the client the command line `roger mcp` with any of
the flags above, and set environment variables for it where the client allows. Nothing else is
needed: the process is a session, it registers when it starts, and the client ends it by
closing its stdin.

| Setting | Order |
| --- | --- |
| Address | `--id`; else the variable named by `--id-env NAME` (so a client's own registration can hand the server a stable per-session id; unset or empty falls through); else `ROGER_ADDRESS`; else a fresh UUID. Used verbatim, never parsed. |
| Socket | `--socket`; else `ROGER_SOCKET`; else `$XDG_RUNTIME_DIR/roger/daemon.sock`. A missing socket, or none of these set, is an error naming what was expected and what was seen. |
| Store | `--store`; else `ROGER_STORE`; else `${XDG_STATE_HOME:-~/.local/state}/roger/mcp/<address>.store`. |

It writes each delivery to its Store, then acks naming that Record id, exactly as the adapter
does; the Ack waits for no reader and for nothing a tool call does. stdout carries only
protocol messages; everything else goes to stderr. When stdin closes it unregisters and exits
0. If the host daemon connection is lost it exits 1 with the reason on stderr (it does not
reconnect). At exit, a Store at the default path that holds no entries is removed with its
`.cursor` file, so a health check leaves nothing; an explicit `--store` is never removed.
`--hold-before-ack <marker>` (feature `test-hooks`) works here as for `session`.

The protocol is newline-delimited JSON-RPC 2.0, one message per line. `initialize` answers
with the client's `protocolVersion` when it is `2025-06-18`, `2025-03-26` or `2024-11-05`
(else `2025-06-18`), `capabilities` `{"tools":{}}` and `serverInfo` `roger` with the crate
version. Notifications get no reply, `ping` gets `{}`, any other method is error -32601, and
unparseable input is -32700 with a null id. Calls run concurrently, so a waiting
`roger_inbox` blocks no other call. A tool failure is a result with `isError: true` and the
reason in its text; an unknown tool or an argument of the wrong type says what was expected
and what was seen. Every result is one `text` item holding one compact JSON object.
Streamable HTTP is not offered.

| Tool | Arguments | Result |
| --- | --- | --- |
| `roger_send` | `to`, `body`, `mail?`, `headers?` | `{"mail"}`; `to` may be `"*"`; `headers` is an object of strings passed through unread, and the core's refusal reason is the error text |
| `roger_inbox` | `limit?` (50, max 200), `wait_seconds?` (10, max 60, 0 = no wait), `peek?` | `{"mail":[{"record","mail","from","body","kind","headers"}],"cursor"}` |
| `roger_board` | `all?`, `window_seconds?` (600) | `{"me","sessions","claims","now_ms"}`, rows as the Board gave them; by default only sessions connected or seen within the window of `now_ms` |
| `roger_label` | `label` | `{"session","label"}` |
| `roger_claim` | `key`, `ttl_seconds?` (900) | `{"ok","op","key","holder"?,"remaining_ms"?,"reason"?}`; acquires, or refreshes a claim this session holds; a refusal is `"ok":false`, not an error |
| `roger_renew` | `key`, `ttl_seconds` | the same claim result |
| `roger_release` | `key` | the same claim result |
| `roger_registered` | `wait_seconds?` (0, at most 60) | `{"registered":true,"superseded":null}` once confirmed; otherwise false after the bounded wait, or immediately with the sticky notice when superseded |
| `roger_receipt` | `mail` | the receipt reply: `mail`, `to`, `from`, `kind`, `headers`, `accepted_at_ms`, and `record` and `receipted_at_ms` once acked; an unknown mail is an error |

`roger_inbox` returns the Store entries after this session's read position, oldest first. With
none it waits up to `wait_seconds` for the next delivery to reach the Store; that write ends
the wait, not a timer scan. Unless `peek` is true the position moves past the entries returned
and is saved before the reply is sent: it is the file `<store>.cursor`, written by temp file,
fsync and rename, and read at start, so it survives SIGKILL and restart. An entry already
returned is not returned again, an entry stored before the process died is returned once, and
`cursor` is the last record returned (null when none). It never gates an Ack. An entry written
before kinds and headers existed shows neither. Which sessions count as present on the Board
is the reader's choice, made here by `all` and `window_seconds`; nothing compares an Address,
a label or a token.

## Checks

Every script accepts `--help` (or `-h`) and prints usage without running its work.
`scripts/acceptance --help` also lists the required cross-host environment variables.

Nothing lands on main until `scripts/check-all` exits 0; what each gate runs and the review
rules are in [AGENTS.md § Coding standards](AGENTS.md#coding-standards).

```sh
scripts/check                     # the core gate
scripts/check-all                 # the core gate, then the seal guard
scripts/check-harness-free        # invariant: no harness identity anywhere in the core
scripts/check-seal                # verifies document integrity when a seal is present; skips otherwise
scripts/acceptance cross-host     # proof 1: mail survives a recipient SIGKILL across hosts
scripts/acceptance no-poll        # invariant 2: mail, receipts, claim expiry, unregister on committed events; loopback
scripts/acceptance independence   # proof 2: a real outside program on `roger mcp` and a Tracer exchange mail both ways; one real model call; loopback
scripts/acceptance broadcast      # proof 3: one broadcast reaches every registered session once, kind broadcast
scripts/acceptance claims         # proof 4: claims are exclusive, renew, expire; board on both hosts
scripts/acceptance references     # proof 6: references keep both tokens verbatim; relabel keeps the Address
scripts/acceptance headers        # proof 7: kind and headers arrive byte-identical; a refused set commits nothing; old logs replay
scripts/acceptance audit          # proof 9: read-only history, findings, old and live records; loopback
scripts/acceptance mcp            # proof 8 (K8): `roger mcp` is a session; tools, position, SIGKILL between Store and Ack; loopback
scripts/check-vocabulary          # invariant: the core never names what a session does with its Mail
checks/portability-pack.sh        # builds, tests and installs in a clean HOME with no checkout path; ./install.sh installs for the current user
```

`cross-host`, `broadcast` and `claims` run on two hosts. Set `ROGER_REMOTE_HOST` to the
remote SSH destination (no default) and `ROGER_ACCEPT_BIND` to this host's IPv4 address
reachable from that host. The coordinator binds that address, with one host daemon here
and one on the remote host, reached with `ssh` and `scp`. For example:
`ROGER_REMOTE_HOST=<ssh-destination> ROGER_ACCEPT_BIND=<local-ipv4> scripts/acceptance cross-host`.
They build a
binary with the test hooks and remove every process and directory they created. The other
checks run on loopback and need `jq`.

What `scripts/check-harness-free` counts as the core, and what it exempts, is review rule 4
in [AGENTS.md § Coding standards](AGENTS.md#coding-standards).

`scripts/acceptance no-poll` runs the real binaries with no sweep (the core has none; a sweep
added later needs a switch that this check switches off). Each phase leaves one event trigger as
the only path to its result, so removing any trigger fails it, and its quiet windows fail if any
core log grows while nobody asks.

`scripts/acceptance references` reads the Board through `roger board` and also searches
`src/` for any code that parses a Reference or inspects a label (invariant 3).

## Layout

| Path | Role |
| --- | --- |
| `AGENTS.md` | development guidance, coding standards and review rules |
| `src/main.rs` | subcommand dispatch and flags |
| `src/audit.rs` | read-only record history and findings |
| `src/coordinator.rs` | central durable store; pushes committed mail |
| `src/claims.rs` | claim decisions; expiry read from each committed deadline, never swept |
| `src/daemon.rs` | one per host; local inbox; serves sessions on a Unix socket |
| `src/session.rs` | contract-shaped session (Tracer), adapter, and the clients |
| `src/mcp.rs` | the MCP stdio face: protocol, tools, the inbox position |
| `src/durlog.rs` | append-only fsynced log; replay drops a crash-truncated tail |
| `src/json.rs`, `src/net.rs` | JSON codec; one-object-per-line framing, per-peer outgoing queues and message helpers |
| `src/headers.rs` | the bounds on a Mail's headers, checked once by the coordinator |
| `tests/process.rs` | process seam, including the SIGKILL window |
| `tests/audit.rs` | exact history fixtures, filtering, findings, JSON, live read-only audit |
| `tests/mcp.rs` | `roger mcp` over stdio against a real coordinator and daemon |
| `scripts/` | gates (`check`, `check-all`, `check-*`), `acceptance`, `deploy` |
| `deploy/` | systemd user units and [deploy/README.md](deploy/README.md) |
| `VECTORS/` | read-only history of what was tried; no new vectors are written here |
| `docs/reviews/` | historical review evidence |

Licensed under the [Apache License 2.0](LICENSE).

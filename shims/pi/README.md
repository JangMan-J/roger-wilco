# Pi shim

Connects each Pi session to Roger-Wilco, so it can send and receive Mail, read the Board and
hold claims, and wakes an idle Pi when directed Mail arrives. The shim is a Pi extension
(`roger.ts`) that runs `roger adapter` for its session, renders each delivered Mail into
Pi's transcript, and adds the `/roger` and `/roger-send` commands and the tools
`roger_send`, `roger_board`, `roger_label`, `roger_claim` and `roger_release`.

## Install

An optional module on top of the roger core, which [deploy/README.md](../../deploy/README.md)
installs. It needs, per host:

- the `roger` binary at `~/.local/bin/roger` (or on `PATH`)
- the host daemon listening on `$XDG_RUNTIME_DIR/roger/daemon.sock`
- a `pi` CLI on `PATH`, and `node` 22.18 or later (the install script and `check` use it;
  `check` runs the TypeScript tests directly)

Installing or removing it changes nothing in the core or in any other shim.

```sh
shims/pi/install              # install or update, from this checkout
shims/pi/install --uninstall  # remove
```

Install copies `roger.ts`, `roger-adapter.ts`, `roger-command.ts`, `roger-target.ts`, `roger-env.ts` and
`package.json` to `~/.local/share/roger-shims/pi/` and lists that directory once in the
`packages` array of Pi's `settings.json` (`${PI_CODING_AGENT_DIR:-~/.pi/agent}`), keeping
every other setting. Pi loads it as a local package: nothing is fetched, and Pi itself
provides `@earendil-works/pi-ai` and `@earendil-works/pi-coding-agent`. Run it again to
update; new Pi sessions pick up the copy. It installs for the current user on the current
host only; run it on each host.

Uninstall removes that entry and that directory. It keeps the Stores.

## At run time

Each Pi session starts its own `roger adapter`. A session's Address is its Pi session id
when that is a UUID (the default), so resuming keeps the same Address and Store; `/new` and
`/fork` give a new Address. Its Store is
`${XDG_STATE_HOME:-~/.local/state}/roger-shims/pi/<address>.store`. The core holds a Store
under an exclusive lock, so a second Pi process on the same session cannot run a second
adapter on it: that adapter fails at start naming the Store, and the shim reports its exit
(`roger shim: roger adapter exited (…)` on stderr; a send then fails with that reason). On
`session_shutdown` the shim closes the adapter's stdin, so the adapter unregisters its
Address, and waits for it to exit (SIGKILL after 3 s) before a new session starts the next
one.

A launcher can name the Address itself with `ROGER_ADDRESS`, a whole Address,
`[[<token>:<label>:<token>]]<uuid>` or a bare `<uuid>`. The shim uses it verbatim: it checks
the shape (base-36 tokens) and never reads a token. It does not depend on Pi's session id, so
the Address and its Store survive `/new` and a restart. Precedence: `ROGER_SESSION`, then
`ROGER_ADDRESS`, then `ROGER_ADDRESS_PREFIX` plus Pi's session id. A malformed value is
reported and no adapter starts. If a second live session claims an Address a first one holds,
its adapter is refused at the Store lock; the shim reports `roger address <address> is already
held by another live session (store <file>)` and does not retry, so Mail is never split. Do
not point two sessions at one Address with different `ROGER_STORE` files: the Store lock is
what refuses the second one.

The shim keeps the link alive. If the adapter exits while the Pi session lives (a crash, a
kill, a restarted host daemon) the shim starts a new one for the same Address and Store after
250 ms, doubling to at most 30 s between failed attempts (an adapter that stayed up 10 s starts
the delay over). Mail Accepted meanwhile stays pending at the core and arrives when the new
adapter registers, once: a record the Store already holds is not rendered twice. A send in
flight when the adapter died fails, and a send while it is down fails with the reason; it is
not queued. A missing binary or a bad setting is reported and not retried. `session_shutdown`
cancels any pending respawn.

Any non-empty environment variable overrides its default: `ROGER_SESSION`, `ROGER_ADDRESS`, `ROGER_ADDRESS_PREFIX` (a Reference that makes the Address the whole composite `<prefix><uuid>`), `ROGER_SOCKET`,
`ROGER_BIN`, `ROGER_STORE`, `ROGER_WAKE_LATENCY_MS`, `ROGER_PRESENCE_WINDOW_MS`. The header
of `roger.ts` describes each one except `ROGER_PRESENCE_WINDOW_MS` (which sessions `/roger`
and `roger_board` show by default: connected, or seen within the window, default 10
minutes), described in `roger-command.ts`.

## Delivery and wake

The shim writes each delivery into Pi's transcript. It declares `wake-latency-ms=<n>`
through `--declare` (`ROGER_WAKE_LATENCY_MS`, default 1000; `off` declares nothing and never
wakes). Pi maps the core's delivery `kind: directed` to its wake behavior (`startsTurn` in
`roger-adapter.ts`); `kind` wins over the older `wake` field, which is read only when `kind` is
absent (an older core). Headers are carried by the core and ignored by this shim. A directed
delivery goes to Pi as
`sendMessage(..., { triggerTurn: true, deliverAs: "followUp" })`: an idle Pi starts a turn, a
busy Pi queues it and hands it to the model at the turn boundary. A `broadcast` kind (or an older
core's `wake: false`) queues no turn and is rendered with `triggerTurn: false`.

## Checks

```sh
shims/pi/check                    # unit tests under test/; no pi binary needed
shims/pi/acceptance pi-shim        # lifecycle and the roger_send tool callback in a real Pi; no model
shims/pi/acceptance wake           # proof 5: directed mail wakes an idle Pi; busy queues to the turn boundary
scripts/acceptance independence   # proof 2: this shim is one of the two harnesses
```

The three acceptance checks run on loopback and need `pi` and `jq`.
`shims/pi/acceptance wake` drives a scripted faux model (`test/faux-model.ts`) and makes no model call. In
`scripts/acceptance independence` Pi runs in RPC mode with wake off and makes no model
call; that check also needs the Claude Code shim's requirements.

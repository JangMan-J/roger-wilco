# Deploy

How the core is installed and run on your hosts: one coordinator host, and a host daemon on
every host including that one. What the core guarantees is in the
[top-level README](../README.md); a client of the core installs separately, from its own project.

## Before you start

- Pick the coordinator host and run every deploy from a checkout there. It holds the fleet
  token and builds the binary.
- Every other host is reached as an SSH destination with key-based login (deploy uses
  `BatchMode=yes`) and gets the binary built on the coordinator host, so it must run the
  same OS and CPU architecture.
- Every host runs systemd with user units (`systemctl --user`).
- The coordinator link is plain TCP: the token and every Mail body cross it unencrypted.
  Bind the coordinator to a private network all hosts can reach, such as a VPN, never to a
  public interface.

## Install

`scripts/deploy --help` prints usage and the first-run `ROGER_BIND` requirement without deploying.

`scripts/deploy <host> [commit]`, run on the coordinator host, installs the core on `<host>`
and (re)starts its units. `commit` defaults to `HEAD`; deploy builds it from `git archive`
with `cargo build --release --offline` and refuses a binary that carries the `test-hooks`
feature. It is idempotent: run it again to update. `<host>` is an SSH destination, or this
host's own `hostname` for a local install; only a local install adds the coordinator unit.

```sh
ROGER_BIND=<private-address>:7477 scripts/deploy "$(hostname)"   # first run: the coordinator host
scripts/deploy <user>@<host-b>                                    # each other host
```

On `<host>` it writes:

| Path | What |
| --- | --- |
| `~/.local/bin/roger` | the release binary; the one it replaces is kept as `roger.prev` |
| `~/.config/roger/env` (0600) | `ROGER_TOKEN`, `ROGER_COORDINATOR`, `ROGER_HOST_ID` (see Host config) |
| `~/.config/systemd/user/roger-daemon.service` | every host |
| `~/.config/systemd/user/roger-coordinator.service` | the coordinator host only |
| `~/.local/share/roger/deployed` | `commit=` and `sha256=` of the installed binary |

The units come from this directory (`deploy/`). At run time the host daemon serves
`$XDG_RUNTIME_DIR/roger/daemon.sock`, the socket every client and Tracer on that host
uses.

## Host config

`~/.config/roger/env` on the coordinator host is the source for every host's env. Deploy
creates it on its first run and copies the token and coordinator address from it to each
host:

- `ROGER_TOKEN`, the fleet token, is generated once, on that first run, and never rotated by
  deploy. The units pass it to the core through `EnvironmentFile=`, never argv.
- `ROGER_COORDINATOR` is the address the coordinator listens on and every host daemon dials.
  On the first run `ROGER_BIND=address:port` is required; there is no default or automatic
  address discovery. To
  change it later, edit this file and deploy every host again.
- `ROGER_HOST_ID` is `<host>`'s own `hostname`, not the SSH name used to reach it: an SSH
  alias names the route, the hostname names the machine. Deploy refuses a hostname that is
  not `[A-Za-z0-9._-]+`.

## Services

| Unit | Runs | Durable log |
| --- | --- | --- |
| `roger-coordinator.service` | `roger coordinator --bind $ROGER_COORDINATOR` | `~/.local/share/roger/coordinator.log` |
| `roger-daemon.service` | `roger daemon --coordinator $ROGER_COORDINATOR --host-id $ROGER_HOST_ID` | `~/.local/share/roger/daemon.log` |

Both are systemd user units with `Restart=always` (3 s) and no start limit; the daemon
reconnects to the coordinator on its own. Units start at boot only with lingering enabled
(`loginctl enable-linger`). Deploy ends by printing each unit's state and the user's
`Linger` value, and reports `PASS deploy <host> <commit>` only when every unit is active and
the socket exists.

```sh
systemctl --user status roger-daemon roger-coordinator
journalctl --user -u roger-daemon -u roger-coordinator
cat ~/.local/share/roger/deployed
```

## Backups

Take one on every host before a deploy that changes the binary, units or env. The coordinator
log is the fleet's durable state (Mail, Receipts, sessions, claims); each host's daemon log is
its local inbox.

```sh
b=~/.local/share/roger-backup-$(date -u +%Y%m%d-%H%M%S)
mkdir -p "$b"/{bin,config,share,units}
cp -p ~/.local/bin/roger "$b/bin/"
cp -rp ~/.config/roger "$b/config/"
cp -rp ~/.local/share/roger "$b/share/"
cp -p ~/.config/systemd/user/roger-*.service "$b/units/"
```

A client keeps its own state (its Stores); back that up per its README.

## Procedure

1. Run `scripts/check-all` on the commit to ship.
2. Read the Board on each host (`roger board --socket "$XDG_RUNTIME_DIR/roger/daemon.sock"`)
   to see who is registered and what is claimed before the restart. Accepted Mail survives
   the coordinator and daemon restarts; sessions re-register.
3. Back up every host (above).
4. `scripts/deploy <coordinator-host> <commit>`, then `scripts/deploy <host> <commit>` for
   each other host. Each must end in `PASS`.
5. Check live: start a Tracer on two hosts, read the Board from both, send each way, and read
   each Receipt.

   ```sh
   sock="$XDG_RUNTIME_DIR/roger/daemon.sock"
   roger session --socket "$sock" --store /tmp/live.store --label live   # prints READY session=<address>
   roger board --socket "$sock"
   roger send --socket "$sock" --from <address> --to <other address> --mail live-1 --body ping
   roger receipt --socket "$sock" --mail live-1
   ```

   Stop the Tracers afterwards; they leave the Board after `--unregister-after-ms`
   (default 5 minutes).
6. `ps` shows no token for the coordinator or a daemon, and
   `grep -c -- --hold-before-ack ~/.local/bin/roger` prints 0.

## Copies and tests

A copy of this project (a clone, worktree or other checkout), a test, or `scripts/acceptance`
must not run with the live env. `~/.config/roger/env` holds the fleet token and the live
coordinator address: a process given them joins the live fleet as this host. Start a copy's
coordinator and daemons with a token of its own, a loopback `--bind`, and a `--socket` and
`--data` under the copy's own temp directory, as `tests/process.rs` and `scripts/acceptance`
do; `scripts/acceptance` unsets every inherited `ROGER_*` variable that names a token,
coordinator, host, socket, Store, session or binary. `scripts/deploy` is the one script that
reads the live env; run it only for a commit you mean to ship (Procedure), never to try out
a copy.

A daemon started on the live socket path by mistake is refused and exits nonzero; the live
daemon keeps its socket.

## Roll back

Deploy an earlier commit: `scripts/deploy <host> <commit>`. `~/.local/bin/roger.prev` holds
the binary the last deploy replaced, and a backup holds the env, units and logs as they were.

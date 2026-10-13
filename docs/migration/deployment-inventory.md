# Deployment inventory

The package exposes the coordinator and host executables through `package.json`.
The systemd examples use `UMask=0077` and `NoNewPrivileges=true`.
Configuration parsing rejects unknown fields and resolves home-relative paths.

The deployed package path is `~/.local/share/roger-wilco/package`.
The configuration files are under `~/.config/roger-wilco`.
The host inbox and coordinator database are under `~/.local/share/roger-wilco`.
The local Pi settings currently reference the source project.
Cutover checks must establish the adapter path on both machines.

The new socket path is the configured inbox path plus `.sock`.
No extra configuration key is necessary.
The daemon owns the socket, database, and registration worker.
The adapter does not open SQLite.

The initial inventory suggested transitional polling and backward-compatible rollback.
The selected contract rejects both suggestions.
Legacy writer views deliberately fail closed.
Rollback must preserve and reconcile post-cutover state before a legacy schema returns.

Relevant files:

- `deploy/roger-wilco-coordinator.service`
- `deploy/roger-wilco-host.service`
- `deploy/provision.mjs`
- `src/bin/config.ts`
- `src/bin/host.ts`
- `docs/operations.md`
- `test/installation/bin.test.ts`
- `test/installation/cross-host.mjs`

The inventory alone does not verify a production deployment.
Installed-package and physical-host checks remain required.

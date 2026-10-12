# Operations

## Install the package

Use the Node version in `.node-version` on each host.
Keep the old messaging path active until isolated acceptance passes.

1. Run the checks in the source checkout:

   ```sh
   npm ci --ignore-scripts
   npm run setup:sdk
   npm run check
   npm pack
   ```

   The coexistence test requires a research-only pi-webtasks checkout.
   If that checkout is not at `../pi-webtasks`, set `WEBTASKS_PACKAGE_DIR` before `npm run check`.
   This checkout is a test prerequisite, not a production dependency.

2. Extract the archive under `~/.local/share/roger-wilco/`.
3. Install production dependencies in the extracted `package` directory:

   ```sh
   cd ~/.local/share/roger-wilco/package
   cp deploy/package-lock.json package-lock.json
   npm ci --omit=dev --ignore-scripts
   ```

4. Copy the examples from `deploy/` to `~/.config/roger-wilco/`.
5. Replace the example credentials with a separate random token for each host.
6. Set the configuration directory mode to `0700`.
7. Set each configuration file mode to `0600`.

Use an absolute path or a `~/` path for each SQLite database.
Use distinct databases and credentials for the old and new services.
Keep the coordinator on loopback behind an HTTPS reverse proxy.
Expose no inbound listener on a host daemon.

## Start the services

1. Install `deploy/roger-wilco-coordinator.service` on the coordinator host.
2. Install `deploy/roger-wilco-host.service` on each host.
3. Copy the units to `~/.config/systemd/user/`.
4. Reload and start the units:

   ```sh
   systemctl --user daemon-reload
   systemctl --user enable --now roger-wilco-coordinator.service
   systemctl --user enable --now roger-wilco-host.service
   ```

5. Register the package with Pi:

   ```sh
   pi install ~/.local/share/roger-wilco/package
   ```

6. Reload existing Pi sessions with `/reload`.

The host file contains the coordinator URL, host ID, credential, namespace, and inbox path.
The coordinator file maps each host ID to its credential.
The namespace must match on both sides.
The daemon and Pi extension must use the same local inbox.

## Inspect

Use these commands to inspect service state:

```sh
systemctl --user status roger-wilco-coordinator roger-wilco-host
journalctl --user -u roger-wilco-host -n 40
curl https://COORDINATOR/health
```

`GET /health` does not require credentials.
The control API and SSE stream require a configured bearer credential.
The coordinator has no MCP endpoint.
Pi tools derive their session address from the current session.
The protocol document lists HTTP routes.

## Cutover

Use one operator for the cutover.
Keep configuration backups private because they contain credentials.

1. Record old pending messages, notification state, cursors, and active claims.
2. Pause new sends through the old coordinator.
3. Drain old delivery or record each unresolved message for reconciliation.
4. Keep the old databases and configuration.
5. Provision separate Roger-Wilco credentials, configuration, and databases.
6. Verify isolated directed messages and Pi wake-ups across two hosts.
7. Enable Roger-Wilco for the selected sessions.
8. Verify directed messages, session isolation, and busy follow-ups.
9. Disable the old agent-message destinations and consumers.
10. Record claim handover.
11. Remove only the agent-message branch from pi-webtasks.
12. Verify research routes and tools.

Do not copy old claims into the new database.
Release old claims, then acquire new claims under the intended session addresses.
Record unresolved claims instead of silently assigning them to new sessions.
Old message IDs and Roger-Wilco IDs remain separate.

The old receiver can also serve research callbacks.
When you remove the agent-message route, keep that service and its research credentials.
The [research split inventory](research-split.md) lists the source boundaries.

## Rollback

1. Disable the Roger-Wilco extension for the selected sessions.
2. Stop the new host daemons.
3. Stop the new coordinator.
4. Preserve both new databases for reconciliation.
5. Restore the old source, configuration, credentials, and destinations.
6. Start the old coordinator and receivers.
7. Reload the selected Pi sessions.
8. Verify one old directed message.
9. Reconcile messages and claims recorded during the cutover.

Run only one wake-up consumer for each selected session.
Do not reset a new cursor or delete a pending inbox during rollback.
The coordinator refuses automatic migration of an existing session to another host.

The historical `test/installation/rollback.mjs` rehearsal uses the old bus and its MCP SDK.
It loads that SDK from `AGENT_BUS_PACKAGE_DIR`, or the sibling `../agent-bus` checkout.
Those dependencies are not part of Roger-Wilco.

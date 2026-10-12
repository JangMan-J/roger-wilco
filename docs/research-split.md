# Research split: remove the pi-webtasks agent-bus branch

## Completed cutover — 2026-10-12

The removal is complete in the maintained checkout and both installed checkouts.
The research receivers remain active on both hosts.
Signed research replay returns 200 without changing stored events.
The retired agent route returns 404.
An agent-message event on the research route returns 400.
The [verification ledger](validation.md) records the final tests and private rollback evidence.

The remaining sections preserve the pre-cutover inventory and removal procedure.
Their working-tree descriptions and line numbers refer to the source baseline, not the current installation.

## Original procedure

Apply this only after Roger-Wilco cutover has succeeded and the old agent-message path is no longer the wake-up consumer. Do not apply it during extraction. Do not edit `reference/pi-webtasks`. Do not revert `7239e524daeb0ce6f126f538ba09d0010c85ed72`.

That commit is `Add advanced research tools and signed agent-bus wake-ups`. It adds `webtasks_submit`, `src/advanced-task.ts`, `src/search-status.ts`, `ResearchEffort` `"custom"`, and the native codemode tests in the same diff as the bus branch. A revert deletes research.

## Authoritative source

Direct inspection of `7239e524daeb0ce6f126f538ba09d0010c85ed72` is authoritative. The graph project `home-jangmanj-.pi-collection-pi-webtasks` at generation `2026-10-12T15:51:31Z` is stale: it reports `src/index.ts` `metadata_changed` and `tests/agent-bus.test.ts` untracked. `git ls-tree` on that commit tracks both `src/agent-bus.ts` and `tests/agent-bus.test.ts`. Do not use the graph as the file list.

| Tree | Role | HEAD | Working tree |
|---|---|---|---|
| `reference/pi-webtasks` | Extraction input. Do not edit. | `7239e524daeb0ce6f126f538ba09d0010c85ed72` | clean |
| `~/Projects/pi-webtasks` | Maintained checkout. Live receiver `ExecStart`. `~/.pi/collection/pi-webtasks` symlink target. | same | clean; only untracked `.pi/group-status-probe*` and `bench/__pycache__` |
| `~/.pi/agent/git/github.com/JangMan-J/pi-webtasks` | Package Pi loads from `~/.pi/agent/settings.json` (`https://github.com/JangMan-J/pi-webtasks.git`) | same | clean; `diff -rq` against the reference, excluding `.git` and `node_modules`, is empty |

`src/index.ts` and `tests/agent-bus.test.ts` have the same SHA-256 in all three trees (`4a83a16f…ace2`, `85e0fe5a…e5a6`).

The patch must land in both live clones. Patching only `~/Projects/pi-webtasks` updates the receiver and the collection symlink, and leaves Pi registering `agent_bus_listen`. Patching only the installed git checkout updates Pi, and leaves the running receiver on the Projects tree. Neither clone is the reference worktree.

## What stays

Research routes, credentials, storage, and tests stay. In particular:

- Tools `web_search`, `interactive_search`, `deep_search`, `webtasks_submit`, and `parallelProvider`.
- `src/advanced-task.ts`, `src/research.ts`, `src/search-status.ts`, `src/outbound.ts`, `src/ops.ts`, `src/webhook.ts`.
- `ResourceKind` members `"task" | "findall" | "monitor"`, and `ResearchEffort` including `"custom"`.
- Research inbox `~/.pi/agent/webtasks/webtasks.sqlite`. Read-only check: tables present; registrations and events are kind `task` only (6 and 6). No `agent-bus` rows.
- `PARALLEL_WEBHOOK_KEY`, `WEBTASKS_DIR`, `WEBTASKS_WEBHOOK_URL`, `WEBTASKS_WEBHOOK_SCHEME`, `WEBTASKS_WEBHOOK_PORT`, `WEBTASKS_WEBHOOK_PATH`.
- Default research path `/webtasks/webhook`.
- User unit `~/.config/systemd/user/agent-bus-receiver.service`. It is active and is the research listener: `ExecStart` runs `~/Projects/pi-webtasks/src/webhook-receiver.ts`, `WEBTASKS_DIR=~/.pi/agent/webtasks`, port `8787`. Tailscale serve publishes `https://decent-spider.fish-munsell.ts.net:8444` to `127.0.0.1:8787` for every path, not only `/agent-bus/webhook`. Disabling the unit removes the research callback listener.
- `~/.pi/agent/settings.json` package entry for pi-webtasks.
- `package.json` scripts and `"pi.extensions": ["./src/index.ts"]`. No script names the bus test. `npm test` is `node --test tests/*.test.ts`.
- Research tests: `tests/webhook-receiver.test.ts`, `tests/webhook-worker.test.ts`, `tests/webhook-consumer.test.ts`, `tests/webhook-lifecycle.test.ts`, `tests/webhook-inbox.test.ts`, `tests/three-tools.test.ts`, `tests/advanced-task.test.ts`, `tests/native-codemode.test.ts`, `tests/search-status.test.ts`, `tests/outbound.test.ts`, and the rest of `tests/` except the file deleted below. `tests/helpers/test-host.ts` is generic. Do not edit it.

`~/.pi/agent/mcp.json` and `~/.config/agent-bus/compose.yaml` are the old coordinator, not the pi-webtasks branch. This patch does not delete them.

## Delete

| Path | Lines | Why |
|---|---|---|
| `src/agent-bus.ts` | 1–38 | Sole definition of `AGENT_BUS_EVENT`, `AGENT_BUS_PATH`, `AgentBusEvent`, `busResource`, `parseBusEvent`. Importers are only the ranges below and the deleted test. |
| `tests/agent-bus.test.ts` | 1–189 | Three tests, all bus-only: signed wake-up and resume, key/route isolation, native idle/busy deferral. No research assertion lives only here. |

## Edit

Line numbers are the current files at `7239e52`.

### `src/index.ts`

| Lines | Change |
|---|---|
| 15 | Delete `import { busResource } from "./agent-bus.ts";`. Keep the `advanced-task.ts` import on line 14. |
| 64–65 | Delete `busInbox` and `busConsumer` from `Runtime`. |
| 87 | Delete `await outgoing.busConsumer?.stop();`. Keep session, collector, consumer, then recovery. |
| 93 | Delete `await outgoing.busInbox?.close().catch(() => {});`. Keep `outgoing.inbox?.close()`. |
| 116–122 | Delete the `if (d.bus)` block, including its `return`. Research notification starts at the `researchForRun` lookup. |
| 167–170 | Stop merging a second consumer. Replace with the research-only loop below. |
| 227–233 | Delete bus inbox construction, `init`, `WebhookConsumer` start, and the `"agent-bus events never fetch research"` client. Keep `runtime.consumer.start(recover)` on line 226 and the collector block that follows. |
| 249–270 | Delete `listenParameters`, `listenTool`, and `pi.registerTool(listenTool)`. Line 272 `web_search` stays. |

`join` stays. `callContext` still uses it. `ToolDefinition` stays for `webtasks_submit`.

Recovery replacement:

```ts
        const research = await owner.consumer!.recover({ retryFailed });
        for (const delivery of research.deliveries) await deliverWebhookNotification(ctx, delivery, owner);
        processed += research.processed;
        delivered += research.deliveries.length;
```

### `src/webhook-consumer.ts`

| Lines | Change |
|---|---|
| 19 | Delete the `agent-bus.ts` import. |
| 27 | Delete `bus?: AgentBusEvent["data"];` from `Delivery`. |
| 166–173 | Delete the `entry.kind === "agent-bus"` branch. The next statement is the existing `entry.kind === "task"` duplicate-notice guard. Leave that guard. |

### `src/webhook-worker.ts`

| Lines | Change |
|---|---|
| 17 | Delete the `agent-bus.ts` import. |
| 48 | Remove `\| "notify"` from `ConsumerAction`. Nothing else assigns `"notify"`. |
| 109–116 | Delete the `type === AGENT_BUS_EVENT` return. `task_run.status` handling on the next line stays. |
| 306–309 | Delete the `norm.action === "notify"` claim. `no_change` stays immediately after the `fetch_failed` check. |

Do not change `idField` or `fetchOnce`. Today a non-task, non-findall kind falls through to `monitor_id` / `monitorEventGroupOnce`. Removing the kind and the notify action closes that hole. Leaving either one would let a leftover bus row fetch as a monitor.

### `src/webhook-inbox.ts`

Line 60 only:

```ts
export type ResourceKind = "task" | "findall" | "monitor";
```

Do not touch the `ResearchEffort` / `"custom"` lines from the same commit (the import and `EFFORTS` set). No schema migration. The research database has no `agent-bus` rows. The separate bus database is not opened after this patch.

### `src/webhook-receiver.ts`

Remove the second route. Restore a research-only receiver. Keep one literal reject so a research-signed `agent_bus.message` cannot enter the research inbox. That is current research-path behavior (`webhook-receiver.ts` 249). Do not import `agent-bus.ts` to do it.

| Lines | Change |
|---|---|
| 11 | Delete the `agent-bus.ts` import. Keep `join`: `defaultResultsDir` still uses it (270–271). |
| 38–41 | `secret` is required again (`string \| string[]`). Delete `agentBusSecret` and `agentBusInbox`. |
| 51–52 | `const secrets = usableSecrets(options.secret);`. Delete `busSecrets` and the either-secret check. |
| 66 | Path check loses `\|\| path === AGENT_BUS_PATH`. |
| 82–93 | Delete the pathname bus branch. One `handleRequest` call, research inbox, research verifier, configured path, no `bus` flag. |
| 206, 210 | Delete `bus` from the `handleRequest` argument type and destructure. |
| 249–253 | Replace the bus ternary with the literal reject below. |
| 256 | Store `webhook_id: header(req, "webhook-id")`. Drop the `bus:` prefix. Research tests already expect the raw id. |
| 310, 317–319, 336 | Delete `agentBusSecret`. Require a trimmed `PARALLEL_WEBHOOK_KEY`. |
| 340, 351–365, 373, 376 | Delete bus inbox, bus worker, second stdout URL, and bus close. `onAccepted` kicks only the research worker. Always print `receiver.url`. |

Research-path reject, immediately after `isBasicEvent`:

```ts
  if (parsed.type === "agent_bus.message") {
    reply(res, 400, "bad request");
    return;
  }
```

CLI secret check:

```ts
  const secret = env[WEBHOOK_SECRET_ENV]?.trim() || "";
  if (!secret) throw new Error(`${WEBHOOK_SECRET_ENV} is required`);
```

Trim stays so today's research key behavior does not change. `WEBTASKS_AGENT_BUS_KEY` alone must not start the process. `tests/webhook-receiver.test.ts` 283 already accepts stderr matching `/PARALLEL_WEBHOOK_KEY|WEBTASKS_DIR/`. No other test calls `startWebhookReceiver` without `secret`. The three call sites that omit `secret` are in the deleted test.

`/agent-bus/webhook` then 404s through the ordinary path check, because the unit does not set `WEBTASKS_WEBHOOK_PATH`. Do not reintroduce a reserved-path constant.

### Docs

| File | Lines | Change |
|---|---|---|
| `README.md` | 179–186 | Delete the `## Agent-bus wake-ups` section, including the blank line before `## Limits`. |
| `docs/webhook-integration.md` | 146–186 | Delete `## Agent-bus messages` through the blank line before `## Verification`. |

No other doc under `docs/` names the bus. `docs/tool-reference.md` has no `agent_bus_listen` entry.

`package.json` needs no edit.

## Associated configuration

Not source. Do this in the same cutover window as the patch, not before cutover succeeds. Do not copy secret values into the patch or this file.

1. `~/.config/agent-bus/webhooks/targets.json`: two destinations, agents `decent-spider-pi` and `jangs-jail-pi`, both posting to port `8444` path `/agent-bus/webhook`. Remove those destinations so the old coordinator stops posting. Restart the `agent-bus` container only as the cutover owner's disable step. This file is not in pi-webtasks.
2. `~/.config/agent-bus/receiver.env`: delete the `WEBTASKS_AGENT_BUS_KEY` assignment only. Keep `PARALLEL_WEBHOOK_KEY`. A cache copy exists at `~/.cache/agent-bus-jail-setup/receiver.env`; it is not the unit's `EnvironmentFile`. Do not treat it as live config.
3. Restart `agent-bus-receiver.service` after the Projects tree patch so the running process drops the bus route. Do not disable or remove the unit.
4. Reload open Pi sessions after the installed git checkout patch. Settings keep the git package URL.

`receiver.env` currently has both keys, so the process accepts research callbacks on `/webtasks/webhook` and bus callbacks on `/agent-bus/webhook`. Removing only the bus key, on today's code, already 404s the bus route and leaves research up. The source patch is still required so restoring the key cannot revive the route.

Local bus store `~/.pi/agent/webtasks/agent-bus/webtasks.sqlite`: one `agent-bus` registration, three `agent-bus` events, all `handled=1` and `notified=1`. Leave the directory. Local `notified` is not a coordinator receipt. Do not delete it in this patch.

## Regression test

Add `tests/research-retained.test.ts`. It fails on `7239e52` and passes after the patch.

Today `startWebhookReceiver({ secret: researchSecret, agentBusSecret: busSecret })` accepts a signed `agent_bus.message` on `/agent-bus/webhook` with status 200, and `agent_bus_listen` is registered. After the patch the extra option is ignored, that path is 404, the event is not stored, and the tool is absent. A normal research callback still returns 200.

```ts
import assert from "node:assert/strict";
import test from "node:test";
import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { WebhookInbox } from "../src/webhook-inbox.ts";
import { startWebhookReceiver } from "../src/webhook-receiver.ts";
import { setup } from "./helpers/test-host.ts";

const researchSecret = "whsec_dGVzdHNlY3JldA==";
const busSecret = `whsec_${Buffer.alloc(32, 7).toString("base64")}`;
const busEvent = {
  type: "agent_bus.message",
  data: { agent: "alice", message: { id: 1, ts: 123, sender: "bob", recipient: "alice", body: "no", data: null, visibility: "private" } },
};
const researchEvent = { type: "task_run.status", data: { run_id: "trun_1", status: "completed" } };

function sign(secret: string, id: string, body: string): Record<string, string> {
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = createHmac("sha256", Buffer.from(secret.slice(6), "base64"))
    .update(`${id}.${timestamp}.${body}`).digest("base64");
  return { "webhook-id": id, "webhook-timestamp": timestamp, "webhook-signature": `v1,${signature}` };
}

async function post(url: string, id: string, value: unknown, secret: string): Promise<number> {
  const body = JSON.stringify(value);
  const response = await fetch(url, { method: "POST", body, headers: sign(secret, id, body) });
  await response.text();
  return response.status;
}

test("agent-bus route and tool are gone; research callback still stores", async t => {
  const root = fileURLToPath(new URL("..", import.meta.url));
  for (const rel of ["src/index.ts", "src/webhook-receiver.ts", "src/webhook-worker.ts", "src/webhook-consumer.ts"]) {
    assert.equal(readFileSync(join(root, rel), "utf8").includes('from "./agent-bus.ts"'), false, rel);
  }
  const h = await setup(t, () => { throw new Error("research-retained must not fetch"); });
  const owner = await h.instance("research-owner");
  await owner.start();
  assert.equal(owner.tools.has("agent_bus_listen"), false);
  for (const name of ["web_search", "interactive_search", "deep_search", "webtasks_submit"]) {
    assert.equal(owner.tools.has(name), true, name);
  }

  const inbox = new WebhookInbox(join(h.dir, "inbox"));
  const receiver = await startWebhookReceiver({
    inbox, secret: researchSecret, agentBusSecret: busSecret, port: 0,
  } as Parameters<typeof startWebhookReceiver>[0]);
  try {
    const busUrl = new URL("/agent-bus/webhook", receiver.url).href;
    assert.equal(await post(busUrl, "leftover", busEvent, busSecret), 404);
    assert.equal(await post(receiver.url, "typed-as-bus", busEvent, researchSecret), 400);
    assert.equal(await post(receiver.url, "research", researchEvent, researchSecret), 200);
    const rows = await inbox.listAll();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.webhook_id, "research");
    assert.equal(JSON.stringify(rows).includes("agent_bus.message"), false);
  } finally {
    await receiver.close();
    await inbox.close();
    await owner.stop();
  }
});
```

The `as` cast is only so the test compiles after `agentBusSecret` is removed from the options type. On today's code the cast is unnecessary and the bus POST returns 200, so the 404 assertion fails. That is the point.

Do not put this assertion in `tests/agent-bus.test.ts`. That file is deleted.

## Validation

The removal owner runs this after the patch, in each edited clone. This discovery did not start services or run the suite.

```sh
npm run typecheck
npm test
```

`npm test` must not try to load `tests/agent-bus.test.ts`. Existing receiver, worker, consumer, lifecycle, three-tools, advanced-task, and native-codemode tests must still pass. Typecheck must fail if any remaining file imports `./agent-bus.ts` or names `agent_bus_listen`.

After the operator restart and reload, not as part of authoring this patch:

- `POST` to `/agent-bus/webhook` on `127.0.0.1:8787` is 404.
- `POST` of `agent_bus.message` to `/webtasks/webhook` with the research key is 400 and adds no research inbox row.
- A signed `task_run.status` on `/webtasks/webhook` is still 200.
- A reloaded session has no `agent_bus_listen` and still has the four research tools.
- `~/.pi/agent/webtasks/webtasks.sqlite` is unmodified by the patch.

## Order

1. Cutover already succeeded. Old sends are paused. Roger-Wilco is the wake-up path.
2. Remove bus destinations, then the bus key. Keep `PARALLEL_WEBHOOK_KEY` and the unit.
3. Apply this patch to both live clones. Do not touch `reference/pi-webtasks`.
4. Restart the receiver unit. Reload Pi. Run validation.
5. Leave both sqlite directories in place for reconciliation.

## Tradeoffs

- The reserved-path guard `path === "/agent-bus/webhook"` goes away with the second route. The live unit does not set `WEBTASKS_WEBHOOK_PATH`, so that URL 404s. An operator who later points the research path at `/agent-bus/webhook` would make the research receiver answer there. Do not do that.
- `WEBTASKS_AGENT_BUS_KEY` becomes an ignored environment variable. The unit file can keep its name. Renaming the unit is a later operations change and is not required to preserve research.
- `"notify"` is removed from `ConsumerAction` because only the bus branch produced it. Research actions stay `fetch`, `failure`, `action_required`, `no_change`, and `unrecognized`.
- Rejecting `agent_bus.message` on the research path is one string compare, not a parser port. It does not validate bus message fields. Invalid bus shapes also 400, which is stricter than storing them as unrecognized, and matches the current research route.

## What this does not promise

- It does not implement Roger-Wilco, migrate messages, or prove coordinator drain.
- It does not retire the agent-bus container, MCP entry, or `AGENT_BUS_WEBHOOKS`.
- It does not delete `~/.pi/agent/webtasks/webtasks.sqlite` or `~/.pi/agent/webtasks/agent-bus/`.
- It does not revoke `PARALLEL_WEBHOOK_KEY` or stop `agent-bus-receiver.service`.
- It does not change research delivery, Pi `sendMessage` acknowledgement, or exactly-once claims.
- It does not update a running process or an unpatched clone. Byte-identical trees today will diverge if only one is patched.
- It does not claim the reference suites were rerun.

# Recovery correctness verification

## Final status — 2026-10-12

**Both remaining abort findings are resolved in the focused verification.** Custom-only aborted follow-ups recover without duplicate submission; recovery wakes require a pending submitted ID and a fresh lease check. The losing runtime does not wake, and a second explicit abort retains delivery without an automatic wake loop.

The original review below is historical evidence. Its unresolved verdict and next-work statements are superseded by the dated resolution at the end. Prior lease/stream results remain applicable; unchanged host source was hash-checked, not retested here. Full-suite and second-host validation remain the parent's separate work.

## Verdict

Dead-lease restart and silent-stream recovery are fixed in the exercised paths. The delayed-receipt submission race is fixed on the ordinary delivery path. **Busy-abort recovery remains broken, and its new control wake bypasses lease validation.** The passing native abort test does not exercise a genuinely aborted provider response.

Scope: direct inspection of `src/pi/index.ts`, `src/host/index.ts`, `test/pi/{native,config,crash}.test.mjs`, `test/pi/crash-child.mjs`, and the host timeout tests, against `docs/contract.md` and `EXTRACTION-PLAN.md`. The stale graph and worker conclusions were not used as behavioral evidence. Only this report was added; probes used temporary fixtures and in-memory test transformations, not source edits.

Runtime: Node v26.11.1; native Pi 1.1.0 loaded from the default `~/pi-main/packages/coding-agent/dist/index.js`, with sibling AI package. `PI_PACKAGE_DIR` was unset. Native implementation evidence below was checked in the loaded `dist` files as well as source. No provider calls were made. The parent's full suite is outside this review's validation claim.

## Remaining defect 1 — custom follow-ups do not satisfy the abort-wake guard

**Location:** `src/pi/index.ts:152–158`, especially `!ctx.hasPendingMessages()` at line 153; native fixture `test/pi/native.test.mjs:63–69,112–124`.

Pi's extension context implements `hasPendingMessages()` as `pendingMessageCount > 0`. That count contains only `_steeringMessages` and `_followUpMessages`, the user-input bookkeeping arrays. `sendCustomMessage(..., {deliverAs:'followUp'})` instead calls `agent.followUp(appMessage)` directly. Thus a Roger-Wilco custom message can remain in the agent queue while `ctx.hasPendingMessages()` returns false.

Evidence in the selected Pi package, paths relative to its root:

- `dist/core/agent-session.js:1782–1808`: custom follow-up ingestion and deferred settled actions.
- `dist/core/agent-session.js:1892–1894,2764`: pending count and extension-context binding.
- `dist/core/agent-session.js:684–705`: settled notification and deferred actions.
- Sibling `../agent/dist/agent-loop.js:143–153,192–198`: an aborted assistant response returns before follow-up draining.

### Actual failure

I ran the existing native fixture in memory with only its import paths adjusted for evaluation, diagnostics added, and its fake stream made signal-aware: after cancellation, both its event reason and assistant `stopReason` became `aborted` rather than `stop`. All delivery, persistence, extension lifecycle, and SQLite code remained real.

After abort and 700 ms (more than two poll intervals):

```text
custom-only {"requests":4,"queued":["fleet:msg:4"],"pendingMessageCount":0,
             "receipt":null,
             "entries":[{"type":"roger-wilco","id":"fleet:msg:1"},
                        {"type":"roger-wilco","id":"fleet:msg:2"}]}
```

A second run used the canonical stream error event `{type:'error', reason:'aborted', error:assistant}` rather than a done event with an aborted stop reason; it reproduced `requests:4`, queued `fleet:msg:4`, `pendingMessageCount:0`, and `receipt:null`.

The unchanged eventual-delivery assertion then failed with `Timed out waiting for native Pi delivery`. No control wake was written; the delivery remained queued, absent from JSONL, and unacked. Subsequent polling encounters `submitted.has(id)` and cannot repair it.

The checked-in test passes because its stream ignores the abort signal and `release()` emits a normal `stop`. The low-level agent loop then drains the follow-up before settling. Its fifth request is that ordinary drain, not proof that `roger-wilco-wake` recovered an aborted queue. The assertion that the control wake and original follow-up share one turn does not establish that a control wake occurred.

### Distinguishing regression

Keep the native fixture, but accept the stream options and make the finish function truthful:

```js
session.agent.streamFunction = (_model, context, options) => {
  // Existing request recording and held-stream fixture.
  const finish = () => {
    const stopReason = options?.signal?.aborted ? 'aborted' : 'stop';
    const message = { /* existing offline assistant fields */, stopReason };
    stream.push(stopReason === 'aborted'
      ? { type: 'error', reason: 'aborted', error: message }
      : { type: 'done', reason: 'stop', message });
    stream.end();
  };
  // Existing hold/release behavior.
};
```

Queue **only the custom delivery**, verify it is absent on disk, abort the held turn, and finish the stream. Assert recovery eventually produces exactly one delivery record and its SQLite receipt, empties that queued ID, and remains stable across several polls. Observe/count `roger-wilco-wake` entries explicitly; do not infer a wake from total request count. With an ID-free wake, the wake prompt can itself consume a model turn before the original follow-up is processed.

Correction needed: recovery eligibility must cover the submitted custom IDs, not rely on the user-input queue count. Retain per-ID submission tracking; clearing it and resubmitting a still-queued custom message would introduce duplicates. Keep a bounded recovery guard, and test multiple settled notifications and an aborted recovery turn so fixing eligibility does not introduce a wake loop. No contract amendment is needed to require the already-specified pending delivery recovery.

## Remaining defect 2 — abort recovery can wake the losing runtime

**Location:** `src/pi/index.ts:152–158,299–300`. Ordinary submission uses `ownsLease()` at lines 143–144 and 192; `resumeAfterAbort()` only checks the cached `bound` boolean.

There is a concrete interval between expiry/takeover and the next 1-second renewal in which `bound` remains true. A settled abort in that interval can issue the ID-free wake even though another runtime owns the inbox. That wake runs the old runtime's queued custom deliveries. The SQLite ACK guard prevents the loser from acknowledging them, but does not prevent Pi file writes or model turns.

### Actual failure

I repeated the signal-aware native abort probe, adding one queued user follow-up so the current queue guard became true. Immediately before abort I expired the lease in SQLite and bound the same session through a second real inbox as `takeover-owner`. This is an expiry/takeover race fixture, not artificial crash-resume repair.

```text
with-user-takeover {"requests":7,"queued":[],"pendingMessageCount":0,
                    "receipt":null,"owner":"takeover-owner",
                    "entries":[{"type":"roger-wilco","id":"fleet:msg:1"},
                               {"type":"roger-wilco","id":"fleet:msg:2"},
                               {"type":"roger-wilco-wake"},
                               {"type":"roger-wilco","id":"fleet:msg:4"}]}
```

The old native session performed three more requests and wrote the original delivery after the new owner had bound. Its ACK remained null. The counterpart without takeover also made seven total requests, wrote one wake and one delivery, and committed a receipt. Neither branch showed an unbounded wake loop during the observation window; the lease bypass is independent of recursion.

### Distinguishing regression

Use the same native fixture and signal-aware abort stream. Queue the custom delivery plus a user follow-up. Before abort:

```js
const db = new DatabaseSync(inboxPath);
try {
  db.prepare('UPDATE leases SET expires_at = ? WHERE session_key = ?')
    .run(Date.now() - 1, `fleet:${a}`);
} finally { db.close(); }
assert.equal((await external.bind(
  { namespace: 'fleet', piSessionId: a }, 'takeover-owner', Date.now()
)).ok, true);
// Abort and release the held response immediately, before the renewal tick.
```

Require **no new control wake, no recovery model request, and no delivery record on the loser**. Confirm the new owner still sees the pending inbox row. Today those loser assertions fail. This does not ask the extension to revoke an already-running Pi turn; it asks it not to initiate a new recovery turn after ownership has moved.

Correction needed: route recovery wake authorization through a fresh lease check, with session/stop checks after asynchronous work, just as ordinary submission now does. Preserve settled-event ordering when making this asynchronous: the native SDK defers `triggerTurn` actions sent during its settled notification. Do not replace the guarded wake with duplicate delivery submission. No broader takeover architecture or exactly-once guarantee is proposed here.

## Verified fixes and boundaries

### Dead lease: fixed in the native restart exercised here

`src/pi/index.ts:257–277` now retains the runtime on initial `owner_conflict` and retries binding every second. `173–185` also rebinds after a live renewal failure. Stop clears both initial-bind retries and established runtime timers (`195–205,229–237`). Host ownership remains transactional and refuses takeover while the earlier lease is unexpired (`src/host/index.ts:79–94`).

The current crash parent resumes the actual extension immediately after SIGKILL, keeps the killed owner's row, and waits for the production lease to expire (`test/pi/crash.test.mjs:78–120`). There is no pre-resume test `bind`, `release`, clock advance, or lease-row update. The later `verified-reopen` bind occurs only after resumed delivery and shutdown.

Locally rerun result:

```text
✔ SIGKILL before-write: actual Pi extension resumes its persistent inbox (31570 ms)
```

It reclaimed the dead lease, produced one delivery record, ACKed it, and made one recovery turn. The new native test's forced live expiry also passed without a test bind repairing the lease. The other three crash stages were inspected but not rerun by this reviewer.

### Receipt-await takeover: fixed on ordinary delivery and ACK paths

Both `pass()` and `deliver()` now renew/check ownership after the asynchronous receipt scan and before submission (`src/pi/index.ts:112–121,133–145,188–193`). The checked-in delayed-read regression passed: the old runtime sent nothing and the new owner's message remained pending (`test/pi/config.test.mjs:60–113`). It uses real SQLite ownership with a filesystem read gate and a send recorder, rather than two native Pi opens; it directly distinguishes the earlier missing check.

I also gated `handle.sync()` after a matching record was found in a real JSONL. Taking the lease during that wait yielded:

```text
PROOF_EDGE takeover-during-sync {"receipt":null,"sent":0,"rejected":true}
PROOF_EDGE shutdown-during-sync {"receipt":null,"sent":0,"rejected":false}
```

The first result comes from the transactional lease check in `HostInbox.acknowledgePi` (`src/host/index.ts:119–131`). In the shutdown case, `stop()` marks the delivery stopped before awaiting its running pass, so the completed receipt is not ACKed (`src/pi/index.ts:104–106,195–204`). These checks do not cure the separate abort-wake bypass above, nor promise revocation of a message already submitted to Pi.

### Silent stream: fixed, including deadline resets

`src/host/index.ts:278–311` creates a per-connection controller, bounds response headers to 10 seconds, arms a 45-second byte-idle deadline, and clears it during teardown. `frames()` calls `onBytes()` for each nonempty read before parsing, including comments (`184–212`). Thus keepalives reset the idle deadline even though the parser does not emit them as messages. Expiry aborts the fetch/body and reconnects through the existing backoff.

Locally rerun results:

```text
✔ silent open stream reconnects and commits the frame offered only on the next connection (45307 ms)
✔ stream fetch without response headers times out and reconnects (10283 ms)
```

A separate real HTTP probe sent an initial comment, another comment at 30 seconds, then a frame at 50 seconds. It kept the original connection past the initial 45-second deadline and committed the frame. Silence after that frame caused reconnect at approximately 95 seconds:

```text
RESET_PROBE frame persisted on original connection {"elapsedMs":50050,"connections":1,"receipts":1}
RESET_PROBE silence deadline after frame {"elapsedMs":95300,"connections":2,"receipts":1}
```

This proves both comment and frame reset behavior. The deadline measures received bytes, not successful semantic message delivery; that is consistent with its role here. All these daemon probes stopped and awaited their run cleanly.

### Persistent receipt and crash-test limits

The receipt still requires a complete matching JSONL record in the correctly identified session, then `handle.sync()`, then the lease-guarded inbox ACK (`src/pi/index.ts:48–65,87–109`). The complete-line/malformed-prefix and other-session tests passed locally.

The `after-write-before-fsync` child signals its checkpoint **inside the wrapped sync method** (`test/pi/crash-child.mjs:44–54`). Removing that invocation would prevent the checkpoint and cause the parent to time out; the current test therefore distinguishes a missing sync invocation. It does not simulate power loss or establish survival of unsynced dirty pages: SIGKILL leaves readable kernel page-cache contents available to the resumed process. Those are validation limits, not evidence that the production code omits fsync.

## Validation inventory and next work

The targeted run had nine passing tests, zero failures/cancellations/skips: four Pi native/config tests, three selected crash/receipt tests, and two host timeout tests. Its three process exit statuses were all zero. The signal-aware custom-only variant failed the delivery assertion; the diagnostic variants reproduced both remaining defects without changing implementation files. The receipt-edge and stream-reset probes passed their explicit assertions.

Next implementation work is confined to abort recovery eligibility and recovery-wake ownership, plus native regressions that actually return an aborted assistant response. Recheck bounded wake behavior and duplicate-ID counts with those regressions before treating busy-abort recovery as accepted. Full-suite results alone will not distinguish the current normal-stop fixture from a working abort fix.


## Resolution — 2026-10-12, bounded final abort verification

### Findings closed

**Finding 1: resolved.** `src/pi/index.ts:152–172` no longer gates custom-message recovery on `ctx.hasPendingMessages()`. It intersects the real inbox's pending IDs with `submitted`, keeps the original ID in flight, and sends an ID-free control wake. The `agent_settled` handler returns the recovery promise (`312–314`), keeping asynchronous authorization within Pi's awaited settled notification and its deferred-turn behavior.

The current native stream (`test/pi/native.test.mjs:63–76`) uses the provider abort signal to set assistant `stopReason:'aborted'` and emits the canonical `{type:'error',reason:'aborted',error:message}` event. The custom-only abort case (`111–131`) does not add user follow-ups to make a queue-count guard pass. It verifies one delivery record, its SQLite receipt, one explicitly counted `roger-wilco-wake`, model context containing the delivery, and no remaining queued copy. This now distinguishes actual recovery from the former normal-stop drain.

The interrupted-recovery case (`141–178`) holds the recovery turn, emits repeated settled notifications, then explicitly aborts that turn too. It checks that the wake count stays at two total (one from each recovery episode), the original ID remains queued exactly once and absent on disk, and a later user turn writes that ID exactly once and commits its receipt. Retention after the second explicit abort is the intended bounded behavior, not a defect or a model-completion guarantee. The observed later receipt establishes that this sequence did not lose the delivery.

**Finding 2: resolved.** After obtaining pending IDs, recovery calls `ownsLease()` before sending the control wake (`src/pi/index.ts:159–167`). That helper performs a real renewal and checks session/stop state again (`112–121`). The native takeover case (`test/pi/native.test.mjs:207–241`) now ends the losing provider turn as aborted. Its assertions require no additional model request, no added control wake, no delivery record on the loser, and the delivery still pending for `takeover-owner`.

`test/pi/config.test.mjs:114–158` additionally keeps an old submitted ID whose proof is written to the takeover owner's file. It ACKs that ID through the real inbox, releases the takeover owner, rebinds the old runtime, and verifies no stale control wake or second ID submission. This checks pending-ID authorization independently of the old runtime's transcript or cached submitted set.

### Focused rerun

Run at `2026-10-12T19:34:23Z`, Node v26.11.1, `PI_PACKAGE_DIR` unset, using the same default native Pi package as the original review:

```sh
node --test test/pi/native.test.mjs test/pi/config.test.mjs
```

```text
✔ ~/ host configuration gives Pi and host daemon the same durable inbox
✔ a delayed JSONL read cannot submit after another runtime takes the lease
✔ a rebound runtime does not wake an ID acknowledged by the takeover owner
✔ Pi applies the host HTTPS and absolute/home-relative inbox requirements
✔ native Pi extension binds automatically, routes only its session, wakes idle and queues busy follow-up; resume does not resend
 tests 5; pass 5; fail 0; cancelled 0; skipped 0
 duration_ms 6266.388236
```

### Distinguishing mutation probes

I also ran four bounded helper probes with real SQLite inboxes and a send recorder. Mutated extension modules were transpiled and imported only in memory; no implementation or test file was changed. These are focused mutation checks, not a claim that the entire native suite was run against mutants.

| Isolated change | Current control wakes | Mutated control wakes | What distinguishes it |
|---|---:|---:|---|
| Restore `!ctx.hasPendingMessages()` guard | 1 | 0 | A custom-only submission must recover although user-input pending count is false |
| Remove recovery's fresh `ownsLease()` check | 0 | 1 | Capture a valid pending snapshot, gate its return, transfer the lease, then release the gate |
| Remove pending/submitted ID intersection | 0 | 1 | Takeover owner ACKs the ID; old runtime rebinds but must not wake its stale submitted ID |
| Remove `recoveringAbort` entry guard | 1 | 3 | Three concurrent recovery notifications must not stack control wakes |

All current expectations passed, and each corresponding mutation produced the opposite result. The gated pending-snapshot probe specifically isolates the fresh lease check: the native loser case also benefits from `pending()`'s ownership restriction, so that case alone would not demonstrate the late check independently.

### Scope and retained evidence

No unresolved loss or unauthorized-wake defect was found in these two findings. The second-abort behavior deliberately waits for later input or restart rather than repeatedly overriding cancellation. This review does not claim model completion, exactly-once delivery across crashes, or validation on the other physical host.

Host source still hashes to `24b192fc7165e4d4e36e001666facc25dda69ae2034dd5b163bcef2107354b11`, matching the prior stream/lease review. Those results were reused; no long stream probe or crash suite was rerun. Reviewed Pi source hashes to `afe714a08946eed32dbdd49e5bae868860dd40ad216efba20fb6b7e7c1b0e958`. Only this report was updated, preserving the original failure evidence above.

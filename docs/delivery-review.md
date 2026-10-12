# Delivery correctness review

Scope: current `src/server/store.ts`, `src/server/delivery/index.ts`, `src/server/control/index.ts`, `src/host/index.ts`, `src/pi/index.ts`, and the crash/native/host/delivery tests, checked against `docs/contract.md` and `EXTRACTION-PLAN.md`. Graph generation `2026-10-12T18:33:32Z` matches store/control/delivery; host and Pi are `metadata_changed`, and `test/pi/crash.test.mjs`, `test/acceptance/native-delivery.test.ts`, and `src/bin` are unindexed. Line citations below are from the current files, not the index. No source was modified. A local inbox probe confirmed the lease results; the full suite was not rerun.

## 1. Crash restart inside the lease window never delivers, and the crash test skips that boundary

`LocalHostInbox.setLease` keeps a dead runtime's row until `expires_at`. A new `runtimeId` is `owner_conflict` while that row is unexpired. `renew` also refuses the same runtime at expiry (`expires_at <= now`), so the only reclaim is a later `bind` of that same id. The extension never does that.

```84:85:src/host/index.ts
      if (renew && (!row || row.runtime_id !== runtimeId || row.expires_at <= now)) return failure('owner_conflict', 'Lease no longer owned');
      if (!renew && row && row.expires_at > now && row.runtime_id !== runtimeId) return failure('owner_conflict', 'Session already active');
```

```224:225:src/pi/index.ts
        const result = await delivery.bind(session, runtimeId);
        if (!result.ok) { status(ctx, `Roger-Wilco: ${result.error}: ${result.message}`); await inbox.close(); return; }
```

SIGKILL does not run `release`. The next `session_start` uses a new `runtimeId` and, on conflict, closes its inbox and returns. It does not wait, and it does not retry after the 30s lease (`LEASE_MS` at `src/host/index.ts:11`). Pending rows stay in SQLite, but this process will not drain them. A second open after the lease expires works only because it is a new `session_start`. Sitting on the conflicted session does not.

Probe, same inbox, `now = 1_700_000_000_000`: first `bind` ok; `bind` of another id at `now+1000` is `owner_conflict`; `renew` of the owner at `now+30000` is `owner_conflict`; `bind` of that same owner at `now+30000` is ok. So the implementation can reclaim, and the extension does not call that path.

`test/pi/crash.test.mjs` knows this and removes the lease before starting the extension:

```63:68:test/pi/crash.test.mjs
  // The killed owner has a 30-second lease; move the explicit lease clock past expiry and release it.
  // No message or ACK is altered here: the resumed extension performs all Pi recovery itself.
  const inbox = createHostInbox(inboxPath);
  try {
    assert.equal((await inbox.bind(address, 'lease-recovery', Date.now() + 31_000)).ok, true);
    await inbox.release(address, 'lease-recovery');
```

That is not the production resume path. The contract requires a closed or killed Pi to deliver pending messages on resume, and duplicate activation to conflict or wait. Immediate conflict without a wait turns a dead owner into a permanent miss for that session lifetime. Messages are not deleted; wake-up is.

The same `renew` failure clears `bound` and never calls `bind` again (`src/pi/index.ts:145-149`). A live session whose event loop is paused, stopped, or blocked across the 30s lease stops draining even when no other runtime exists. `pending` then returns `[]` because `owns` requires `expires_at > Date.now()` (`src/host/index.ts:100-103`). Rows remain. Idle wake-up does not resume until another `session_start`.

Distinguishing test: SIGKILL a bound native session that already has one unacked inbox row. Immediately `session_start` the same session file with `Date.now()`, without writing `expires_at` or calling `bind`/`release` from the test. The extension must submit that id once it is absent from the JSONL, or wait out the dead lease and then submit. Today `bind` returns `owner_conflict`, status is set, and no `sendMessage` runs. A second case, no SIGKILL: bind, set `expires_at` to the past on the open database, let the real renew interval run, then `receive` a new frame. The live extension must deliver it. Today renew sets `bound = false` and the new frame stays pending.

## 2. Busy abort strands the follow-up; the in-flight set blocks the idle retry

`submit` records the id before `sendMessage` and skips it forever in this runtime unless the JSONL entry appears or `sendMessage` throws synchronously (`src/pi/index.ts:108-116`). `sendMessage` is void and Pi swallows asynchronous rejection, so a queued follow-up is not a known failure.

Pi's abort path does not drain that queue. `AgentSession.abort` sets `_agentRunAbortRequested` and aborts the run (`packages/coding-agent/src/core/agent-session.ts:2434-2442` in the selected Pi tree). The agent loop returns on `stopReason === "aborted"` before `getFollowUpMessages` ((`packages/agent/src/agent-loop.ts:245-255`). `_runAgentPrompt` then refuses `continue()` because the abort flag is set (`agent-session.ts:1834-1836` and `1890`). The custom message stays in the in-memory follow-up queue and is not on disk.

The next `pass` sees absence, then `submitted.has` returns without another `sendMessage`. Pi is idle, the inbox row is still unacked, and no model turn starts. A later user prompt can drain the stranded queue; a crash restarts with an empty `submitted` set and will resubmit. The live idle session does not. That breaks the busy-follow-up rule once the turn is cancelled, and it breaks idle wake-up for that id. A newer id in the same pass can still be submitted, so delivery order is not held either.

Distinguishing test: native session, hold the model turn, `receive` one frame, wait until `peekQueuedMessages()` contains that `deliveryId` and the JSONL does not. Call `session.abort()`, wait past two poll intervals while idle. The contract outcome is one new turn or one JSONL `custom_message` with that id, and one inbox ACK after the file is durable. Today the queue still holds the id, `turns` stays at the aborted turn, the file has no matching entry, and `pending` still returns it.

## 3. An in-flight pass can submit after the lease has moved

`pass` checks ownership only inside `pending`. After that snapshot it checks `active()` (session id and `stopped`), not the lease, and `submit` does not call `owns` (`src/pi/index.ts:118-126`, `108-110`). `acknowledgePi` does check the lease (`src/host/index.ts:123-124`), so a stolen lease cannot be ACKed by the old runtime. It can still `sendMessage`.

Takeover is allowed once `expires_at <= now` (`src/host/index.ts:85` and `test/host/host.test.ts` expiry bind). The new runtime's `pass` will submit the same id. Both writes land in the same session JSONL if both processes have that file open. The contract allows the stale owner to expire without dropping rows; it does not allow both runtimes to deliver.

Distinguishing test: two native opens of one session file. Let A bind and block inside `persistedReceipt` after `pending` has returned a not-yet-written id (the crash child's `open` wrapper is enough). Expire A's lease and let B `session_start` successfully. Release A's read. Count `custom_message` entries with that `deliveryId` and model turns on B. Today A still calls `sendMessage` after B owns the lease, so the file can contain two entries or both sessions can turn. One entry and zero turns on the loser is the required result.

## 4. Missed keepalives do not reconnect a silent stream

Coordinator keepalives are comments every 15s (`src/server/delivery/index.ts:152-155`). The host parser ignores `:` lines and has no read deadline (`src/host/index.ts:199`, `278-301`). Reconnect runs only after `fetch` throws or the body ends. A probe against `node:http` left the client blocked on `reader.read()` for 12s after one SSE comment and no further bytes; Roger-Wilco did not abort it. `requestTimeout = 15_000` on `src/bin/coordinator.ts:27` does not close a response that has already started (separate 18s probe with 1s writes stayed open).

`inFlight` then suppresses a resend on that connection until a receipt deletes it (`src/server/delivery/index.ts:145-148`, `112-115`). A host that has stopped reading eventually fills the socket and hits the 5s drain deadline; that path is tested. A peer that vanishes without a close, or a coordinator that stops writing while the TCP socket stays up, does not. Pending SQLite rows are not deleted, but reconnect is not bounded by the keepalive the contract requires. Shutdown of a responsive socket is bounded and is not this bug.

Distinguishing test: connect the real host daemon, write one SSE comment, then leave the coordinator socket open and silent. The daemon must drop that stream and open another within a small multiple of 15s, then ACK a frame offered only on the second connection. Today the first `fetch` is still pending at 12s and no second connection is opened.

## What the current code does hold

These were the loss/replay/routing paths most likely to be wrong. They are not.

- Registration commits `cursor = initial_cursor = MAX(id)` in one immediate transaction and retry does not write `cursor` or `offered` (`src/server/store.ts:111-122`). A lost register response cannot skip a message inserted after that commit. `test/acceptance/coordinator.test.ts` drops the response and checks the cursor.
- `pending`/`acknowledge` will not advance past an earlier unacked id for that subscription. `offered` is committed before the frame is returned. A receipt for a future or never-offered id is rejected; a repeat of an already-advanced id is idempotent (`src/server/store.ts:162-202`). Reopen keeps `offered`.
- Broadcast routing excludes the sender and does not treat `public` as a different route. Cross-host receipt is `owner_conflict`. Another host's stream is a different `pending` query, so one stalled `inFlight` map does not block it.
- The host ACKs the coordinator only after `receive` returns. That insert is the auto-committed primary key `(session_key, message_id)` with `synchronous=FULL`. Replay inserts no second row (`src/host/index.ts:63-68`, `293-301`). `test/host/host.test.ts` opens the inbox from the receipt handler and again after process-style reopen.
- Pi ACK is the JSONL `custom_message` with `customType === 'roger-wilco'` and `details.deliveryId`, after `handle.sync()`, and not the void `sendMessage` (`src/pi/index.ts:42-61`, `82-105`). A header id from another session is ignored. Readonly `fsync` succeeds on this host. Coordinator receipt and `PiReceipt` are different columns; a host receipt does not clear `pending`.
- Closed Pi does not spawn Pi. The daemon only inserts. Resume delivery is the extension's `pending` scan, and a forked session id does not read the old session's rows.

## Acceptance that does not prove the claim

`test/pi/crash.test.mjs` does restart the real extension and checks one submit only when the id is absent, and no extra JSONL entry when it is already present. That part hits the file boundary. It does so only after the test itself steals and releases the lease. It does not prove production crash resume.

SIGKILL also does not drop dirty pages. The `after-write-before-fsync` stage hangs in `persistedReceipt`'s `sync` (`test/pi/crash-child.mjs`) after Pi has already made the line readable. The parent asserts the entry is already there, then expects zero recovery turns. Removing `handle.sync` from `persistedReceipt` would not fail that test. The code does sync before ACK; the crash test does not discriminate a missing sync, and it does not simulate power loss.

`test/acceptance/native-delivery.test.ts` covers idle wake, busy follow-up, closed-Pi retain, resume, fork/switch, duplicate live bind, and host reconnect after a dropped receipt. It does not abort the busy turn, expire a live lease, or restart inside an unexpired dead lease. `getBranch()` after `SessionManager.open` is a file reload, not the production receipt, so those assertions are not the ACK bug the contract forbids. They also do not replace the missing abort and lease cases.

No cursor-skip, cross-host ACK, broadcast-to-self, registration-cursor-reset, or host-replay-duplicate defect showed up in the current store, stream, and inbox code. Those are covered by tests that drive the real SQLite and HTTP boundaries rather than mirroring a helper.

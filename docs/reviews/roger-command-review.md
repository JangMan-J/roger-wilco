# Review: roger-wilco ee311ab..d6764ec (/roger command, Board, Presence last_seen, adapter board/relabel/claim)

Method: `review` skill, three axes (fitness, intent-fit, syntactical) in parallel sub-agents plus a direct read. Static only: `git show`/`git diff`. The syntactical agent ran `check-harness-free`, `check-no-poll` and the invariant-3 grep on an archive of d6764ec, and all three passed. No acceptance proofs were rerun; the vector reports them passing at 4b7758a.
Line numbers are at d6764ec.

## Verdict

No kernel violations: KERNEL.md, INVARIANTS.md and GLOSSARY.md are unchanged, and invariants 1–4 hold. Do not merge yet. Findings 1–4 are correctness bugs in target resolution and the adapter. Finding 6 means the acceptance run could pass with most of the new resolution and presence logic broken.

## Ranked findings

### 1. HIGH: a full Address that is not on the Board is refused, and a label can capture an address-shaped target
- `shims/pi/roger-command.ts:41-55`
- `exact` matches only against Board rows. A well-formed Address that has not registered yet (or has not since a coordinator restart) falls through to label matching, then prefix matching, then `no session matches`.
- The core accepts mail to an unregistered Address (coordinator.rs send arm, ~315-340) and delivers it on Register. The old `resolveRecipient` routed any canonical key before reading the Board (`if (canonical) return {recipient: canonical…}`, roger-wilco@3ba6126 src/pi/tools.ts:121-122).
- Worse: any session labelled with that UUID string (relabel is open to any session, see finding 5) receives the mail. That breaks GLOSSARY Address, "Not: a label".
- Fix: if `first` parses as a UUID, return `{to: first}` before any Board or label lookup. Never let a label match text that has Address shape.
- Verdict: must fix.

### 2. HIGH: `/roger label <a> <b…>` relabels the wrong session, silently
- `shims/pi/roger-command.ts:126-135`
- With three or more words, the first word is always tried as a target:
  - `/roger label Builder Two` (meant as your own label) relabels the session already labelled `Builder` to `Two`.
  - A first word of 3+ hex characters that prefixes an Address (`dead`, `cafe`, `abc`…) relabels that session.
  - When resolution fails, is ambiguous, or hits `*` (no `row`), the error string is thrown away and you relabel yourself with the whole text: `/roger label zzzz foo` sets your label to `zzzz foo`, and `/roger label * x` sets it to `* x`.
- Acceptance `/roger label [[0:FLEET:0]] pi` passes only through this fallback.
- Fix: make the other-session form explicit (for example `/roger label @<address|prefix> <text>`, or only a full Address or unique prefix). When resolution returns an error string, show it; never fall back to self.
- Verdict: must fix.

### 3. MEDIUM: a label silently shadows an Address prefix; no ambiguity across stages
- `shims/pi/roger-command.ts:45-54`
- The label stage returns before the prefix stage. If session A is labelled `2222` and session B's Address starts `2222…`, `/roger 2222 hi` goes to A with no warning.
- Also:
  - Matching is now case-sensitive (old code NFC-folded and lower-cased: tools.ts:127-131).
  - Labels `all`, `help`, `label` and `*` can never be targets, because the keyword checks run first (:40, :125-126, :137).
  - If labels `Bob` and `Bob Smith` both exist, `/roger Bob Smith…` always picks `Bob Smith`.
- Fix:
  - Collect label hits and prefix hits together; if they name different sessions, return `ambiguous`.
  - Case-fold the hex prefix.
  - Optionally accept a quoted target (`@"…"`, as tools.ts:54-68 did).
- Verdict: should fix.

### 4. MEDIUM: the adapter forwards any claim `op`, and a bad one drops the whole host's coordinator connection
- `src/session.rs:105` passes `net::field(v,"op")?` through without checking it.
- `src/coordinator.rs:421` calls `claim(&mut st, &v)?`, and `:467` makes `Op::parse` failure an `Err`. `serve` returns, and the coordinator closes that host daemon's upstream connection. Every session on the host is cut off until it reconnects.
- The CLI only ever sends acquire/renew/release, so this exposure is new: any shim line `{"type":"claim","op":"x","key":"k"}` triggers it.
- Fix: in `shim_request`, accept only `acquire|renew|release` and return an error naming them. Better still, have the coordinator answer `{"type":"error",reason}` for a bad op instead of `?`, as it already does for a bad ttl.
- Verdict: should fix.

### 5. MEDIUM: adapter relabel lets any session relabel any other session, including addresses nobody registered
- `src/session.rs:118-122` (`session` from the shim, default `me`)
- Coordinator `set_label` (coordinator.rs:191) checks nothing and writes a durable log row.
- Together with label routing (findings 1 and 3), one session can redirect another's `/roger <label>` traffic.
- Relabelling others matches the old rank-assign-any-session behaviour, so the feature itself is intended.
- Fix: refuse relabel of an Address that is not registered, and stop label lookup from capturing address-shaped text (finding 1).
- Verdict: accept the feature; fix the routing side.

### 6. MEDIUM: the tests pass while key behaviour is broken
These breakages would all go unnoticed:
- `recent()` always returning true, or `view()` ignoring `all`: both acceptance sessions are recent, so `Recent … (2)` (scripts/acceptance:410) and `All registered sessions (2)` (:427) both pass. Staleness was checked only live.
- Ambiguous-label and ambiguous-prefix paths, the 3-character minimum, label-vs-prefix shadowing, the full-Address-not-on-Board case, and the `/roger label` fallback: none is exercised. Only `no session matches` is (:434).
- Correctness of `roger_board`: `\"last_seen_ms\":` is grepped for existence only, not checked for non-zero or for filtering.
- `tests/process.rs:1299` "…and nothing else": it only checks an anonymous read. Stamping the recipient on a send *to* it would pass, because the ack that follows moves the timestamp anyway.
- Nothing typechecks or unit-tests `resolve()`.

Fix:
- Export `resolve` and `recent`, and unit-test them on a fixture of rows: ambiguity, label-vs-prefix, `[[n:…]]` labels, spaced labels, address-shaped labels, full Address absent from the Board.
- In acceptance, set `ROGER_PRESENCE_WINDOW_MS` small with a stale tracer and assert it is hidden by `/roger` and shown by `/roger all`.
- In process.rs, assert the recipient's `last_seen_ms` is unchanged after a send *to* it, before the ack.

Verdict: should fix before merge.

### 7. LOW: reply-queue ordering is correct today but fragile
- `shims/pi/roger.ts:26, 81-82`
- Ordering holds: the adapter has a single worker that answers each stdin line exactly once, in order (session.rs:58-66). Coordinator and daemon `error` replies become one `{"type":"error"}`. The only unsolicited lines are `ready` and `mail`, and neither shifts the queue.
- The risk: the `REPLIES` allowlist must be kept in step by hand with the reply types. A missing type leaves its pending entry behind, every later caller then gets the previous request's reply, and `send` returns `.mail` as `undefined` (:108) without noticing.
- Head-of-line blocking is bounded by the daemon's `UPSTREAM_WAIT`, not unbounded. Completions call `board()` on each request (roger-command.ts:111), which adds to the queue and stamps presence.
- Fix: treat every line that is not `ready` or `mail` as the reply to the oldest pending request, or echo a per-request id. Check the reply type in `send`.
- Verdict: should harden.

### 8. LOW: presence can be stamped by things other than the session's own act
- Coordinator `seen` is set by:
  - any `from` on send, including refused sends (coordinator.rs:306-307);
  - `roger board --from <any>` (:431, session.rs board);
  - daemon re-register on reconnect (daemon.rs:194-198 → :256);
  - replayed acks (→ :377).
- The map takes arbitrary strings and is never pruned.
- No glossary term is broken: Presence is "Not: a guarantee that a session is alive", and the daemon re-registers only sessions with an open local socket (daemon.rs:357-363). But the test name "own activity and nothing else" over-claims.
- Fix:
  - Stamp only Addresses already in `st.sessions`.
  - Rename the test, or document that a daemon reconnect counts.
- Verdict: accept, with the doc or test-name fix.

### 9. LOW: `recent()` compares coordinator epoch ms with the local clock
- `shims/pi/roger-command.ts:28` (also `:24`)
- `recent()` compares coordinator epoch ms with the local `Date.now()`. Cross-host clock skew larger than the window hides every session or shows stale ones.
- Fix: return `now_ms` in the Board reply and compute age against it.
- Same area: `Number(env) || default` (:24) ignores `0`, and a negative value hides everything.
- Verdict: should fix (cheap).

### 10. LOW: smaller standards and smells
- `src/session.rs:128`: `unknown request {other}` names what was seen but not what was expected (send|board|relabel|claim). This breaks AGENTS.md § Coding standards "a failure names what was expected and what was seen".
- Duplicated request-object builders: session.rs:92/100/114.
- `call-roger-board` (call-tool.ts:49) duplicates `/call-roger-tool roger_board {}`.
- Completion stops at the first space, so spaced labels such as `[[153:Meteor Shower:7]]` don't complete (:107).
- Claim keys print raw (old code JSON-quoted them).
- The `note` parameter on claim/release is dropped compared with the old code.
- Verdict: nits.

### 11. INFO: the questions asked, answered
- **Invariant 3 and the references source search:** not violated.
  - `resolve()` compares whole labels as opaque text (`===`, `startsWith`). It never extracts or orders by a Reference integer.
  - "Longest" is chosen among labels that are all prefixes of the same text. That is prefix specificity, not ranking by integer value.
  - The search scans only `src/*.rs` (scripts/acceptance:541-545), so it still passes. If it were ever extended to `shims/`, `r.label && (text === r.label` (:45) would trip `\blabel\b[^;]*[=!<>]=`.
- **Can a session claim as another owner through the adapter?** No.
  - `shim_request` always sends `("owner", s(me))` (session.rs:107) and ignores any owner the shim supplies, so claim, renew and release are bound to the adapter's own Address.
  - Pre-existing, outside this diff: the raw daemon socket (`roger claim --owner X`) still trusts any registered owner (coordinator.rs:489 checks only registration).
- **Invariant 2:** holds. `seen` is stamped when the act happens and the window is applied when the Board is read; there is no timer.
- **Invariant 4:** holds. No harness is named in `src/`.
- **Frozen files:** unchanged.
- **Glossary:** "stale", "recent window", "relabel", "picker" and "label" have no entries. "Label" is now central to targeting; add a non-kernel entry.

## Axis summary
- Fitness: one standards break (session.rs:128) and several smells. Worst: tests pass with the window filter and ambiguity paths broken.
- Intent-fit: 7 findings. Worst: a full Address not on the Board is refused, and a label can capture it.
- Syntactical: 0 blocking. Worst: daemon reconnect and replayed acks stamp presence, which sits awkwardly with Register's "a session announcing".
- Note: the intent agent also claimed that `acquire` without `ttl_ms` grants an already-expired claim. That is false: the coordinator refuses `ttl_ms <= 0` (coordinator.rs ~476), so it was left out.

# Host adapter specification

This document specifies the current host adapter interface: inputs from the
harness, outputs to the core, and their order. `agent-edge.md` proposes a
replacement; until it is accepted, this document applies.

The code form is `EvidenceSource` in `src/evidence.ts` and `HostInbox` in
`src/protocol/index.ts`. `src/muse/`, `src/claude/`, and `src/agent/` are
reference implementations. The key words MUST, MUST NOT, SHOULD, and MAY
are to be interpreted as described in RFC 2119.

## Acknowledgement semantics

An acknowledgement asserts that the session's model received the message.
The adapter MUST base it on a record in the session's own durable store
that the harness wrote as model input for this session.

The following MUST NOT be treated as acknowledgement evidence:

- the return of a delivery call, a print, or a socket write;
- a human prompt record, including one that contains the exact record;
- an assistant output record;
- a subagent record or another session's record.

## Harness inputs

An adapter MUST obtain the following from its harness. A harness that
cannot supply all four has no conforming adapter.

1. **Session identity.** An opaque token naming the current session. It
   MUST be stable across resume, MUST differ for new and forked sessions,
   and MUST satisfy `isSessionId`.
2. **Store location.** The identity of the session's durable store, such
   as a transcript path. It MAY be undefined until the harness creates it.
3. **Delivery channel.** A means of placing the delivered record into
   model input such that the harness persists it in the store. For a CLI
   adapter this is standard output of a shell tool call.
4. **Receipt predicate.** A function that decides, for one store record,
   whether it is harness-written model input, on the main chain, for this
   session.

## Adapter outputs

An adapter MUST provide exactly these harness-specific parts:

1. **Identity mapping.** One module mapping the harness token to
   `SessionAddress {namespace, sessionId}`. No other module reads the token.
2. **Evidence source.** One `EvidenceSource<Receipt>`:
   - `location()` returns the current store identity or undefined.
   - `lookup(location, messageId, expected)` returns `persisted` with a
     receipt, `absent`, or `store_missing`. It MUST throw when the store
     state is indeterminate.
   - `locatedAt(receipt, location)` returns true only when the receipt was
     read from `location`.
   - `acknowledge(receipt)` sends `DeliveryEvidence
     {messageId, sessionId, recordId, location}` to the daemon. `recordId`
     MUST be stable for a given record, since a repeated acknowledgement
     MUST present the same record and location.
3. **Tool surface.** `roger_identity`, `roger_send`, `roger_inbox`,
   `roger_entity`, `roger_board`, `roger_claim`, `roger_release`, over
   lease-bound core access. Sender and claim owner MUST come from the bound
   session, not from parameters.
4. **Wake level.** One of `forceful`, `next-turn`, `mailbox-only`, as
   defined in `CONTEXT.md`. The adapter MUST declare the level it
   implements, not the strongest level the harness offers.

## Evidence rules

`lookup` MUST return `persisted` only if all of E1 to E6 hold.

- **E1, exact record.** One complete output line parses as
  `{event:'delivered', message}` and `message` equals the pending message
  in `id`, `sequence`, `timestamp`, `sender`, `recipient`, `body`,
  `visibility`, and `data`.
- **E2, model channel.** The record is on a channel the harness writes as
  model input, such as a tool result or injected hook context. Human
  prompt, assistant output, and subagent records do not qualify.
- **E3, session binding.** The record is bound to this session by a
  per-record session ID where the store has one, otherwise by the store's
  session identity.
- **E4, complete record.** A torn final record is ignored.
- **E5, durability.** The adapter syncs the opened store and verifies that
  device, inode, size, mtime, and ctime did not change during the scan;
  on change it throws.
- **E6, read-only.** The adapter MUST NOT write to the store.

## Order of operations

1. **Start.** Load host configuration (`ROGER_WILCO_CONFIG` or
   `~/.config/roger-wilco/host.json`). Resolve the session token and map it
   to an address. Connect to the daemon socket. Bind the lease with a new
   runtime ID. Enqueue a registration; enqueueing MUST NOT block startup.
2. **Run.** Renew the lease at most every 10 s (lease length 30 s). After
   a lost lease, rebind and re-enqueue registration. Enqueue a heartbeat
   registration every 30 s.
3. **Receive pass.** Verify `authority()` is unchanged. Read one pending
   page. For each message, in order:
   1. Look up evidence at the current location; on a throw, skip it.
   2. If `persisted` and `locatedAt` holds, acknowledge.
   3. Otherwise, if not yet handed over by this runtime, deliver once.
   4. After delivery, look up again, so a synchronously persisting harness
      is acknowledged in the same pass.
4. **Stop.** Clear timers and close the socket, releasing the lease.

A session has at most one live runtime. A CLI adapter binds per command;
a second concurrent command for the same session fails with
`owner_conflict`.

## Wake levels

The wake level affects only when a session reads its mailbox. Mailbox
contents, delivery, and acknowledgement are identical at every level.

- `forceful`: start a turn in an idle live session.
- `next-turn`: inject at the next turn boundary.
- `mailbox-only`: the session reads the mailbox when it runs `receive`.

## Conformance

`test/claude/claude.test.ts` and `test/muse/muse.test.ts` contain the
minimum set: identity mapping; rejection for each failing case of E1 to
E4; end-to-end exchange through a real daemon; no acknowledgement before
evidence; no redelivery after restart; no import of another adapter.

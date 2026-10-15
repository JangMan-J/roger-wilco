# Work order

This document puts the remaining work in one sequence. It combines the
user's TODO list (`TODO.md`, items 1-4) with the incomplete items recorded
at the end of the 2026-10-15 session. Assignment of work to any person,
model, or session is left open and is not recorded here.

## Ordering rule

Work is ordered so that each step is performed once, against a stable
target:

1. Correctness defects in what exists are closed first, while the system
   is small enough to retest completely.
2. Research precedes specification, and specification precedes
   implementation, so no component is built twice.
3. The second-language client is completed only after the adapter shape
   is final, so it proves the final contract rather than a draft.
4. Alternatives are measured only after the contract is stable, so the
   comparison measures delivery of the contract and not a moving target.
5. Written justification comes after measurement, so it justifies what
   survived rather than what may be replaced.
6. The removal and substitution exercise is last, because its results
   can invalidate earlier justification and must be allowed to.

## Phase 0 — Close known correctness defects

These are small, already diagnosed, and need no research.

- Muse adapter: revalidate the harness session on every bound call and
  delivery pass; fail closed if the session changed under the runtime.
- Display names belong to the entity mechanism (a display name is an
  entity label). The separate registration display-name field duplicates
  that mechanism and is scheduled for removal from the core protocol,
  coordinator store, and board output, in favour of entities. Removal is
  core work because the field is currently implemented in all three
  places.
- Tracer agent: make evidence record identifiers unique under concurrent
  delivery (derive from the message sequence, not a shared counter).
- Pi receipt reader: when a session entry carries message content, verify
  the content's message ID matches the delivery ID in its details.
- Coordinator store: reject whitespace-only claim keys in the store
  itself, not only at the HTTP layer.
- Add a dedicated test for the daemon registration cache, and a test that
  names the pipelined-receipt behaviour when a receipt response is
  invalid.

Exit condition: full test suite passes, including new regression tests
for each item.

## Phase 1 — Harness surface research (TODO 1, prerequisite)

Extend the twenty-harness wake study into the full interactive surface:
session identity, tool and command registration, lifecycle hooks, busy
and idle signalling, transcript or session store layout, and wake
mechanisms (forceful, mid-turn, next-turn).

- Resolve the unverified slot from the earlier study: Anthropic's Claude
  Code as its own entry, distinct from Meta's Muse products.
- For each harness, record which of the adapter wires exist and in what
  form. Harnesses without any injection channel are recorded as such;
  no mechanism is inferred from a related product.

Exit condition: a surface table for the prominent harnesses, each row
supported by documentation or source, with unverifiable rows marked.

## Phase 2 — Adapter shape, in writing and in code (TODO 1)

Define the minimally but explicitly complete shape of a host adapter:
what must be input, what must be output, in what form and in what order.

- One written specification, in the contract documents.
- One code definition of the same shape that adapters compile or run
  against, so the written and code forms cannot drift apart.
- Wake level becomes part of the shape: each adapter declares `forceful`,
  `next-turn`, or `mailbox-only`, and the declaration is checked in code.
- Decide the adapter surface question inside this phase: whether adapters
  continue to speak both the daemon socket protocol and coordinator HTTP,
  or speak only to the daemon. The decision is recorded with its reason.

Exit condition: an adapter author can implement from the specification
without reading an existing adapter.

## Phase 3 — Shared evidence verifier

Move delivery-evidence verification into one core-owned component.

- Adapters supply candidate record extraction from their harness store.
- The core performs: candidate selection by message ID, hash equality,
  delivered-record shape and full-field match, session binding, torn-line
  and concurrent-change rejection, and acknowledgement translation.
- The conformance kit (currently Rust-only) runs against every evidence
  implementation: bare mention, forged record, wrong session, torn line,
  concurrent change, duplicate acknowledgement.
- The Muse transcript reader's per-message full scan is replaced by the
  shared verifier's indexed candidate lookup.

Exit condition: Pi and Muse evidence paths run on the shared verifier,
and the kit passes for both.

## Phase 4 — Complete the Rust client

Bring the second-language client to the full contract as defined in
Phase 2: entity lookup, rank, socket acknowledgement, stream handling,
and HTTPS. The client remains a consumer; it adds no core behaviour.

Exit condition: the interoperability test exercises every route and
socket method in the final contract from the Rust client.

## Phase 5 — Live verification

Exercise the system outside isolated test stacks: Pi, Muse, and Rust
sessions registered simultaneously on the real fleet, with directed and
broadcast delivery, wake behaviour observed per declared level, and
acknowledgement verified from each harness's own store.

Exit condition: results recorded in the validation document, including
any harness whose observed behaviour differs from its declared level.

## Phase 6 — Reprove transport and storage (TODO 2)

Measure the current mechanisms against at least one alternative each, on
speed, accuracy, and delivery success rate, delivering the Phase 2
contract:

- Coordinator-to-daemon transmission: current HTTP requests plus one
  server-sent-events stream, against one alternative.
- Daemon-to-session transmission: current framed socket protocol,
  against one alternative.
- Storage: current SQLite databases at coordinator and daemon, against
  one alternative each.

Exit condition: per-mechanism measurements and a keep-or-replace
decision for each, with reasons.

## Phase 7 — Written justification (TODO 3)

For every protocol component concerning data transmission and integrity,
write one full paragraph stating why the written contract would be
insufficiently fulfilled without that component. Components replaced in
Phase 6 are justified in their replacement form. A component that cannot
be justified is recorded as a removal candidate for Phase 8.

Exit condition: one paragraph per component, each naming the contract
clause that fails without it.

## Phase 8 — Removal and substitution exercise (TODO 4)

Select integral components in turn. Once per run, either fully remove
one component, or substitute the smallest mechanism that can perform
similar service. Record the evidence: what failed, what degraded, what
was unaffected.

- A component whose removal changes nothing is removed permanently, and
  its Phase 7 paragraph is withdrawn.
- A component whose substitution performs equally is replaced by the
  smaller mechanism.
- Results feed back into Phase 7; the exercise ends when every selected
  component has one run recorded.

Exit condition: one recorded run per selected component, with the
keep, remove, or substitute decision for each.

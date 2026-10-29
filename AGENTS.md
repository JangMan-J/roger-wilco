This project has a director: run `slice status`, then read NEXT.md (`slice path next`), before changing anything. Its purpose, proofs, invariants and terms are sealed in the director's records outside this repo, not here. `VECTORS/` is read-only history; write no new vectors.

The sealed documents name no harness or model; provenance frontmatter (`agent:`, `session:`) may. Harness specifics live in shims/ and their tests. Work is delegated as slices (`slice define`, `slice new`); the director gates each attempt and merges with `slice merge <id> <n> --check scripts/check-all`.

## Directives

- "just remember, this is a standalone project. meant to be as applicable to a call center as to agents. dont let the harness symantics take it over" (2026-10-22)
- "my vision is this to be a ruthlessly efficient foundation that any sort of adapter can be placed on top of. modular" (2026-10-22)
- "It was never a goal to turn delivery into turns, 'turns and wakes' shouldn't actually be present in roger-wilco's vocabulary... its foundational layer should be as suitable to use for an office call center as for an autonomous agent team. However, on our side of things, turns and wakes are important - it's just a matter of separating boundaries and what transports implement them/the shape of the wire information." (2026-10-29; supersedes the 2026-10-22 directive "Wake (proof 5) is landed and is not a focus": wake leaves Roger, and its behavior belongs to the consumer's side)
- "I think MCP alongside socket connections is fine... It's just the decisions of what to place where that matter most and how to reduce the amount of adapters that other potential clients 'need' in order to use the service. Redundancy is not inefficiency." (2026-10-29)

## Coding standards

The core gate is `scripts/check`: fmt, clippy with warnings denied, the test suite,
`scripts/check-harness-free`, `scripts/check-vocabulary`, and `scripts/check-no-poll`. It names no harness.
`scripts/check-all` runs it, then every `shims/<name>/check` it finds, then `scripts/check-seal`
(the director's seal on the sealed documents is intact; it passes quietly where no seal exists).
A shim brings its own check and the core gate never changes for it. Nothing lands on main until
`scripts/check-all` exits 0. Clippy's `all` group is enabled in `Cargo.toml` `[lints.clippy]`; the gate turns
those warnings into errors.

### Review rules

These are INVARIANTS.md read as rules for a diff. KERNEL.md and INVARIANTS.md win over this one.

1. **No loss, no duplicate.** Any path that touches a Mail between Accepted and the
   recipient's Ack keeps the body durable across coordinator restart, daemon restart,
   and recipient SIGKILL, and the Store holds each body once. A change here extends
   `tests/process.rs` and keeps `scripts/acceptance cross-host` green.
2. **Event-driven only.** No fixed-interval scan on the correctness path. Mail,
   receipts, and claim expiry follow committed events or a Register. A sweep may
   exist only as cleanup that can be disabled without losing or duplicating work.
   `scripts/check-no-poll` is the check; a non-correctness timed wait carries
   `// no-poll: <reason>`.
3. **Opaque tokens stay opaque.** Nothing in `src/` compares, branches on, ranks,
   looks up by, or names the tokens in a Reference or on a session. They pass
   through byte-identical. `scripts/acceptance references` is the check.
4. **No harness in the core.** The core names no harness or model, reads no harness store,
   and `[dependencies]` stays empty. Only `shims/` may know a harness. The core is every
   file in the checkout, tracked or untracked but not ignored, outside `shims/`, `VECTORS/`, and
   `docs/reviews/` (historical review evidence): source, tests, scripts, deploy units, and
   docs (`README.md` and `deploy/README.md` included). Exempt are markdown frontmatter
   (`agent:`, `session:` record who wrote a file), `scripts/acceptance`, since proofs 2 and
   5 run real harnesses by definition, and `scripts/check-harness-free`, which holds the
   list of names. `scripts/check-harness-free` is the check.
5. **No workflow.** The core stores and routes; it never decides what work exists or
   who takes it.
6. **No vocabulary of what a session does with its Mail.** No turn, wake, idle, or busy
   state, request, field, flag, or doc exists in the core; a delivery carries
   its `kind` (`directed` or `broadcast`) and the sender's opaque `headers`, and what a
   consumer does on arrival is the consumer's. A shim may have such words; the core may
   not. `scripts/check-vocabulary` is the check. A line that must name the old wire field for
   one transition release carries `legacy-wire:` and a reason; the release that drops the
   field drops the line.
7. **Opaque headers stay opaque.** Nothing in `src/` compares, branches on, ranks, or
   names a header key or value; they pass through byte-identical, bounded, with a refusal
   that names what was expected and what was seen.
8. **Faces are equal and additive.** The socket protocol, the `roger adapter` stream, and
   `roger mcp` give a session the same Mail, Store, Ack, Board, claim and receipt
   semantics. A new face adds no semantics of its own and no crate.

### Code

- One binary, standard library only. Prefer a plain function over a trait and a
  small struct over a builder.
- Tests per proof: each KERNEL.md proof has a `scripts/acceptance <name>` subcommand
  that exercises real processes, plus the narrowest unit or process test that would
  catch a regression. Match test depth to the risk the proof names.
- Errors are `io::Result`; a failure names what was expected and what was seen.
- Claim deadlines are read on the coordinator's wall clock (`now_ms`), and nothing in
  `src/` reads another clock for a claim. Why, and what a clock step does: README.md
  `## Claims`.
- Keep `src/main.rs` dispatch flat: add an arm, do not restructure.
- Test-only hooks live behind the `test-hooks` feature and never reach a deploy build.
- The director's records, not comments, record what was tried; GLOSSARY.md, not comments, defines terms.

This project has a director: run `slice status`, then read NEXT.md (`slice path next`), before changing anything. Its purpose, proofs, invariants and terms are sealed in the director's records outside this repo, not here. `VECTORS/` is read-only history; write no new vectors.

The sealed documents name no harness or model; provenance frontmatter (`agent:`, `session:`) may. Harness specifics live in shims/ and their tests. Work is delegated as slices (`slice define`, `slice new`); the director gates each attempt and merges with `slice merge <id> <n> --check scripts/check-all`.

## Directives

- "just remember, this is a standalone project. meant to be as applicable to a call center as to agents. dont let the harness symantics take it over" (2026-10-22)
- "my vision is this to be a ruthlessly efficient foundation that any sort of adapter can be placed on top of. modular" (2026-10-22)
- "Wake (proof 5) is landed and is not a focus: do not extend, prototype or queue wake work unless Jang asks." (2026-10-22)

## Coding standards

The core gate is `scripts/check`: fmt, clippy with warnings denied, the test suite,
`scripts/check-harness-free`, and `scripts/check-no-poll`. It names no harness.
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

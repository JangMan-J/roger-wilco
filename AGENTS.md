# roger-wilco

Standalone, harness-free message foundation. `VECTORS/` is read-only history; write no new vectors. Nothing lands on `main` until `scripts/check-all` (and `scripts/check-modules` when `modules/` is touched) exits 0.

## Directives

- "just remember, this is a standalone project. meant to be as applicable to a call center as to agents. dont let the harness symantics take it over" (2026-10-22)
- "my vision is this to be a ruthlessly efficient foundation that any sort of adapter can be placed on top of. modular" (2026-10-22)
- "It was never a goal to turn delivery into turns, 'turns and wakes' shouldn't actually be present in roger-wilco's vocabulary... its foundational layer should be as suitable to use for an office call center as for an autonomous agent team. However, on our side of things, turns and wakes are important - it's just a matter of separating boundaries and what transports implement them/the shape of the wire information." (2026-10-29)
- "I think MCP alongside socket connections is fine... It's just the decisions of what to place where that matter most and how to reduce the amount of adapters that other potential clients 'need' in order to use the service. Redundancy is not inefficiency." (2026-10-29)
- "minimal requirement, as currently stated already, permissively optional modules" / "reword the invariants to fit optional modules not tailored to cheat an invariant" (2026-11-14)

## Layout & Gates

- **Core** (`Cargo.toml`, `src/`, `vendor/`): one standalone `roger` binary that builds offline with clean `HOME` and satisfies every proof on its own. Plain SQL stays in `src/sql.rs`.
- **Shared wire** (`crates/roger-wire/`): zero-dependency crate for JSON, SHA-256, durable logs, and wire constants.
- **Optional modules** (`modules/<name>/`): standalone `[workspace]` packages so root `Cargo.toml` never depends on them.
- **Gates**: `scripts/check` (fmt, clippy `-D warnings`, tests, `check-harness-free`, `check-vocabulary`, `check-no-poll`), `scripts/check-all` (+ `check-seal`), `scripts/check-modules`.

## Rules (Core & Optional Modules)

1. **No loss, no duplicate.** Any path touching a Mail between Accepted and Ack keeps the body durable (Store-before-Ack) across coordinator, daemon, or module restart and recipient SIGKILL, stored once.
2. **Event-driven only.** No fixed-interval scan on the correctness path in `src/` or `modules/*/src/`. Non-correctness timed waits carry `// no-poll: <reason>`.
3. **Opaque tokens stay opaque.** Never compare, branch on, rank, look up by, or name tokens in a Reference, Address, or session; pass them through byte-identical.
4. **No harness in the repo.** Never name a harness or model, read a harness store, link a harness SDK, or ship per-client code (enforced by `scripts/check-harness-free`).
5. **No workflow.** Core stores and routes; Optional modules expose/bridge faces, read records, or ship cold files. Neither decides what work exists or who takes it.
6. **No consumer-activity vocabulary.** No turn, wake, idle, or busy state, field, flag, or doc in Core or Optional modules (enforced by `scripts/check-vocabulary`). Delivery carries `kind` (`directed`, `broadcast`, `group`) and opaque `headers`. Legacy log-reader lines carry `legacy-wire:` and a reason; nothing writes the old field.
7. **Opaque headers stay opaque.** Header keys and values carry no meaning for Roger and pass through byte-identical and bounded. An Optional module may read named fields only to verify recorded integrity (signatures, hashes, sequence continuity) with no path to delivery.
8. **Faces are equal and additive.** Socket, `roger adapter`, `roger mcp`, and Optional-module faces give the same Mail, Store, Ack, Board, Group, claim, and receipt semantics without adding message semantics.
9. **Optional modules obey Rules 1–8 without exception.** Moving code into `modules/<name>/` exempts nothing.

## Code

- Prefer a plain function over a trait and a small struct over a builder.
- Errors are `io::Result`; name what was expected and what was seen.
- Claim deadlines use the coordinator's wall clock (`now_ms`) only (`README.md` `## Claims`).
- Keep `src/main.rs` dispatch flat: add an arm, do not restructure.
- Test-only hooks live behind the `test-hooks` feature and never reach deploy builds.
- Pair `scripts/acceptance <name>` with narrow unit/process tests for regressions.

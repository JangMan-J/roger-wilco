This project has a director: orient with `slice status` and NEXT.md (`slice path next`). Directors spawn Agents in git worktrees on disjoint files; Agents run the touched module's fast tests and read the diff before committing. Sealed slices are opt-in for delegated tricky state-machine, security, or protocol contracts where reward-hacking or spec drift is a real risk, never mandatory for ordinary features, fixes, config, docs, or installers. Its purpose, proofs, invariants and terms are sealed in the director's records outside this repo. `VECTORS/` is read-only history; write no new vectors.

The sealed documents name no harness or model; provenance frontmatter (`agent:`, `session:`) may. The core ships no per-client code: a program that uses Roger connects through a face and its code lives in that program's own project. When opting into sealed slices, use `slice define`, visible/held-out acceptance, `slice new`, `slice gate`, then `slice merge <id> <n> --check scripts/check-all`. Direct work and plain delegated work also require `scripts/check-all` before landing on main.

## Directives

- "just remember, this is a standalone project. meant to be as applicable to a call center as to agents. dont let the harness symantics take it over" (2026-10-22)
- "my vision is this to be a ruthlessly efficient foundation that any sort of adapter can be placed on top of. modular" (2026-10-22)
- "It was never a goal to turn delivery into turns, 'turns and wakes' shouldn't actually be present in roger-wilco's vocabulary... its foundational layer should be as suitable to use for an office call center as for an autonomous agent team. However, on our side of things, turns and wakes are important - it's just a matter of separating boundaries and what transports implement them/the shape of the wire information." (2026-10-29; supersedes the 2026-10-22 directive "Wake (proof 5) is landed and is not a focus": wake leaves Roger, and its behavior belongs to the consumer's side)
- "I think MCP alongside socket connections is fine... It's just the decisions of what to place where that matter most and how to reduce the amount of adapters that other potential clients 'need' in order to use the service. Redundancy is not inefficiency." (2026-10-29)
- "minimal requirement, as currently stated already, permissively optional modules" / "reword the invariants to fit optional modules not tailored to cheat an invariant" (2026-11-14)

## Coding standards

The Core (`Cargo.toml` and `src/`, building the `roger` binary) stays one standalone binary
satisfying every proof on its own without any Optional module, with vendored offline crates under
`vendor/` so `--offline` clean-HOME builds of `roger` always succeed; plain SQL is isolated in
one module (`src/sql.rs`) so storage stays portable. Optional modules (`modules/<name>/`) are
standalone packages in the repository — each carrying its own `[workspace]` table in
`modules/<name>/Cargo.toml` so root `Cargo.toml` never depends on them — that may link external
crates to expose or bridge faces over network transports, read records, or ship cold archives.

The offline core gate is `scripts/check`: fmt, clippy with warnings denied (`-D warnings`), the core
test suite, `scripts/check-harness-free`, `scripts/check-vocabulary`, and `scripts/check-no-poll`.
It names no harness. `scripts/check-modules` checks every standalone Optional module
(`modules/*/Cargo.toml`: workspace isolation, fmt, clippy `-D warnings`, tests, and the invariant
checks) when modules exist, and must also pass before changes touching `modules/` land on `main`.
`scripts/check-all` runs `scripts/check`, then `scripts/check-seal`
(the director's seal on the sealed documents is intact; it passes quietly where no seal exists).
Nothing lands on `main` until `scripts/check-all` (and `scripts/check-modules` when `modules/` is
touched) exits 0. Clippy's `all` group is enabled in `Cargo.toml` `[lints.clippy]`; the gate turns
those warnings into errors.

### Review rules

These are INVARIANTS.md read as rules for a diff across the Core and every Optional module.
KERNEL.md and INVARIANTS.md win over this one.

1. **No loss, no duplicate.** Any path in the Core or an Optional module that touches a Mail between
   Accepted and the recipient's Ack keeps the body durable (Store-before-Ack) across coordinator
   restart, daemon restart, module restart, and recipient SIGKILL, and the Store holds each body
   once. A change here extends `tests/process.rs` (and the module's tests when in `modules/`) and
   keeps `scripts/acceptance cross-host` green.
2. **Event-driven only.** No fixed-interval scan on the correctness path in `src/` or
   `modules/*/src/`. Mail, receipts, and claim expiry follow committed events or a Register. A sweep
   may exist only as cleanup that can be disabled without losing or duplicating work.
   `scripts/check-no-poll` is the check across `src` and `modules/*/src`; a non-correctness timed
   wait carries `// no-poll: <reason>`.
3. **Opaque tokens stay opaque.** Nothing in `src/` or `modules/` compares, branches on, ranks,
   looks up by, or names the tokens in a Reference or Address or on a session. They pass through
   byte-identical. `scripts/acceptance references` is the check.
4. **No harness in the repository.** Neither the Core nor any Optional module names a harness or
   model, reads a harness store, links a harness SDK (in `Cargo.toml`, `Cargo.lock`,
   `modules/*/Cargo.toml`, `modules/*/Cargo.lock`, or `vendor/`), or ships per-client code. This
   rule covers every project file in the checkout, tracked or untracked but not ignored (`modules/`
   included), outside `VECTORS/`, `docs/reviews/` (historical review evidence), and upstream
   `vendor/` source bodies: source, manifests and lockfiles, tests, scripts, deploy units, and docs
   (`README.md` and `deploy/README.md` included). Exempt are markdown frontmatter (`agent:`,
   `session:` record who wrote a file), `scripts/acceptance`, since proof 2 runs a real outside
   program by definition, and `scripts/check-harness-free`, which holds the list of names.
   `scripts/check-harness-free` is the check.
5. **No workflow.** Neither the Core nor any Optional module decides what work exists or who takes
   it; the Core stores and routes, and Optional modules expose or bridge faces, read records, or
   ship cold files without adding workflow.
6. **No vocabulary of what a session does with its Mail.** No turn, wake, idle, or busy state,
   request, field, flag, or doc exists in the Core or any Optional module (`modules/` manifests,
   source, tests, and docs included); a delivery carries its `kind` (`directed`, `broadcast`, or
   `group`) and the sender's opaque `headers`, and what a consumer does on arrival is the
   consumer's. A client's own code may have such words; this repository may not.
   `scripts/check-vocabulary` is the check. A line that must name the old wire field (a reader of
   logs written before `kind`) carries `legacy-wire:` and a reason; nothing writes the field.
7. **Opaque headers stay opaque.** Header keys and values carry no meaning for Roger. Nothing in
   `src/` compares, branches on, routes, ranks, or names a header key or value; they pass through
   byte-identical, bounded, with a refusal that names what was expected and what was seen. An
   Optional module may read named fields only to check mechanically that recorded data matches
   (a signature, a body hash, sequence continuity) and to report match or mismatch; such a module
   only reads records and has no path to delivery (no socket, listener, spawned process, or file
   write), and no module infers meaning from a header or changes delivery by one.
   `scripts/acceptance headers` (its source search) is the check.
8. **Faces are equal and additive.** The socket protocol, the `roger adapter` stream, `roger mcp`,
   and any face exposed or bridged by an Optional module give a session the same Mail, Store, Ack,
   Board, Group, claim, and receipt semantics. An Optional module under `modules/<name>/` may link
   external crates for its transport and adds no message semantics of its own.
9. **Optional modules never evade invariants.** Moving work into `modules/<name>/` exempts nothing.
   Every Optional module obeys Rules 1–8 without exception: Store-before-Ack durability (Rule 1),
   event-driven operation under `scripts/check-no-poll` with `// no-poll: <reason>` for
   non-correctness waits (Rule 2), opaque Reference and Address tokens (Rule 3), no harness or model
   names, store reads, SDKs in `modules/*/Cargo.toml` or `modules/*/Cargo.lock`, or per-client code
   (Rule 4), no workflow (Rule 5), no turn/wake/idle/busy or retired vocabulary (Rule 6), opaque
   bounded headers (Rule 7), and equal face semantics with no added message semantics (Rule 8). Each
   module carries its own `[workspace]` table in `modules/<name>/Cargo.toml`, and the Core satisfies
   every proof on its own without any Optional module.

### Code

- Core (`Cargo.toml` + `src/`): one standalone binary (`roger`), satisfying every proof on its own
  without any Optional module, with plain SQL isolated in `src/sql.rs`. Prefer a plain function
  over a trait and a small struct over a builder.
- Optional modules (`modules/<name>/`): standalone packages carrying `[workspace]` in
  `modules/<name>/Cargo.toml` so root `Cargo.toml` stays isolated and `--offline` clean-HOME builds
  of `roger` never depend on them; external crates never include harness or model SDKs and never
  relax Rules 1–9.
- Tests per proof: each KERNEL.md proof has a `scripts/acceptance <name>` subcommand
  that exercises real processes, plus the narrowest unit or process test that would
  catch a regression. Match test depth to the risk the proof names.
- Errors are `io::Result`; a failure names what was expected and what was seen.
- Claim deadlines are read on the coordinator's wall clock (`now_ms`), and nothing in
  `src/` or `modules/` reads another clock for a claim. Why, and what a clock step does: README.md
  `## Claims`.
- Keep `src/main.rs` dispatch flat: add an arm, do not restructure.
- Test-only hooks live behind the `test-hooks` feature and never reach a deploy build.
- The director's records, not comments, record what was tried; GLOSSARY.md, not comments, defines terms.

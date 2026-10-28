# Contributing

Read [AGENTS.md](AGENTS.md) before changing code. This project has a director: its purpose, proofs, invariants and terms are sealed in the director's records (`slice status`), and a slice's worktree carries the reduced copy it needs as KERNEL.md, INVARIANTS.md and GLOSSARY.md. Keep changes focused and add tests appropriate to their risk.

Run `scripts/check-all`; it must pass before landing. Run affected acceptance suites too; `scripts/acceptance --help` lists suites and required environment variables.

Do not edit the sealed documents or the acceptance of a slice; a change to either comes from the director, as a new slice attempt.

`VECTORS/` is read-only history; the director keeps the project's state in its records instead.

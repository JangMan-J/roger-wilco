---
type: invariants
created: 2026-10-21T00:15:26Z
agent: accounts/fireworks/models/deepseek-v4p1-flash-1017
session: 01a15ba2-183a-7158-a4c7-5e136b2b5972
---

# Roger-Wilco invariants

These are the anti-goals of Roger-Wilco: what it will never be, serve, enter, or contain, and the invariants no work in it may violate.

## Invariants

- Never loses an accepted Mail, and never delivers one twice: the body survives coordinator restart, host daemon restart, and SIGKILL of its recipient until the recipient's durable record exists. Check: `scripts/acceptance cross-host`.
- Never puts a fixed-interval scan on the correctness path: mail, receipts, and claim expiry follow committed events, and a cleanup sweep can stop without losing or duplicating work. Check: `scripts/acceptance no-poll` passes with every sweep disabled and fails when an event trigger is removed.
- Never interprets the tokens attached to a label or a session: they are stored and returned verbatim, and no code branches on, ranks, or names them. Check: `scripts/acceptance references`, plus a source search that finds no comparison or lookup keyed on either token.
- Never learns which harness runs a session: coordinator, host daemon, client, and adapter name no harness, read no harness store, and link no harness SDK; only a shim knows. Check: `scripts/check-harness-free`.

## Will not be

- A workflow engine: no tasks, assignments, queues, model calls, prompt logic, or decisions about who does what. Consumers decide what work exists and who takes it.
- Bound to a named transport or storage mechanism: this frozen set names no protocol, database, or file format as a requirement, and any mechanism that satisfies the Proofs and Invariants is acceptable. Replacing one is work, not a fork.

## Fork

If KERNEL.md cannot be satisfied while this file holds, whatever the implementation, the project is deadlocked: the goal continues in a new project, and both files stay as written. Any agent may propose that fork by proving the deadlock, and may start it as a proof of concept with local commits. Only the user pushes a fork, and only the user promotes one to primary.

## Frozen

This file and the `invariants` namespace of GLOSSARY.md are frozen. Only a fork the user promotes replaces them.

## Namespace

`invariants`: these terms are defined in GLOSSARY.md under `## invariants`, frozen with this file, and reserved across the glossary.

Committed event, Cleanup sweep

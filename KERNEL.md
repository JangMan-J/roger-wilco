---
type: kernel
created: 2026-10-21T00:15:26Z
agent: accounts/fireworks/models/deepseek-v4p1-flash-1017
session: 01a15ba2-183a-7158-a4c7-5e136b2b5972
---

# Roger-Wilco kernel

Roger-Wilco provides durable mail, presence, and exclusive work claims for any agent session on the owner's machines, across hosts, through a core that knows sessions and not harnesses. It stores opaque token metadata without assigning it meaning, and leaves workflow, meaning, and next actions to consumers.

## Proofs

1. **Cross-host mail survives a recipient kill**
   Check: `scripts/acceptance cross-host` exits 0. It sends directed mail from a session on `decent-spider` to one on `agent-jail`, SIGKILLs the recipient after its store write and before its ack, restarts it, and passes only if the body appears exactly once in the recipient's store, its receipt names that record, and the coordinator holds no duplicate row.
2. **Harness independence**
   Check: `scripts/acceptance independence` exits 0. Sessions from two different harnesses exchange mail through the same coordinator, host daemon, client, and adapter binaries, and `scripts/check-harness-free` exits 0: no harness name in core source and no harness SDK in the dependency graph.
3. **Broadcast reaches every registered session**
   Check: `scripts/acceptance broadcast` exits 0. One broadcast is read by every registered session, including an idle one, and starts no turn.
4. **Claims are exclusive and expire**
   Check: `scripts/acceptance claims` exits 0. A second owner is refused, the owner renews, expiry frees the key, and the board read from both hosts shows one holder.
5. **Wake on demand**
   Check: `scripts/acceptance wake` exits 0. Directed mail to an idle live session starts a turn within its declared wake latency; the same mail to a busy session queues and is delivered at the turn boundary.
6. **References keep both tokens verbatim**
   Check: `scripts/acceptance references` exits 0. `[[153:Meteor Shower:7]]` and `[[0:FLEET:0]]` return byte-identical through send, store, delivery, and board, and a session's label prefix changes its display and never its address or mailbox. An Address `[[153:Meteor Shower:7]]<uuid>` or `[[1a:FLEET:zz]]<uuid>` (a token is base 36 and may be several characters) also returns byte-identical through send, store, delivery, reply, and board, and a relabel changes its display and never that Address or its mailbox.

## Frozen

This file and the `kernel` namespace of GLOSSARY.md are frozen. Only a fork the user promotes replaces them.

## Namespace

`kernel`: these terms are defined in GLOSSARY.md under `## kernel`, frozen with this file, and reserved across the glossary.

Agent session, Mail, Ack, Address, Receipt, Store, Core, Coordinator, Host daemon, Harness, Shim, Tracer, Board, Claim, Wake, Reference, Opaque token, Broadcast, Record id, Host id, Deliver, Register, Accepted, Presence

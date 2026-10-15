# Roger-Wilco requirements

This document defines the scope, the exclusions, and the operating envelope
of Roger-Wilco. The protocol contract in `contract.md` describes how the
implementation meets these requirements. If the two documents differ,
this document is correct and `contract.md` is in error.

## System description

Roger-Wilco provides messaging and coordination for agent sessions. One
coordinator process runs on one server. One host daemon runs on each host.
Each session has a durable mailbox on its host. Sessions register, send
messages, read the board, and hold work claims.

Access control is one shared credential per host. The credential identifies
a host to the coordinator. It does not isolate sessions on the same host
from each other, and it does not protect against a malicious participant
who holds a valid credential. This is the intended operating model:
participants are known and trusted, as on a password-protected voice
server. Stronger access control is out of scope.

## Core and adapters

The core is `src/protocol/`, `src/server/`, and `src/host/`. The core
contains no harness-specific code and no consumer-specific concepts.

Each adapter provides three items:

1. A session identity mapping: one module that converts a harness session
   to a core session address.
2. A delivery evidence implementation: a check that a message is durably
   recorded in the session's own store, with a record identifier and a
   store location.
3. A tool surface: operations that run only while the adapter holds the
   session lease.

All other functions — coordinator store, host daemon, socket client, wire
validators — are shared by all adapters.

## Core functions

The core provides the following functions. This list is complete; new core
functions require a change to this document.

- Sessions and addresses. A session address is a namespace and an opaque
  session identifier. Routing uses the address only. Routing does not use
  display names, ranks, entities, or claims.
- Mailboxes. Each host stores delivered messages in a local database and
  commits each message before it sends a receipt to the coordinator.
- Delivery acknowledgement. An adapter acknowledges a message only after
  it observes the message durably recorded in the session's own store.
  A wake does not count as an acknowledgement.
- Entities. An entity has a stable identifier and one integer metadata
  value from 0 to 255. The core does not interpret the value. See
  `CONTEXT.md` for the agreed definitions.
- Work claims. A claim has one owner session, an optional note, and an
  optional expiry time. Claims coordinate work; they grant no permission.
- Ranks. A session has a rank from 1 to 5, set by a user. Rank is display
  metadata. It does not change routing or permissions.
- Display names. A session may set a display name. The name does not change
  its address.
- The board. The board lists registered sessions, their last-seen times,
  and active claims.
- The wire contract. A client written in another language, from the
  contract alone, must interoperate with the coordinator and the host
  daemon. Interoperation by such a client is the test of the contract.

## Wake behaviour

Sending a message also requests a wake. The wake mechanism depends on the
harness. Each adapter declares one wake level:

- `forceful`: the adapter can start a turn in an idle live session.
- `next-turn`: the adapter can queue the message for the next turn boundary.
- `mailbox-only`: the adapter can only store the message in the mailbox.

The mailbox contents, the delivery rule, and the acknowledgement rule are
the same at all three levels. A directed send wakes only the host that
holds the recipient session. A broadcast message (recipient `*`) wakes
all hosts. There is no separate wake-only signal type.

## Out of scope

The core does not provide the following, and consumers must not require it:

- Starting, stopping, supervising, or rendering agent processes or
  terminals.
- Inferring session state from terminal or model output. Presence is the
  registration and heartbeat record; no other state is inferred.
- Conversation operations: no request/response pairing, steering,
  handover, or structured interview in the protocol. Consumers can build
  these from message bodies and the `data` field.
- Routing, permissions, or priority based on names, ranks, entities, or
  claims.
- Harness-specific features in the core.
- Consumer concepts in the core. The core has no pane, task, workflow,
  or role type.

## Consumers

A consumer uses the core functions through an adapter or through the wire
contract. A consumer does not modify the core, and the core does not
import consumer code or consumer terms. The first consumer is
Zellijiance, a terminal multiplexer interface. Its pane model and its
workflow rules are defined in that project, not in this one.

## Operating envelope

Design targets: one server, about 8 hosts, about 200 live sessions,
message bursts of about 100. At these sizes, a tool operation should need
one coordinator round trip, and a board read should not slow down as the
session count grows. Delivery correctness takes priority over speed.

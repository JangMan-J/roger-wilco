# Roger-Wilco

Roger-Wilco lets sessions exchange messages and coordinate without defining an application's workflow.

## Language

**Entity**:
A named subject that text can reference, independently of tasks, claims, roles, or harnesses.
Its identity does not change when its metadata changes.
_Example_: An agent named FLEET and a subject named Meteor Shower can each have entity references.
_Confirmed_: 2026-10-13, user clarification about agent identities and arbitrary text.
_Avoid_: Task, claim

**Entity metadata**:
An opaque integer attached to an entity, separate from its identity.
Its meaning belongs to consumers outside Roger-Wilco.
_Example_: A consumer interprets a change from 0 to 153 as a status change for the same entity.
_Confirmed_: 2026-10-13, user clarification that the integer is metadata.
_Avoid_: Entity type, authority, rank

**Session address**:
The core identity of a session: a namespace and an opaque session identifier.
Routing, leases, mailboxes, and claims use the address only.
Two sessions may share a call sign; their addresses still tell them apart.
_Example_: `fleet:550e8400-e29b-41d4-a716-446655440000` identifies one session whatever it is called.
_Confirmed_: 2026-10-15, disambiguation of address, entity label, and call sign.
_Avoid_: Display name, session name, user ID

**Call sign**:
An optional label a consumer associates with a session address, through an entity.
The core tracks the entity; the association and its meaning belong to the consumer.
A session needs no call sign, and the core routes only by address.
_Example_: A consumer associates the entity label FLEET with one session address as its call sign.
_Confirmed_: 2026-10-15, user clarification that names are opt-in and not part of this project.
_Avoid_: Display name, session name

**Wake**:
A request, made when a directed message is sent, for the receiving session to attend
to its mailbox. A broadcast makes no wake request.
A wake is not a message type and it is not an acknowledgement.
_Example_: A message sent to an idle Pi session causes a new turn in that session.
_Confirmed_: 2026-10-15, user requirement that sending must be able to wake an agent.
_Avoid_: Signal, ping, notification

**Wake level**:
The strongest wake an adapter can perform, declared by the adapter. One of:
`forceful` (start a turn in an idle live session), `next-turn` (queue for
the next turn boundary), or `mailbox-only` (store only).
_Example_: The Pi adapter is `forceful`; the Muse and Claude Code CLI adapters are `mailbox-only`.
_Confirmed_: 2026-10-15, harness surface study across twenty harnesses.
_Avoid_: Wake capability tier, wake mode

## Unresolved

- Meanings of metadata values belong to consumers.

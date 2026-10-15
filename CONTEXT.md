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

**Wake**:
A request, made when a message is sent, for the receiving session to attend
to its mailbox. A wake is not a message type and it is not an acknowledgement.
_Example_: A message sent to an idle Pi session causes a new turn in that session.
_Confirmed_: 2026-10-15, user requirement that sending must be able to wake an agent.
_Avoid_: Signal, ping, notification

**Wake level**:
The strongest wake an adapter can perform, declared by the adapter. One of:
`forceful` (start a turn in an idle live session), `next-turn` (queue for
the next turn boundary), or `mailbox-only` (store only).
_Example_: The Pi adapter is `forceful`; the Muse CLI adapter is `mailbox-only`.
_Confirmed_: 2026-10-15, harness surface study across twenty harnesses.
_Avoid_: Wake capability tier, wake mode

## Unresolved

- Meanings of metadata values belong to consumers.

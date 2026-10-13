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

## Unresolved

- Meanings of metadata values belong to consumers.

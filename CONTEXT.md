# Roger-Wilco

Roger-Wilco lets sessions exchange messages and coordinate without defining an application's workflow.

## Language

**Entity**:
A named subject that messages can reference, independently of tasks, claims, or roles.
_Example_: Alice and Bob refer to the same `fix-parser` entity in different messages.
_Confirmed_: 2026-10-12, user request for tracked symbolic words or phrases.
_Avoid_: Task, claim

**Entity type**:
A consumer-defined classification of an entity. Roger-Wilco assigns no workflow or authority to it.
_Example_: A consumer distinguishes two meanings of `fix-parser` with different type values.
_Confirmed_: 2026-10-12, user request for arbitrary entity types with room for a byte.

## Unresolved

- Names and meanings of application-specific entity types belong to consumers.

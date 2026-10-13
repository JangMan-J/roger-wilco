# Pi receipt feasibility

The review inspected Pi revision `911b6c98310a70480ab73896c9a9c146519f6ce8`.
Pi's run-path `message_end` event precedes the session append.
Its context-only append path bypasses extension message events.
The SDK's existing `entry_appended` event does not cover ordinary custom-message appends.

The reviewer ran a synthetic in-memory probe without provider network calls.
It observed the following order:

| Path | Observation |
|---|---|
| Idle context-only message | SDK message end followed entry insertion. The extension received no message-end event. |
| Idle trigger-turn message | Message end preceded insertion. Later turn-end and settled events saw the entry. |
| Busy follow-up | Consumed message end preceded insertion. Later lifecycle events saw the entry. |
| Busy context-only message | The current turn-end preceded the deferred flush. |

That probe established entry ordering, not disk durability.
The source writes session entries synchronously but does not sync the file.
A new session can buffer custom entries until another message creates its file.

Roger uses trigger-turn follow-ups.
Initial catch-up, filesystem hints, turn-end, and settled events trigger receipt reconciliation.
This choice avoids a Pi-core change but can delay acknowledgment until a later lifecycle boundary.
It does not promise a universal append notification.

An exact entry in the current session JSONL remains necessary.
Roger syncs a coherent file snapshot and rechecks the session, file, and connection grant.
Neither a lifecycle event nor a socket hint establishes a receipt.
Startup and reconnect cover crashes between persistence and acknowledgment.

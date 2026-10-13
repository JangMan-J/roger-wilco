# Registration scheduling decision

The baseline stored one pending registration per session.
Completion deleted a row only when its key and JSON matched the submitted request.
The worker then waited 500 milliseconds and scanned again.

The migration preserves the queue and exact-match completion rule.
Startup, committed enqueue, and stream reconnect trigger reconciliation.
A failed request retains its durable row and arms a bounded retry delay.
An empty queue has no discovery timer.

The review proposed operation IDs, persistent generations, retry states, and per-key timers.
The selected contract does not require those additions.
Coordinator registration already preserves the cursor on repeated same-host requests.
A lost local reply can cause another registration request, but cannot create another subscription.
The contract promises idempotent effects, not exactly one HTTP attempt.

Presence refresh remains a known periodic operation.
It cannot serve as a hidden inbox scan or the only wakeup for new registration work.

# Coordinator replay decision

The baseline SSE handler queried pending deliveries every 250 milliseconds.
Transport keepalives and receipt requests did not remove that polling loop.

The selected design subscribes to store commit notifications before its first replay read.
A committed send, registration, or receipt marks the relevant connection dirty.
The handler retains dirty state across asynchronous writes.
It rereads durable pending state before returning to idle.

The handler marks a subscription in flight before it writes the frame.
Receipt handling clears that state and requests another replay pass.
That explicit wake matters because the store notification can precede the in-flight deletion.

Keepalive deadlines write comments only.
They do not query pending rows.
SQLite remains the replay authority after reconnect.
Backpressure retains the existing write deadline and closes stalled connections.

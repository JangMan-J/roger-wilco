---
type: glossary
created: 2026-10-21T00:15:26Z
updated: 2026-10-23T05:42:19Z
agent: claude-opus-5-5
session: 01a165ae-cb97-75ae-af83-749678e1ae5e
updated_by: claude-opus-5-5
updated_session: 01a16746-edb6-75b8-917f-a2de7cf2a17c
---

# Roger-Wilco

Durable mail, presence, and work claims between agent sessions on one owner's hosts.

## kernel

Frozen with KERNEL.md.

**Agent session**:
A process that keeps a durable record of what it has received and speaks the adapter contract; the core never asks which harness runs it.
_Example_: a live interactive session and a standalone process with no harness.
_Not_: a shell, a terminal pane, a host.
_Confirmed_: 2026-10-21, Jang, at init

**Mail**:
One record the core stores and delivers: an identifier, a sender Address, a recipient Address, and a body.
_Example_: `accept-1` from one session to another, body `start [[0:build:3]] now`.
_Not_: a wake, a receipt, a tool call, the body alone.
_Confirmed_: 2026-10-21, Jang, at promotion

**Ack**:
The act of naming, to the core, the durable record in which a recipient stored a delivered body.
_Example_: a recipient answering with the identifier of the record it wrote and made durable.
_Not_: a model's reply, an open, a read without a record.
_Confirmed_: 2026-10-21, Jang, at init

**Address**:
The routing identity of an agent session: an opaque string, a UUID with an optional leading Reference (`[[<integer>:<label>:<integer>]]<uuid>`), carried verbatim and never changed by a relabel.
_Example_: a session registered as `[[3:AGENT:12]]<uuid>` keeps that Address when its display label goes from `AGENT` to `BUILDER`.
_Not_: a display label, a display prefix, a call sign, the UUID alone when the session registered with a Reference.
_Confirmed_: 2026-10-21, Jang, at init

**Receipt**:
The record the core keeps once a recipient acks delivery, naming that recipient's own durable record.
_Example_: a receipt naming record `r-1` after the recipient acked it.
_Not_: a notice that mail was sent, a lease.
_Confirmed_: 2026-10-21, Jang, at init

**Store**:
The durable record an agent session keeps of the mail it has received, written and made durable before that session acks.
_Example_: the append-only record a tracer writes, then names when it acks.
_Not_: the coordinator's state, an inbox, a harness transcript.
_Confirmed_: 2026-10-21, Jang, at init

**Core**:
The coordinator, host daemon, client, and adapter: every part that must not know a harness.
_Example_: the binaries installed on both hosts.
_Not_: a shim.
_Confirmed_: 2026-10-21, Jang, at init

**Coordinator**:
The single central process that holds durable state for the fleet and connects to each host daemon.
_Example_: the process owning the mail, session, and claim store that sessions on both hosts reach.
_Not_: a host daemon, a shim.
_Confirmed_: 2026-10-21, Jang, at init

**Host daemon**:
The one process per host that connects outbound to the coordinator and keeps the local inbox for the sessions on that host.
_Example_: the agent-jail process that keeps the local inbox for the sessions on agent-jail.
_Not_: the coordinator, a shim.
_Confirmed_: 2026-10-21, Jang, at init

**Harness**:
The program that runs an agent session and owns its model loop, transcript, and interface.
_Example_: an interactive coding agent, a batch agent, a plain command-line agent.
_Not_: a shim, the adapter.
_Confirmed_: 2026-10-21, Jang, at init

**Shim**:
The mechanical, per-harness program that reports what its harness observes, forwards tool calls, and renders results; it never acknowledges a delivery.
_Example_: the shim that forwards its harness's observations and tool calls to the adapter.
_Not_: the adapter, the core.
_Confirmed_: 2026-10-21, Jang, at init

**Tracer**:
A standalone process with no harness, used as the second session in the independence proof.
_Example_: a process that registers, reads mail, writes a durable record, acks, and prints the body.
_Not_: a fixture that fakes the core.
_Confirmed_: 2026-10-21, Jang, at init

**Board**:
The readable list of registered sessions and active claims, open to every session.
_Example_: the presence view a sender reads to find an address.
_Not_: a mailbox.
_Confirmed_: 2026-10-21, Jang, at init

**Claim**:
Exclusive ownership of one key by one session, renewed by that session alone, and freed by release or TTL expiry.
_Example_: one session holding `[[0:build:0]]` for 900 seconds.
_Not_: a lock on a host, a task assignment.
_Confirmed_: 2026-10-21, Jang, at init

**Wake**:
A request, made when mail is sent, for the receiving session to attend to its mailbox; a broadcast makes no wake request.
_Example_: a forceful wake starting a turn in an idle live session.
_Not_: a message type, a receipt.
_Confirmed_: 2026-10-21, Jang, at init

**Reference**:
A label with an opaque integer on each side, written `[[<integer>:<label>:<integer>]]`; it names a subject and carries nothing the core reads.
_Example_: `[[153:Meteor Shower:7]]` and `[[0:FLEET:0]]`.
_Not_: an address by itself (an Address may begin with one), a claim key with reserved meaning.
_Confirmed_: 2026-10-21, Jang, at init

**Opaque integer**:
A number attached to a label or a session, stored and returned verbatim and never interpreted by the core; a consumer may give it meaning.
_Example_: the `153` and `7` in `[[153:Meteor Shower:7]]`, or a supervisor's rank and spawn sequence in an Address, which the core never reads.
_Not_: a rank, a type, or a status to the core.
_Confirmed_: 2026-10-21, Jang, at init

**Broadcast**:
One mail body addressed to every registered session; it carries no wake request.
_Example_: `build finished` sent to all sessions on both hosts.
_Not_: mail addressed to a list of sessions.
_Confirmed_: 2026-10-21, Jang, at init

**Record id**:
The identifier a recipient's Store gives the entry in which it wrote a delivered Mail body; the Receipt names it.
_Example_: `r-1`, named by the ack that follows the Store write.
_Not_: a Mail's identifier, an Address.
_Confirmed_: 2026-10-21, Jang, at promotion


**Host id**:
The name a host daemon gives the machine it serves; the coordinator uses it to decide which host daemon holds a Mail's recipient.
_Example_: `agent-jail` for the daemon serving the sessions on agent-jail.
_Not_: an Address, a harness.
_Confirmed_: 2026-10-21, Jang, at promotion


**Deliver**:
The act of handing one Mail to the recipient's host daemon, or from that daemon to the recipient session, following a committed event or a Register rather than a scan that looks for work; it is not an acknowledgement and starts no turn.
_Example_: the Mail that reaches a session and is written to its Store as soon as the coordinator commits it.
_Not_: an Ack, a Wake, a send, a fixed-interval scan.
_Confirmed_: 2026-10-21, Jang, at promotion

**Register**:
A session announcing its Address to its host daemon, which tells the coordinator and then delivers every Mail already pending for that Address.
_Example_: a restarted session re-announcing itself and receiving the Mail it was killed before acking.
_Not_: a Claim, a presence report.
_Confirmed_: 2026-10-21, Jang, at promotion

**Accepted**:
The coordinator's answer that a Mail is durable in its own store; from that moment the invariant against loss covers it.
_Example_: the answer to a send, after which killing the recipient cannot lose the body.
_Not_: a Receipt, a Deliver.
_Confirmed_: 2026-10-21, Jang, at promotion



**Presence**:
The fact that an Address is registered and recently seen, which the Board reports.
_Example_: a sender reading the Board to find a live recipient.
_Not_: a Claim, a Wake, a guarantee that a session is alive.
_Confirmed_: 2026-10-21, Jang, at promotion

## invariants

Frozen with INVARIANTS.md.

**Committed event**:
A change to stored state that is durable and that triggers notification, rather than being found by a fixed-interval scan.
_Example_: a mail insert that wakes the local delivery path.
_Not_: a timer tick.
_Confirmed_: 2026-10-21, Jang, at init

**Cleanup sweep**:
A pass that removes expired state, which correctness may not depend on.
_Example_: the pass that drops mail acked seven days ago.
_Not_: the delivery path.
_Confirmed_: 2026-10-21, Jang, at init

## Language

**Label**:
Display text the core stores and returns verbatim and never parses: either an agent session's label, which the session gives at Register or any session sets by relabel and which the Board shows beside its Address, or the text between the two integers of a Reference. A session's label may itself contain References.
_Example_: a session labelled `[[0:FLEET:0]] builder`; `Meteor Shower` in `[[153:Meteor Shower:7]]`.
_Not_: an Address (a display label never routes in the core, and the label inside an Address is part of its opaque string; a shim may resolve a typed label to an Address before it sends), a claim key, an Opaque integer.
_Avoid_: name, call sign, rank
_Confirmed_: 2026-10-22, by Jang (first requested by the director in the roger-command-polish slice, 2026-10-21).

**Unregister**:
The end of the registration an Agent session made by Register: its Address leaves the Board, loses Presence, and receives no later Broadcast, while every Accepted Mail for that Address stays pending and is delivered if the Address registers again. It happens explicitly, which the adapter does when its stdin closes, or once the session has been disconnected from its host daemon for `--unregister-after-ms` (default 300000), a timeout the coordinator reads at its next Broadcast, Board read, Claim or relabel, as it reads Claim expiry, and never by a Cleanup sweep.
_Example_: a session whose adapter's stdin closes leaves the Board at once; Mail sent to its Address afterwards is Accepted and delivered when that Address registers again.
_Not_: a disconnect (a disconnected session stays registered until its timeout), a Claim release, removal of the session's Store or its pending Mail.
_Avoid_: deregister, logout
_Confirmed_: 2026-10-22, Jang ("Unregister is a very reasonable function to add").

**Epoch**:
The instant 1970-01-01T00:00:00Z, from which the core's clock readings count milliseconds.
_Confirmed_: 2026-10-23, Jang

**Accepted at**:
The coordinator's clock, in milliseconds since the Epoch, when it commits a send. Broadcast copies carry it under `<broadcast>/<address>`.
_Confirmed_: 2026-10-23, Jang

**Receipted at**:
The coordinator's clock, in milliseconds since the Epoch, when it records a Receipt, one per broadcast copy.
_Confirmed_: 2026-10-23, Jang

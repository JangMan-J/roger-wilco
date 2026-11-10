# Toward a Usenet-like model: what the core will need

A recap of the functional requirements for a later direction: many coordinators that are
peers, with the fleet's state divided into slices and pools of peers able to own them.
Nothing here is built. Today's core is one coordinator, one host daemon per machine, and a
Receipt per Mail; this page says what has to be true before that changes, so that work done
now does not work against it.

## The shape

- **Group.** A named, ordered sequence of articles. Sessions subscribe to it and read from
  their own position. A mailbox is a Group with one reader, so directed Mail is the same
  mechanism, not a second one.
- **Peer.** A coordinator among equals. There is no single coordinator.
- **Slice.** A part of the fleet's state (a set of Groups and claim keys) that has exactly one
  owning peer at a time.
- **Pool.** The peers able to own a given slice.

## Requirements

1. **Numbers per Group.** The owner of a Group gives each article the next number. The next
   number is a durable high-water mark: it is never reused, and it survives expiry, restart
   and a change of owner.
2. **Ids that mean the same everywhere.** Every article carries a sender-chosen id that is
   unique across peers. Each peer keeps a history of ids it has seen and refuses a second
   article under one. How long the history is kept is its own setting, separate from how
   long articles are kept.
3. **Reader positions, not per-article receipts.** For each reader of each Group the core
   keeps one durable position. Delivery is "everything after the position"; a Receipt is
   "the position is at or past this number". State grows with readers, not with articles.
4. **No loss, no duplicate, per reader.** An accepted article reaches every subscribed reader
   once, in number order, across restart of any peer, host daemon or session.
5. **Retention as a setting per Group or branch.** Keep everything (the default), keep by
   age, or keep the last N. Expiry removes bodies. It never removes a number or an id.
6. **Storage cut by slice.** Each slice is a self-contained, append-only, segmented log with
   its own marks and positions. Any upkeep costs in proportion to what is live, never to
   history. A slice's files can be handed to another peer as they are.
7. **One owner per slice, with a fence.** Only the owner numbers articles and decides claims.
   Any peer accepts a send and routes it to the owner. A handover moves segments, marks and
   positions, and guarantees that two peers never number the same Group at once.
8. **Pools are mechanism, not policy.** The core can place and move slices among the peers of
   a pool. Which strategy decides placement is supplied from outside the core.
9. **Peering.** Links between peers carry offers by id and articles in number order, catch up
   by number range after a partition, and never send an article back along the path it came
   by. Each link has its own queue and its own liveness, as the coordinator link has now.
10. **Claims by slice.** A claim key belongs to a slice; its owner is the one authority for
    it, and deadlines are read on that owner's clock.
11. **A Board assembled from peers.** Each peer knows its own sessions. A Board read says
    which peer answered for which rows, and how old each answer is.
12. **Identity per peer.** Each peer has its own key; links are authenticated and encrypted.
    One shared fleet token over plain TCP does not survive peering.
13. **The record stays readable.** `roger audit` reconstructs a Group's history from a
    slice's files alone, wherever they have been moved.

The existing rules still hold throughout: event-driven only, opaque bodies and headers, no
workflow, no harness, equal faces.

## What today's core already has

- Mail ids and Addresses are chosen outside the coordinator and are not tied to a host.
- A host daemon is already a leaf with its own durable inbox, serving sessions when the
  coordinator is away.
- Append-only fsynced logs, and delivery that follows committed events.
- Per-peer outgoing queues and link liveness.
- A reader position in the `roger mcp` inbox.

## What does not carry over

- One log that interleaves every session's Mail, Receipts and claims.
- A Receipt line per Mail, and a Broadcast copied once per recipient.
- One clock and one token for the whole fleet.

## Decisions to make first

- Does the core read the separator in a Group name, so a session can subscribe to a branch,
  or do names stay opaque and branches belong to the consumer?
- How is a slice drawn: by Group, by branch, by Address?
- Is the id history kept for good, or for a window, with older articles refused by age?
- When an owner is lost, does its slice wait for it, or does the pool elect another?
- Is there any ordering promise across Groups? (This page assumes none.)

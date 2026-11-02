# Consumer conventions

Recommended, not enforced. Everything on this page is a convention a consumer may adopt on
top of Roger; **the core carries each of these fields byte-identical and never reads,
checks, compares or ranks any of them.** A consumer that ignores this page loses nothing
from the core, and a consumer that follows it gains nothing from the core beyond what it
already has: the record. The point of the page is that two consumers who follow it can
verify each other's Mail mechanically, from the record alone, without trusting a body.

Terms (Mail, Address, Register, Receipt, Store, Record id, Board, Header, Kind) are the
core's, as the README uses them.

## What the record proves and what it does not

The coordinator is the one writer of the fleet's durable log and the one clock. What it
stamps itself is evidence; what it carries for a session is that session's word.

| Field | Origin | Evidence of |
| --- | --- | --- |
| `accepted_at_ms` | coordinator clock at commit | when the core first held the Mail |
| `receipted_at_ms` | coordinator clock at the Ack | when the recipient's durable record existed; `latency_ms` is the difference, one clock, no skew |
| `kind` | coordinator | how the Mail was routed: `directed`, or a `broadcast` copy `<mail>/<address>` |
| `record` | the recipient's Store | which durable entry holds the body; the same id on a redelivery |
| `host` on a Register | the host daemon's `--host-id` | which host daemon the session spoke to |
| `at_ms` on a Register, unregister, label or declared line | coordinator clock at commit | when the registration event happened (absent on lines written before it existed) |
| log order | the append-only log | the order the coordinator committed events |
| `from` on a send | the sender, unchecked | nothing: any string is forwarded, registered or not |
| the Address at Register | the session, unchecked | nothing: any session may register any Address, including one another session holds; the newest Register takes it |
| `--declare` text | the session, unchecked | nothing by itself |
| label | any client on the socket, unchecked | nothing: any session may relabel any registered Address |
| headers, body | the sender, bounded, unchecked | nothing by themselves |
| tokens in an Address or Reference | whoever composed them | nothing to the core; a session's own claim about itself |

Access to the host daemon is the Unix socket, so every process that can reach it has
every right above. Nothing a session says about itself is evidence. The conventions below
make the unchecked fields evidence by having **a party other than the session**, the
consumer's own authority, sign them, and by having the reader verify the signature against
a key it already trusts.

## Replacement registration

Newest Register wins without refusal or a recovery secret. A client receiving `superseded`
should stop sending as that Address, tell its supervisor/user, and not automatically
re-register. This is the consumer's convention, not enforcement by the core: the connection
stays alive and sends, Mail and Acks remain usable.

The notice is `{"type":"superseded","session":"<full Address>","host":"<new host>","at_ms":<integer>}`.
The coordinator stamps it and commits a `takeover`; audit exposes `TAKEOVER` and
`ADDRESS_TAKEOVER` with the full Address, old/new hosts, stamp and log line. Neither event
nor stamp authenticates the new registrant or proves the old connection received it.

Raw socket consumers opt in with `notify_superseded:true` on Register, independently of
`confirm_register`. Adapter stderr always warns; stdout emits the notice only with
`--superseded`, not merely `--registered`. Tracer prints `SUPERSEDED <JSON notice>`.
MCP stderr warns and `roger_registered` retains the notice in `superseded` (otherwise null),
reports `registered:false`, and ends any registration wait; a late confirmation cannot
clear it. Tools are not disabled. See README Registration/Faces for the complete contract.

Observer notices are best-effort, not durable Mail. Transport failure or process death can
lose one; use the audit for recorded evidence. A daemon reconnect still re-announces its
latest live local connections, including a previously superseded connection: receiving a
notice does not change that old recovery behavior. The client must implement its own action.

## The authority

A consumer has something the core does not: it knows which sessions it started, at which
Address, with which tokens, under which parent. Call that the consumer's **authority**: the
launcher, the registry, or the service that holds that roster and a signing key. The core
never knows it exists. Every verification below ends at the authority's key, never at a
body, a `from`, a label, or a token.

## `--declare`: a signed statement of who the session is

Register with `--declare` (the Tracer, the adapter and `roger mcp` all take it) set to one
JSON object:

```json
{"scheme":"roger-declare/1",
 "address":"[[3:BUILDER:1]]3b0d5b6e-7e3d-4a0e-9b1c-2a7f1c9d0e11",
 "facts":{"parent":"[[1:LEAD:0]]9e2a…","role":"builder","launch":"L-4417"},
 "nonce":"f3a1…",
 "issued_at_ms":1793545146038,
 "kid":"authority-2026",
 "sig":"<base64 Ed25519 signature>"}
```

- `scheme` names this convention and its version so a reader can refuse what it does not know.
- `address` is the exact Address string the session registers with, so a declaration copied
  onto another Address fails verification.
- `facts` is the consumer's: parent Address, role, launch id, whatever its authority
  asserts. Put what the consumer will later want to read; read it only after the signature
  holds.
- `nonce` differs for every process start. The coordinator commits a `declared` line only
  when the text differs from the one it holds for that Address (a second Register with the
  same text leaves no line), so a nonce makes every start visible in the log, including a
  second process taking an Address the first still holds.
- `sig` is the authority's signature over the canonical form below, with every field but
  `sig` itself. The authority signs when it launches the session and hands the text to it;
  the session never holds the authority's key.

Canonical form to sign: the JSON object without `sig`, keys sorted, no whitespace, UTF-8.
Any signature algorithm; name it in `scheme` (`roger-declare/1` means Ed25519 over that form).

The core stores the text, returns it on the Board (`DECLARED address= declared=`) and in the
coordinator log (`{"ev":"declared","session","declared"}`), and `roger audit` lists it in
log order. It bounds headers, not declared text; keep the object small.

**Verifying a declaration.** Take the text for the Address from the Board (live), from the
coordinator log or `roger audit` (after the fact); parse it; check `scheme`; check `address`
equals the Address the text is attached to; verify `sig` with the key named by `kid`, which
the reader must already trust; then, and only then, read `facts`. A session whose current
declared text fails any step is unverified, whatever its tokens, label or body say. A
declared line that clears the text (`{"ev":"declared","session"}` with no `declared`) means
a Register without `--declare`: unverified from that line on.

## Headers: `sent_at_ms`, `seq`, `sig`

Attach to every Mail (`send --header`, `"headers"` on the adapter's `send`, `headers` on
`roger_send`):

| Header | Value | Use |
| --- | --- | --- |
| `sent_at_ms` | the sender's clock at send, ms since the epoch, decimal | compare with `accepted_at_ms` (coordinator clock) for the sender's skew and the hop's latency; never order Mail from two senders by it |
| `seq` | a per-sender counter, decimal, starting at `1`, never reused across restarts (persist it) | a gap or a repeat at the recipient shows a Mail lost or duplicated outside the core, or two processes sending as one `from` |
| `body_sha256` | hex SHA-256 of the body bytes | integrity before the signature is checked; cheap to compare |
| `kid` | which key signed | key rotation |
| `sig` | base64 signature over the canonical form below | the Mail was produced by the holder of that key, for this recipient, under this id |

Canonical form to sign (`roger-sig/1`, Ed25519), seven lines joined by `\n`, no trailing newline:

```
roger-sig/1
<from>
<to>
<mail id>
<seq>
<sent_at_ms>
<hex sha256 of body>
```

`from`, `to` and the Mail id are the strings the send names. For a Broadcast, `to` is `*`
and every copy carries the same headers, so a recipient verifies with `to` = `*`, not its own
Address. The Mail id is in the form so a body replayed under a new id does not verify. The
bounds hold easily: at most 32 headers, keys 1 to 128 bytes, values up to 4096 bytes.

**Verifying a Mail.** For a delivery (a `mail` line on the adapter stream, a `roger_inbox`
entry, a Store entry): `from` names an Address; verify that Address's declaration (above) to
learn whose key to expect; check `body_sha256`; recompute the canonical form from the
delivery's own `from`, the recipient's Address (or `*` for kind `broadcast`), the Mail id and
the headers; verify `sig`; check `seq` against the last one seen from that `from`. Then
apply the consumer's own policy to `facts`. A Mail that fails any step is unverified mail
from an unverified sender; what to do with it is the consumer's, not the core's.

## Who is who: the authority, never the body

What a consumer reads into a token (a level in its own hierarchy, a parent, a role) is its
own, and the core does not stop a session from registering an Address whose tokens say
anything. So:

- A token proves nothing. A declaration signed by the authority that binds this exact
  Address to those facts proves that the authority launched a session there with them.
- `from` proves nothing. A header `sig` under the key the declaration names proves the
  holder of that key sent this Mail.
- A label proves nothing; it is display text any client can change.
- A body proves nothing. Never read identity, parentage or permission from a body.
- The Board proves presence as the core saw it (`connected`, `last_seen_ms`), not identity.

The authority's roster is the one source: which Addresses it launched, under which parent,
with which key. A consumer that must know "may this sender ask this of me" asks its
authority (or verifies the authority's signature on what the sender declared), never the
sender.

## After a crash

Stop nothing; copy the coordinator log, the host daemon log and the Stores read-only, and
run `roger audit` over the copies (see the README). It lists, for one Address or all, every
Register with its declared text, every label, every unregister, every Mail with both stamps,
latency, kind, headers verbatim and the recipient's Record id, and it flags Mail accepted
and never receipted, a Record id naming more than one Mail, and a Record id that differs
between the coordinator's receipt, the host daemon's receipt and the Store. Declared texts
and headers come out byte-identical, so every verification on this page can be repeated
from the copies. The audit reads nothing's meaning; it does not verify a signature. That is
the consumer's.

## What the core does with all this

Nothing. It bounds the header set once (count, key and value sizes, unique keys, string
values) and refuses one outside the bounds with a reason; it stores and returns everything
else byte-identical. No code in the core parses a declared text, a header value or a token,
and the gates (`scripts/acceptance references`, `scripts/acceptance headers`,
`scripts/check-vocabulary`) keep it so. A convention here may change without the core
noticing, and the core may change without any convention noticing.

## Not covered

Bodies and logs are plain text readable by any process of the same user; this page does not
encrypt anything. Access control to the host daemon's socket is the operating system's.

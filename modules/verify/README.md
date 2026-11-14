# `roger-verify`

An Optional module that checks the consumer provenance conventions of
[docs/CONVENTIONS.md](../../docs/CONVENTIONS.md) over a `roger audit` listing:
`roger-declare/1` declared texts on Registers and `roger-sig/1` signature headers on
Mail. It reads records only. It never connects to a host daemon or coordinator, and it
sends, stores and acknowledges nothing. The core does not change and never learns that
this module exists.

## Build

```sh
cargo build --release --manifest-path modules/verify/Cargo.toml
```

## CLI

```text
roger-verify --roster <file> [--audit <file>|-] [--json] [--allow-undeclared]
```

- `--roster <file>`: the keys the reader already trusts (format below).
- `--audit <file>`: output of `roger audit --bodies --json`. Without it, or with `-`,
  stdin is read. Run the audit **unfiltered** (no `--address`) and **with `--bodies`**.
  With `--address`, other senders' Mail and declarations are missing, which shows up as
  sequence gaps and undeclared senders. Without `--bodies`, every signed Mail fails
  `body_unavailable`.
- `--json`: one JSON object per line, `type` set to the word, the same fields. Without
  it, output is one `WORD key=value ...` line per record (`absent` for null, lists
  comma-joined, `none` for an empty list). Use JSON when values may contain spaces.
- `--allow-undeclared`: do not require a Mail's `from` to hold a verified declaration.

```sh
roger audit --coordinator-log coord.log --bodies --json | roger-verify --roster roster.json
```

### Exit status

| Code | Meaning |
| --- | --- |
| `0` | every Mail and every declared text verified, and no sequence gap |
| `1` | at least one failed Mail, failed declaration or `SEQ_GAP` |
| `2` | usage error, unreadable or invalid roster, unreadable input, or an input line that is not JSON |

## Roster format

One JSON object, `roger-roster/1`:

```json
{"scheme":"roger-roster/1",
 "authorities":{"authority-2026":"<base64 Ed25519 public key>"},
 "signers":{"[[3:BUILDER:1]]3b0d5b6e-7e3d-4a0e-9b1c-2a7f1c9d0e11":
              {"builder-key-1":"<base64 Ed25519 public key>"}}}
```

- `authorities`: `kid` -> key that signs `roger-declare/1` texts (the declaration's `kid`).
- `signers`: exact Address -> (`kid` -> key) that may sign Mail sent with that `from`
  (the Mail's `kid` header). More than one `kid` per Address allows key rotation.
- Keys are 32-byte Ed25519 public keys in standard base64 with padding (RFC 4648 §4).
- Addresses are whole strings, compared for equality only. No token in them is read.

The roster is the authority's word, which CONVENTIONS.md describes ("which Addresses it
launched ... with which key"). The verifier trusts nothing a session says about itself.

## Output records

Records appear in audit order (declarations and Mail interleaved as the coordinator
committed them), then the gaps, then the summary.

| WORD | Fields |
| --- | --- |
| `DECLARATION` | `address= line= kid= nonce= status=verified\|failed failures= detail=` |
| `DECLARATION` | `address= line= status=cleared failures=` (an `UNDECLARED` line) |
| `MAIL` | `mail= kind= from= to= line= kid= seq= status=verified\|failed failures= detail=` |
| `SEQ_GAP` | `from= missing_first= missing_last=` (one contiguous range of missing `seq`) |
| `VERIFY` | `mails= mails_verified= mails_failed= declarations= declarations_verified= declarations_failed= seq_gaps= exit=` |

`mail`, `kind`, `from`, `to` and `line` are copied from the audit's `MAIL`.
`detail` gives the reason for each failure in expected/saw form.

### Mail failure classes

| Class | When |
| --- | --- |
| `missing_signature` | no `sig` header (the headers are absent or empty, or the set has no `sig`) |
| `malformed_headers` | `sig` is present, but `kid`, `seq`, `sent_at_ms` or `body_sha256` is missing or not a string; `seq` is not a decimal number of at least 1; `sent_at_ms` is not decimal; `body_sha256` is not 64 hex digits; or `sig` is not base64 of 64 bytes |
| `unknown_signer` | the roster has no key under the `kid` header for this exact `from` |
| `body_hash_mismatch` | `body_sha256` is not the SHA-256 of the body's UTF-8 bytes |
| `body_unavailable` | no `BODY` line (the audit was run without `--bodies`) |
| `invalid_signature` | the Ed25519 signature (strict verification) over the `roger-sig/1` form fails |
| `replayed_seq` | a validly signed Mail reuses a `seq` that an earlier validly signed Mail with a different Mail id from the same `from` already used |
| `sender_undeclared` | `from` has no declared text at this point in the log, or its text was cleared |
| `sender_declaration_invalid` | `from`'s current declared text failed verification |

### Declaration failure classes

| Class | When |
| --- | --- |
| `malformed` | not a JSON object; a field of `scheme`, `address`, `nonce`, `kid`, `sig` (strings), `facts` (object) or `issued_at_ms` (integer) is missing or the wrong type; or a key repeats |
| `unknown_scheme` | `scheme` is not `roger-declare/1` |
| `address_mismatch` | `address` is not the Address the text is declared on |
| `unknown_key` | `kid` is not a roster authority |
| `invalid_signature` | the Ed25519 signature over the canonical form fails |
| `replayed_nonce` | the `nonce` already appeared in an earlier declared line (for any Address) |

## Readings of CONVENTIONS.md

Where the page is silent, this module takes the most literal reading:

- **Declared canonical form.** "The JSON object without `sig`, keys sorted, no
  whitespace, UTF-8" means the declared text with all whitespace between tokens removed,
  the top-level `sig` member dropped, and members sorted at **every** depth (`facts`
  included) by the UTF-8 bytes of the decoded key. Every token is otherwise kept
  **exactly as written**: string escapes (`\u00e9` stays `\u00e9`, `\/` stays `\/`),
  number spellings (`1.50`, `1E3`), and literals. An authority that serializes compactly
  with sorted keys (and signs that output) produces a text this form reproduces. A
  repeated key has no canonical form, so it is `malformed`. Only the top-level `sig` is
  removed; a `sig` inside `facts` is signed.
- **Mail canonical form.** Seven lines joined by `\n`, no trailing newline:
  `roger-sig/1`, `from`, `to`, Mail id, and the `seq`, `sent_at_ms` and `body_sha256`
  header values **verbatim**. The hash line is the header's value, not a recomputed hash.
  The body is checked against it separately, so a body changed after signing reads
  `body_hash_mismatch` while the signature still holds. The hash comparison ignores
  ASCII case. The signature covers the header's exact spelling.
- **Broadcast.** The audit lists each copy as `<mail>/<address>`. The send named
  `<mail>`, so the form uses that id (the copy id without its `/<to>` suffix) and `to`
  = `*`. All copies of one Broadcast share one `seq` and are not replays of each other.
- **Required headers.** The page says to attach all five headers to every Mail, so a
  signed Mail that lacks any one of them is `malformed_headers`.
- **Signer key.** The page says a sender's declaration tells "whose key to expect". It
  does not say how, so the roster's `signers` entry for the exact `from` names the
  keys, selected by the `kid` header. The sender's declared text must also verify,
  unless `--allow-undeclared` is given.
- **Current declaration.** The declared text that applies to a Mail is the most recent
  `DECLARED`/`UNDECLARED` line for `from` **before** the Mail in log order.
- **seq.** It is tracked per `from` and only for Mail whose signature verified, because
  an unsigned `seq` is the sender's unchecked word. It starts at `1`, so a first seen
  `seq` above 1 is a gap. Gaps are counted against every `seq` seen, so out-of-order
  commits are not replays. A gap fails the run but is not attributed to any one Mail.
- **Nonce.** "Differs for every process start" is read as global: a nonce seen on any
  earlier declared line, for any Address, is `replayed_nonce`.
- **Base64.** Standard alphabet, padding required (RFC 4648 §4), for signatures and keys.
- **What is checked.** Only the coordinator timeline in the audit is checked: `MAIL` with
  the `HEADERS`/`BODY` lines right after it, `DECLARED` and `UNDECLARED`. Daemon and
  Store records in the same listing are passed over. A `MAIL` with no `HEADERS` line has
  an explicitly empty header set.

Nothing here changes what the core does. The core still carries every header and
declared text byte-identical and checks none of them. This module is one consumer's
reader of those fields, as CONVENTIONS.md invites.

Invariant `I3` permits exactly this: a module may read named header fields only to check
that recorded data matches and to report match or mismatch, and a module that names a
header key has no path to delivery (no socket, listener, spawned process, or file write).
The source search in `scripts/acceptance headers` fails if this module gains one.

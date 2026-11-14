# `roger-ws-gateway`

`roger-ws-gateway` is an optional WebSocket face for Roger-Wilco. Each WebSocket connection is
one session speaking exactly the `roger adapter` JSON-line protocol (documented at `adapter` in
`src/session.rs` of the core): one text frame carries one JSON line, in both directions. It adds
no message semantics: Mail, Store, Ack, Board, claim and receipt behave as on the adapter.

## Build

```sh
cargo build --release --manifest-path modules/ws-gateway/Cargo.toml
```

## CLI

```text
Usage: roger-ws-gateway --bind <address:port> --socket <path|tcp://address:port> --store-dir <dir>
                        [--port-file <path>] [--token-env <NAME>]
```

- `--bind <address:port>`: TCP listener for WebSocket upgrades.
- `--socket <path|tcp://address:port>`: the host `roger daemon` Unix socket, or its
  `--listen` endpoint as `tcp://<address:port>` (dialed with `TCP_NODELAY`).
- `--store-dir <dir>`: per-Address Store files, named `<hex_sha256_of_address>.store`, in the
  core's Store format (readable by `roger audit --store`).
- `--port-file <path>`: written atomically (`<path>.tmp`, `fsync`, `rename`) with the bound port.
- `--token-env <NAME>`: environment variable holding a Bearer secret. Every upgrade must carry
  `Authorization: Bearer <token>` (compared in constant time) or is answered
  `401 Unauthorized` with `WWW-Authenticate: Bearer` before any session exists.
- `--hold-before-ack <marker>` (`test-hooks` feature only): after the first delivery is stored
  and sent to the client, create `<marker>` and park before the Ack.

Environment: when `ROGER_CLIENT_TOKEN` is set and non-empty and `--socket` is `tcp://`, every
connection to the host daemon starts with `{"type":"auth","token":"<value>"}`. Unix-socket
connections never send it.

## Upgrade

```text
GET /?id=<address>[&label=<text>][&declare=<text>][&registered=1][&superseded=1][&after=<record>]
```

- `id` (alias `session`): the Address, percent-decoded and otherwise passed through
  byte-identical (`+` is a literal plus; encode a space as `%20`). Absent or empty: a fresh
  UUIDv4, reported in `ready`.
- `label`, `declare`: as `roger adapter --label` / `--declare`.
- `registered=1`: as `--registered`; `superseded=1`: as `--superseded` (`0` or absent: off).
- `after=<record>`: resume (below). `after=` with an empty value replays the whole Store.

Refusals, all before the upgrade: `401` (Bearer), `404` (path other than `/`), `400` (unknown
or repeated query key, bad flag value or percent escape, `after` naming no record in this
Address's Store), `409` (the Address's Store is held by another live connection).

## Protocol

Client to gateway: `{"type":"send","to","body"}` (optional `"mail"`, `"headers"`),
`{"type":"board"}`, `{"type":"relabel","label"}` (optional `"session"`),
`{"type":"claim","op","key"}` (optional `"ttl_ms"`). Each is answered, in order, by one of
`accepted`, `board`, `relabeled`, `claim_result`, or `{"type":"error","error":"..."}`. A frame
that is not JSON, or a binary frame, is answered with `error`.

Gateway to client, unsolicited: `{"type":"ready","session"}` once the Register is written;
`{"type":"registered","session"}` (only with `registered=1`); `{"type":"superseded","session",
"host","at_ms"}` (only with `superseded=1`; stderr always warns); and per delivery
`{"type":"mail","mail","from","body","record","kind","headers"}`.

Per delivery the gateway appends the entry to the Address's Store and `fsync`s it, then sends the
`mail` frame, then sends the Ack to the host daemon. Mail already in the Store reuses its record.
When the client closes the connection the gateway sends `{"type":"unregister"}` and releases
the Store; Mail accepted later stays pending at the coordinator. When the host daemon
connection ends, the gateway closes the WebSocket.

## Resume

With `after=<record>`, after `ready` the gateway sends every Store entry that follows the line
holding `<record>`, in Store line order (record ids are never ranked), as `mail` frames, and then
goes live. A replayed Mail that the host daemon redelivers on this connection is stored once
and acked, not sent to the client twice. Without `after`, nothing is replayed: the host daemon
redelivers what was never acked.

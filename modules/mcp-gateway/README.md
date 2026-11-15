# `roger-mcp-gateway`

`roger-mcp-gateway` is an optional multi-session Model Context Protocol (MCP) Streamable HTTP and Server-Sent Events (SSE) push gateway for Roger-Wilco. It serves HTTP/1.1 and prior-knowledge HTTP/2 (`h2c`) from a single TCP listener so containerized MCP clients across local or remote network namespaces can register sessions, receive real-time push notifications as Mail commits to their durable Store, resume seamlessly with `Last-Event-ID` or `roger_inbox` Cursor files, and invoke all 9 Roger MCP tools with exact wire and semantic parity to `roger mcp`.

## Build

```sh
cargo build --release --manifest-path modules/mcp-gateway/Cargo.toml
```

## CLI

```text
Usage: roger-mcp-gateway --bind <address:port> --socket <path|tcp://address:port> --store-dir <dir>
                         [--port-file <path>] [--endpoint <path>] [--token-env <NAME>]
```

- `--bind <address:port>`: TCP listener serving both HTTP/1.1 and HTTP/2 (`h2c`).
- `--socket <path|tcp://address:port>`: host `roger daemon` Unix socket path (Linux/macOS) or `tcp://<host:port>` endpoint (Linux, macOS, Windows; dialed with `TCP_NODELAY`). When `ROGER_CLIENT_TOKEN` is set, the gateway sends `{"type":"auth","token":...}` as the first line of every `tcp://` daemon connection, as a `roger daemon --client-token-env` requires.
- `--store-dir <dir>`: directory holding per-Address append-only Store files (`<hex_sha256_of_address>.store`) and atomic read cursors (`<hex_sha256_of_address>.store.cursor`).
- `--port-file <path>`: optional path atomically populated (`<path>.tmp` -> `fsync` -> `rename`) with the bound TCP port immediately after `bind`.
- `--endpoint <path>`: MCP HTTP endpoint path (default `"/mcp"`). `GET /healthz` is always served (`200 OK` `{"ok":true}`).
- `--token-env <NAME>`: optional environment variable holding the required Bearer secret. When set, every request to `<endpoint>` must carry `Authorization: Bearer <token>` (checked in constant time) or receives `401 Unauthorized` with `WWW-Authenticate: Bearer`.

## Multi-Container & Cross-Host Topology

Run one `roger daemon` per host connected to the central `roger coordinator`, and run `roger-mcp-gateway` alongside that host daemon with a persistent `--store-dir` volume:

1. Each container initializes its own MCP session over HTTP/1.1 or HTTP/2 (`h2c`) against `<endpoint>`.
2. Every session's Address (including composite Reference Addresses such as `[[153:Meteor Shower:7]]<uuid>`) is kept verbatim and mapped to `<sha256_hex(address)>.store` inside `--store-dir`.
3. Every incoming Mail delivery is appended and `fsync`ed to that session's Store and enqueued onto its SSE event log **before** sending `ack` to `roger daemon`.
4. If a container restarts and sends a fresh `initialize` for an Address still held by an earlier `Mcp-Session-Id` in the same gateway process, the gateway cleanly closes the stale `Mcp-Session-Id` (releasing its OS file lock without sending `unregister`) before opening the Store for the new session. A second gateway process pointed at the same `--store-dir` is refused immediately by `File::try_lock()`.

## Streamable HTTP & SSE Protocol

Negotiated MCP protocol versions: `2025-06-18` (default), `2025-03-26`, `2024-11-05`.

### 1. `POST <endpoint>` — `initialize`

Allowed without `Mcp-Session-Id` (request body limit: 16 MiB). Session parameters are extracted verbatim in precedence order:

- **Address**: `X-Roger-Address` header -> URL query `id` or `address` (percent-decoded UTF-8) -> `params._meta.roger_address` -> generated UUIDv4.
- **Label**: `X-Roger-Label` header -> URL query `label` -> `params._meta.roger_label`.
- **Declaration**: `X-Roger-Declare` header -> URL query `declare` -> `params._meta.roger_declare`.

Response (`200 OK`) returns `Mcp-Session-Id: <uuid>` and `X-Roger-Address: <address>` headers with the JSON-RPC `initialize` result. Any Store entries after `.cursor` are seeded into the session's SSE log at startup.

### 2. `POST <endpoint>` — Requests & Notifications

Requires `Mcp-Session-Id` (`400` if absent, `404` if unknown or closed):

- JSON-RPC notifications (e.g. `notifications/initialized`) return `202 Accepted` with an empty body.
- `ping` returns `{}`.
- `tools/list` and `tools/call` expose all 9 Roger tools concurrently (`roger_send`, `roger_inbox`, `roger_board`, `roger_label`, `roger_claim`, `roger_renew`, `roger_release`, `roger_receipt`, `roger_registered`). Short-lived daemon connections handle outbound requests so a waiting `roger_inbox` call never blocks `roger_send`, `roger_board`, or SSE delivery.

### 3. `GET <endpoint>` — Real-Time SSE Push & `Last-Event-ID`

Requires `Mcp-Session-Id` (`400` / `404`; `406` if `Accept` excludes `text/event-stream` and `*/*`). Streams frames formatted as:

```text
id: <seq>
event: message
data: <compact-json>

```

Push notification payloads:

- `{"jsonrpc":"2.0","method":"notifications/roger/mail","params":{"record":"r-1","mail":"...","from":"...","body":"...","kind":"directed","headers":{...}}}`
- `{"jsonrpc":"2.0","method":"notifications/roger/registered","params":{"session":"..."}}`
- `{"jsonrpc":"2.0","method":"notifications/roger/superseded","params":{"session":"...","host":"...","at_ms":...}}`

Resumption rules:

- **With `Last-Event-ID: <n>`**: replays all events in the session log with `seq > n`, followed by live events as they commit.
- **Without `Last-Event-ID`**: starts from `delivered_up_to`, delivering any events committed in the startup window between `POST initialize` and `GET <endpoint>` without duplicating frames already streamed on an earlier SSE connection.
- **Across gateway restarts**: clients using `roger_inbox` resume from `<store>.cursor`; push-driven clients receive all unconsumed Store records after `<store>.cursor` seeded immediately upon `initialize`.

### 4. `DELETE <endpoint>` — Unregister & Clean Shutdown

Requires `Mcp-Session-Id`. Sends `{"type":"unregister"}` to `roger daemon`, closes active SSE streams, releases the Store file lock (removing empty `.store` and `.cursor` files when zero records were written), and returns `200 OK` `{"ok":true}`.

## Auditing Gateway Stores with `roger audit`

Gateway Store files use the exact durable JSON-line format of `roger`, preserving sender header key order byte-for-byte. Pass any gateway `.store` file directly to `roger audit`:

```sh
roger audit \
  --coordinator-log /var/lib/roger/coordinator.log \
  --daemon-log /var/lib/roger/daemon.log \
  --store /var/lib/roger/gateway-stores/<sha256_address>.store \
  --bodies
```

## Named Groups and positions

Opt in during initialize with `X-Roger-Groups: 1`, `?groups=1`, or
`_meta.roger_groups: true`, in that precedence order. Each accepts `1`, `0`,
`true`, or `false`; the default is false and retains the existing nine tools.
Opted-in sessions additionally expose `roger_subscribe` (non-empty `group`,
optional nonnegative `after` and boolean `latest`), `roger_unsubscribe`
(non-empty `group`), and `roger_groups`. Positions are durable in the service.
`roger_send` accepts exactly one of `to` and `group`; Group replies include
`mail`, `group`, and `article`. Group deliveries retain `group` and `article`
in the Store, inbox, and SSE mail notification, including event recovery.

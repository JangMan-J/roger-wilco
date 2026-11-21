# `roger-mcp-gateway`

`roger-mcp-gateway` is an optional multi-session Model Context Protocol (MCP `2026-07-28`) Streamable HTTP and SEP-3415 (`io.modelcontextprotocol/events`) push/poll gateway for Roger-Wilco. It serves HTTP/1.1 and prior-knowledge HTTP/2 (`h2c`) from a single TCP listener so containerized MCP clients across local or remote network namespaces can manage explicit durable Roger session handles (`roger_register` / `roger_unregister`), consume real-time push or cursor-resumable poll event streams backed directly by `DurLog` + `Notify`, and invoke all Roger MCP tools with exact wire and semantic parity to `roger mcp`.

## Build

```sh
cargo build --release --manifest-path modules/mcp-gateway/Cargo.toml
```

## CLI

```text
Usage: roger-mcp-gateway --bind <address:port> --socket <path|tcp://address:port> --store-dir <dir>
                         [--port-file <path>] [--endpoint <path>] [--token-env <NAME>] [--groups]
```

- `--bind <address:port>`: TCP listener serving both HTTP/1.1 and HTTP/2 (`h2c`).
- `--socket <path|tcp://address:port>`: host `roger daemon` Unix socket path (Linux/macOS) or `tcp://<host:port>` endpoint (Linux, macOS, Windows; dialed with `TCP_NODELAY`). When `ROGER_CLIENT_TOKEN` is set, the gateway sends `{"type":"auth","token":...}` as the first line of every `tcp://` daemon connection, as a `roger daemon --client-token-env` requires.
- `--store-dir <dir>`: directory holding per-Address append-only Store files (`<hex_sha256_of_address>.store`) and atomic read cursors (`<hex_sha256_of_address>.store.cursor`).
- `--port-file <path>`: optional path atomically populated (`<path>.tmp` -> `fsync` -> `rename`) with the bound TCP port immediately after `bind`.
- `--endpoint <path>`: MCP HTTP endpoint path (default `"/mcp"`). `GET /healthz` is always served (`200 OK` `{"ok":true}`).
- `--token-env <NAME>`: optional environment variable holding the required Bearer secret. When set, every request to `<endpoint>` must carry `Authorization: Bearer <token>` (checked in constant time) or receives `401 Unauthorized` with `WWW-Authenticate: Bearer`.
- `--groups`: enable Named Group tools (`roger_subscribe`, `roger_unsubscribe`, `roger_groups`, `roger_articles`, `roger_group_policy`) and `group` sends on `roger_send` statically across this gateway deployment.

## MCP `2026-07-28` Streamable HTTP

- **Sessionless HTTP**: `POST <endpoint>` handles all JSON-RPC requests and notifications. `GET <endpoint>` and `DELETE <endpoint>` return `405 Method Not Allowed` (`Allow: POST`). Any legacy `Mcp-Session-Id` or `Last-Event-ID` headers are ignored.
- **Required Headers**: Every JSON-RPC request POST must carry `MCP-Protocol-Version` (matching `_meta["io.modelcontextprotocol/protocolVersion"]`), `Mcp-Method` (matching `method`), and `Mcp-Name` (for `tools/call`, `events/poll`, `events/stream`, `events/subscribe`, `events/unsubscribe`, supporting `=?base64?<b64>?=` sentinel decoding) — mismatches return `400 Bad Request` + JSON-RPC `-32020` (`HeaderMismatch`).
- **Per-Request `_meta`**: Every request carries `_meta["io.modelcontextprotocol/protocolVersion"]` (`"2026-07-28"`, else `400` + `-32022`) and `_meta["io.modelcontextprotocol/clientCapabilities"]` (object, else `400` + `-32602`).
- **`Origin` & `Accept`**: Non-loopback or invalid `Origin` headers on local bindings return `403 Forbidden`; incompatible `Accept` headers return `406 Not Acceptable`.
- **Discovery & Listen**: `server/discover` returns `supportedVersions: ["2026-07-28"]`, `tools`, `io.modelcontextprotocol/events`, `ttlMs`, and `cacheScope: "public"`. `subscriptions/listen` returns an SSE stream with `notifications/subscriptions/acknowledged`, keepalive comments, and graceful closure on server shutdown.

## Explicit Session Handles (`roger_register` / `roger_unregister`)

1. Call `roger_register` (`{ id?, label?, declare?, watch? }`) to register or reattach an Address and receive `{ session, address }`. The handle survives across HTTP requests; re-registering the same `id` cleanly replaces the previous handle in this gateway without sending `unregister`.
2. Pass `session` to `roger_send`, `roger_inbox`, `roger_board`, `roger_label`, `roger_claim`, `roger_renew`, `roger_release`, `roger_receipt`, `roger_registered` (plus all Named Group tools with `--groups`), and `events/poll` / `events/stream` `arguments`.
3. Call `roger_unregister` (`{ session }`) to send `unregister` to `roger daemon`, close active streams, release the Store lock, and remove empty Store/Cursor files.

## Group history and retention (`--groups`)

- `roger_articles` (`{ session, group, after?, limit? }`) reads a page of retained articles without subscribing. `after` is an exclusive article number (default `0`); `limit` is `1`–`500` (default `100`). The result includes `group`, `high_water`, `retired`, and `articles`. Pass the last returned article number as `after` for the next page.
- `roger_group_policy` (`{ session, group, keep_last?, ttl_ms?, retired? }`) returns the resulting group policy and counts. Omit a field to leave it unchanged. Set `keep_last` or `ttl_ms` to a non-negative integer to set the respective count or age limit, or to `null` to clear that limit. `retired` takes a boolean.

## SEP-3415 (`io.modelcontextprotocol/events`)

- `events/list` advertises `roger.mail` (`["push", "poll"]`), `roger.board` (`["push"]`), and `roger.registration` (`["push"]`).
- `events/poll` (`roger.mail`) reads directly from the durable `.store` using `cursor` (`null` = start from now, `"r-0"` = from first record, `"r-N"` = after `r-N`), `maxAgeMs`, `maxEvents`, and `group`/`kind` filters without touching `.store.cursor`.
- `events/stream` opens a dedicated SSE stream (`notifications/events/active`, replay + live `notifications/events/event`, `notifications/events/heartbeat` `data:` frames on `heartbeatMs`, and `notifications/events/terminated` on session teardown; closing the HTTP stream cancels immediately).

## Auditing Gateway Stores with `roger audit`

Gateway Store files use the exact durable JSON-line format of `roger`, preserving sender header key order byte-for-byte. Pass any gateway `.store` file directly to `roger audit`:

```sh
roger audit \
  --coordinator-log /var/lib/roger/coordinator.log \
  --daemon-log /var/lib/roger/daemon.log \
  --store /var/lib/roger/gateway-stores/<sha256_address>.store \
  --bodies
```

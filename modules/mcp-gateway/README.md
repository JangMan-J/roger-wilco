# `roger-mcp-gateway`

`roger-mcp-gateway` is an optional multi-session Model Context Protocol (MCP `2026-07-28`) Streamable HTTP gateway for Roger-Wilco. It serves HTTP/1.1 and prior-knowledge HTTP/2 (`h2c`) from a single TCP listener so containerized or remote MCP clients can manage explicit durable Roger session handles (`roger_register` / `roger_unregister`), consume real-time push or cursor-resumable poll event streams backed directly by `DurLog` + `Notify`, and invoke all Roger MCP tools with exact wire and semantic parity to `roger mcp`.

## Protocol Boundary

`roger-mcp-gateway` implements a strict, explicit subset of MCP `2026-07-28` and finalized SEPs:

- **Core `2026-07-28` (SEP-2567, SEP-2575, SEP-2243, SEP-2549)**: Sessionless Streamable HTTP (`POST <endpoint>`), per-request `_meta` (`protocolVersion`, `clientCapabilities`, `clientInfo`), `_meta["io.modelcontextprotocol/serverInfo"]` on responses, `server/discover`, `subscriptions/listen`, `resultType`, `ttlMs`/`cacheScope`, and HTTP header-body mirroring (`MCP-Protocol-Version`, `Mcp-Method`, `Mcp-Name`, `Mcp-Param-*`).
- **Events (SEP-3415, `io.modelcontextprotocol/events`)**: `events/list` (with pagination), `events/poll`, and `events/stream` (`active`, `event`, `heartbeat`, `terminated`); webhook RPCs (`events/subscribe`, `events/unsubscribe`) return `-32026` (`deliveryMode: "webhook"`).
- **Ops**: Deterministic `tools/list` with cursor pagination, `annotations` (`readOnlyHint`, `idempotentHint`, `destructiveHint`, `openWorldHint`), `structuredContent` alongside text JSON content, `resources/list` / `resources/templates/list` / `resources/read` (`Mcp-Name: <uri>`), and `x-mcp-header` (`Mcp-Param-*`) validation.
- **Tasks (SEP-2663, `io.modelcontextprotocol/tasks`)**: Async task creation (`resultType: "task"`) on `tools/call` when `_meta["io.modelcontextprotocol/task"]: true` is requested by a `tasks`-capable client, `tasks/get`, `tasks/cancel`, `tasks/update` (enforcing `Mcp-Name: <taskId>`), and `notifications/tasks` push over `subscriptions/listen` (`notifications.taskIds`, `-32021` capability gating).
- **Skills (SEP-2640, `io.modelcontextprotocol/skills`)**: `skills/list`, `skills/get`, `resources/read`, and `resources/directory/read` (`directoryRead: true`) serving the SHA-256-verified `skill://roger-wilco/SKILL.md` guide and `skill://roger-wilco/references/protocol.md` reference.
- **MRTR (SEP-2322)**: HMAC-SHA256 `requestState` and `inputResponses` validation on `tools/call`, permitting only `elicitation/create` when `clientCapabilities.elicitation` is declared.
- **Server Cards (SEP-2127)**: Public, ETag-cached pre-connection discovery at `GET <endpoint>/server-card` (`application/mcp-server-card+json`) and `GET /.well-known/ai-catalog.json` (`application/ai-catalog+json`), strictly omitting primitives and negotiated capabilities.
- **Auth (RFC 9728 / OAuth 2.1 Resource Server + Bearer)**: Constant-time Bearer token enforcement (`--token-env`), optional RFC 9728 Protected Resource Metadata at `GET /.well-known/oauth-protected-resource<endpoint>` and `GET /.well-known/oauth-protected-resource` (`--auth-server`, `--scope`, `--public-url`), and `WWW-Authenticate: Bearer resource_metadata="...", scope="..."` challenges on `401 Unauthorized`.
- **Excluded Deprecated Features (SEP-2577, SEP-2596)**: Roots, Sampling (`sampling/createMessage`, `includeContext`), Logging (`logging/setLevel`, `notifications/message`, `_meta["io.modelcontextprotocol/logLevel"]`), Dynamic Client Registration, and legacy `2024-11-05` HTTP+SSE are intentionally absent.
- **Pre-`2026-07-28` Compatibility Scope**: Limited strictly to version-negotiation diagnostics and transport tolerance (`GET`/`DELETE` -> `405 Allow: POST`, `Mcp-Session-Id`/`Last-Event-ID` ignored, unheadered legacy `initialize` -> `400` + `-32022` naming `"2026-07-28"`, modern-header `initialize` -> `404` + `-32601` naming `"2026-07-28"`). Stdio `roger mcp` additionally retains dual-era `initialize` handshake negotiation for pre-`2026-07-28` stdio clients.

## Build

```sh
cargo build --release --manifest-path modules/mcp-gateway/Cargo.toml
```

## CLI

```text
Usage: roger-mcp-gateway --bind <address:port> --socket <path|tcp://address:port> --store-dir <dir>
                         [--port-file <path>] [--endpoint <path>] [--token-env <NAME>]
                         [--public-url <url>] [--auth-server <issuer-url>] [--scope <scope>] [--groups]
```

- `--bind <address:port>`: TCP listener serving both HTTP/1.1 and HTTP/2 (`h2c`).
- `--socket <path|tcp://address:port>`: host `roger daemon` Unix socket path (Linux/macOS) or `tcp://<host:port>` endpoint (Linux, macOS, Windows; dialed with `TCP_NODELAY`). When `ROGER_CLIENT_TOKEN` is set, the gateway sends `{"type":"auth","token":...}` as the first line of every `tcp://` daemon connection, as a `roger daemon --client-token-env` requires.
- `--store-dir <dir>`: directory holding per-Address append-only Store files (`<hex_sha256_of_address>.store`) and atomic read cursors (`<hex_sha256_of_address>.store.cursor`).
- `--port-file <path>`: optional path atomically populated (`<path>.tmp` -> `fsync` -> `rename`) with the bound TCP port immediately after `bind`.
- `--endpoint <path>`: MCP HTTP endpoint path (default `"/mcp"`). `GET /healthz` is always served (`200 OK` `{"ok":true}`).
- `--token-env <NAME>`: optional environment variable holding the required Bearer secret. When set, every request to `<endpoint>` must carry `Authorization: Bearer <token>` (checked in constant time) or receives `401 Unauthorized`.
- `--public-url <url>`: optional canonical public URL of the MCP endpoint advertised in the Server Card and RFC 9728 Protected Resource Metadata.
- `--auth-server <issuer-url>`: optional OAuth 2.1 Authorization Server issuer URL (repeatable). Enables RFC 9728 Protected Resource Metadata endpoints and `resource_metadata` parameters on `401 WWW-Authenticate` challenges.
- `--scope <scope>`: optional required scope token (repeatable) advertised in Protected Resource Metadata and `401 WWW-Authenticate` challenges.
- `--groups`: enable Named Group tools (`roger_subscribe`, `roger_unsubscribe`, `roger_groups`, `roger_articles`, `roger_group_policy`) and `group` sends on `roger_send` statically across this gateway deployment.

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

# Source provenance

Roger-Wilco adapts behavior and implementation from two fixed source commits.
The reference directories are read-only extraction inputs.
`reference/` and `.runtime/` are ignored, untracked local directories.
The latter holds validation artifacts, not source inputs.
They are not package contents or runtime dependencies.
The initial Roger-Wilco implementation is commit `5282762`.

| Source | Commit | Source files | Destination |
|---|---|---|---|
| Pinax Network agent-bus, JangMan-J fork | `5c373b269e3afb60b7f52d2b0fd00408f2ee4142` | `src/db.ts`, `test/bus.test.ts` | Coordinator SQLite storage and storage tests |
| Same agent-bus commit | Same | `src/server.ts`, `src/index.ts`, `src/config.ts` | HTTP control, authentication, and coordinator startup. The extracted MCP route was later removed. |
| Same agent-bus commit | Same | `src/webhooks.ts`, `test/webhooks.test.ts` | Delivery cursor and retry behavior, replaced with SSE |
| JangMan-J pi-webtasks | `7239e524daeb0ce6f126f538ba09d0010c85ed72` | `src/agent-bus.ts` | Versioned protocol validation |
| Same pi-webtasks commit | Same | `src/webhook-inbox.ts`, `src/webhook-receiver.ts`, `src/webhook-worker.ts`, `src/webhook-consumer.ts` | Durable local inbox and host client |
| Same pi-webtasks commit | Same | `src/index.ts`, `tests/agent-bus.test.ts`, `tests/helpers/test-host.ts` | Pi lifecycle, native SDK fixtures, and acceptance tests |

The agent-bus portions retain the Pinax Network MIT notice in `THIRD-PARTY-NOTICES`.
The pi-webtasks commit has no tracked license file.
The package remains private, with no public distribution license for those portions.
`LICENSE` records these terms.

Research-provider code, webhook signatures, public feeds, and release watchers are excluded.
Node SQLite replaces Bun SQLite.
The test stack uses `node:test` and `node:assert/strict`.

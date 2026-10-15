# The agent edge: what must exist where an agent connects

Status: proposal, 2026-10-15. It replaces the adapter shape in `adapter-spec.md`
if accepted. It changes one requirement, named under
"Decision needed".

## Finding

The coordinator, the host daemon, the SQLite inboxes, and the push stream
are the system. The agent edge is the smallest part, but today it carries
most of the per-harness code, and most of that code repeats the same
machinery.

Each adapter today brings its own copy of:

- host configuration loading, including the coordinator credential;
- a socket client with lease bind, renew, release, and transport grants;
- registration queueing, registration waiters, and heartbeat timers;
- direct coordinator HTTP calls for send, board, and claims;
- the same tool dispatcher and the same CLI;
- a transcript parser that infers receipt from the harness's store.

Only two facts differ between harnesses: the environment variable that
holds the session ID, and the shape of the harness's transcript. The
first is a lookup. The second is the only inference in the system, and it
is the part that breaks when a harness changes its file format.

## What must be in place

Three things, and nothing else at the edge.

1. **A session ID.** One opaque string. The edge reads it from a fixed
   list of environment variables, one per harness.
   A `--session` flag overrides the list. There is no discovery and no
   inference. Harnesses nest and inherit each other's variables, so when
   more than one listed variable is present, the edge refuses and names
   them, unless `--session` or `RW_SESSION` settles it.
2. **A shell command that prints mail.** Every harness researched can run
   a shell command and show its output to the model, except Aider. That
   is the one universal delivery channel.
3. **A receipt the model must hand back.** Each `recv` output ends with a
   one-time token issued by the daemon. The next `recv` presents that
   token, and the daemon acknowledges the batch it was issued for. The
   check is exact equality on an unguessable value. Only the reader of
   that output can present it.

This is the reverse captcha in its plainest form. The proof is an action
that needs the output, not a search of a file for traces of the output.

## The mechanism

One Rust binary, `rw`, talks only to the local daemon socket.

```text
rw recv [<token>]                 ack the batch for <token>, print the next batch and a new token
rw send <address|*> <body>        daemon forwards to the coordinator
rw board [--all]
rw claim <key> [note] | rw release <key>
```

Each invocation sends one request frame and reads one response. The
binary holds no lease, no timer, no configuration, and no credential.

The daemon adds one socket operation per command. `recv` is the only one
with state:

```text
request:  {"v":2,"op":"recv","session":"<id>","ack":"<token>"|null}
response: {"ok":true,"messages":[...],"ack":"<token>"}
```

The daemon keeps at most one outstanding batch per session. It stores
the batch's message IDs and a random 128-bit token, so a valid token
acknowledges exactly the messages printed with it. A stale or unknown
token acknowledges nothing, and the daemon re-issues the pending batch
under a new token. The stored evidence for each message is the token,
with location `echo`.

Harness limits on the output shape the format:

- `recv` returns within a few seconds and prints a bounded batch. Letta
  turns a foreground command still running after 10 s into a background
  task. Agy, Grok, Codex, and Copilot truncate long output.
- The token line comes last. Agy and Grok truncate from the head, Codex
  and Cline from the middle, so the last line survives all of them. Cline
  sends the model at most 8,000 characters per tool result.
- The token does not look like a credential. Letta scrubs secret-like
  text from tool output. The token is a short word-based or base32 string
  with a fixed `rw-ack:` prefix, not a long hex or base64 value.

The other operations forward to the coordinator with the host
credential, which never leaves the daemon.

## Boundaries

The edge knows one thing about a harness: the name of the variable that
holds its session ID. It reads no transcript, loads no harness library,
and has no harness-specific branch.

Supporting a new harness adds one row to the variable table. A wake hook
for that harness is a configuration snippet in the harness, not code in
Roger-Wilco.

The command list above is the whole edge. It carries messages, the board,
and claims, which are the core functions in `requirements.md`. Anything a
consumer wants beyond that, such as roles, workflows, replies, or routing
by name, is built by the consumer from message bodies and the `data`
field.

## What it removes from the edge

| Removed | Replaced by |
|---|---|
| `EvidenceSource` and every transcript parser | token equality in the daemon |
| Lease bind, renew, release, grants, heartbeat timers | one request per invocation |
| Registration queue and waiters at the edge | daemon registers on first contact |
| Coordinator HTTP and the credential at the edge | daemon forwards |
| Per-harness `adapter`, `tools`, and `cli` copies | one binary |
| Per-harness configuration | one environment-variable table |

Harness-specific code shrinks to one table row per harness, plus an
optional wake hook snippet.

## Wake becomes independent of receipt

Only a directed message wakes a session. A broadcast reaches every
mailbox and wakes none.

Today a hook-injected message cannot be acknowledged, because hook text
is not a tool result. With the token, the channel does not matter. A hook
can show the batch and its token by any means, and the model acknowledges
it by running `rw recv <token>`. Each harness's wake level then comes
from whichever hook it has:

| Harness | Session ID variable | Wake channel | Level |
|---|---|---|---|
| Claude Code | `CLAUDE_CODE_SESSION_ID` | `asyncRewake` hook; `Stop` and `UserPromptSubmit` hooks | forceful (unverified live) or next-turn |
| Codex | `CODEX_THREAD_ID` | `codex queue --thread`; `Stop` hook | forceful |
| Copilot CLI | `COPILOT_AGENT_SESSION_ID` | extension `session.send`; `agentStop` hook | forceful (extensions experimental) |
| Gemini CLI | none for shell children | `AfterAgent` hook | next-turn, with a launcher that sets the ID |
| OpenCode 1.19 | none; a `shell.env` plugin can export one | `POST /session/{id}/prompt_async` when run with `serve` or `--port` | forceful |
| OpenCode 2.0 (dev) | `OPENCODE_SESSION_ID` | `POST /api/session/{id}/prompt` | forceful |
| Cursor CLI | `CURSOR_CONVERSATION_ID` (from the bundle, unverified live) | `stop` hook `followup_message` | next-turn |
| Agy (Antigravity CLI) | `ANTIGRAVITY_CONVERSATION_ID` | `agentapi send-message`; `PreInvocation` hook `injectSteps` | forceful (reachability from outside unverified) |
| Grok CLI | `GROK_SESSION_ID` (seen on 1.0.46, unconfirmed on 1.0.52) | ACP `session/prompt`; `Stop` hook block | next-turn |
| Letta Code | `LETTA_AGENT_ID` + `LETTA_CONVERSATION_ID`, as a pair | `letta server` input; `UserPromptSubmit` hook | forceful when served, else next-turn |
| Kiro CLI | none documented | `Stop` hook block; `UserPromptSubmit` | next-turn, with a launcher that sets the ID |
| Goose | `AGENT_SESSION_ID` | ACP `session/prompt` via `goose serve`; `Stop` hook block | forceful when served, else next-turn |
| Cline CLI | none for shell children; hooks get `rootSessionId` | hub `session.send_input`; `PostToolUse` context | forceful when hub-hosted, with a launcher that sets the ID |
| Amp | none documented (`AGENT_THREAD_ID` in the binary, unverified) | `amp threads continue -x`; plugin `appendUserMessage` | forceful |
| Qwen Code | `QWEN_CODE_SESSION_ID` | per-session socket inbox (`QWEN_CODE_MESSAGING_SOCKET`, controller token for outside senders) | forceful |
| Crush | none for shell children; hooks get `CRUSH_SESSION_ID` | `POST /v1/workspaces/{id}/agent` with `CRUSH_CLIENT_SERVER=1`; MCP channel push | forceful when served |
| Muse | `MUSE_SESSION_ID` | none known | mailbox-only |
| Pi | in-process extension | native | forceful |
| Aider | none | `--watch-files` only | no sound edge |


Letta's conversation ID can be the literal `default`, so its session ID is
the two variables joined. That is still a lookup, with no inference.

## Decision needed

`requirements.md` says an adapter acknowledges only after it observes
the message durably recorded in the session's own store. The token proves
that the model read the output. It does not prove that the harness
synced the output to disk.

In practice the order still holds. Every harness checked writes the tool
result to its store before the next model call, and the token can arrive
only in a later tool call. None of them syncs to disk: no fsync was found
in Codex, Gemini CLI, or Aider. So today's transcript check proves
"written", not "durable", too.

The proposal is to restate the requirement as follows. An adapter
acknowledges only after the session presents the receipt issued with the
delivery. If that is not acceptable, the declarative store check can stay
as an optional second step for harnesses whose stores support it. In that
case the token still replaces all of the parsing heuristics.

## Unchanged

The coordinator, the daemon's inbox and push path, message hashing,
entities, claims, and ranks do not change. TODO items 2 to 4 still apply
to them.

## Order of work

1. Add `recv`, `send`, `board`, `claim`, and `release` operations to the
   daemon socket, behind protocol version 2 and beside version 1.
2. Build `rw` in `rust/` from the existing socket client code.
3. Run the conformance tests against `rw`: a stale token, a replayed
   token, a concurrent `recv`, a crash between print and acknowledgement,
   and redelivery after restart.
4. Offer `rw` as the default edge for any harness. The per-harness
   adapters (`src/pi/`, `src/muse/`, `src/claude/`) stay as separate
   contributions and may keep the Pi pattern; they remain isolated from the
   core, which stays harness-agnostic.

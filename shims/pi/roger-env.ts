// Shim defaults for the Pi shim. Pure, so node --test shims/pi/test/roger-env.test.ts
// covers it without Pi.
import { existsSync } from "node:fs";
import { join } from "node:path";

export type Env = Record<string, string | undefined>;
export type Settings = { socket: string; bin: string; session: string; store: string };

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** A base-36 token: lowercase `0-9a-z`, no leading zeros, zero written `0`. The shim never reads its value. */
export const TOKEN = "(?:0|[1-9a-z][0-9a-z]*)";
/** A Reference, `[[<token>:<label>:<token>]]`: the optional start of an Address. The shim only checks its shape. */
const REFERENCE = new RegExp(`^\\[\\[${TOKEN}:[^\\[\\]\\n]{1,64}:${TOKEN}\\]\\]$`);

/**
 * The adapter settings: an explicit, non-empty env value wins, then the
 * defaults. `sessionId` is Pi's own session id, used as the Address when it is
 * a UUID, so a resumed session keeps its Address and Store; else `fresh()`.
 * `ROGER_ADDRESS_PREFIX`, a Reference, makes the Address the whole composite
 * `<prefix><uuid>`; it is carried verbatim and never read here. An explicit
 * `ROGER_SESSION` still wins.
 */
export function settings(env: Env, sessionId: string | undefined, home: string, fresh: () => string, exists = existsSync): Settings {
	const get = (k: string) => env[k] || undefined; // empty counts as unset
	const runtime = get("XDG_RUNTIME_DIR");
	const socket = get("ROGER_SOCKET") ?? (runtime ? join(runtime, "roger", "daemon.sock") : undefined);
	if (!socket) throw new Error("ROGER_SOCKET is unset and XDG_RUNTIME_DIR is unset: no host daemon socket to use");
	const installed = join(home, ".local", "bin", "roger");
	const bin = get("ROGER_BIN") ?? (exists(installed) ? installed : "roger");
	const prefix = get("ROGER_ADDRESS_PREFIX") ?? "";
	if (prefix && !REFERENCE.test(prefix)) throw new Error(`ROGER_ADDRESS_PREFIX must be a Reference [[<token>:<label>:<token>]] (base-36 tokens), saw ${JSON.stringify(prefix)}`);
	const session = get("ROGER_SESSION") ?? prefix + (sessionId && UUID.test(sessionId) ? sessionId : fresh());
	const state = get("XDG_STATE_HOME") ?? join(home, ".local", "state");
	// The Address names the file, made path-safe: a composite holds `[`, `:` and spaces.
	const store = get("ROGER_STORE") ?? join(state, "roger-shims", "pi", `${session.replace(/[^A-Za-z0-9._-]/g, "_")}.store`);
	return { socket, bin, session, store };
}

// The Pi user surface over the adapter: the /roger command (Board, picker,
// direct send, broadcast, relabel) and the roger_board, roger_label,
// roger_claim and roger_release tools. Everything goes through the adapter
// request lines that roger.ts owns; nothing here touches delivery or acks.
//
// Presence: the Board says whether each session is connected (its session
// connection and its host daemon's link are open) and gives last_seen_ms (the
// read time while connected, else its disconnect or last own activity). The
// default view is decided here, at read time: connected sessions plus any seen
// within ROGER_PRESENCE_WINDOW_MS (default 10 minutes).
// `/roger all` and roger_board {all: true} skip the filter.
import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { BROADCAST, completions, present, resolve, windowMs, type Row } from "./roger-target.ts";

type Reply = Record<string, unknown>;
export type Core = {
	me: () => string;
	request: (req: Reply) => Promise<Reply>;
	send: (to: string, body: string) => Promise<string>;
};
type ClaimRow = { key: string; holder?: string; remaining_ms?: number };
/** One Board read; `now` is the coordinator's clock at that read. */
type Board = { sessions: Row[]; claims: ClaimRow[]; now: number };

const WINDOW_MS = windowMs(process.env.ROGER_PRESENCE_WINDOW_MS);
const USAGE = [
	"Usage:",
	"  /roger [all]                          the Board (all: include stale sessions), then a picker",
	"  /roger <target> [message]             message one session; no message opens the editor",
	"  /roger * [message]                    broadcast to every registered session",
	"  /roger label <text>                   set this session's label",
	"  /roger relabel <target> <text>        set another session's label",
	'A target is an address, a unique address prefix (3+ hex), or a label; write @<label> or @"<label>" for a label',
	"that is a keyword or that another label also fits.",
].join("\n");

const ago = (r: Row, now: number) =>
	r.connected
		? "connected"
		: r.last_seen_ms > 0
		? `last seen ${new Date(r.last_seen_ms).toISOString()} (${Math.max(0, Math.floor((now - r.last_seen_ms) / 1000))}s ago)`
		: "last seen unknown";

export function registerRogerCommand(pi: ExtensionAPI, core: Core): void {
	/** One adapter request whose answer must be of type `want`. */
	const ask = async (req: Reply, want: string) => {
		const r = await core.request(req);
		if (r.type !== want) throw new Error(`expected ${want}, saw ${JSON.stringify(r)}`);
		return r;
	};
	const board = async (): Promise<Board> => {
		const b = await ask({ type: "board" }, "board");
		const now = typeof b.now_ms === "number" ? b.now_ms : Date.now();
		return { sessions: (b.sessions as Row[]) ?? [], claims: (b.claims as ClaimRow[]) ?? [], now };
	};
	const view = (b: Board, all: boolean): Board => ({
		...b,
		sessions: all ? b.sessions : b.sessions.filter((r) => present(r, b.now, WINDOW_MS)),
	});
	const relabel = async (label: string, session?: string) =>
		(await ask({ type: "relabel", label, ...(session ? { session } : {}) }, "relabeled")) as {
			session: string;
			label: string;
		};
	const rowText = (r: Row) =>
		`${r.session}${r.session === core.me() ? " (you)" : ""} — host ${r.host}${r.label ? ` — ${r.label}` : ""}`;
	const claimText = (c: ClaimRow) =>
		`${JSON.stringify(c.key)} — ${c.holder ?? "?"}${c.remaining_ms !== undefined ? ` (${Math.ceil(c.remaining_ms / 1000)}s left)` : ""}`;

	const boardText = (full: Board, shown: Board, all: boolean) => {
		const now = full.now;
		const own = full.sessions.find((r) => r.session === core.me());
		return [
			"Roger-Wilco",
			`Your address: ${core.me() || "(unknown: adapter not ready)"}`,
			`Your host: ${own ? own.host : "(not on the Board)"}`,
			"",
			`${all ? "All registered sessions" : `Present sessions (connected or seen within ${Math.round(WINDOW_MS / 1000)}s)`} (${shown.sessions.length}):`,
			...shown.sessions.map((r, i) => `${i + 1}. ${rowText(r)} — ${ago(r, now)}`),
			"",
			`Active claims (${shown.claims.length}):`,
			...shown.claims.map(claimText),
			"",
			"Connected means its session connection and host link are open, not that it is working.",
			"Labels are display only; the core routes by address.",
			...(all ? [] : ["Use /roger all to include stale sessions."]),
			"Pick a session to message it or set its label, or use /roger <target|*> <message> (/roger help).",
		].join("\n");
	};

	const sendTo = async (ctx: ExtensionContext, to: string, title: string, body: string) => {
		if (!body && ctx.hasUI) body = (await ctx.ui.editor(title))?.trim() ?? "";
		if (!body) return ctx.ui.notify(USAGE, "warning");
		ctx.ui.notify(`accepted ${await core.send(to, body)} to ${to}`, "info");
	};

	pi.registerCommand("roger", {
		description: "Show the Board, message a session or broadcast, or set a label: /roger help",
		getArgumentCompletions: async (prefix: string) => {
			let rows: Row[] = [];
			try {
				rows = (await board()).sessions;
			} catch {
				// no adapter: complete keywords only
			}
			const hits = completions(rows, prefix);
			return hits.length ? hits.map((v) => ({ value: v, label: v.trim() })) : null;
		},
		handler: async (args, ctx) => {
			const text = args.trim();
			const words = text.split(/\s+/).filter(Boolean);
			try {
				if (words[0] === "help") return ctx.ui.notify(USAGE, "info");
				if (words[0] === "label") {
					const label = text.slice("label".length).trim();
					if (!label) return ctx.ui.notify(USAGE, "warning");
					const r = await relabel(label);
					return ctx.ui.notify(`${r.session} is now ${r.label}`, "info");
				}
				if (words[0] === "relabel") {
					const hit = resolve((await board()).sessions, text.slice("relabel".length));
					if (typeof hit === "string") return ctx.ui.notify(hit, "error");
					if (!hit.row) return ctx.ui.notify(`${hit.to} is not a session on the Board`, "error");
					if (!hit.rest) return ctx.ui.notify(USAGE, "warning");
					const r = await relabel(hit.rest, hit.row.session);
					return ctx.ui.notify(`${r.session} is now ${r.label}`, "info");
				}
				if (words.length && text !== "all") {
					const hit = resolve((await board()).sessions, text);
					if (typeof hit === "string") return ctx.ui.notify(hit, "error");
					const title =
						hit.to === BROADCAST
							? "Broadcast to every registered session"
							: `Message to ${hit.row ? rowText(hit.row) : hit.to}`;
					return await sendTo(ctx, hit.to, title, hit.rest);
				}
				const all = text === "all";
				const full = await board();
				const shown = view(full, all);
				ctx.ui.notify(boardText(full, shown, all), "info");
				if (!ctx.hasUI) return;
				const viewOnly = "View only (no changes)";
				const broadcast = "Broadcast message…";
				const labels = shown.sessions.map(rowText);
				const picked = await ctx.ui.select("Choose a Roger session", [viewOnly, broadcast, ...labels]);
				if (picked === broadcast)
					return await sendTo(ctx, BROADCAST, "Broadcast to every registered session", "");
				const target = shown.sessions[labels.indexOf(picked ?? "")];
				if (!target) return;
				const message = `Message ${target.session}`;
				const setLabel = "Set label…";
				const action = await ctx.ui.select(`Action for ${rowText(target)}`, [message, setLabel, viewOnly]);
				if (action === message) return await sendTo(ctx, target.session, `Message to ${rowText(target)}`, "");
				if (action !== setLabel) return;
				const label = (await ctx.ui.input(`Label for ${target.session}`, target.label))?.trim();
				if (!label) return;
				const r = await relabel(label, target.session);
				ctx.ui.notify(`${r.session} is now ${r.label}`, "info");
			} catch (e) {
				ctx.ui.notify(`roger: ${(e as Error).message}`, "error");
			}
		},
	});

	const json = (value: unknown) => ({ content: [{ type: "text" as const, text: JSON.stringify(value) }], details: value });

	pi.registerTool(
		defineTool({
			name: "roger_board",
			label: "Roger board",
			description:
				"Read the Board: registered sessions (address, host, label, connected, last_seen_ms) and active claims. " +
				`By default only sessions connected or seen within ${Math.round(WINDOW_MS / 1000)}s; set all to include stale ones. ` +
				"Connected means the session's connection is open, not that it is working.",
			parameters: Type.Object({
				all: Type.Optional(Type.Boolean({ description: "include sessions neither connected nor seen recently" })),
			}),
			async execute(_id, params) {
				return json({ me: core.me(), ...view(await board(), params.all ?? false) });
			},
		}),
	);

	pi.registerTool(
		defineTool({
			name: "roger_label",
			label: "Roger label",
			description: "Set this session's label, shown on the Board. Display only: the address does not change.",
			parameters: Type.Object({ label: Type.String({ description: "label text, stored verbatim" }) }),
			async execute(_id, params) {
				const r = await relabel(params.label);
				return { content: [{ type: "text", text: `${r.session} is now ${r.label}` }], details: r };
			},
		}),
	);

	pi.registerTool(
		defineTool({
			name: "roger_claim",
			label: "Roger claim",
			description:
				"Acquire, or renew if this session already holds it, an exclusive claim on a key for ttl_seconds " +
				"(default 900). ok is false when another holder has it; the reply names that holder.",
			parameters: Type.Object({
				key: Type.String({ description: "claim key, stored verbatim" }),
				ttl_seconds: Type.Optional(Type.Integer({ minimum: 1, description: "seconds until the claim expires" })),
			}),
			async execute(_id, params) {
				const ttl_ms = (params.ttl_seconds ?? 900) * 1000;
				return json(await ask({ type: "claim", op: "acquire", key: params.key, ttl_ms }, "claim_result"));
			},
		}),
	);

	pi.registerTool(
		defineTool({
			name: "roger_release",
			label: "Roger release",
			description: "Release a claim this session holds, by its exact key.",
			parameters: Type.Object({ key: Type.String({ description: "claim key, exactly as claimed" }) }),
			async execute(_id, params) {
				return json(await ask({ type: "claim", op: "release", key: params.key }, "claim_result"));
			},
		}),
	);
}

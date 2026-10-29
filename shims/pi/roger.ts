// Pi shim: the one place that knows Pi runs this session. It starts the
// harness-free `roger adapter`, renders each delivered Mail into the
// transcript, and forwards send calls as lines. It never acknowledges a
// delivery: the adapter acks from its own Store.
//
// Wake: Pi maps the core's delivery `kind: directed` to its wake behavior: such
// a Mail starts a turn when Pi is idle. A `broadcast` kind queues no turn. An
// older core sends no `kind`; its `wake` field (false on a broadcast) is read
// then. Headers are carried by the core and ignored here. When Pi
// is busy it is queued as a follow-up that Pi hands the model at the turn
// boundary. Pi decides idle or busy; the core never learns either. The shim
// declares its wake latency to the Board as `wake-latency-ms=<n>`.
//
// Env (an explicit, non-empty value wins; defaults in roger-env.ts):
// ROGER_SOCKET, default $XDG_RUNTIME_DIR/roger/daemon.sock; ROGER_BIN, default
// ~/.local/bin/roger when it exists, else `roger` on PATH; ROGER_SESSION, the
// Address, default this Pi session's id when it is a UUID, else a fresh UUID;
// ROGER_ADDRESS, a whole Address from the launcher, `[[<token>:<label>:<token>]]<uuid>`
// or a bare `<uuid>`, used verbatim (its shape is checked, its tokens never read), so
// it survives /new and a restart; it beats ROGER_ADDRESS_PREFIX and Pi's id;
// ROGER_ADDRESS_PREFIX, a Reference that makes the Address the whole composite <prefix><uuid>; ROGER_STORE, default
// ${XDG_STATE_HOME:-~/.local/state}/roger-shims/pi/<address>.store. ROGER_WAKE_LATENCY_MS is the
// declared wake latency (default 1000), or `off` to render Mail without ever
// starting a turn and declare nothing.
//
// One Pi session is one Address. Resume reopens the same Pi session, so the
// same Address and Store. /new and /fork make a new Pi session id, so a new
// Address with its own Store: Pi emits session_shutdown first, and this shim
// waits for the old adapter to exit before the replacement runtime's
// session_start starts the next one. Within one runtime a recurring
// session_start reuses the running adapter. Across Pi processes, the core
// holds each Store under an exclusive lock: a second adapter on the same Store
// is refused at start, and the shim reports "address already held" and does
// not retry, so two live sessions never split one Address's Mail.
//
// Link: roger-adapter.ts supervises the adapter. While the session lives an
// adapter that exits is replaced after a growing delay (250 ms, doubling, at
// most 30 s); session_shutdown ends that for good.
import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { Type } from "@earendil-works/pi-ai";
import { defineTool, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { registerRogerCommand } from "./roger-command.ts";
import { startsTurn, superviseAdapter } from "./roger-adapter.ts";

/** The declared wake latency in ms, or undefined when wake is off. */
export function wakeLatency(env: string | undefined): number | undefined {
	if (env === "off") return undefined;
	if (env === undefined || env === "") return 1000;
	const n = Number(env);
	if (!Number.isInteger(n) || n <= 0) throw new Error(`ROGER_WAKE_LATENCY_MS must be a positive integer or off, saw ${env}`);
	return n;
}

export default function (pi: ExtensionAPI) {
	const wakeMs = wakeLatency(process.env.ROGER_WAKE_LATENCY_MS);
	const link = superviseAdapter({
		env: process.env,
		home: homedir(),
		fresh: randomUUID,
		wakeMs,
		onMail: (ev) => {
			const wake = startsTurn(ev, wakeMs);
			// Idle: triggerTurn starts a turn now. Busy: followUp waits for the
			// current turn to end, then Pi continues with this Mail.
			pi.sendMessage(
				{
					customType: "roger-mail",
					content: `Mail ${ev.mail} from ${ev.from}:\n${ev.body}`,
					display: true,
					details: { mail: ev.mail, from: ev.from, record: ev.record },
				},
				wake ? { triggerTurn: true, deliverAs: "followUp" } : { triggerTurn: false },
			);
		},
	});
	const request = link.request;
	const send = async (to: string, body: string) => {
		const r = await request({ type: "send", to, body });
		if (r.type !== "accepted" || typeof r.mail !== "string")
			throw new Error(`expected accepted with a mail id, saw ${JSON.stringify(r)}`);
		return r.mail;
	};

	pi.on("session_start", async (_event, ctx) => link.start(ctx.sessionManager.getSessionId()));
	pi.on("session_shutdown", async () => link.stop());

	registerRogerCommand(pi, { me: link.me, request, send });

	pi.registerCommand("roger-send", {
		description: "Send Mail: /roger-send <address> <body>",
		handler: async (args, ctx) => {
			const [to] = args.trim().split(/\s+/);
			const body = args.trim().slice(to?.length ?? 0).trim();
			if (!to || !body) return ctx.ui.notify("Usage: /roger-send <address> <body>", "warning");
			try {
				ctx.ui.notify(`accepted ${await send(to, body)}`, "info");
			} catch (e) {
				ctx.ui.notify(`roger-send failed: ${(e as Error).message}`, "error");
			}
		},
	});

	pi.registerTool(
		defineTool({
			name: "roger_send",
			label: "Roger send",
			description: "Send Mail to another agent session by Address.",
			parameters: Type.Object({
				to: Type.String({ description: "recipient Address (the whole string, e.g. a UUID or [[<token>:<label>:<token>]]<uuid>)" }),
				body: Type.String({ description: "mail body, delivered verbatim" }),
			}),
			async execute(_id, params) {
				const mail = await send(params.to, params.body);
				return { content: [{ type: "text", text: `accepted ${mail}` }], details: { mail } };
			},
		}),
	);
}

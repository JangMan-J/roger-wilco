// Test support, not shipped behaviour: loads the real Pi shim and adds
// commands that invoke the shim's registered tool callbacks directly
// (/call-roger-send, and /call-roger-tool <name> <json> for any tool), so the tool path runs in a real Pi runtime without a model call.
// It skips Pi's tool dispatch (argument validation, tool_call hooks).
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import shim from "../roger.ts";

export default function (pi: ExtensionAPI) {
	const tools: Record<string, any> = {};
	shim(
		new Proxy(pi, {
			get(target, key) {
				if (key === "registerTool")
					return (def: any) => {
						tools[def.name] = def;
						return target.registerTool(def);
					};
				const v = (target as any)[key];
				return typeof v === "function" ? v.bind(target) : v;
			},
		}),
	);
	pi.registerCommand("call-roger-send", {
		description: "Invoke the roger_send tool callback: /call-roger-send <address> <body>",
		handler: async (args, ctx) => {
			const [to] = args.trim().split(/\s+/);
			const body = args.trim().slice(to.length).trim();
			try {
				const r = await tools.roger_send.execute("direct", { to, body }, undefined, undefined, ctx);
				ctx.ui.notify(`tool result: ${r.content[0].text}`, "info");
			} catch (e) {
				ctx.ui.notify(`tool failed: ${(e as Error).message}`, "error");
			}
		},
	});
	pi.registerCommand("call-roger-tool", {
		description: "Invoke a registered tool callback: /call-roger-tool <name> <json params>",
		handler: async (args, ctx) => {
			const [name] = args.trim().split(/\s+/);
			try {
				const params = JSON.parse(args.trim().slice(name.length).trim() || "{}");
				const r = await tools[name].execute("direct", params, undefined, undefined, ctx);
				ctx.ui.notify(`tool result: ${r.content[0].text}`, "info");
			} catch (e) {
				ctx.ui.notify(`tool failed: ${(e as Error).message}`, "error");
			}
		},
	});
}

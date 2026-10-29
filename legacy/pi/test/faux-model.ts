// Test support, not shipped behaviour: a scripted model so a Pi session can
// run real turns with no network and no model call. A prompt `busy <ms>`
// holds its turn for that long; every other request answers at once with
// `saw: <last message text>`, so the transcript shows what the model was
// given. Use with `--model faux/faux-1`.
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const text = (m: any): string =>
	typeof m?.content === "string"
		? m.content
		: (m?.content ?? []).map((c: any) => (c.type === "text" ? c.text : "")).join("");

export default function (pi: ExtensionAPI) {
	const faux = fauxProvider();
	const reply = async (context: any) => {
		const last = text(context.messages.at(-1));
		const busy = /^busy (\d+)/.exec(last);
		if (busy) {
			await new Promise((r) => setTimeout(r, Number(busy[1])));
			return fauxAssistantMessage(`busy done`);
		}
		return fauxAssistantMessage(`saw: ${last}`);
	};
	faux.setResponses(Array.from({ length: 100 }, () => reply));
	pi.registerProvider(faux.provider);
}

// Pure helpers for /roger: who a typed target names, and who counts as
// present. No Pi and no I/O, so `node --test shims/pi/test/roger-target.test.ts`
// covers them. Labels are compared as whole opaque text (case-folded); nothing
// here reads the tokens inside a Reference.

import { TOKEN } from "./roger-env.ts";

export type Row = { session: string; host: string; label: string; connected: boolean; last_seen_ms: number };

export const BROADCAST = "*";
export const DEFAULT_WINDOW_MS = 10 * 60_000;

const ADDRESS = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/**
 * An Address may begin with a Reference: `[[<token>:<label>:<token>]]<uuid>`; the label may hold spaces. A token is
 * lowercase base 36 and is matched case-sensitively: nothing folds the Reference, and a composite routes exactly as typed.
 */
const COMPOSITE = new RegExp(`^\\[\\[${TOKEN}:[^\\[\\]\\n]{1,64}:${TOKEN}\\]\\](?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})`);
/** The UUID an Address ends in: the whole text when it has no Reference. */
const uuidOf = (session: string) => session.replace(/^\[\[[^\]]*\]\]/, "");
/** Address-shaped text always routes as an Address, never as a label. */
export const isAddress = (text: string) => ADDRESS.test(text) || new RegExp(`${COMPOSITE.source}$`).test(text);

const fold = (s: string) => s.normalize("NFC").toLocaleLowerCase();

/** The presence window from its env text: a non-negative integer of ms, else the default. */
export function windowMs(env: string | undefined): number {
	const n = env === undefined || env.trim() === "" ? NaN : Number(env);
	return Number.isInteger(n) && n >= 0 ? n : DEFAULT_WINDOW_MS;
}

/**
 * Present: connected, or last seen within `window` of `now`. `now` is the
 * coordinator's clock from the same Board read, so host clock skew does not
 * move the window.
 */
export const present = (r: Row, now: number, window: number) =>
	r.connected || (r.last_seen_ms > 0 && now - r.last_seen_ms <= window);

/** `to` is the recipient; `row` is set when it is a session on the Board; `rest` is the text after the target. */
export type Target = { to: string; row?: Row; rest: string };

/**
 * Resolve the target at the start of `text`:
 * - `*` is the Broadcast Address;
 * - Address-shaped text is that Address, on the Board or not;
 * - otherwise a label (the whole label, case-folded, may hold spaces) or a
 *   unique Address prefix of 3+ hex characters. Hits from both, or labels of
 *   different lengths that both fit, are ambiguous.
 * `@word` or `@"text"` forces the target reading (for a label such as `all`
 * or `Bob` beside `Bob Smith`); a quoted target must match whole.
 * Returns an error string when nothing, or more than one session, matches.
 */
export function resolve(rows: Row[], input: string): Target | string {
	let text = input.trim();
	let quoted: string | undefined;
	if (text.startsWith("@")) {
		text = text.slice(1).trimStart();
		const q = text[0];
		if (q === '"' || q === "'") {
			const end = text.indexOf(q, 1);
			if (end < 0) return `unterminated quote in ${JSON.stringify(input)}`;
			quoted = text.slice(1, end);
			text = text.slice(end + 1);
		}
	}
	const first = quoted ?? text.split(/\s+/, 1)[0] ?? "";
	const restAfter = (n: number) => (quoted !== undefined ? text : text.slice(n)).trim();
	if (!first) return "no target given";
	if (first === BROADCAST) return { to: BROADCAST, rest: restAfter(1) };
	// A composite Address: the whole text through the UUID, kept exactly as typed.
	const composite = quoted === undefined && text.startsWith("[[") ? COMPOSITE.exec(text)?.[0] : undefined;
	if (composite) {
		const row = rows.find((r) => r.session === composite);
		return { to: composite, row, rest: restAfter(composite.length) };
	}
	if (isAddress(first)) {
		const row = rows.find((r) => fold(r.session) === fold(first));
		if (row) return { to: row.session, row, rest: restAfter(first.length) };
		// A bare UUID names the one Board session whose Address ends in it.
		const ends = rows.filter((r) => fold(uuidOf(r.session)) === fold(first));
		if (ends.length > 1) return `ambiguous target ${JSON.stringify(first)} (address): ${ends.map((r) => r.session).join(", ")}`;
		if (ends[0]) return { to: ends[0].session, row: ends[0], rest: restAfter(first.length) };
		return { to: first.toLowerCase(), row: undefined, rest: restAfter(first.length) };
	}

	// Labels that fit: the whole target when quoted, else a prefix of the text
	// ending at a space or the end. Address-shaped labels never route.
	const labelled = rows.filter((r) => r.label && !isAddress(r.label));
	const fits = labelled.filter((r) => {
		if (quoted !== undefined) return fold(r.label) === fold(quoted);
		const head = text.slice(0, r.label.length);
		const next = text.charAt(r.label.length);
		return fold(head) === fold(r.label) && (next === "" || /\s/.test(next));
	});
	const lengths = new Set(fits.map((r) => r.label.length));
	if (lengths.size > 1)
		return `ambiguous target: labels ${fits.map((r) => JSON.stringify(r.label)).join(", ")} all fit; quote one as @"<label>"`;

	const hex = fold(first);
	const byPrefix = /^[0-9a-f-]{3,}$/.test(hex) ? rows.filter((r) => fold(uuidOf(r.session)).startsWith(hex)) : [];
	const hits = new Map<string, Row>();
	for (const r of [...fits, ...byPrefix]) hits.set(r.session, r);
	if (hits.size > 1) {
		const why = fits.length && byPrefix.length ? "label and address prefix" : fits.length ? "label" : "address prefix";
		return `ambiguous target ${JSON.stringify(first)} (${why}): ${[...hits.keys()].join(", ")}`;
	}
	const [row] = hits.values();
	if (!row) return `no session matches ${JSON.stringify(first)}`;
	const used = fits.length ? fits[0]!.label.length : first.length;
	return { to: row.session, row, rest: restAfter(used) };
}

const KEYWORDS = ["all", "help", "label", "relabel"];

/**
 * How to type a session's label as a target: as-is when that resolves to it,
 * else quoted as `@"<label>"` (a keyword, a label another label extends, or a
 * label shared or Address-shaped). Address-shaped labels never route at all.
 */
function labelTarget(rows: Row[], r: Row): string | undefined {
	if (!r.label || isAddress(r.label) || r.label.includes('"')) return undefined;
	const t = resolve(rows, r.label);
	if (typeof t === "object" && t.row?.session === r.session && !KEYWORDS.includes(fold(r.label))) return r.label;
	const q = resolve(rows, `@"${r.label}"`);
	return typeof q === "object" && q.row?.session === r.session ? `@"${r.label}"` : undefined;
}

/**
 * Completions for the /roger argument typed so far. The whole prefix is
 * matched, so a label with spaces completes; `relabel ` is followed by a
 * target. Returns values that replace the whole argument.
 */
export function completions(rows: Row[], prefix: string): string[] {
	const lead = prefix.startsWith("relabel ") ? "relabel " : "";
	const typed = fold(prefix.slice(lead.length));
	// [what the user may type, what to insert]; a quoted label also matches
	// its bare text.
	const targets: [string, string][] = rows.flatMap((r) => {
		const out: [string, string][] = [[r.session, r.session]];
		const t = labelTarget(rows, r);
		if (t) out.push([t, t], [r.label, t]);
		return out;
	});
	const words: [string, string][] = [...KEYWORDS, BROADCAST].map((k) => [k, k]);
	const values = (lead ? targets : [...words, ...targets])
		.filter(([key]) => fold(key).startsWith(typed))
		.map(([, value]) => value);
	return [...new Set(values)].map((v) => `${lead}${v} `);
}

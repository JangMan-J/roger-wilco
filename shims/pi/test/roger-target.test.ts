// Unit tests for /roger target resolution and presence: node --test shims/pi/test/roger-target.test.ts
import assert from "node:assert/strict";
import { test } from "node:test";
import { completions, DEFAULT_WINDOW_MS, present, resolve, windowMs, type Row, type Target } from "../roger-target.ts";

const A = "aaaaaaaa-0000-4000-8000-00000000000a";
const B = "bbbbbbbb-0000-4000-8000-00000000000b";
const C = "abc00000-0000-4000-8000-00000000000c";
const ABSENT = "dddddddd-0000-4000-8000-00000000000d";
const row = (session: string, label = "", extra: Partial<Row> = {}): Row => ({
	session,
	host: "h",
	label,
	connected: true,
	last_seen_ms: 1,
	...extra,
});

const to = (rows: Row[], text: string) => {
	const r = resolve(rows, text);
	assert.equal(typeof r, "object", `expected a target for ${JSON.stringify(text)}, got ${JSON.stringify(r)}`);
	return r as Target;
};
const refused = (rows: Row[], text: string, pattern: RegExp) => {
	const r = resolve(rows, text);
	assert.equal(typeof r, "string", `expected a refusal for ${JSON.stringify(text)}, got ${JSON.stringify(r)}`);
	assert.match(r as string, pattern);
};

test("a full Address routes even when it is not on the Board", () => {
	const t = to([row(A)], `${ABSENT} hello there`);
	assert.deepEqual([t.to, t.row, t.rest], [ABSENT, undefined, "hello there"]);
	assert.equal(to([], `${ABSENT.toUpperCase()} x`).to, ABSENT);
});

test("an Address-shaped label never captures that Address", () => {
	const rows = [row(A, B), row(B)];
	assert.equal(to(rows, `${B} hi`).to, B);
	const absent = [row(A, ABSENT)];
	assert.deepEqual([to(absent, `${ABSENT} hi`).to, to(absent, `${ABSENT} hi`).row], [ABSENT, undefined]);
});

test("broadcast", () => {
	assert.deepEqual(to([row(A)], "* all hands"), { to: "*", rest: "all hands" });
});

test("labels: Reference labels, spaces, and case-folding", () => {
	const rows = [row(A, "[[153:Meteor Shower:7]]"), row(B, "Builder Two")];
	assert.deepEqual([to(rows, "[[153:Meteor Shower:7]] go").to, to(rows, "[[153:Meteor Shower:7]] go").rest], [A, "go"]);
	assert.deepEqual([to(rows, "builder two  ship it").to, to(rows, "builder two  ship it").rest], [B, "ship it"]);
	refused(rows, "Builder go", /no session matches/);
	refused(rows, "[[153:Meteor go", /no session matches/);
});

test("the same label on two sessions is ambiguous", () => {
	refused([row(A, "Bob"), row(B, "bob")], "Bob hi", /ambiguous.*label/);
});

test("a shorter label that also fits is ambiguous unless quoted", () => {
	const rows = [row(A, "Bob"), row(B, "Bob Smith")];
	refused(rows, "Bob Smith hi", /ambiguous.*quote/);
	assert.deepEqual([to(rows, '@"Bob" Smith hi').to, to(rows, '@"Bob" Smith hi').rest], [A, "Smith hi"]);
	assert.deepEqual([to(rows, '@"bob smith" hi').to, to(rows, '@"bob smith" hi').rest], [B, "hi"]);
	assert.equal(to(rows, "Bob hi").to, A);
});

test("address prefixes: 3+ hex characters, case-folded, unique", () => {
	const rows = [row(A), row(B), row(C)];
	assert.equal(to(rows, "AAAA hi").to, A);
	assert.equal(to(rows, "abc hi").to, C);
	refused(rows, "aa hi", /no session matches/);
	refused([row(A), row("aaaaaaaa-1111-4000-8000-00000000000a")], "aaaa hi", /ambiguous.*address prefix/);
});

test("a label never silently shadows an address prefix", () => {
	refused([row(A, "bbbb"), row(B)], "bbbb hi", /ambiguous.*label and address prefix/);
	// Both naming the same session is not ambiguous.
	assert.equal(to([row(B, "bbbb")], "bbbb hi").to, B);
});

test("@ forces a target reading for keyword labels", () => {
	const rows = [row(A, "all"), row(B, "help")];
	assert.deepEqual([to(rows, "@all hi").to, to(rows, "@all hi").rest], [A, "hi"]);
	assert.equal(to(rows, "@help x").to, B);
	refused(rows, '@"unterminated', /unterminated/);
});

test("present: connected always; otherwise within the window of the coordinator's now", () => {
	const now = 1_000_000;
	assert.equal(present(row(A, "", { connected: true, last_seen_ms: 0 }), now, 0), true);
	assert.equal(present(row(A, "", { connected: false, last_seen_ms: now - 500 }), now, 1000), true);
	assert.equal(present(row(A, "", { connected: false, last_seen_ms: now - 1500 }), now, 1000), false);
	assert.equal(present(row(A, "", { connected: false, last_seen_ms: 0 }), now, DEFAULT_WINDOW_MS), false);
	assert.equal(present(row(A, "", { connected: false, last_seen_ms: now - 1 }), now, 0), false);
});

test("windowMs: non-negative integer ms, else the default", () => {
	assert.equal(windowMs(undefined), DEFAULT_WINDOW_MS);
	assert.equal(windowMs(""), DEFAULT_WINDOW_MS);
	assert.equal(windowMs("0"), 0);
	assert.equal(windowMs("1500"), 1500);
	assert.equal(windowMs("-5"), DEFAULT_WINDOW_MS);
	assert.equal(windowMs("abc"), DEFAULT_WINDOW_MS);
	assert.equal(windowMs("1.5"), DEFAULT_WINDOW_MS);
});

test("completions: whole-prefix match, spaced labels, quoting, relabel", () => {
	const rows = [row(A, "[[153:Meteor Shower:7]]"), row(B, "Bob"), row(C, "Bob Smith")];
	assert.deepEqual(completions(rows, "[[153:Met"), ["[[153:Meteor Shower:7]] "]);
	assert.deepEqual(completions(rows, "[[153:Meteor Sh"), ["[[153:Meteor Shower:7]] "]);
	// "Bob Smith" is ambiguous beside "Bob" (both fit), so it completes quoted;
	// "Bob" alone resolves as typed.
	assert.deepEqual(completions(rows, "bo"), ["Bob ", '@"Bob Smith" ']);
	assert.deepEqual(completions(rows, "bob s"), ['@"Bob Smith" ']);
	assert.deepEqual(completions(rows, '@"bob s'), ['@"Bob Smith" ']);
	assert.deepEqual(completions(rows, "relabel aaaa"), [`relabel ${A} `]);
	assert.deepEqual(completions(rows, "rel"), ["relabel "]);
	assert.deepEqual(completions([row(A, "all")], "@"), ['@"all" ']);
	assert.deepEqual(completions(rows, "zz"), []);
	// The message after a full target is not completed.
	assert.deepEqual(completions(rows, "Bob Smith hi"), []);
});

const COMP = `[[1:Meteor Shower:12]]${A}`;

test("a whole composite Address routes verbatim, label spaces and all, on the Board or not", () => {
	const t = to([row(COMP)], `${COMP} hello there`);
	assert.deepEqual([t.to, t.row?.session, t.rest], [COMP, COMP, "hello there"]);
	const absent = `[[2:FLEET:5]]${ABSENT}`;
	const u = to([], `${absent} hi`);
	assert.deepEqual([u.to, u.row, u.rest], [absent, undefined, "hi"], "kept as typed: nothing is folded or stripped");
});

test("a bare UUID or UUID prefix resolves to the one Board session whose Address ends in it", () => {
	const rows = [row(COMP, "agent"), row(B)];
	assert.equal(to(rows, `${A} hi`).to, COMP);
	assert.equal(to(rows, `aaaaaaaa hi`).to, COMP);
	assert.equal(to(rows, `${B} hi`).to, B);
	const twice = [row(COMP), row(`[[2:X:1]]${A}`)];
	refused(twice, `${A} hi`, /ambiguous/);
	assert.equal(to([row(COMP)], `${ABSENT} hi`).to, ABSENT, "not on the Board: the bare UUID routes as typed");
});

test("completions offer a composite Address whole", () => {
	assert.ok(completions([row(COMP)], "[[1").includes(`${COMP} `));
});

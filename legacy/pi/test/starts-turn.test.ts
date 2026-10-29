import assert from "node:assert/strict";
import { test } from "node:test";
import { startsTurn, type AdapterMail } from "../roger-adapter.ts";

const m = (x: Partial<AdapterMail>): AdapterMail => ({ mail: "m", from: "f", body: "b", record: "r", ...x });

test("kind only", () => {
	assert.equal(startsTurn(m({ kind: "directed" }), 1000), true);
	assert.equal(startsTurn(m({ kind: "broadcast" }), 1000), false);
});
test("wake only", () => {
	assert.equal(startsTurn(m({ wake: true }), 1000), true);
	assert.equal(startsTurn(m({ wake: false }), 1000), false);
});
test("both agree", () => {
	assert.equal(startsTurn(m({ kind: "directed", wake: true }), 1000), true);
	assert.equal(startsTurn(m({ kind: "broadcast", wake: false }), 1000), false);
});
test("both disagree: kind wins", () => {
	assert.equal(startsTurn(m({ kind: "directed", wake: false }), 1000), true);
	assert.equal(startsTurn(m({ kind: "broadcast", wake: true }), 1000), false);
});
test("neither", () => assert.equal(startsTurn(m({}), 1000), true));
test("unknown kind", () => assert.equal(startsTurn(m({ kind: "other", wake: true }), 1000), false));
test("wakeMs undefined", () => {
	assert.equal(startsTurn(m({ kind: "directed" }), undefined), false);
	assert.equal(startsTurn(m({}), undefined), false);
});

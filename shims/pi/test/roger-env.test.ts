// Unit tests for the shim defaults: node --test shims/pi/test/roger-env.test.ts
import assert from "node:assert/strict";
import { test } from "node:test";
import { settings } from "../roger-env.ts";

const PI_ID = "019a0000-0000-7000-8000-000000000001";
const never = () => {
	throw new Error("fresh() must not be called when Pi gives a session id");
};

test("defaults: Pi session id is the Address, Store and socket follow it", () => {
	const s = settings({ XDG_RUNTIME_DIR: "/run/user/1000" }, PI_ID, "/home/u", never, () => false);
	assert.deepEqual(s, {
		socket: "/run/user/1000/roger/daemon.sock",
		bin: "roger",
		session: PI_ID,
		store: `/home/u/.local/state/roger-shims/pi/${PI_ID}.store`,
	});
});

test("~/.local/bin/roger is the default binary when it exists", () => {
	const s = settings({ XDG_RUNTIME_DIR: "/r" }, PI_ID, "/home/u", never, (p) => p === "/home/u/.local/bin/roger");
	assert.equal(s.bin, "/home/u/.local/bin/roger");
});

test("explicit env wins over every default", () => {
	const env = { XDG_RUNTIME_DIR: "/r", ROGER_SOCKET: "/s", ROGER_BIN: "/b", ROGER_SESSION: "x-id", ROGER_STORE: "/st" };
	assert.deepEqual(settings(env, PI_ID, "/home/u", never, () => true), { socket: "/s", bin: "/b", session: "x-id", store: "/st" });
});

test("ROGER_SESSION alone moves the default Store with it", () => {
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r", ROGER_SESSION: "x-id" }, PI_ID, "/h", never).store, "/h/.local/state/roger-shims/pi/x-id.store");
});

test("XDG_STATE_HOME moves the default Store; empty counts as unset", () => {
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r", XDG_STATE_HOME: "/st" }, PI_ID, "/h", never).store, `/st/roger-shims/pi/${PI_ID}.store`);
	const empty = { XDG_RUNTIME_DIR: "/r", XDG_STATE_HOME: "", ROGER_SOCKET: "", ROGER_BIN: "", ROGER_SESSION: "", ROGER_STORE: "" };
	assert.deepEqual(settings(empty, PI_ID, "/h", never, () => false), {
		socket: "/r/roger/daemon.sock",
		bin: "roger",
		session: PI_ID,
		store: `/h/.local/state/roger-shims/pi/${PI_ID}.store`,
	});
});

test("a Pi session id that is not a UUID is not an Address: a fresh UUID", () => {
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r" }, "my-custom-id", "/h", () => "fresh-1").session, "fresh-1");
});

test("no Pi session id: a fresh UUID", () => {
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r" }, undefined, "/h", () => "fresh-1").session, "fresh-1");
});

test("no socket and no XDG_RUNTIME_DIR is an error naming both", () => {
	assert.throws(() => settings({}, PI_ID, "/h", never), /ROGER_SOCKET .*XDG_RUNTIME_DIR/);
});

test("ROGER_ADDRESS_PREFIX makes the Address the whole composite: Reference, then Pi's session id", () => {
	const prefix = "[[3:Meteor Shower:12]]";
	const s = settings({ XDG_RUNTIME_DIR: "/r", ROGER_ADDRESS_PREFIX: prefix }, PI_ID, "/h", never);
	assert.equal(s.session, `${prefix}${PI_ID}`);
	assert.equal(s.store, `/h/.local/state/roger-shims/pi/__3_Meteor_Shower_12__${PI_ID}.store`, "the Store file name is path-safe and unique per UUID");
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r", ROGER_ADDRESS_PREFIX: prefix }, undefined, "/h", () => "fresh-1").session, `${prefix}fresh-1`);
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r", ROGER_ADDRESS_PREFIX: "" }, PI_ID, "/h", never).session, PI_ID, "empty counts as unset");
});

test("an explicit ROGER_SESSION wins over the prefix, and a malformed prefix is refused", () => {
	assert.equal(settings({ XDG_RUNTIME_DIR: "/r", ROGER_ADDRESS_PREFIX: "[[1:A:1]]", ROGER_SESSION: "[[2:B:2]]x" }, PI_ID, "/h", never).session, "[[2:B:2]]x");
	for (const bad of ["[[1:A]]", "1:A:1", "[[a:A:1]]", "[[1:A:1]]x", "[[1:A\n:1]]"])
		assert.throws(() => settings({ XDG_RUNTIME_DIR: "/r", ROGER_ADDRESS_PREFIX: bad }, PI_ID, "/h", never), /ROGER_ADDRESS_PREFIX/, bad);
});

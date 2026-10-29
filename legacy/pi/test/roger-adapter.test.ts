// Process tests for the adapter supervisor: node --test legacy/pi/test/roger-adapter.test.ts
// A real coordinator, host daemon and `roger adapter` on loopback; no Pi. Every
// process is killed by the exact pid this file started or the supervisor reports.
import assert from "node:assert/strict";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { delayMs, superviseAdapter, type AdapterMail, type Supervisor } from "../roger-adapter.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const TOKEN = "adapter-test-token";
const SENDER = "99999999-9999-4999-8999-999999999999";
const UUID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
const ADDR = `[[1a:FLEET:zz]]${UUID}`;
const FAST = { initialMs: 100, maxMs: 400, stableMs: 60_000 };

let dir: string;
let bin: string;
let socket: string;
let coordLog: string;
const procs: ChildProcess[] = [];
const sups: Supervisor[] = [];

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
async function until(what: string, ok: () => boolean, ms = 15_000) {
	const end = Date.now() + ms;
	while (Date.now() < end) {
		if (ok()) return;
		await sleep(50);
	}
	assert.fail(`timed out waiting for: ${what}`);
}

before(async () => {
	// The checks build with test-hooks (the binary needs --hold-before-ack below).
	execFileSync("cargo", ["build", "--offline", "-q", "--features", "test-hooks"], { cwd: ROOT, stdio: "inherit" });
	bin = join(ROOT, "target", "debug", "roger");
	dir = mkdtempSync(join(tmpdir(), "roger-adapter-test-"));
	socket = join(dir, "a.sock");
	coordLog = join(dir, "coord.log");
	const env = { ...process.env, ROGER_TOKEN: TOKEN };
	procs.push(spawn(bin, ["coordinator", "--bind", "127.0.0.1:0", "--port-file", join(dir, "port"), "--data", coordLog], { env, stdio: "ignore" }));
	await until("coordinator port", () => existsSync(join(dir, "port")) && readFileSync(join(dir, "port"), "utf8").length > 0);
	const coord = `127.0.0.1:${readFileSync(join(dir, "port"), "utf8").trim()}`;
	procs.push(spawn(bin, ["daemon", "--coordinator", coord, "--host-id", "host-a", "--socket", socket, "--data", join(dir, "a.log")], { env, stdio: "ignore" }));
	await until("daemon socket", () => existsSync(socket));
});

after(async () => {
	for (const s of sups) await s.stop();
	for (const p of procs) p.kill("SIGTERM");
	rmSync(dir, { recursive: true, force: true });
});

/** A supervisor for `address` with its own Store, plus what it reported. */
function link(address: string, opts: { store?: string; bin?: string; env?: Record<string, string> } = {}) {
	const mails: AdapterMail[] = [];
	const logs: string[] = [];
	const store = opts.store ?? join(dir, `${mails.length}-${sups.length}.store`);
	const sup = superviseAdapter({
		env: { XDG_RUNTIME_DIR: dir, ROGER_SOCKET: socket, ROGER_BIN: opts.bin ?? bin, ROGER_ADDRESS: address, ROGER_STORE: store, ...(opts.env ?? {}) },
		home: dir,
		fresh: () => {
			throw new Error("a full ROGER_ADDRESS needs no fresh uuid");
		},
		wakeMs: 1000,
		onMail: (m) => mails.push(m),
		log: (l) => logs.push(l),
		backoff: FAST,
		killAfterMs: 2000,
	});
	sups.push(sup);
	return { sup, mails, logs, store };
}

/** Pids of adapters started by this run for `address` (by /proc; exact args). */
function adapterPids(address: string): number[] {
	const out: number[] = [];
	for (const name of readdirSync("/proc")) {
		if (!/^\d+$/.test(name)) continue;
		try {
			const argv = readFileSync(`/proc/${name}/cmdline`, "utf8").split("\0");
			if (argv[1] === "adapter" && argv.includes(socket) && argv.includes(address)) out.push(Number(name));
		} catch {
			// the process ended between the listing and the read
		}
	}
	return out;
}

const send = (to: string, mail: string, body: string) => {
	const out = execFileSync(bin, ["send", "--socket", socket, "--from", SENDER, "--to", to, "--mail", mail, "--body", body], { encoding: "utf8" });
	assert.match(out, /^ACCEPTED/);
};
const lines = (file: string, needle: string) => (existsSync(file) ? readFileSync(file, "utf8").split("\n").filter((l) => l.includes(needle)).length : 0);
const connected = (address: string) => execFileSync(bin, ["board", "--socket", socket], { encoding: "utf8" }).includes(`address=${address} host=host-a connected=true`);
const alive = (pid: number) => {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
};

test("backoff doubles from the first delay and is capped", () => {
	const b = { initialMs: 250, maxMs: 30_000, stableMs: 10_000 };
	assert.deepEqual([0, 1, 2, 3, 4, 7, 8, 9, 100].map((n) => delayMs(b, n)), [250, 500, 1000, 2000, 4000, 30_000, 30_000, 30_000, 30_000]);
});

test("a killed adapter is replaced; mail queued meanwhile arrives once; ROGER_ADDRESS is the Address", async () => {
	const a = link(ADDR);
	a.sup.start("019a0000-0000-7000-8000-000000000001"); // Pi's id is not the Address
	await until("ready", () => a.sup.me() === ADDR && connected(ADDR));
	for (let i = 1; i <= 3; i++) {
		const old = a.sup.pid()!;
		assert.ok(old, "an adapter runs");
		process.kill(old, "SIGKILL");
		send(ADDR, `down-${i}`, `queued while down ${i} [[153:Meteor Shower:7]]`);
		await until(`replacement after kill ${i}`, () => a.sup.pid() !== undefined && a.sup.pid() !== old && connected(ADDR));
		await until(`mail ${i} delivered`, () => a.mails.length === i);
		assert.equal(a.mails[i - 1].body, `queued while down ${i} [[153:Meteor Shower:7]]`);
		assert.equal(adapterPids(ADDR).length, 1, "never two adapters");
	}
	send(ADDR, "after", "after the last reconnect");
	await until("mail after reconnect", () => a.mails.length === 4);
	await sleep(300);
	assert.equal(a.mails.length, 4, "each mail once");
	assert.equal(readFileSync(a.store, "utf8").trim().split("\n").length, 4, "the Store holds each body once");
	assert.equal(lines(coordLog, '"ev":"mail"'), 4);
	await until("four receipts", () => lines(coordLog, '"ev":"receipt"') === 4);
	assert.ok(a.logs.some((l) => /reconnecting in/.test(l)));
	assert.ok(a.logs.some((l) => l.includes(`reconnected as ${ADDR}`)));
	await a.sup.stop();
});

test("an adapter killed after the Store write and before the ack redelivers once to the shim", async () => {
	const addr = "[[2:HOLD:2]]bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb";
	const marker = join(dir, "held.marker");
	const wrapper = join(dir, "hold-adapter");
	writeFileSync(wrapper, `#!/bin/sh\nexec '${bin}' "$@" --hold-before-ack '${marker}'\n`);
	chmodSync(wrapper, 0o755);
	const a = link(addr, { bin: wrapper });
	a.sup.start(undefined);
	await until("ready", () => connected(addr));
	send(addr, "held-1", "stored, not acked");
	await until("held before the ack", () => existsSync(marker));
	await until("rendered once", () => a.mails.length === 1);
	assert.equal(lines(coordLog, '"ev":"receipt","mail":"held-1"'), 0, "no receipt before the ack");
	process.kill(a.sup.pid()!, "SIGKILL");
	await until("receipt after the replacement acked", () => lines(coordLog, '"ev":"receipt","mail":"held-1"') === 1);
	await sleep(300);
	assert.equal(a.mails.length, 1, "the redelivery of a rendered record is not rendered again");
	assert.equal(lines(a.store, "stored, not acked"), 1, "the Store holds the body once");
	assert.equal(lines(coordLog, '"ev":"mail","mail":"held-1"'), 1);
	await a.sup.stop();
});

test("no respawn after stop, including one that was pending", async () => {
	const a = link("cccccccc-3333-4333-8333-cccccccccccc");
	a.sup.start(undefined);
	await until("ready", () => connected("cccccccc-3333-4333-8333-cccccccccccc"));
	const pid = a.sup.pid()!;
	await a.sup.stop();
	assert.equal(alive(pid), false, "stop waits for the adapter to exit");
	await sleep(4 * FAST.initialMs);
	assert.equal(a.sup.pid(), undefined);
	assert.equal(adapterPids("cccccccc-3333-4333-8333-cccccccccccc").length, 0);

	// Kill the adapter, then stop while the respawn is waiting.
	const b = link("dddddddd-4444-4444-8444-dddddddddddd", { env: {} });
	b.sup.start(undefined);
	await until("ready", () => connected("dddddddd-4444-4444-8444-dddddddddddd"));
	process.kill(b.sup.pid()!, "SIGKILL");
	await until("the exit is seen", () => b.sup.pid() === undefined);
	await b.sup.stop();
	await sleep(4 * FAST.initialMs);
	assert.equal(b.sup.pid(), undefined, "the pending respawn was cancelled");
	assert.equal(adapterPids("dddddddd-4444-4444-8444-dddddddddddd").length, 0);

	// A request after stop fails fast and says why.
	await assert.rejects(b.sup.request({ type: "board" }), /shut down/);
});

test("start while running or while a respawn waits never makes a second adapter", async () => {
	const addr = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee";
	const a = link(addr);
	a.sup.start(undefined);
	a.sup.start(undefined);
	await until("ready", () => connected(addr));
	a.sup.start(undefined);
	assert.equal(adapterPids(addr).length, 1);
	process.kill(a.sup.pid()!, "SIGKILL");
	await until("exit seen", () => a.sup.pid() === undefined);
	a.sup.start(undefined); // during the delay
	a.sup.start(undefined);
	await until("replacement", () => a.sup.pid() !== undefined && connected(addr));
	await sleep(300);
	assert.equal(adapterPids(addr).length, 1);
	await a.sup.stop();
});

test("a second session on a held Address is refused with a clear error and never retried", async () => {
	const addr = "[[3:DUP:3]]ffffffff-6666-4666-8666-ffffffffffff";
	const store = join(dir, "dup.store");
	const first = link(addr, { store });
	first.sup.start(undefined);
	await until("first ready", () => connected(addr));
	const holder = first.sup.pid()!;

	const second = link(addr, { store });
	second.sup.start(undefined);
	await until("second refused", () => second.logs.some((l) => l.includes("already held")));
	const msg = second.logs.find((l) => l.includes("already held"))!;
	assert.ok(msg.includes(addr) && msg.includes(store), msg);
	await sleep(6 * FAST.initialMs); // several backoff periods
	assert.equal(second.sup.pid(), undefined, "no retry");
	assert.equal(second.logs.filter((l) => l.includes("already held")).length, 1);
	assert.deepEqual(adapterPids(addr), [holder], "one adapter, the holder's");
	await assert.rejects(second.sup.request({ type: "board" }), /already held/);

	// The holder is untouched and still gets the mail, once.
	send(addr, "dup-1", "not split");
	await until("holder got it", () => first.mails.length === 1);
	await sleep(300);
	assert.equal(first.mails.length, 1);
	assert.equal(second.mails.length, 0);
	await second.sup.stop();
	assert.equal(first.sup.pid(), holder);
	await first.sup.stop();
});

test("a missing binary is reported once and not retried", async () => {
	const a = link("12345678-7777-4777-8777-123456789abc", { bin: join(dir, "no-such-binary") });
	a.sup.start(undefined);
	await until("reported", () => a.logs.length === 1);
	assert.match(a.logs[0], /ENOENT/);
	await sleep(6 * FAST.initialMs);
	assert.equal(a.logs.length, 1);
	await assert.rejects(a.sup.request({ type: "board" }), /ENOENT/);
	await a.sup.stop();
});

test("a bad ROGER_ADDRESS is reported and starts nothing", async () => {
	const a = link("[[A:Bad:1]]aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa");
	a.sup.start(undefined);
	assert.equal(a.logs.length, 1);
	assert.match(a.logs[0], /ROGER_ADDRESS must be/);
	assert.equal(a.sup.pid(), undefined);
	await a.sup.stop();
});

test("a request in flight when the adapter dies fails, and the link recovers", async () => {
	const addr = "87654321-8888-4888-8888-123456789abc";
	const a = link(addr);
	a.sup.start(undefined);
	await until("ready", () => connected(addr));
	process.kill(a.sup.pid()!, "SIGKILL");
	await until("exit seen", () => a.sup.pid() === undefined);
	await assert.rejects(a.sup.request({ type: "board" }), /roger adapter exited \(SIGKILL\); reconnecting/);
	await until("replacement", () => a.sup.pid() !== undefined && connected(addr));
	const board = await a.sup.request({ type: "board" });
	assert.equal(board.type, "board");
	await a.sup.stop();
});

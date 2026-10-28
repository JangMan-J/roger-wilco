// The Pi shim's supervisor of the `roger adapter` process. Pure Node, no Pi, so
// node --test covers it with a real adapter.
//
// One supervisor is one session's link to Roger. It runs at most one adapter
// at a time and, while the session lives, replaces an adapter that exits (a
// crash, a stray kill, a restarted host daemon) after a growing delay. Mail
// Accepted for the Address meanwhile stays pending at the core and is
// delivered when the new adapter registers; the new adapter redelivers a
// record its Store already holds under the same record id, and `seen` drops
// that one, so the transcript gets each Mail once.
//
// Not retried: a configuration error; a binary that cannot be started; an
// adapter refused because another live process holds this Address's Store.
// The last one is the answer to two live sessions claiming one Address: the
// second is refused and stays refused, it does not take the Address over when
// the first ends. stop() ends the session's link for good: it cancels any
// pending respawn and waits for the adapter to exit.
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import { settings, type Env, type Settings } from "./roger-env.ts";

export type Reply = Record<string, unknown>;
type Pending = { ok: (reply: Reply) => void; fail: (e: Error) => void };
/** A `mail` line of the adapter, or any other event; the shim reads `type`. */
export type AdapterMail = { mail: string; from: string; body: string; record: string; wake?: boolean };

export type Backoff = {
	/** First delay in ms; each consecutive failure doubles it. */
	initialMs: number;
	/** The delay never exceeds this. */
	maxMs: number;
	/** An adapter that ran this long before exiting starts the delay over. */
	stableMs: number;
};
export const BACKOFF: Backoff = { initialMs: 250, maxMs: 30_000, stableMs: 10_000 };

/** The core's wording when another process holds the Store: the adapter exits and says so. */
export const STORE_HELD = "held by another process";

export type Options = {
	env: Env;
	home: string;
	fresh: () => string;
	/** The declared wake latency in ms, or undefined for none. */
	wakeMs: number | undefined;
	/** Called once per distinct delivered record. */
	onMail: (mail: AdapterMail) => void;
	/** One line of operator-facing report; default: stderr. */
	log?: (line: string) => void;
	backoff?: Backoff;
	/** Stops waiting for a clean exit after this long and kills the adapter. */
	killAfterMs?: number;
};

export type Supervisor = {
	/** Start the link; a no-op while an adapter runs or a respawn is pending. */
	start: (sessionId: string | undefined) => void;
	/** One request line to the adapter; replies come back in request order. */
	request: (req: Reply) => Promise<Reply>;
	/** End the link: no respawn, and returns once the adapter has exited. */
	stop: () => Promise<void>;
	/** The Address: set at start, confirmed by the adapter's ready line. */
	me: () => string;
	/** The running adapter's pid, if any. */
	pid: () => number | undefined;
};

/** The delay before the nth consecutive respawn (0-based). */
export function delayMs(b: Backoff, failures: number): number {
	return Math.min(b.maxMs, b.initialMs * 2 ** Math.min(failures, 32));
}

export function superviseAdapter(opts: Options): Supervisor {
	const log = opts.log ?? ((line: string) => void process.stderr.write(`roger shim: ${line}\n`));
	const backoff = opts.backoff ?? BACKOFF;
	let adapter: ChildProcess | undefined;
	let cfg: Settings | undefined; // fixed at start(): a respawn keeps the Address and the Store
	let why = "roger adapter is not running"; // why there is no adapter
	let closed = false; // stop() ran and no start() since: never spawn
	let timer: NodeJS.Timeout | undefined; // the pending respawn
	let failures = 0; // consecutive adapters that did not stay up
	let everReady = false;
	const seen = new Set<string>(); // a redelivery after a crash shares its record id
	const pending: Pending[] = []; // requests awaiting their reply, in order
	let me = opts.env.ROGER_SESSION ?? "";

	const failAll = (e: Error) => {
		for (const p of pending.splice(0)) p.fail(e);
	};

	const spawnAdapter = () => {
		// At most one adapter per session: a second would be a second writer of
		// the same Store.
		if (adapter || closed || !cfg) return;
		const c = cfg;
		const args = ["adapter", "--socket", c.socket, "--store", c.store, "--id", c.session];
		if (opts.wakeMs !== undefined) args.push("--declare", `wake-latency-ms=${opts.wakeMs}`);
		const child = spawn(c.bin, args, { stdio: ["pipe", "pipe", "pipe"] });
		adapter = child;
		const startedAt = Date.now();
		let ready = false;
		let tail = ""; // the end of the adapter's stderr, to tell a refused start
		child.stderr!.on("data", (chunk: Buffer) => {
			process.stderr.write(chunk);
			tail = (tail + chunk.toString()).slice(-2048);
		});
		// 'exit' ends this adapter. 'error' with no pid is a failed spawn; with a
		// pid it is a signal or write problem on a live process, and 'exit' follows.
		const gone = (e: Error, retry: boolean) => {
			if (adapter !== child) return;
			adapter = undefined;
			if (closed) return failAll(e);
			if (Date.now() - startedAt >= backoff.stableMs) failures = 0;
			if (!ready && tail.includes(STORE_HELD)) {
				why = `roger address ${c.session} is already held by another live session (store ${c.store}); this session is not connected to Roger`;
				failAll(new Error(why));
				return log(why);
			}
			failAll(e);
			if (!retry) {
				why = e.message;
				return log(why);
			}
			const wait = delayMs(backoff, failures++);
			why = `${e.message}; reconnecting in ${wait} ms`;
			log(why);
			// no-poll: a reconnect backoff after the adapter exited, not a scan; mail and receipts follow the core's events.
			timer = setTimeout(() => {
				timer = undefined;
				spawnAdapter();
			}, wait);
			timer.unref();
		};
		child.on("error", (e) => {
			if (child.pid === undefined) gone(e, false);
		});
		// "close", not "exit": the adapter's stderr is drained first.
		child.on("close", (code, sig) => gone(new Error(`roger adapter exited (${code ?? sig})`), true));
		child.stdin!.on("error", () => {}); // the exit/error handler reports it
		createInterface({ input: child.stdout! }).on("line", (line) => {
			if (adapter !== child) return; // a replaced adapter says nothing more
			let ev;
			try {
				ev = JSON.parse(line);
			} catch {
				return; // not an adapter event; nothing to correlate it with
			}
			if (ev.type === "ready") {
				me = ev.session;
				ready = true;
				if (everReady) log(`roger adapter reconnected as ${me}`);
				everReady = true;
			} else if (ev.type === "mail") {
				if (seen.has(ev.record)) return;
				seen.add(ev.record);
				opts.onMail(ev);
			} else if (ev.type === "error") pending.shift()?.fail(new Error(ev.error));
			else pending.shift()?.ok(ev);
		});
	};

	return {
		start(sessionId) {
			closed = false;
			// session_start may recur while this runtime lives.
			if (adapter || timer) return;
			try {
				cfg = settings(opts.env, sessionId, opts.home, opts.fresh);
			} catch (e) {
				why = (e as Error).message;
				log(why);
				return;
			}
			me = cfg.session;
			failures = 0;
			spawnAdapter();
		},
		request: (req) =>
			new Promise<Reply>((ok, fail) => {
				if (!adapter?.stdin?.writable) return fail(new Error(why));
				pending.push({ ok, fail });
				adapter.stdin.write(JSON.stringify(req) + "\n");
			}),
		async stop() {
			closed = true;
			if (timer) clearTimeout(timer);
			timer = undefined;
			const child = adapter;
			adapter = undefined;
			why = "roger shim shut down";
			failAll(new Error(why));
			if (!child || child.exitCode !== null || child.signalCode !== null) return;
			// The adapter exits when its stdin closes. Wait for it, SIGKILL after
			// 3 s and wait again, so a session started right after never overlaps
			// it on the same Store.
			await new Promise<void>((done) => {
				// no-poll: bounded wait that turns an adapter that ignores EOF into a kill.
				const t = setTimeout(() => child.kill("SIGKILL"), opts.killAfterMs ?? 3000);
				child.once("exit", () => (clearTimeout(t), done()));
				child.stdin?.end();
			});
		},
		me: () => me,
		pid: () => adapter?.pid,
	};
}

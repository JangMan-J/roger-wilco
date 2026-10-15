import { createServer, createConnection, type Socket } from 'node:net';
import { chmod, lstat, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import {
  MAX_JSON_BYTES, isRegistration, isSessionAddress, sessionKey,
  type HostConfig, type Outcome, type RegistrationResult, type SessionAddress,
} from '../protocol/index.ts';
import type { LocalHostInbox } from './index.ts';

const MAX_FRAME = MAX_JSON_BYTES + 8192;
const DEADLINE = 5000;
/** Waiter deadline starts when the daemon begins an attempt for its session. */
const AWAIT_ATTEMPT_MS = 15_000;
/** Leak guard for a waiter whose session is never attempted. */
const AWAIT_QUEUE_MS = 45_000;
const ok = <T>(value: T): Outcome<T> => ({ ok: true, value });
const invalid = (message: string): Outcome<never> => ({ ok: false, error: 'invalid_request', message });
const conflict = (): Outcome<never> => ({ ok: false, error: 'owner_conflict', message: 'Connection lease is not owned' });

/** Caller must hold the inbox singleton lock before opening the socket. */
export async function listenLocal(config: HostConfig, inbox: LocalHostInbox): Promise<{ close(): Promise<void> }> {
  const path = `${config.inboxPath}.sock`;
  if (Buffer.byteLength(path) > 107) throw new Error('Inbox socket path exceeds 107 bytes');
  if ((await lstat(dirname(path))).mode & 0o077) throw new Error('Inbox directory must be private (mode 0700)');
  try {
    const entry = await lstat(path);
    if (!entry.isSocket()) throw new Error('Refusing to replace a non-socket inbox path');
    await new Promise<void>((resolve, reject) => {
      const probe = createConnection(path);
      probe.setTimeout(200, () => { probe.destroy(); reject(new Error('Inbox socket probe timed out')); });
      probe.once('connect', () => { probe.destroy(); reject(new Error('Inbox socket already has a listener')); });
      probe.once('error', (error: NodeJS.ErrnoException) => {
        probe.destroy();
        if (error.code === 'ECONNREFUSED' || error.code === 'ENOENT') resolve();
        else reject(error);
      });
    });
    await unlink(path).catch((error: NodeJS.ErrnoException) => { if (error.code !== 'ENOENT') throw error; });
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }

  const sockets = new Set<Socket>();
  const unsubscribeFault = inbox.onFault(() => { for (const socket of sockets) socket.destroy(); });
  const server = createServer(socket => {
    sockets.add(socket);
    let input = Buffer.alloc(0);
    let queued = 0, lastId = 0;
    let chain = Promise.resolve();
    let session: SessionAddress | undefined;
    let token: string | undefined;
    let retired = false, dirty = false;
    let leaseExpiresAt = 0;
    let expiry: NodeJS.Timeout | undefined, pressure: NodeJS.Timeout | undefined;
    let unsubscribe: (() => void) | undefined;
    let unsubscribeRegistration: (() => void) | undefined;
    type RegWaiter = { id: number; key: string; timer?: NodeJS.Timeout; attempted: boolean;
      offResult: () => void; offAttempt: () => void };
    const regWaiters = new Map<number, RegWaiter>();
    socket.setTimeout(10_000, () => socket.destroy());
    const close = () => { socket.destroy(); };
    const send = (value: unknown) => {
      if (socket.destroyed) return;
      const frame = Buffer.from(`${JSON.stringify(value)}\n`);
      if (frame.length > MAX_FRAME || socket.writableLength + frame.length > 2 * MAX_FRAME) { close(); return; }
      if (!socket.write(frame) && !pressure) pressure = setTimeout(close, DEADLINE);
    };
    const hint = () => {
      if (!token || socket.destroyed) return;
      if (socket.writableNeedDrain) { dirty = true; return; }
      dirty = false;
      send({ v: 1, event: 'changed' });
    };
    socket.on('drain', () => {
      clearTimeout(pressure); pressure = undefined;
      if (dirty) hint();
    });
    const arm = (expiresAt: number) => {
      leaseExpiresAt = expiresAt;
      clearTimeout(expiry);
      const remaining = expiresAt - Date.now();
      if (remaining <= 0) { close(); return; }
      expiry = setTimeout(() => {
        if (Date.now() >= leaseExpiresAt) close();
        else arm(leaseExpiresAt);
      }, Math.min(remaining, 30_000));
    };
    const owns = (args: Record<string, unknown>) =>
      !!token && args.token === token && !!session && !socket.destroyed;
    const watchRegistration = () => {
      if (unsubscribeRegistration || !session) return;
      unsubscribeRegistration = inbox.onRegistrationResult((key, result) => {
        if (!session || sessionKey(session) !== key || socket.destroyed) return;
        send(result.ok ? { v: 1, event: 'registered' } : { v: 1, event: 'registered', error: result.error, message: result.message });
      });
    };
    // Settle exactly once. A result replies and frees the outstanding slot;
    // cancellation (close/release/shutdown) frees it without a reply.
    const settleWaiter = (waiter: RegWaiter, result?: Outcome<RegistrationResult>) => {
      if (!regWaiters.delete(waiter.id)) return;
      clearTimeout(waiter.timer);
      waiter.offResult(); waiter.offAttempt();
      if (result !== undefined) send({ v: 1, id: waiter.id, result });
      queued--;
      if (!regWaiters.size && !token && !socket.destroyed) socket.setTimeout(10_000, () => socket.destroy());
    };
    const cancelWaiters = () => { for (const waiter of [...regWaiters.values()]) settleWaiter(waiter); };
    // Returns undefined when the reply is deferred to a registration waiter.
    async function command(id: number, method: string, args: Record<string, unknown>): Promise<Outcome<unknown> | undefined> {
      if (method === 'enqueueRegistration' || method === 'awaitRegistration') {
        if (!isRegistration(args.registration)) return invalid('Invalid registration');
        const registration = args.registration;
        const key = sessionKey(registration.session);
        if (session && sessionKey(session) !== key) return invalid('A connection serves one session');
        session = registration.session;
        watchRegistration();
        // Only validation and enqueue stay on the serialized chain. The wait
        // moves to a waiter keyed by request ID so renew/pending/ack proceed.
        if (method === 'enqueueRegistration') {
          // On protocol 2, presence keeps a registered session fresh, so a
          // heartbeat that the lifetime registration answers completes locally:
          // no row, no POST, the same reply.
          if (!(inbox.presenceActive && inbox.registeredResult(key))) await inbox.enqueueRegistration(registration);
          return ok(null);
        }
        // Registered during this daemon's lifetime with nothing pending for
        // the session: answer without a queue write or a coordinator POST.
        const known = inbox.registeredResult(key);
        if (known) return ok(known);
        const timedOut: Outcome<RegistrationResult> = { ok: false, error: 'temporarily_unavailable', message: 'Registration timed out' };
        // Every request for a session queues the same bytes, so the next
        // result for the session answers this waiter. Install it before
        // enqueueing so a fast attempt cannot report unheard.
        const waiter: RegWaiter = { id, key, attempted: false, offResult: () => {}, offAttempt: () => {} };
        const attempted = () => {
          if (waiter.attempted) return;
          waiter.attempted = true;
          clearTimeout(waiter.timer);
          waiter.timer = setTimeout(() => settleWaiter(waiter, timedOut), AWAIT_ATTEMPT_MS);
        };
        waiter.offResult = inbox.onRegistrationResult((got, result) => { if (got === key) settleWaiter(waiter, result); });
        waiter.offAttempt = inbox.onRegistrationAttempt(got => { if (got === key) attempted(); });
        waiter.timer = setTimeout(() => settleWaiter(waiter, timedOut), AWAIT_QUEUE_MS);
        regWaiters.set(id, waiter);
        socket.setTimeout(0); // a held waiter is activity, not idleness
        try { await inbox.enqueueRegistration(registration); }
        catch (error) { settleWaiter(waiter); throw error; }
        // A queued row that already failed answers with that attempt; an
        // attempt for the session may instead be in flight.
        const failed = inbox.queuedFailure(key);
        if (failed) settleWaiter(waiter, failed);
        else if (!socket.destroyed && regWaiters.has(id) && inbox.isRegistrationAttempting(key)) attempted();
        return undefined;
      }
      if (method === 'bind') {
        if (token || retired) return invalid('A connection binds only once');
        if (!isSessionAddress(args.session) || typeof args.runtimeId !== 'string' || !args.runtimeId || args.runtimeId.length > 200)
          return invalid('Invalid binding');
        const grant = randomUUID();
        const result = await inbox.bind(args.session, grant, Date.now());
        if (!result.ok) return result;
        if (socket.destroyed) { await inbox.release(args.session, grant); return conflict(); }
        session = args.session; token = grant;
        // Subscribe before replying. The client catches up even if a hint preceded
        // the ready reply, so there is no snapshot/subscription gap.
        unsubscribe = inbox.subscribe(key => { if (session && key === sessionKey(session)) hint(); });
        socket.setTimeout(0);
        watchRegistration();
        arm(result.value.leaseExpiresAt);
        return ok({ token, leaseExpiresAt: result.value.leaseExpiresAt });
      }
      if (!owns(args)) return conflict();
      const owner = token!, address = session!;
      switch (method) {
        case 'renew': {
          const result = await inbox.renew(address, owner, Date.now());
          if (result.ok && !socket.destroyed) arm(result.value.leaseExpiresAt);
          else close();
          return result;
        }
        case 'release':
          token = undefined; retired = true;
          cancelWaiters();
          unsubscribe?.(); unsubscribeRegistration?.(); unsubscribeRegistration = undefined; clearTimeout(expiry);
          await inbox.release(address, owner);
          socket.setTimeout(10_000, close);
          return ok(null);
        case 'pending':
          if (args.afterSequence !== undefined &&
              (!Number.isSafeInteger(args.afterSequence) || (args.afterSequence as number) < 0)) return invalid('Invalid cursor');
          return inbox.pendingPage(address, owner, args.afterSequence as number | undefined);
        case 'ack':
          return inbox.acknowledgeDelivery(address, owner, args.evidence as Parameters<LocalHostInbox['acknowledgeDelivery']>[2]);
        default: return invalid('Unknown command');
      }
    }
    socket.on('data', (chunk: Buffer) => {
      // Parse multiple bounded frames without accepting an unbounded partial line.
      let start = 0;
      while (start < chunk.length && !socket.destroyed) {
        const end = chunk.indexOf(10, start);
        const part = chunk.subarray(start, end < 0 ? chunk.length : end + 1);
        if (input.length + part.length > MAX_FRAME) { close(); return; }
        input = Buffer.concat([input, part]);
        start += part.length;
        if (end < 0) break;
        let request: { v?: unknown; id?: unknown; method?: unknown; args?: unknown };
        try { request = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(input)); }
        catch { close(); return; }
        input = Buffer.alloc(0);
        if (!request || request.v !== 1 || !Number.isSafeInteger(request.id) || (request.id as number) <= lastId ||
            typeof request.method !== 'string' || !['bind', 'renew', 'release', 'pending', 'ack', 'enqueueRegistration', 'awaitRegistration'].includes(request.method) ||
            !request.args || typeof request.args !== 'object' || Array.isArray(request.args) || ++queued > 4) { close(); return; }
        lastId = request.id as number;
        const { id, method, args } = request;
        chain = chain.then(async () => {
          if (socket.destroyed) { queued--; return; }
          const result = await command(id as number, method as string, args as Record<string, unknown>);
          // A deferred awaitRegistration reply owns its outstanding slot
          // until settleWaiter replies or cancels it.
          if (result !== undefined) { send({ v: 1, id, result }); queued--; }
        }).catch(() => {
          // A storage/notifier exception is never a successful reply. Disconnect
          // all readers so their next connection must reconcile durable state.
          for (const peer of sockets) peer.destroy();
        });
      }
    });
    socket.on('error', () => close());
    socket.once('close', () => {
      sockets.delete(socket);
      cancelWaiters(); // daemon shutdown reaches here by destroying sockets
      unsubscribe?.();
      unsubscribeRegistration?.();
      clearTimeout(expiry); clearTimeout(pressure);
      const owner = token;
      token = undefined; retired = true;
      if (owner && session) void inbox.release(session, owner).catch(() => {});
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(path, () => { server.off('error', reject); resolve(); });
  });
  try { await chmod(path, 0o600); }
  catch (error) { server.close(); throw error; }
  let closing: Promise<void> | undefined;
  return {
    close: () => closing ??= (async () => {
      unsubscribeFault();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
      await unlink(path).catch((error: NodeJS.ErrnoException) => { if (error.code !== 'ENOENT') throw error; });
    })(),
  };
}

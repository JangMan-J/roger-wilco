import { createServer, createConnection, type Socket } from 'node:net';
import { chmod, lstat, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import {
  MAX_JSON_BYTES, isRegistration, isSessionAddress, sessionKey,
  type HostConfig, type Outcome, type SessionAddress,
} from '../protocol/index.ts';
import type { LocalHostInbox } from './index.ts';

const MAX_FRAME = MAX_JSON_BYTES + 8192;
const DEADLINE = 5000;
const LEASE_MS = 30_000;
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
    let expires = 0;
    let expiry: NodeJS.Timeout | undefined, pressure: NodeJS.Timeout | undefined;
    let unsubscribe: (() => void) | undefined;
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
    const arm = () => {
      expires = performance.now() + LEASE_MS;
      clearTimeout(expiry);
      expiry = setTimeout(close, LEASE_MS);
    };
    const owns = (args: Record<string, unknown>) =>
      !!token && args.token === token && !!session && performance.now() < expires && !socket.destroyed;
    async function command(method: string, args: Record<string, unknown>): Promise<Outcome<unknown>> {
      if (method === 'enqueueRegistration') {
        if (!isRegistration(args.registration)) return invalid('Invalid registration');
        await inbox.enqueueRegistration(args.registration);
        return ok(null);
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
        arm();
        return ok({ token, leaseExpiresAt: result.value.leaseExpiresAt });
      }
      if (!owns(args)) return conflict();
      const owner = token!, address = session!;
      switch (method) {
        case 'renew': {
          const result = await inbox.renew(address, owner, Date.now());
          if (result.ok && !socket.destroyed) arm();
          else close();
          return result;
        }
        case 'release':
          token = undefined; retired = true;
          unsubscribe?.(); clearTimeout(expiry);
          await inbox.release(address, owner);
          socket.setTimeout(10_000, close);
          return ok(null);
        case 'pending':
          if (args.afterSequence !== undefined &&
              (!Number.isSafeInteger(args.afterSequence) || (args.afterSequence as number) < 0)) return invalid('Invalid cursor');
          return inbox.pendingPage(address, owner, args.afterSequence as number | undefined);
        case 'ack':
          return inbox.acknowledgePi(address, owner, args.proof as Parameters<LocalHostInbox['acknowledgePi']>[2]);
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
            typeof request.method !== 'string' || !['bind', 'renew', 'release', 'pending', 'ack', 'enqueueRegistration'].includes(request.method) ||
            !request.args || typeof request.args !== 'object' || Array.isArray(request.args) || ++queued > 4) { close(); return; }
        lastId = request.id as number;
        const { id, method, args } = request;
        chain = chain.then(async () => {
          if (!socket.destroyed) send({ v: 1, id, result: await command(method as string, args as Record<string, unknown>) });
        }).catch(() => {
          // A storage/notifier exception is never a successful reply. Disconnect
          // all readers so their next connection must reconcile durable state.
          for (const peer of sockets) peer.destroy();
        }).finally(() => { queued--; });
      }
    });
    socket.on('error', () => close());
    socket.once('close', () => {
      sockets.delete(socket);
      unsubscribe?.();
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

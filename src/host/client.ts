/** Linux pathname-socket client. The daemon owns the inbox database. */
import { lstat } from 'node:fs/promises';
import { connect, type Socket } from 'node:net';
import { dirname, resolve } from 'node:path';
import {
  isDeliveryEvidence, isFailureCode, isRegistration, isRegistrationResult, isSessionAddress, isStreamFrame, MAX_JSON_BYTES, sessionKey,
  type FailureCode, type HostInbox, type Message, type Outcome, type DeliveryEvidence,
  type Registration, type RegistrationResult, type SessionAddress, type StreamFrame,
} from '../protocol/index.ts';

const MAX_FRAME = MAX_JSON_BYTES + 8192;
const MAX_OUTSTANDING = 4;
const MAX_PAGE = 32;
const REQUEST_MS = 10_000;
/** Exceeds the daemon waiter's queue guard plus its per-attempt deadline. */
const AWAIT_REQUEST_MS = 65_000;
const CONNECT_MS = 5_000;
const DRAIN_MS = 5_000;
type Waiter = { method: string; resolve: (value: Outcome<unknown>) => void; reject: (error: Error) => void; timer: NodeJS.Timeout };
const failure = (error: FailureCode, message: string): Outcome<never> => ({ ok: false, error, message });
const rec = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const exact = (value: Record<string, unknown>, keys: readonly string[]) =>
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const isFailure = isFailureCode;
const safeTime = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const validToken = (value: unknown): value is string => typeof value === 'string' && value.length > 0 && value.length <= 512 &&
  value.isWellFormed() && !/[\u0000-\u001f\u007f-\u009f\s]/u.test(value);
function acceptMessage(value: unknown, subscriptionId: string): value is Message {
  return isStreamFrame({ type: 'message', subscriptionId, message: value });
}
function parseOutcome(method: string, value: unknown): Outcome<unknown> | undefined {
  if (!rec(value)) return;
  if (value.ok === true && exact(value, ['ok', 'value'])) {
    const body = value.value;
    const ok = method === 'release' || method === 'ack' || method === 'enqueueRegistration' ? body === null
      : method === 'awaitRegistration' ? isRegistrationResult(body)
      : rec(body) && (method === 'bind' ? exact(body, ['token', 'leaseExpiresAt']) && validToken(body.token) && safeTime(body.leaseExpiresAt)
        : method === 'renew' ? exact(body, ['leaseExpiresAt']) && safeTime(body.leaseExpiresAt)
          : method === 'pending' && exact(body, ['messages', 'more']) && Array.isArray(body.messages) && body.messages.length <= 1 && typeof body.more === 'boolean');
    return ok ? { ok: true, value: body } : undefined;
  }
  if (value.ok === false && exact(value, ['ok', 'error', 'message']) && isFailure(value.error) && typeof value.message === 'string')
    return { ok: false, error: value.error, message: value.message };
  return;
}

export function createHostClient(inboxPath: string): HostInbox {
  return new HostClient(inboxPath);
}

class HostClient implements HostInbox {
  private readonly socketPath: string;
  private socket: Socket | undefined;
  private opening: Promise<void> | undefined;
  private writing: Promise<void> = Promise.resolve();
  private input = Buffer.alloc(0);
  private nextId = 1;
  private readonly waiters = new Map<number, Waiter>();
  private readonly listeners = new Set<() => void>();
  private readonly registrationListeners = new Set<(result: Outcome<null>) => void>();
  private token: string | undefined;
  private leaseExpiresAt: number | undefined;
  private boundKey: string | undefined;
  private bindSpent = false;
  private bindFlight: Promise<Outcome<{ leaseExpiresAt: number }>> | undefined;
  private closed = false;

  constructor(inboxPath: string) {
    if (process.platform !== 'linux') throw new Error('Host client requires a Linux Unix socket');
    if (typeof inboxPath !== 'string' || !inboxPath) throw new TypeError('Invalid inbox path');
    this.socketPath = `${inboxPath}.sock`;
    if (Buffer.byteLength(this.socketPath) > 107 || Buffer.byteLength(resolve(this.socketPath)) > 107)
      throw new Error('Inbox socket path exceeds 107 bytes');
  }
  onChange(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  }
  onRegistrationResult(listener: (result: Outcome<null>) => void): () => void {
    this.registrationListeners.add(listener);
    return () => { this.registrationListeners.delete(listener); };
  }
  authority(): string | undefined { return this.token; }

  async bind(session: SessionAddress, runtimeId: string): Promise<Outcome<{ leaseExpiresAt: number }>> {
    if (this.closed) return failure('temporarily_unavailable', 'Host client is closed');
    if (!isSessionAddress(session) || typeof runtimeId !== 'string' || !runtimeId || runtimeId.length > 200)
      return failure('invalid_request', 'Invalid lease input');
    const key = sessionKey(session);
    if (this.boundKey && this.boundKey !== key) return failure('owner_conflict', 'Session already active');
    if (this.token && this.leaseExpiresAt !== undefined && this.socket?.writable)
      return { ok: true, value: { leaseExpiresAt: this.leaseExpiresAt } };
    if (this.bindFlight) return this.bindFlight;
    this.boundKey = key;
    const flight = this.bindNew(session, runtimeId, key);
    this.bindFlight = flight;
    try { return await flight; }
    finally { if (this.bindFlight === flight) this.bindFlight = undefined; }
  }
  private async bindNew(session: SessionAddress, runtimeId: string, key: string): Promise<Outcome<{ leaseExpiresAt: number }>> {
    if (this.closed) return failure('temporarily_unavailable', 'Host client is closed');
    if (this.socket && (this.bindSpent || this.token)) this.disconnect(new Error('Stale inbox connection'));
    this.boundKey = key;
    try { if (!this.socket?.writable) await this.open(); }
    catch (error) {
      this.disconnect(error instanceof Error ? error : new Error(String(error)));
      return failure('temporarily_unavailable', error instanceof Error ? error.message : String(error));
    }
    if (this.closed || !this.socket?.writable) return failure('temporarily_unavailable', 'Inbox socket unavailable');
    this.bindSpent = true;
    const socket = this.socket;
    const result = await this.invoke('bind', { session, runtimeId });
    if (this.socket !== socket || this.closed) return failure('temporarily_unavailable', 'Inbox socket closed');
    if (!result.ok) { this.disconnect(new Error(result.message)); return result; }
    const value = result.value as { token: string; leaseExpiresAt: number };
    this.token = value.token; this.boundKey = key; this.leaseExpiresAt = value.leaseExpiresAt;
    this.emit();
    return { ok: true, value: { leaseExpiresAt: value.leaseExpiresAt } };
  }
  async renew(session: SessionAddress, runtimeId: string): Promise<Outcome<{ leaseExpiresAt: number }>> {
    if (!isSessionAddress(session) || typeof runtimeId !== 'string' || !runtimeId) return failure('invalid_request', 'Invalid lease input');
    const token = this.token;
    if (!token || this.boundKey !== sessionKey(session)) return failure('owner_conflict', 'Lease no longer owned');
    const result = await this.invoke('renew', { token });
    if (this.token !== token) return failure('owner_conflict', 'Lease no longer owned');
    if (result.ok) this.leaseExpiresAt = (result.value as { leaseExpiresAt: number }).leaseExpiresAt;
    else if (result.error === 'owner_conflict') this.disconnect(new Error('Lease no longer owned'));
    return result as Outcome<{ leaseExpiresAt: number }>;
  }
  async release(session: SessionAddress, runtimeId: string): Promise<void> {
    if (!isSessionAddress(session) || typeof runtimeId !== 'string' || !runtimeId) return;
    const token = this.token;
    if (!token || this.boundKey !== sessionKey(session)) return;
    this.token = undefined; this.leaseExpiresAt = undefined; this.boundKey = undefined;
    try { if (this.socket?.writable) await this.send('release', { token }); }
    finally { this.disconnect(new Error('Inbox lease released')); }
  }
  async pending(session: SessionAddress, runtimeId: string): Promise<Message[]> {
    if (!isSessionAddress(session) || typeof runtimeId !== 'string' || !runtimeId) throw new TypeError('Invalid pending query');
    const token = this.token;
    if (!token || this.boundKey !== sessionKey(session)) throw new Error('owner_conflict: Lease no longer owned');
    const messages: Message[] = [];
    let afterSequence: number | undefined;
    while (messages.length < MAX_PAGE) {
      if (this.token !== token) throw new Error('Inbox authority lost');
      const result = await this.send('pending', afterSequence === undefined ? { token } : { token, afterSequence });
      if (this.token !== token) throw new Error('Inbox authority lost');
      if (!result.ok) {
        if (result.error === 'owner_conflict') this.disconnect(new Error(result.message));
        throw new Error(`${result.error}: ${result.message}`);
      }
      const page = result.value as { messages: Message[]; more: boolean };
      const message = page.messages[0];
      if (!message) break;
      if (afterSequence !== undefined && message.sequence <= afterSequence) {
        this.disconnect(new Error('Malformed pending cursor')); throw new Error('Malformed pending cursor');
      }
      messages.push(message); afterSequence = message.sequence;
      if (!page.more) break;
    }
    return messages;
  }
  async acknowledgeDelivery(session: SessionAddress, runtimeId: string, evidence: DeliveryEvidence): Promise<Outcome<null>> {
    if (!isSessionAddress(session) || typeof runtimeId !== 'string' || !runtimeId || !isDeliveryEvidence(evidence) ||
      evidence.sessionId !== session.sessionId || !evidence.messageId.startsWith(`${session.namespace}:msg:`))
      return failure('invalid_request', 'Invalid delivery evidence');
    const token = this.token;
    if (!token || this.boundKey !== sessionKey(session)) return failure('owner_conflict', 'Lease no longer owned');
    const result = await this.invoke('ack', { token, evidence });
    if (this.token !== token) return failure('owner_conflict', 'Lease no longer owned');
    if (!result.ok && result.error === 'owner_conflict') this.disconnect(new Error('Lease no longer owned'));
    return result as Outcome<null>;
  }
  async enqueueRegistration(registration: Registration): Promise<void> {
    if (this.closed) throw new Error('Host client is closed');
    if (!isRegistration(registration)) throw new TypeError('Invalid registration');
    if (!this.socket?.writable) await this.open();
    const result = await this.send('enqueueRegistration', { registration });
    if (!result.ok) throw new Error(`${result.error}: ${result.message}`);
  }
  async awaitRegistration(registration: Registration): Promise<Outcome<RegistrationResult>> {
    if (this.closed) return failure('temporarily_unavailable', 'Host client is closed');
    if (!isRegistration(registration)) return failure('invalid_request', 'Invalid registration');
    try {
      if (!this.socket?.writable) await this.open();
      return await this.send('awaitRegistration', { registration }, AWAIT_REQUEST_MS) as Outcome<RegistrationResult>;
    } catch (error) {
      return failure('temporarily_unavailable', error instanceof Error ? error.message : String(error));
    }
  }
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.registrationListeners.clear();
    const token = this.token;
    this.token = undefined; this.leaseExpiresAt = undefined; this.boundKey = undefined;
    if (token && this.socket?.writable && this.waiters.size < MAX_OUTSTANDING) {
      try { await this.send('release', { token }); } catch { /* disconnect retires the token */ }
    }
    this.disconnect(new Error('Host client closed'));
  }
  private async invoke(method: string, args: unknown): Promise<Outcome<unknown>> {
    try { return await this.send(method, args); }
    catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return failure('temporarily_unavailable', message.startsWith('temporarily_unavailable') ? 'Too many outstanding inbox requests' : message);
    }
  }
  private async open(): Promise<void> {
    if (this.closed) throw new Error('Host client is closed');
    if (this.socket?.writable) return;
    this.opening ??= this.connect().finally(() => { this.opening = undefined; });
    await this.opening;
    if (!this.socket?.writable) throw new Error('Inbox socket unavailable');
  }
  private async connect(): Promise<void> {
    const directory = await lstat(dirname(this.socketPath));
    const info = await lstat(this.socketPath);
    if (!directory.isDirectory() || (directory.mode & 0o077) !== 0) throw new Error('Inbox directory is not private');
    if (!info.isSocket() || (info.mode & 0o777) !== 0o600) throw new Error('Inbox socket is not private');
    await new Promise<void>((resolvePromise, reject) => {
      const socket = connect(this.socketPath);
      let settled = false;
      const timer = setTimeout(() => finish(new Error('Inbox socket connect timed out')), CONNECT_MS);
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true; clearTimeout(timer); socket.off('connect', onConnect); socket.off('error', onError);
        if (error) { socket.destroy(); reject(error); return; }
        this.attach(socket); resolvePromise();
      };
      const onConnect = () => finish();
      const onError = (error: Error) => finish(error);
      timer.unref(); socket.once('connect', onConnect); socket.once('error', onError);
    });
  }
  private attach(socket: Socket): void {
    if (this.closed) { socket.destroy(); return; }
    this.socket = socket; this.bindSpent = false; this.input = Buffer.alloc(0); this.nextId = 1;
    socket.on('data', (chunk: Buffer) => this.onData(socket, chunk));
    socket.on('error', () => this.disconnect(new Error('Inbox socket failed'), socket));
    socket.on('end', () => { if (!socket.destroyed) socket.destroy(); });
    socket.on('close', () => this.disconnect(new Error('Inbox socket closed'), socket));
  }
  private onData(socket: Socket, chunk: Buffer): void {
    if (socket !== this.socket) return;
    let start = 0;
    while (start < chunk.length && this.socket === socket) {
      const end = chunk.indexOf(10, start);
      const part = chunk.subarray(start, end < 0 ? chunk.length : end + 1);
      if (this.input.length + part.length > MAX_FRAME) { this.disconnect(new Error('Oversized frame'), socket); return; }
      this.input = Buffer.concat([this.input, part]); start += part.length;
      if (end < 0) break;
      let text: string;
      try { text = new TextDecoder('utf-8', { fatal: true }).decode(this.input); }
      catch { this.disconnect(new Error('Invalid UTF-8'), socket); return; }
      this.input = Buffer.alloc(0);
      if (text === '\n') { this.disconnect(new Error('Malformed frame'), socket); return; }
      this.onFrame(socket, text.endsWith('\n') ? text.slice(0, -1) : text);
    }
  }
  private onFrame(socket: Socket, line: string): void {
    let value: unknown;
    try { value = JSON.parse(line); }
    catch { this.disconnect(new Error('Malformed frame'), socket); return; }
    if (!rec(value) || value.v !== 1) { this.disconnect(new Error('Malformed envelope'), socket); return; }
    if ('event' in value) {
      if (exact(value, ['v', 'event']) && value.event === 'changed') this.emit();
      else if (exact(value, ['v', 'event']) && value.event === 'registered') this.emitRegistration();
      else if (exact(value, ['v', 'event', 'error', 'message']) && value.event === 'registered' && isFailure(value.error) && typeof value.message === 'string')
        this.emitRegistration(value.error, value.message);
      else this.disconnect(new Error('Malformed event'), socket);
      return;
    }
    const id = value.id;
    const waiter = exact(value, ['v', 'id', 'result']) && Number.isSafeInteger(id) && (id as number) > 0 ? this.waiters.get(id as number) : undefined;
    if (!waiter) { this.disconnect(new Error('Malformed response'), socket); return; }
    this.waiters.delete(id as number); clearTimeout(waiter.timer);
    const result = parseOutcome(waiter.method, value.result);
    if (!result || (result.ok && waiter.method === 'pending' && !this.pageOk(result.value))) {
      waiter.reject(new Error('Malformed response')); this.disconnect(new Error('Malformed response'), socket); return;
    }
    waiter.resolve(result);
  }
  private pageOk(value: unknown): boolean {
    const page = value as { messages: unknown[]; more: boolean };
    const key = this.boundKey;
    return !!key && !(page.messages.length === 0 && page.more) && page.messages.every(item => acceptMessage(item, key));
  }
  private send(method: string, args: unknown, timeout = REQUEST_MS): Promise<Outcome<unknown>> {
    const socket = this.socket;
    if (!socket?.writable) return Promise.reject(new Error('Inbox socket unavailable'));
    if (this.waiters.size >= MAX_OUTSTANDING) return Promise.reject(new Error('temporarily_unavailable: Too many outstanding inbox requests'));
    const id = this.nextId++;
    if (!Number.isSafeInteger(id)) return Promise.reject(new Error('Inbox request id exhausted'));
    const body = JSON.stringify({ v: 1, id, method, args });
    if (Buffer.byteLength(body) + 1 > MAX_FRAME) return Promise.reject(new Error('Inbox request exceeds frame limit'));
    return new Promise((resolvePromise, reject) => {
      const timer = setTimeout(() => this.disconnect(new Error('Inbox request timed out'), socket), timeout);
      timer.unref();
      this.waiters.set(id, { method, resolve: resolvePromise, reject, timer });
      const frame = `${body}\n`;
      this.writing = this.writing.then(() => this.writeFrame(socket, frame), () => this.writeFrame(socket, frame));
      void this.writing.catch(error => { if (this.socket === socket) this.disconnect(error instanceof Error ? error : new Error(String(error)), socket); });
    });
  }
  private writeFrame(socket: Socket, frame: string): Promise<void> {
    if (this.socket !== socket || !socket.writable) return Promise.reject(new Error('Inbox socket unavailable'));
    const bytes = Buffer.from(frame);
    if (socket.writableLength + bytes.length > 2 * MAX_FRAME) return Promise.reject(new Error('Inbox socket output exceeded'));
    return new Promise((resolvePromise, reject) => {
      let settled = false;
      const finish = (error?: Error) => { if (settled) return; settled = true; clearTimeout(timer); if (error) reject(error); else resolvePromise(); };
      const timer = setTimeout(() => { socket.destroy(); finish(new Error('Inbox socket write stalled')); }, DRAIN_MS);
      timer.unref();
      try { socket.write(bytes, error => finish(error ?? undefined)); }
      catch (error) { finish(error instanceof Error ? error : new Error(String(error))); }
    });
  }
  private disconnect(error: Error, socket?: Socket): void {
    if (socket && socket !== this.socket) return;
    const notify = this.socket !== undefined || this.token !== undefined || this.waiters.size > 0;
    const current = this.socket;
    this.socket = undefined; this.token = undefined; this.leaseExpiresAt = undefined; this.boundKey = undefined; this.bindSpent = false; this.input = Buffer.alloc(0);
    for (const waiter of this.waiters.values()) { clearTimeout(waiter.timer); waiter.reject(error); }
    this.waiters.clear();
    if (current && !current.destroyed) current.destroy();
    if (notify) this.emit();
  }
  private emit(): void {
    for (const listener of this.listeners) queueMicrotask(() => {
      if (!this.listeners.has(listener)) return;
      try { listener(); }
      catch {
        this.listeners.delete(listener);
        this.disconnect(new Error('Inbox observer failed'));
      }
    });
  }
  private emitRegistration(error?: FailureCode, message?: string): void {
    const result: Outcome<null> = error === undefined ? { ok: true, value: null } : { ok: false, error, message: message! };
    for (const listener of this.registrationListeners) queueMicrotask(() => {
      if (!this.registrationListeners.has(listener)) return;
      try { listener(result); }
      catch {
        this.registrationListeners.delete(listener);
        this.disconnect(new Error('Inbox observer failed'));
      }
    });
  }
}

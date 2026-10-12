import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { persistedReceipt } from '../../src/pi/index.ts';

test('repeated pending IDs reuse an unchanged disk snapshot; mutations invalidate receipts', async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'rw-receipt-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.jsonl');
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const header = `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n`;
  const entry = `${JSON.stringify({ type: 'custom_message', customType: 'roger-wilco', id: 'entry-one', details: { deliveryId: 'fleet:msg:1' } })}\n`;
  const filler = `${JSON.stringify({ type: 'message', content: 'x'.repeat(1024) })}\n`.repeat(6500);
  await fs.writeFile(file, header + filler);
  const originalOpen = fs.open;
  let reads = 0, syncs = 0;
  fs.open = async (...args) => {
    const handle = await originalOpen(...args);
    if (args[0] === file) {
      const read = handle.readFile.bind(handle), sync = handle.sync.bind(handle);
      handle.readFile = (...args) => { reads++; return read(...args); };
      handle.sync = (...args) => { syncs++; return sync(...args); };
    }
    return handle;
  };
  syncBuiltinESMExports();
  t.after(() => { fs.open = originalOpen; syncBuiltinESMExports(); });
  const start = performance.now();
  for (let i = 0; i < 100; i++) assert.equal(await persistedReceipt(file, session, `fleet:msg:${i + 1}`), undefined);
  t.diagnostic(`${Buffer.byteLength(header + filler)} bytes, 100 pending lookups: ${(performance.now() - start).toFixed(2)}ms; full reads: ${reads}`);
  assert.equal(reads, 1, 'unchanged JSONL must not be reread for each pending ID/poll');
  assert.equal(syncs, 0, 'absence is not a receipt');
  await fs.appendFile(file, entry.slice(0, -1));
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'partial final line is not committed');
  await fs.appendFile(file, '\n');
  assert.equal((await persistedReceipt(file, session, 'fleet:msg:1')).entryId, 'entry-one');
  assert.equal(syncs, 1);
  assert.equal((await persistedReceipt(file, session, 'fleet:msg:1')).entryId, 'entry-one');
  assert.equal(syncs, 2, 'cached matches still fsync before returning a receipt');
  await fs.writeFile(file, header);
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'truncation discards the index');
  await fs.writeFile(file, header + entry);
  assert.ok(await persistedReceipt(file, session, 'fleet:msg:1'));
  const replacement = join(dir, 'replacement.jsonl');
  await fs.writeFile(replacement, header + entry.replace('roger-wilco', 'other-type!'));
  await fs.rename(replacement, file);
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'same-size replacement discards the index');
  await fs.writeFile(file, header + entry);
  assert.ok(await persistedReceipt(file, session, 'fleet:msg:1'));
  await fs.writeFile(file, header + entry.replace('roger-wilco', 'other-type!'));
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'same-inode, same-size rewrite discards the index');
  await fs.writeFile(file, header.replace(session.piSessionId, randomUUID()) + entry);
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'another session cannot supply a receipt');
  await fs.writeFile(file, header + '{bad json}\n' + entry);
  await assert.rejects(persistedReceipt(file, session, 'fleet:msg:1'), SyntaxError);
  await fs.writeFile(file, header + entry);
  assert.ok(await persistedReceipt(file, session, 'fleet:msg:1'));
  await fs.unlink(file);
  await assert.rejects(persistedReceipt(file, session, 'fleet:msg:1'), { code: 'ENOENT' });
  await fs.writeFile(file, header);
  assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined, 'recreation after deletion cannot retain a receipt');
});

test('mutation during scan or fsync cannot return a stale receipt', async t => {
  const dir = await fs.mkdtemp(join(tmpdir(), 'rw-receipt-race-'));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'session.jsonl');
  const session = { namespace: 'fleet', piSessionId: randomUUID() };
  const header = `${JSON.stringify({ type: 'session', id: session.piSessionId })}\n`;
  const entry = `${JSON.stringify({ type: 'custom_message', customType: 'roger-wilco', id: 'entry-one', details: { deliveryId: 'fleet:msg:1' } })}\n`;
  const originalOpen = fs.open;
  t.after(() => { fs.open = originalOpen; syncBuiltinESMExports(); });
  for (const boundary of ['readFile', 'sync']) {
    await fs.writeFile(file, header + entry);
    fs.open = async (...args) => {
      const handle = await originalOpen(...args);
      if (args[0] === file) {
        const original = handle[boundary].bind(handle);
        handle[boundary] = async (...args) => {
          const result = await original(...args);
          if (boundary === 'readFile') await fs.truncate(file, Buffer.byteLength(header));
          else {
            const replacement = join(dir, 'replacement.jsonl');
            await fs.writeFile(replacement, header);
            await fs.rename(replacement, file);
          }
          return result;
        };
      }
      return handle;
    };
    syncBuiltinESMExports();
    await assert.rejects(persistedReceipt(file, session, 'fleet:msg:1'), /changed during receipt/);
    fs.open = originalOpen;
    syncBuiltinESMExports();
    assert.equal(await persistedReceipt(file, session, 'fleet:msg:1'), undefined);
  }
});

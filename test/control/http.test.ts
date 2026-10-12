import test from 'node:test';
import assert from 'node:assert/strict';
import { IncomingMessage } from 'node:http';
import { Socket } from 'node:net';
import { MAX_JSON_BYTES } from '../../src/protocol/index.ts';
import { readBody } from '../../src/server/http.ts';

// Drive the incoming stream directly so byte limits and deadlines need no network timing.
test('JSON body reader accepts the byte boundary and rejects overflow without Content-Length', async t => {
  const value = 'é'.repeat((MAX_JSON_BYTES - 2) / 2);
  const body = Buffer.from(JSON.stringify(value));
  assert.equal(body.length, MAX_JSON_BYTES);
  for (const overflow of [false, true]) {
    const request = new IncomingMessage(new Socket());
    t.after(() => request.destroy());
    request.headers['content-type'] = 'application/json; charset=utf-8';
    const result = readBody(request);
    request.emit('data', body.subarray(0, 5));
    request.emit('data', body.subarray(5));
    if (overflow) {
      request.emit('data', Buffer.from(' '));
      await assert.rejects(result, { code: 413, message: 'Request body exceeds 1 MiB' });
      assert.equal(request.isPaused(), true);
    } else {
      request.emit('end');
      assert.equal(await result, value);
    }
    for (const event of ['data', 'end', 'error', 'aborted']) assert.equal(request.listenerCount(event), 0);
  }
});

test('JSON body reader settles and releases listeners on deadline or interruption', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  for (const reason of ['deadline', 'aborted', 'error']) {
    const request = new IncomingMessage(new Socket());
    t.after(() => request.destroy());
    request.headers['content-type'] = 'application/json';
    const result = readBody(request);
    request.emit('data', Buffer.from('{'));
    if (reason === 'deadline') t.mock.timers.tick(10_000);
    else request.emit(reason);
    await assert.rejects(result, { code: reason === 'deadline' ? 408 : 400 });
    assert.equal(request.isPaused(), true);
    for (const event of ['data', 'end', 'error', 'aborted']) assert.equal(request.listenerCount(event), 0);
  }
});

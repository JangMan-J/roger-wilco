// Host daemon child for the v2 crash tests: it kills itself with SIGKILL at a
// named point. `before-commit`: at the COMMIT of the first batch that inserts
// a message. `before-receipt`: when the first v2 receipt POST starts, after its
// batch committed. `hello`: at the hello COMMIT. HOST and CREDENTIAL default to the
// fake coordinator's `host-a` and `secret`.
import { DatabaseSync } from 'node:sqlite';
import { createDaemonInbox, createHostDaemon } from '../../src/host/index.ts';

const [inboxPath, coordinatorUrl, at, hostId = 'host-a', credential = 'secret'] = process.argv.slice(2);
const die = () => process.kill(process.pid, 'SIGKILL');
let armed = false;
const prepare = DatabaseSync.prototype.prepare;
DatabaseSync.prototype.prepare = function (sql) {
  if (at === 'before-commit' && sql.startsWith('INSERT INTO inbox_messages')) armed = true;
  if (at === 'hello' && sql.startsWith('UPDATE inbox_stream SET stream_id')) armed = true;
  return prepare.call(this, sql);
};
const exec = DatabaseSync.prototype.exec;
DatabaseSync.prototype.exec = function (sql) {
  if (armed && sql === 'COMMIT') die();
  return exec.call(this, sql);
};
const fetch = globalThis.fetch;
globalThis.fetch = (input, init) => {
  if (at === 'before-receipt' && String(input).endsWith('/v2/hosts/receipts')) die();
  return fetch(input, init);
};
const inbox = createDaemonInbox(inboxPath);
await createHostDaemon({ coordinatorUrl, hostId, credential, inboxPath }, inbox).start(new AbortController().signal);

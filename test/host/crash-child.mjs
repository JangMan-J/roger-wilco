import { createDaemonInbox as createHostInbox } from '../../src/host/index.ts';
import { listenLocal } from '../../src/host/ipc.ts';

const inboxPath = process.argv[2];
const inbox = createHostInbox(inboxPath);
// Registered before the runtime subscriber. receive() calls this only after
// SQLite commits, and SIGKILL prevents the next subscriber from writing a hint.
inbox.subscribe(() => process.kill(process.pid, 'SIGKILL'));
await listenLocal({ inboxPath, coordinatorUrl: 'http://127.0.0.1:1',
  hostId: 'host-a', credential: 'offline' }, inbox);
process.on('message', frame => {
  void inbox.receive(frame).catch(error => { console.error(error); process.exit(1); });
});
process.send({ ready: true });

import test from 'node:test';
import assert from 'node:assert/strict';
import { access, readFile, realpath } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

// setup:sdk links the selected source checkout. Never substitute an npm SDK.
const root = await realpath(resolve(process.env.PI_PACKAGE_DIR ??
  fileURLToPath(new URL('../../node_modules/@earendil-works/pi-coding-agent/', import.meta.url))));
const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
assert.equal(manifest.name, '@earendil-works/pi-coding-agent');
// The probe drives Pi's own test harness, which exists only in a Pi source
// checkout. With a built npm SDK (CI) there is nothing to probe; skip.
const sourceResolver = join(root, 'src/experimental/source-resolver.ts');
const hasSource = await access(sourceResolver).then(() => true, () => false);
if (!hasSource) process.exit(0); // no Pi source checkout (built SDK only, e.g. CI): nothing to probe
await import(pathToFileURL(sourceResolver).href);
const { createHarness } = await import(pathToFileURL(join(root, 'test/suite/harness.ts')).href);
const { fauxAssistantMessage } = await import('@earendil-works/pi-ai/compat');

test('Pi receipt hints: message_end precedes run-path append; lifecycle boundaries follow',
  { timeout: 10_000 }, async () => {
    let api, manager, phase = '', release, entered;
    const log = [];
    const ids = () => manager.getEntries()
      .filter(entry => entry.type === 'custom_message')
      .map(entry => entry.details?.deliveryId);
    const observe = (who, event) => {
      if (event.type === 'message_end' && event.message.role === 'custom') {
        const id = event.message.details.deliveryId;
        log.push({ phase, who, type: event.type, id, present: ids().includes(id) });
      }
      if (['turn_end', 'agent_settled', 'entry_appended'].includes(event.type)) {
        log.push({ phase, who, type: event.type, ids: ids() });
      }
    };
    // Real Pi session and extension dispatch. The provider is entirely local.
    // This checks entry ordering, not JSONL durability or fsync.
    const h = await createHarness({
      settings: { retry: { enabled: false }, compaction: { enabled: false } },
      extensionFactories: [pi => {
        api = pi;
        for (const type of ['message_end', 'turn_end', 'agent_settled']) {
          pi.on(type, event => observe('extension', event));
        }
      }],
    });
    manager = h.sessionManager;
    h.session.subscribe(event => observe('sdk', event));
    const message = id => ({
      customType: 'receipt-probe', content: id, display: false,
      details: { deliveryId: id },
    });
    try {
      await h.session.bindExtensions({});
      phase = 'idle-no-turn';
      await h.session.sendCustomMessage(message('idle'), { triggerTurn: false });
      assert.equal(log.filter(e => e.phase === phase && e.who === 'extension').length, 0);
      assert.equal(log.find(e => e.id === 'idle' && e.who === 'sdk').present, true);

      phase = 'idle-trigger-turn';
      h.setResponses([fauxAssistantMessage('done')]);
      await h.session.sendCustomMessage(message('trigger'),
        { triggerTurn: true, deliverAs: 'followUp' });
      for (const who of ['extension', 'sdk']) {
        assert.equal(log.find(e => e.id === 'trigger' && e.who === who).present, false);
      }
      for (const type of ['turn_end', 'agent_settled']) {
        assert.ok(log.some(e => e.phase === phase && e.who === 'extension' &&
          e.type === type && e.ids.includes('trigger')));
      }

      phase = 'busy';
      const seen = new Promise(resolve => { entered = resolve; });
      const gate = new Promise(resolve => { release = resolve; });
      h.setResponses([
        async () => { entered(); await gate; return fauxAssistantMessage('done'); },
        fauxAssistantMessage('followup'),
      ]);
      const work = h.session.prompt('hold');
      await seen;
      api.sendMessage(message('busy-followup'), { deliverAs: 'followUp', triggerTurn: true });
      api.sendMessage(message('busy-context'), { triggerTurn: false });
      assert.equal(ids().includes('busy-followup'), false);
      assert.equal(ids().includes('busy-context'), false);
      release();
      await work;
      for (const who of ['extension', 'sdk']) {
        assert.equal(log.find(e => e.id === 'busy-followup' && e.who === who).present, false);
      }
      assert.equal(log.find(e => e.id === 'busy-context' && e.who === 'sdk').present, true);
      assert.equal(log.some(e => e.id === 'busy-context' && e.who === 'extension'), false);
      for (const type of ['turn_end', 'agent_settled']) {
        assert.ok(log.some(e => e.phase === phase && e.who === 'extension' &&
          e.type === type && e.ids.includes('busy-followup')));
      }
      assert.equal(log.some(e => e.type === 'entry_appended'), false);
      console.log(JSON.stringify({ piVersion: manifest.version, observations: log }));
    } finally { release?.(); h.cleanup(); }
  });

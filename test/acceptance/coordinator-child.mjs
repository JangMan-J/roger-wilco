// A coordinator process for the v2 end-to-end tests: `node coordinator-child.mjs BIN CONFIG [CRASH]`.
// BIN is a coordinator module (this tree's or the pre-v2 tree's); its startCoordinator runs
// with the config file and reads settings from $ROGER_WILCO_SETTINGS when it supports them.
// CRASH `after-send-commit` kills the process with SIGKILL right after the COMMIT of the
// first message insert, before the store notifies any stream.
import { readFile } from 'node:fs/promises';
import { DatabaseSync } from 'node:sqlite';
import { pathToFileURL } from 'node:url';

const [bin, configPath, crash] = process.argv.slice(2);
if (crash === 'after-send-commit') {
  let armed = false;
  const prepare = DatabaseSync.prototype.prepare;
  DatabaseSync.prototype.prepare = function (sql) {
    if (sql.startsWith('INSERT INTO messages')) armed = true;
    return prepare.call(this, sql);
  };
  const exec = DatabaseSync.prototype.exec;
  DatabaseSync.prototype.exec = function (sql) {
    const result = exec.call(this, sql);
    if (armed && sql === 'COMMIT') process.kill(process.pid, 'SIGKILL');
    return result;
  };
}
const { startCoordinator } = await import(pathToFileURL(bin).href);
const runtime = await startCoordinator(JSON.parse(await readFile(configPath, 'utf8')));
console.log(JSON.stringify({ url: runtime.url }));
process.once('SIGTERM', () => { void runtime.close().then(() => process.exit(0)); });

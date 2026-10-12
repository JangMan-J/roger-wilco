import { copyFile } from 'node:fs/promises';
// npm omits the root package-lock from tarballs. Ship an exact generated copy.
await copyFile(new URL('../package-lock.json', import.meta.url), new URL('../deploy/package-lock.json', import.meta.url));

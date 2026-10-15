import { mkdir, readFile, symlink, lstat, realpath } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const coding = resolve(process.env.PI_PACKAGE_DIR ?? join(homedir(), 'pi/packages/coding-agent'));
for (const [name, target] of [['pi-coding-agent', coding], ['pi-ai', resolve(coding, '../ai')]]) {
  const manifest = JSON.parse(await readFile(join(target, 'package.json'), 'utf8'));
  if (manifest.name !== `@earendil-works/${name}`) throw new Error(`Unexpected SDK package: ${manifest.name}`);
  const link = join(root, 'node_modules/@earendil-works', name);
  await mkdir(dirname(link), { recursive: true });
  const existing = await lstat(link).catch(error => { if (error.code !== 'ENOENT') throw error; });
  if (!existing) await symlink(target, link, 'dir');
  else if (await realpath(link) !== await realpath(target)) throw new Error(`SDK dependency ${name} does not match PI_PACKAGE_DIR`);
  console.log(`SDK type dependency: ${manifest.name}@${manifest.version}`);
}

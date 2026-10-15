import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { DEFAULT_SETTINGS, SETTING_BOUNDS, isSettingValue, type SettingName, type Settings } from '../protocol/index.ts';
import { homePath } from './config.ts';

/** File key, the Settings field it sets, and whether its value is a duration. */
const KEYS: Record<string, { name: SettingName; duration: boolean }> = {
  window: { name: 'window', duration: false },
  retention: { name: 'retentionMs', duration: true },
  keepalive: { name: 'keepaliveMs', duration: true },
  presence_window: { name: 'presenceWindowMs', duration: true },
};
const UNIT_MS: Record<string, number> = { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 };

export function defaultSettingsPath(): string {
  return process.env.ROGER_WILCO_SETTINGS ?? join(homedir(), '.config/roger-wilco/roger-wilco.conf');
}

/**
 * Parse `key = value` lines. `#` starts a comment; blank lines are ignored. Durations need a
 * unit (`s`, `m`, `h`, `d`); `window` is a bare integer. An unknown or repeated key, a malformed
 * line, or a value outside SETTING_BOUNDS throws. Absent keys take their defaults.
 */
export function parseSettings(text: string, source = 'settings'): Settings {
  const settings: Record<string, number> = { ...DEFAULT_SETTINGS };
  const seen = new Set<string>();
  const lines = text.split(/\r?\n/);
  for (let index = 0; index < lines.length; index++) {
    const where = `${source}:${index + 1}`;
    const line = lines[index]!.replace(/#.*/s, '').trim();
    if (!line) continue;
    const match = /^([^=\s]+)\s*=\s*(\S+)$/.exec(line);
    if (!match) throw new Error(`${where}: expected "key = value"`);
    const [, key, raw] = match as unknown as [string, string, string];
    const spec = Object.hasOwn(KEYS, key) ? KEYS[key]! : undefined;
    if (!spec) throw new Error(`${where}: unknown setting ${key}`);
    if (seen.has(key)) throw new Error(`${where}: repeated setting ${key}`);
    seen.add(key);
    const parsed = spec.duration ? /^([0-9]+)([smhd])$/.exec(raw) : /^([0-9]+)$/.exec(raw);
    if (!parsed) throw new Error(`${where}: ${key} must be ${spec.duration ? 'an integer with unit s, m, h or d' : 'an integer'}`);
    const value = Number(parsed[1]) * (spec.duration ? UNIT_MS[parsed[2]!]! : 1);
    if (!isSettingValue(spec.name, value)) {
      const { min, max } = SETTING_BOUNDS[spec.name];
      throw new Error(`${where}: ${key} is outside its bounds (${spec.duration ? `${min} ms to ${max} ms` : `${min} to ${max}`})`);
    }
    settings[spec.name] = value;
  }
  return Object.freeze(settings as Settings);
}

/** Read the settings file. An absent file means the defaults apply. */
export async function readSettings(path = defaultSettingsPath()): Promise<Settings> {
  const file = homePath(path);
  let text: string;
  try { text = await readFile(file, 'utf8'); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return DEFAULT_SETTINGS;
    throw error;
  }
  return parseSettings(text, file);
}

import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { parseSettings, readSettings, defaultSettingsPath } from '../../src/bin/settings.ts';
import { DEFAULT_SETTINGS, SETTING_BOUNDS } from '../../src/protocol/index.ts';

const DAY = 86_400_000;

test('an empty file or comments only give the defaults from SETTING_BOUNDS', () => {
  const defaults = { window: 32, retentionMs: 7 * DAY, keepaliveMs: 15_000, presenceWindowMs: 90_000 };
  assert.deepEqual(parseSettings(''), defaults);
  assert.deepEqual(parseSettings('# nothing set\n\n   \n  # indented comment\r\n'), defaults);
  for (const name of Object.keys(SETTING_BOUNDS) as (keyof typeof SETTING_BOUNDS)[])
    assert.equal(DEFAULT_SETTINGS[name], SETTING_BOUNDS[name].default, name);
});

test('keys map to settings, and durations take s, m, h or d', () => {
  const settings = parseSettings([
    '# roger-wilco.conf',
    'window = 64',
    'retention=30d',
    '  keepalive =  20s   # trailing comment',
    'presence_window = 2m\r',
  ].join('\n'));
  assert.deepEqual(settings, { window: 64, retentionMs: 30 * DAY, keepaliveMs: 20_000, presenceWindowMs: 120_000 });
  assert.equal(Object.isFrozen(settings), true);
  assert.equal(parseSettings('retention = 48h').retentionMs, 2 * DAY);
  assert.equal(parseSettings('retention = 1440m').retentionMs, DAY);
  assert.equal(parseSettings('retention = 86400s').retentionMs, DAY);
  assert.deepEqual(parseSettings('keepalive = 30s'), { ...DEFAULT_SETTINGS, keepaliveMs: 30_000 }, 'absent keys keep defaults');
});

test('bounds are inclusive and fixed', () => {
  const edges: [string, string, string, string][] = [
    ['window', '1', '256', 'window'],
    ['retention', '1d', '90d', 'retentionMs'],
    ['keepalive', '5s', '60s', 'keepaliveMs'],
    ['presence_window', '15s', '15m', 'presenceWindowMs'],
  ];
  for (const [key, min, max, name] of edges) {
    const bounds = SETTING_BOUNDS[name as keyof typeof SETTING_BOUNDS];
    assert.equal(parseSettings(`${key} = ${min}`)[name as keyof typeof SETTING_BOUNDS], bounds.min, `${key} min`);
    assert.equal(parseSettings(`${key} = ${max}`)[name as keyof typeof SETTING_BOUNDS], bounds.max, `${key} max`);
  }
  for (const line of [
    'window = 0', 'window = 257', 'window = 99999999999999999999',
    'retention = 23h', 'retention = 91d', 'retention = 86399s',
    'keepalive = 4s', 'keepalive = 61s', 'keepalive = 2m',
    'presence_window = 14s', 'presence_window = 16m', 'presence_window = 1h',
  ]) assert.throws(() => parseSettings(line), /outside its bounds/, line);
});

test('unknown, repeated or malformed lines fail', () => {
  assert.throws(() => parseSettings('window = 32\nunknown = 1'), /settings:2: unknown setting unknown/);
  assert.throws(() => parseSettings('Window = 32'), /unknown setting Window/);
  assert.throws(() => parseSettings('retentionMs = 7d'), /unknown setting retentionMs/);
  assert.throws(() => parseSettings('__proto__ = 1'), /unknown setting __proto__/);
  assert.throws(() => parseSettings('window = 32\nwindow = 64'), /repeated setting window/);
  for (const line of ['window', 'window =', '= 32', 'window = 3 2', 'window: 32'])
    assert.throws(() => parseSettings(line), /expected "key = value"/, line);
  for (const line of ['window = 32s', 'window = -1', 'window = 1.5', 'window = 0x20', 'window = 1e2'])
    assert.throws(() => parseSettings(line), /window must be an integer$/, line);
  for (const line of ['keepalive = 15', 'keepalive = 15000ms', 'keepalive = 15S', 'keepalive = 1.5m', 'keepalive = -15s', 'keepalive = 15sec'])
    assert.throws(() => parseSettings(line), /keepalive must be an integer with unit/, line);
});

test('readSettings: absent file gives defaults; a file is parsed with its path in errors', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'rw-settings-'));
  try {
    assert.equal(await readSettings(join(dir, 'missing.conf')), DEFAULT_SETTINGS);
    const file = join(dir, 'roger-wilco.conf');
    await writeFile(file, 'window = 8\nkeepalive = 10s\n');
    assert.deepEqual(await readSettings(file), { ...DEFAULT_SETTINGS, window: 8, keepaliveMs: 10_000 });
    await writeFile(file, 'window = 8\nwindow = 9\n');
    await assert.rejects(readSettings(file), (error: Error) => error.message.startsWith(`${file}:2:`));
    await assert.rejects(readSettings(dir), /EISDIR/, 'a read error other than absence fails');
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('ROGER_WILCO_SETTINGS overrides the default path', () => {
  const saved = process.env.ROGER_WILCO_SETTINGS;
  try {
    delete process.env.ROGER_WILCO_SETTINGS;
    assert.match(defaultSettingsPath(), /[/\\]\.config[/\\]roger-wilco[/\\]roger-wilco\.conf$/);
    process.env.ROGER_WILCO_SETTINGS = 'custom.conf';
    assert.equal(defaultSettingsPath(), 'custom.conf');
  } finally {
    if (saved === undefined) delete process.env.ROGER_WILCO_SETTINGS;
    else process.env.ROGER_WILCO_SETTINGS = saved;
  }
});

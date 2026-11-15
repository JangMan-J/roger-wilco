# `roger-cold-shipper`

An Optional module that ships sealed coordinator cold day files (`day-<bucket>.cold`)
to an archive directory, verifies archived integrity against `manifest.json` and the
`RWCOLD01` format, and restores missing cold day files on demand.

The core binary never depends on this module. `roger-cold-shipper` operates strictly on
sealed `RWCOLD01` files on disk, never connects to a coordinator or host daemon socket,
never inspects Mail header keys or Reference tokens (`#I3`), and runs either one-shot or
driven by explicit trigger lines on `stdin` with zero timers and zero polling (`#I2`).

## Build

```sh
cargo build --release --manifest-path modules/cold-shipper/Cargo.toml
```

## CLI

```text
roger-cold-shipper ship --slices-dir <path> --dest <archive-dir> [--json]
roger-cold-shipper restore --dest <archive-dir> --slice <slice-name> --cold-dir <local-cold-dir> [--day <bucket>] [--json]
roger-cold-shipper verify --dest <archive-dir> [--json]
roger-cold-shipper watch-stdin --slices-dir <path> --dest <archive-dir> [--json]
```

### Subcommands

- `ship`: scans `<slices-dir>/slice-*/cold/day-*.cold` (and single-slice
  `<slices-dir>/cold/day-*.cold`, mapped to `slice-00` unless `<slices-dir>` itself is
  named `slice-*`). Before writing anything to `<dest>`, verifies every candidate cold
  file's `RWCOLD01\n` header, every JSON record's `body_sha256` against its `body`, and
  the trailing `SHA256:<64-hex>\n` digest over all preceding bytes (matching `src/cold.rs`).
  Atomically copies unshipped verified files to `<dest>/<slice-name>/day-<bucket>.cold`
  with read-only permissions, skipping an already-present destination file **only** after
  verifying that the destination copy passes `RWCOLD01` verification and matches the
  source SHA-256 digest. Atomically updates `<dest>/manifest.json`.
- `restore`: validates `<dest>/manifest.json` and verifies every requested archived cold
  day file for `--slice <slice-name>` (restricted to `--day <bucket>` when given) against
  both `manifest.json` and its `RWCOLD01` trailer and per-record `body_sha256`, then
  atomically restores missing `day-<bucket>.cold` files into `<local-cold-dir>` with
  read-only permissions.
- `verify`: checks `<dest>/manifest.json`, verifies every listed `day-*.cold` file's
  whole-file SHA-256, `SHA256:` trailer, byte size, record count, and per-record
  `body_sha256`, and flags any missing or unlisted `.cold` file in `<dest>`. Exits `0`
  when intact and `1` on any missing or corrupted file or manifest.
- `watch-stdin`: event-driven line reader on `stdin`. Each input line triggers one
  incremental `ship` pass and flushes one summary line to `stdout`, exiting `0` on `stdin`
  EOF. Uses blocking `stdin` reads with zero timers and zero polling loops.

### Exit status

| Code | Meaning |
| --- | --- |
| `0` | intact / completed cleanly |
| `1` | missing or corrupted cold file or manifest, checksum mismatch, or operational I/O failure |
| `2` | CLI flag or subcommand usage error |

## Archive layout and `manifest.json`

```text
<dest>/
  manifest.json
  slice-00/
    day-19000.cold
    day-19001.cold
  slice-01/
    day-19000.cold
```

`manifest.json` uses scheme `roger-cold-manifest/1`:

```json
{
  "scheme": "roger-cold-manifest/1",
  "total_files": 1,
  "total_bytes": 248,
  "total_records": 1,
  "slices": {
    "slice-00": [
      {
        "day_bucket": 19000,
        "file": "slice-00/day-19000.cold",
        "bytes": 248,
        "records": 1,
        "sha256": "<64-hex whole-file SHA-256>",
        "trailer_sha256": "<64-hex RWCOLD01 payload SHA-256>"
      }
    ]
  }
}
```

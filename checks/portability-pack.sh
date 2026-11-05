#!/usr/bin/env bash
# Portability: the working tree builds, tests and installs in a clean HOME with
# no ~/Projects, from a copy of the tracked files, with the real toolchain and
# the real binaries. Also fails if a tracked file names a machine-specific path.
# Usage: checks/portability-pack.sh
set -euo pipefail
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then echo "usage: $0 (build, test, install in a clean HOME under /tmp)"; exit 0; fi
[[ $# == 0 ]] || { echo "usage: $0" >&2; exit 2; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

step() { echo "== $*"; "$@"; }

echo "== no machine-specific paths in tracked files"
if git grep -nI -e '/home/[a-z]' -e 'Projects/' -e 'roger-wilco-rebuild' -- . ':!checks/portability-pack.sh'; then
  echo "FAIL portability: machine-specific path above" >&2; exit 1
fi

tmp="$(mktemp -d /tmp/roger-portability.XXXXXX)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/home" "$tmp/src"
# Tracked files as the working tree has them (uncommitted edits included).
git ls-files -z | while IFS= read -r -d '' f; do [[ -e "$f" ]] && printf '%s\0' "$f"; done \
  | tar --null -T - -cf - | tar -C "$tmp/src" -xf -

# Clean environment: PATH, a fresh HOME and a private target dir. rustup's
# toolchain directory is passed through so the real compiler is found.
clean=(env -i PATH="$PATH" HOME="$tmp/home" TMPDIR="$tmp" CARGO_TARGET_DIR="$tmp/target")
[[ -n "${RUSTUP_HOME:-}" ]] && clean+=(RUSTUP_HOME="$RUSTUP_HOME")
[[ -z "${RUSTUP_HOME:-}" && -d "$HOME/.rustup" ]] && clean+=(RUSTUP_HOME="$HOME/.rustup")

cd "$tmp/src"
step "${clean[@]}" cargo test --locked --offline --features test-hooks
step "${clean[@]}" ./install.sh

bin="$tmp/home/.local/bin/roger"
[[ -L "$bin" && -x "$bin" ]] || { echo "FAIL portability: $bin is not an installed link" >&2; exit 1; }
[[ "$(readlink "$bin")" == "$tmp/home/.local/share/roger/bin/roger" ]] \
  || { echo "FAIL portability: link target" >&2; exit 1; }
# The installed file is the release binary cargo built, byte for byte.
cmp "$bin" "$tmp/target/release/roger" \
  || { echo "FAIL portability: installed binary differs from the build" >&2; exit 1; }
echo "PASS portability-pack"

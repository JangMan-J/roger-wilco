#!/usr/bin/env bash
# Install roger for the current user, no root and no fixed paths:
#   binary  $XDG_DATA_HOME/roger/bin/roger   (default ~/.local/share/roger/bin/roger)
#   link    ~/.local/bin/roger -> that binary (override the directory with BINDIR)
# Builds the release binary from this checkout (no crates are fetched). It
# installs no services and no config; see deploy/README.md for those.
# Usage: ./install.sh
set -euo pipefail
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo "usage: $0 (build and install roger under \${XDG_DATA_HOME:-~/.local/share}/roger, link it into \${BINDIR:-~/.local/bin})"
  exit 0
fi
[[ $# == 0 ]] || { echo "usage: $0" >&2; exit 2; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${HOME:?HOME must be set}"
data="${XDG_DATA_HOME:-$HOME/.local/share}/roger"
bindir="${BINDIR:-$HOME/.local/bin}"
dest="$data/bin/roger"
link="$bindir/roger"

# Never replace a real file: a deployed binary lives at ~/.local/bin/roger.
if [[ -e "$link" && ! -L "$link" ]]; then
  echo "install: $link exists and is not a symlink (a deployed binary?); remove or move it, or set BINDIR" >&2
  exit 1
fi

cargo build --release --locked --offline --manifest-path "$ROOT/Cargo.toml"
built="${CARGO_TARGET_DIR:-$ROOT/target}/release/roger"
mkdir -p "$data/bin" "$bindir"
tmp="$(mktemp "$data/bin/roger.XXXXXX")"
cp "$built" "$tmp"
chmod 755 "$tmp"
mv -f "$tmp" "$dest"
ln -sfn "$dest" "$link"
echo "installed $dest"
echo "linked    $link"
case ":$PATH:" in *":$bindir:"*) ;; *) echo "note: $bindir is not on PATH" ;; esac

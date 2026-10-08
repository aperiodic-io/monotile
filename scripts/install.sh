#!/bin/sh
# Installs the latest brrrrr release for this machine into ~/.local/bin (or $BRRRRR_INSTALL):
#   curl -fsSL https://aperiodic-io.github.io/monotile/install.sh | sh
# BRRRRR_VERSION=v0.2.0 picks a release.
set -eu
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
  Linux-aarch64 | Linux-arm64) target=aarch64-unknown-linux-gnu ;;
  Darwin-arm64) target=aarch64-apple-darwin ;;
  Darwin-x86_64) target=x86_64-apple-darwin ;;
  *) echo "brrrrr has no binary for $(uname -s) $(uname -m): try pip install brrrrr, or cargo install" >&2; exit 1 ;;
esac
repo=https://github.com/aperiodic-io/monotile
version="${BRRRRR_VERSION:-$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$repo/releases/latest" | sed 's#.*/##')}"
dest="${BRRRRR_INSTALL:-$HOME/.local/bin}"
name="brrrrr-$version-$target"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "downloading brrrrr $version for $target"
curl -fsSL "$repo/releases/download/$version/$name.tar.gz" -o "$tmp/b.tar.gz"
curl -fsSL "$repo/releases/download/$version/$name.tar.gz.sha256" -o "$tmp/b.sha256"
want="$(cut -d' ' -f1 < "$tmp/b.sha256")"
got="$( (sha256sum "$tmp/b.tar.gz" 2>/dev/null || shasum -a 256 "$tmp/b.tar.gz") | cut -d' ' -f1)"
[ "$want" = "$got" ] || { echo "checksum mismatch: expected $want, got $got" >&2; exit 1; }
tar xzf "$tmp/b.tar.gz" -C "$tmp"
mkdir -p "$dest"
install -m 755 "$tmp/$name/brrrrr" "$dest/brrrrr"
echo "installed $dest/brrrrr"
case ":$PATH:" in *":$dest:"*) ;; *) echo "add $dest to your PATH: export PATH=\"$dest:\$PATH\"" ;; esac
"$dest/brrrrr" --version

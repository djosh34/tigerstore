#!/usr/bin/env bash
# Builds two tailcat binaries from a pinned commit into $OUT (default ./out):
#   tailcat-stock   the official release build (build-tags.txt)
#   tailcat-proxy   the same plus `import _ "tailscale.com/feature/useproxy"`
#                   and without ts_omit_useproxy, so DERP dials honour
#                   HTTPS_PROXY via CONNECT. Stock tailcat never does.
# And the stand-in CONNECT proxy. Throwaway (#630).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${OUT:-$here/out}
rev=${TAILCAT_REV:-e00cdde6ff787f801361ff85ed085775c775d4c5}
mkdir -p "$out"
src=$(mktemp -d)
git clone -q https://github.com/tailscale/tailcat "$src/tailcat"
git -C "$src/tailcat" checkout -q "$rev"
cd "$src/tailcat"
tags=$(cat build-tags.txt)
go build -tags "$tags" -o "$out/tailcat-stock" ./cmd/tailcat
cat >cmd/tailcat/zz_useproxy.go <<'EOF'
package main

import _ "tailscale.com/feature/useproxy"
EOF
go build -tags "${tags//ts_omit_useproxy,/}" -o "$out/tailcat-proxy" ./cmd/tailcat
cd "$here/connectproxy" && go build -o "$out/connectproxy" .
echo "built into $out from tailcat $rev"

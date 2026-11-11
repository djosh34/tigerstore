#!/usr/bin/env bash
# Runs inside a Codex Cloud VM (#630). Builds three tailcat binaries into
# /tmp/tc/bin from a pinned commit, with the environment's Go:
#   tailcat-stock  release build (build-tags.txt), as tailscale ships it
#   tailcat-proxy  + tailscale.com/feature/useproxy: DERP dials go through
#                  HTTPS_PROXY with CONNECT, then send "Upgrade: DERP"
#   tailcat-ws     + ts_debug_websockets: with TS_DEBUG_DERP_WS_CLIENT=1,
#                  DERP runs over a WebSocket (net/http, so HTTPS_PROXY too)
# Throwaway. Log: /tmp/tc/build.log, exit code: /tmp/tc/build.exit
set -uo pipefail
export GOFLAGS=-mod=mod
export PATH=/workspace/.tigerstore-tools/go/bin:$PATH
rev=${TAILCAT_REV:-e00cdde6ff787f801361ff85ed085775c775d4c5}
mkdir -p /tmp/tc/bin
cd /tmp/tc
{
  set -e
  date
  go version
  [ -d tailcat ] || git clone -q https://github.com/tailscale/tailcat
  cd tailcat
  git checkout -q "$rev"
  git log -1 --format='tailcat %H %cd'
  tags=$(cat build-tags.txt)
  go build -tags "$tags" -o /tmp/tc/bin/tailcat-stock ./cmd/tailcat
  echo built stock
  printf 'package main\n\nimport _ "tailscale.com/feature/useproxy"\n' >cmd/tailcat/zz_useproxy.go
  notags=${tags//ts_omit_useproxy,/}
  go build -tags "$notags" -o /tmp/tc/bin/tailcat-proxy ./cmd/tailcat
  echo built proxy
  go build -tags "$notags,ts_debug_websockets" -o /tmp/tc/bin/tailcat-ws ./cmd/tailcat
  echo built ws
  ls -la /tmp/tc/bin
  date
} >/tmp/tc/build.log 2>&1
echo $? >/tmp/tc/build.exit

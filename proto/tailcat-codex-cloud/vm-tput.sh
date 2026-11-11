#!/usr/bin/env bash
# Runs in the client VM (#630): tailcat forward to the server's tcbench port,
# then capped transfers. Needs $TC_ADDR. Args: list of "bytes:streams".
set -uo pipefail
export TS_DEBUG_DERP_WS_CLIENT=1 HOME=/tmp/tc/home-c
[ -x /tmp/tcbench/tcbench ] || (cd /tmp/tcbench && PATH=/workspace/.tigerstore-tools/go/bin:$PATH go build -o tcbench .)
pkill -f 'tailcat-ws forward' 2>/dev/null
setsid nohup /tmp/tc/bin/tailcat-ws forward "$TC_ADDR" 17000:7000 >/tmp/tc/forward.log 2>&1 </dev/null &
for i in $(seq 1 40); do (exec 3<>/dev/tcp/127.0.0.1/17000) 2>/dev/null && break; sleep 0.5; done
for spec in "$@"; do
  b=${spec%%:*}; s=${spec##*:}
  echo "$(date -u +%T) tput bytes=$b streams=$s: $(/tmp/tcbench/tcbench tput 127.0.0.1:17000 $b $s)"
done

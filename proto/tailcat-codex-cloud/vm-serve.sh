#!/usr/bin/env bash
# Runs inside the server VM (#630). Starts tcbench's sink/echo on 127.0.0.1:7000
# and `tailcat serve 7000,perf` with the given binary, detached. The tailcat
# address goes to /tmp/tc/addr (a secret: never print it or commit it).
#   vm-serve.sh <variant: stock|proxy|ws> [region]
set -uo pipefail
v=$1
region=${2:-auto}
cd /tmp/tc
pkill -f 'tailcat-.* serve' 2>/dev/null
pkill -f 'tcbench serve' 2>/dev/null
sleep 1
rm -f addr serve.log
[ -x /tmp/tcbench/tcbench ] || (cd /tmp/tcbench && PATH=/workspace/.tigerstore-tools/go/bin:$PATH go build -o tcbench .)
setsid nohup /tmp/tcbench/tcbench serve 127.0.0.1:7000 >/tmp/tc/sink.log 2>&1 </dev/null &
export TS_DEBUG_DERP_WS_CLIENT=$([ "$v" = ws ] && echo 1 || echo 0)
export HOME=/tmp/tc/home-$v
mkdir -p "$HOME"
bin=/tmp/tc/bin/tailcat-$v
# A key with a fixed region so the address does not depend on netcheck,
# which has no UDP here. "auto" lets tailcat pick (and logs what it saw).
if [ "$region" != auto ]; then
  $bin genkey --key=default --force --region="$region" >/dev/null 2>/tmp/tc/genkey.log
fi
TAILCAT_ADDR_FILE=/tmp/tc/addr setsid nohup $bin --verbose serve 7000,perf >/tmp/tc/serve.log 2>&1 </dev/null &
for i in $(seq 1 60); do [ -s /tmp/tc/addr ] && break; sleep 0.5; done
if [ -s /tmp/tc/addr ]; then echo "serve $v up, address in /tmp/tc/addr ($(wc -c </tmp/tc/addr) bytes)"; else echo "serve $v: no address after 30s"; fi
sleep 5
# Show how DERP went, minus anything that looks like a tailcat address or key.
grep -iE 'derp|websocket|region|proxy|connect|error|fail|403|netcheck' /tmp/tc/serve.log | sed -E 's/tc[0-9a-zA-Z_-]{20,}/<addr>/g; s/(nodekey|discokey|mkey|privkey):[0-9a-f]+/\1:<redacted>/g' | head -40

#!/usr/bin/env bash
# What each of two concurrent Codex Cloud tasks would run, once a Codex
# environment for this repo exists (none did: see README). NOT RUN.
#
#   server:  codex-task.sh server <minutes>
#   client:  codex-task.sh client <tc-addr> <stability secs>
#
# Rendezvous without a live channel: generate the server key beforehand
# (tailcat genkey --key=default --region=<nearest>) so its address is known
# up front, put the key file's JSON in the environment as a secret, and have
# the setup script (the only phase that sees secrets) write it to
# ~/.config/tailcat/keys/default.private.json. The client task gets the
# address in its prompt. Delete the key after the run.
#
# The setup script also runs build.sh (setup has internet access) so
# out/tailcat-proxy exists: stock tailcat ignores HTTPS_PROXY for DERP.
# The agent phase needs internet access On (All domains, or at least
# tc*.ipn.dev and tailcat.dev, methods incl. GET).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tc=$here/out/tailcat-proxy
echo "nproc=$(nproc) mem=$(free -g | awk '/Mem/{print $2}')G"
env | grep -i _proxy || echo "no proxy env"
timeout 5 bash -c 'echo x >/dev/udp/1.1.1.1/53' && echo "udp send ok" || echo "udp send failed"
case $1 in
server)
	python3 -I "$here/probe.py" echo 9000 &
	python3 -I "$here/probe.py" sink 9001 &
	timeout "$(( $2 * 60 ))" "$tc" --verbose serve perf,9000,9001 2>&1 | grep -E 'listening|derp|direct|CONNECT' || true
	;;
client)
	addr=$2
	"$tc" ping --until-direct --timeout=30s "$addr" || echo "no direct path"
	"$tc" perf --parallel=1 "$addr" || true # direct path only
	"$tc" perf --parallel=4 "$addr" || true
	"$tc" forward "$addr" 19000:9000 19001:9001 &
	sleep 5
	python3 -I "$here/probe.py" send 127.0.0.1:19001 32 1
	python3 -I "$here/probe.py" send 127.0.0.1:19001 8 4
	python3 -I "$here/probe.py" rtt 127.0.0.1:19000 "$3" 100
	"$tc" ping --timeout=5s "$addr"
	;;
esac

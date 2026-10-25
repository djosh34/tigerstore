#!/usr/bin/env bash
# Runs seeds of the simulator's Kubernetes-shaped load, the long profile
# of TestKubernetesLoad, one after the other while less than MINUTES have
# passed (docs/testing.md), as kube-sim.yml does in each of its jobs:
#
#   FIRST=<seed> MINUTES=<minutes> [TWICE=1] scripts/kube-sim.sh
#
# With TWICE=1 the first seed runs twice and must give the same hashes. A
# seed may take 120 minutes, twice that when it runs twice. Each seed is a
# gosim process of its own, with a Go memory limit of 12 GiB. A sampler
# reads the memory of it and of its driver every 2 s; the seed's line
# gives the peak, and a seed past 14.5 GiB is killed, so that the machine
# and the logs survive. It
# needs the binaries of scripts/sim.sh ./internal/sim '^$' and writes a
# log per seed to OUT, $RUNNER_TEMP or /tmp by default.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${OUT:-${RUNNER_TEMP:-/tmp}}
limit=$((14 * 1024 * 1024 + 512 * 1024)) # KiB
export GOSIMTOOL=$root/.gosim/bin/gosim SIM_LONG=1 SIM_SEEDS=1 GOMEMLIMIT=12GiB
cd "$root/internal/sim"
seed=$FIRST
twice=${TWICE:-0}
status=0
while [ "$SECONDS" -lt $((MINUTES * 60)) ]; do
	log=$out/seed-$seed.log
	echo "seed $seed: replay: SIM_LONG=1 SIM_SEED=$seed SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestKubernetesLoad\$'"
	start=$SECONDS
	SIM_SEED=$seed SIM_TWICE=$twice "$root/.gosim/bin/sim.test" -test.run '^TestKubernetesLoad$' -test.v -test.timeout $((120 * (1 + twice)))m >"$log" 2>&1 &
	pid=$!
	peak=0
	killed=""
	while kill -0 "$pid" 2>/dev/null; do
		# The gosim run is a process of its own, named after the
		# translated test binary; sim.test drives it and takes in its
		# files.
		rss=$(ps -eo rss=,comm= | awk '$2 ~ /^translated|^sim\.test$/ { s += $1 } END { print s + 0 }')
		peak=$((rss > peak ? rss : peak))
		if [ "$rss" -gt "$limit" ] && [ -z "$killed" ]; then
			killed=yes
			pkill -KILL '^translated' || true
			kill -KILL "$pid" || true
		fi
		sleep 2
	done
	wait "$pid" || status=1
	grep -E -- '^--- FAIL|^    --- FAIL|^(ok|FAIL|PASS)$|known failure|kube_test.go|gosim run of seed|test timed out' "$log" | cut -c1-2000 | head -20 || true
	echo "seed $seed: peak $((peak / 1024)) MiB, $((SECONDS - start)) s${killed:+, killed past 14.5 GiB}"
	if [ -n "$killed" ]; then
		status=1
	fi
	PKG=./internal/sim "$root/scripts/replay.sh" "$log" -gosim
	seed=$((seed + 1))
	twice=0
done
exit $status

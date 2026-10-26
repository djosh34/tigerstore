#!/usr/bin/env bash
# Research #449, exp branch only: runs one cell of TestAgedForks with the
# SIM_AGE_* and SIM_FORK* variables of the environment, samples the peak
# memory of the gosim run and its driver every 2 s as kube-sim.sh does,
# and prints the time of each gosim run. It needs the binaries of
# scripts/sim.sh ./internal/sim '^$'.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${OUT:-${RUNNER_TEMP:-/tmp}}
limit=$((14 * 1024 * 1024 + 512 * 1024)) # KiB
export GOSIMTOOL=$root/.gosim/bin/gosim SIM_SEEDS=1 GOMEMLIMIT=12GiB
cd "$root/internal/sim"
log=$out/cell.log
start=$SECONDS
"$root/.gosim/bin/sim.test" -test.run '^TestAgedForks$' -test.v -test.timeout 340m >"$log" 2>&1 &
pid=$!
peak=0
killed=""
while kill -0 "$pid" 2>/dev/null; do
	rss=$(ps -eo rss=,comm= | awk '$2 ~ /^translated|^sim\.test$/ { s += $1 } END { print s + 0 }')
	peak=$((rss > peak ? rss : peak))
	if [ "$rss" -gt "$limit" ] && [ -z "$killed" ]; then
		killed=yes
		pkill -KILL '^translated' || true
		kill -KILL "$pid" || true
	fi
	sleep 2
done
wait "$pid"
status=$?
pkill -KILL '^translated' || true
grep -E -- '--- (FAIL|PASS)|aged_test.go|gosim run|^(ok|FAIL|PASS)$|timed out|panic' "$log" | cut -c1-3000 | head -80
echo "cell: exit $status, peak $((peak / 1024)) MiB, $((SECONDS - start)) s${killed:+, killed past 14.5 GiB}"
exit $status

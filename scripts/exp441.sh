#!/usr/bin/env bash
# Research #441, exp branch only: one long seed with timing marks, a CPU
# profile of the gosim run, and its history saved; then the checkers
# natively on that history, one after the other and at once.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${OUT:-${RUNNER_TEMP:-/tmp}/exp441}
mkdir -p "$out"
export GOSIMTOOL=$root/.gosim/bin/gosim SIM_LONG=1 SIM_SEEDS=1 GOMEMLIMIT=12GiB
export EXP441_OUT=$out GOSIM_CPUPROFILE=$out/cpu
cd "$root/internal/sim"
start=$SECONDS
SIM_SEED=$SEED "$root/.gosim/bin/sim.test" -test.run '^TestKubernetesLoad$' -test.v -test.timeout 250m >"$out/seed.log" 2>&1 &
pid=$!
peak=0
while kill -0 "$pid" 2>/dev/null; do
	rss=$(ps -eo rss=,comm= | awk '$2 ~ /^translated|^sim\.test$/ { s += $1 } END { print s + 0 }')
	peak=$((rss > peak ? rss : peak))
	sleep 2
done
wait "$pid"
st=$?
[ -n "${NOPKILL:-}" ] || pkill -KILL '^translated' || true
echo "seed $SEED: exit $st, peak $((peak / 1024)) MiB, $((SECONDS - start)) s"
grep -E 'REALMARK|hashes:|gosim run of seed|^--- |^(ok|FAIL|PASS)$|kube_test.go' "$out/seed.log" | grep -v progress | cut -c1-600
grep -E 'progress' "$out/seed.log" | awk 'NR%6==0' | cut -c1-300
[ -z "${NOCHECK:-}" ] || exit $st
cd "$root"
go build -o "$out/checkhistory" ./cmd/checkhistory
if [ -f "$out/history.json.gz" ]; then
	ls -la "$out/history.json.gz"
	/usr/bin/time -v "$out/checkhistory" -timeout 150m -parallel -witness-variant ultimate -checkers durable,revisions,watches,witness-log,witness-revisions "$out/history.json.gz" 2>&1 | grep -vE '^\s+(Average|Socket|Signals|Swaps|Page size|File system|Exit status|Minor|Major|Voluntary|Involuntary|Percent)'
fi
exit $st

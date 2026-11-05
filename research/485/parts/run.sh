#!/usr/bin/env bash
# Research #485, exp branch only: one cell of the short-seed sweep. Runs
# $SEEDS seeds of $TEST from seed $FIRST with a faulted phase of $D
# simulated seconds (0: the profile's own) in each of $PROCS processes at
# once, with real-time marks, a peak RSS sample of the gosim processes,
# and with PROFILE=1 a CPU profile of every gosim run.
set -uo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
out=${OUT:?}
mkdir -p "$out"
export GOSIMTOOL=$root/.gosim/bin/gosim
[ "${D:-0}" -gt 0 ] && export EXP485_DURATION=$D
cd "$root/internal/sim"
start=$SECONDS
pids=()
for p in $(seq 0 $((PROCS - 1))); do
	if [ "${PROFILE:-}" = 1 ]; then
		mkdir -p "$out/prof-$p"
		export GOSIM_CPUPROFILE=$out/prof-$p/cpu
	fi
	SIM_SEED=$((FIRST + p * SEEDS)) SIM_SEEDS=$SEEDS "$root/.gosim/bin/sim.test" -test.run "^$TEST\$" -test.v -test.timeout 340m >"$out/seeds-$p.log" 2>&1 &
	pids+=($!)
done
peak=0; used=0
while kill -0 "${pids[@]}" 2>/dev/null; do
	rss=$(ps -eo rss=,comm= | awk '$2 ~ /^translated|^sim\.test$/ { s += $1 } END { print s + 0 }')
	m=$(free -m | awk '/Mem:/ {print $3}')
	peak=$((rss > peak ? rss : peak)); used=$((m > used ? m : used))
	sleep 1
done
st=0
for pid in "${pids[@]}"; do wait "$pid" || st=1; done
echo "cell test=$TEST D=$D seeds=$SEEDS procs=$PROCS: exit $st, wall $((SECONDS - start)) s, peak rss of sim processes $((peak / 1024)) MiB, peak runner used $used MiB"
grep -h -E 'gosim run of seed|^--- FAIL|^(ok|FAIL|PASS)$' "$out"/seeds-*.log | head -80
exit 0

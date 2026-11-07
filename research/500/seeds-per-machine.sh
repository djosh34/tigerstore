#!/usr/bin/env bash
# seeds-per-machine.sh out procs seeds [test]: runs procs processes of the
# gosim test test (TestSeeds), seeds seeds each, at once, and writes to out
# the wall time, seeds per minute, each process's result and peak memory
# (VmHWM of its simulation process, read every 2 s) and the least
# MemAvailable seen. With CAMPAIGN set, process 0 runs one seed of
# TestCampaignShapeSeeds instead, and the others run until it ends.
set -uo pipefail
out=$1 procs=$2 seeds=$3 test=${4:-TestSeeds}
mkdir -p "$out"
export GOSIMTOOL="$GITHUB_WORKSPACE/.gosim/bin/gosim"
cd "$GITHUB_WORKSPACE/internal/sim"
start=$SECONDS
pids=()
for p in $(seq 0 $((procs - 1))); do
  if [ -n "${CAMPAIGN:-}" ] && [ "$p" = 0 ]; then
    SIM_SEED=1 SIM_SEEDS=1 SIM_WALL=3h "$GITHUB_WORKSPACE/.gosim/bin/sim.test" -test.run '^TestCampaignShapeSeeds$' -test.v -test.timeout 5h >"$out/proc-$p.log" 2>&1 &
  else
    SIM_SEED=$((1 + p * seeds)) SIM_SEEDS=$seeds "$GITHUB_WORKSPACE/.gosim/bin/sim.test" -test.run "^$test\$" -test.v -test.timeout 5h >"$out/proc-$p.log" 2>&1 &
  fi
  pids+=($!)
done
minavail=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
declare -A peak
while :; do
  alive=0
  for pid in "${pids[@]}"; do kill -0 "$pid" 2>/dev/null && alive=1; done
  [ "$alive" = 1 ] || break
  for sp in $(pgrep -f 'translated-github.com-djosh34-tigerstore-internal_-sim.test'); do
    kb=$(awk '/VmHWM/ {print $2}' "/proc/$sp/status" 2>/dev/null)
    [ -n "$kb" ] && [ "$kb" -gt "${peak[$sp]:-0}" ] && peak[$sp]=$kb
  done
  avail=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
  [ "$avail" -lt "$minavail" ] && minavail=$avail
  # The campaign-shaped seed's companions stop once it ended.
  if [ -n "${CAMPAIGN:-}" ] && ! kill -0 "${pids[0]}" 2>/dev/null; then
    for pid in "${pids[@]:1}"; do kill "$pid" 2>/dev/null; done
  fi
  sleep 2
done
wall=$((SECONDS - start))
{
  echo "procs $procs, seeds $seeds a process, test $test${CAMPAIGN:+, process 0 the campaign-shaped seed}"
  echo "cpus $(nproc), $(grep MemTotal /proc/meminfo)"
  echo "wall $wall s"
  done_seeds=0
  for p in $(seq 0 $((procs - 1))); do
    n=$(grep -cE '^    --- (PASS|FAIL): Test[A-Za-z]+/[0-9]+ ' "$out/proc-$p.log")
    f=$(grep -cE '^    --- FAIL' "$out/proc-$p.log")
    done_seeds=$((done_seeds + n))
    echo "process $p: $n seeds ended, $f failed"
  done
  echo "seeds per minute: $(awk -v n="$done_seeds" -v w="$wall" 'BEGIN {printf "%.1f", n * 60 / w}')"
  for sp in "${!peak[@]}"; do echo "simulation process $sp: peak $(awk -v k="${peak[$sp]}" 'BEGIN {printf "%.2f", k / 2^20}') GiB"; done
  echo "least MemAvailable: $(awk -v k="$minavail" 'BEGIN {printf "%.2f", k / 2^20}') GiB"
} | tee "$out/summary.txt"

#!/usr/bin/env bash
# Peak memory of #500's long seeds on a Codex VM, one at a time, read from
# the simulation process's own VmHWM while it runs: the test runner does
# not wait for that process as its child, so getrusage misses it. The
# campaign-shaped seed runs on the heap-profile build (mem1.sh), with a
# heap profile every 2 minutes. Writes /tmp/m500/out3.txt and "done" to
# /tmp/m500/exit3.
set -uo pipefail
OUT=/tmp/m500
exec >"$OUT/out3.txt" 2>&1
export PATH=/workspace/.tigerstore-tools/go/bin:$PATH
echo "cpus $(nproc), $(grep MemTotal /proc/meminfo)"

# measure label test checkout [env...]: runs seed 1 of test, and prints
# its exit status, the simulation process's peak memory, and the wall time.
measure() {
  local label=$1 test=$2 co=$3
  shift 3
  local peak=0 start=$SECONDS
  (cd "$co/internal/sim" && env "$@" GOSIMTOOL="$co/.gosim/bin/gosim" SIM_SEED=1 SIM_SEEDS=1 \
    "$co/.gosim/bin/sim.test" -test.run "^$test\$" -test.v -test.timeout 6h >"$OUT/$label.log" 2>&1) &
  local job=$!
  while kill -0 "$job" 2>/dev/null; do
    for pid in $(pgrep -f 'translated-github.com-djosh34-tigerstore-internal_-sim.test'); do
      kb=$(awk '/VmHWM/ {print $2}' "/proc/$pid/status" 2>/dev/null)
      [ -n "$kb" ] && [ "$kb" -gt "$peak" ] && peak=$kb
    done
    sleep 1
  done
  wait "$job"
  local rc=$?
  echo "$label: exit $rc, peak $(awk -v k="$peak" 'BEGIN {printf "%.2f", k / 2^20}') GiB, wall $((SECONDS - start)) s"
}

measure busy TestBusySeeds /workspace/tigerstore
measure kube TestKubernetesLoadLong /workspace/tigerstore
rm -rf "$OUT/heap" && mkdir -p "$OUT/heap"
measure campaign TestCampaignShapeSeeds /workspace/ts-hook GOSIM_HEAPPROFILE="$OUT/heap/campaign" GOSIM_HEAPEVERY=2m
echo done >"$OUT/exit3"

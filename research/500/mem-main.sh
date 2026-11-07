#!/usr/bin/env bash
# mem-main.sh: peak memory of the long seeds on main, one at a time.
# Writes /tmp/m505/out.txt and "done" to /tmp/m505/exit.
set -uo pipefail
OUT=/tmp/m505
mkdir -p "$OUT"
exec >"$OUT/out.txt" 2>&1
export PATH=/workspace/.tigerstore-tools/go/bin:$PATH
export GOMODCACHE=/workspace/.tigerstore-tools/gopath/pkg/mod
export GOCACHE=/workspace/.tigerstore-tools/cache
cd /workspace/tigerstore
echo "commit $(git rev-parse HEAD)"
grep -m1 'model name' /proc/cpuinfo
echo "cpus $(nproc), $(grep MemTotal /proc/meminfo)"
scripts/sim.sh ./internal/sim '^$' >"$OUT/build.log" 2>&1 || { echo "build failed"; echo done >"$OUT/exit"; exit 1; }
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
measure campaign TestCampaignShapeSeeds /workspace/tigerstore SIM_WALL=3h
echo done >"$OUT/exit"

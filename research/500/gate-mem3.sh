#!/usr/bin/env bash
# The speed gate of tigerstore #500 on a Codex Cloud VM, interleaved: each
# round runs the native floor once (GOMAXPROCS=1), the floor under gosim
# once and one calm seed, so that the VM's changing speed hits all three
# alike. Writes /tmp/m500/out.txt, one line per measurement, and "done" to
# /tmp/m500/exit.
set -uo pipefail
OUT=${OUT:-/tmp/m506}
ROUNDS=${ROUNDS:-8}
mkdir -p "$OUT"
exec >"$OUT/out.txt" 2>&1
export PATH=/workspace/.tigerstore-tools/go/bin:$PATH
export GOMODCACHE=/workspace/.tigerstore-tools/gopath/pkg/mod
export GOCACHE=/workspace/.tigerstore-tools/cache
cd /workspace/tigerstore
echo "commit $(git rev-parse HEAD)"
grep -m1 'model name' /proc/cpuinfo
go test -c -o "$OUT/native.test" ./internal/sim
scripts/sim.sh ./internal/sim '^$' >/dev/null
export GOSIMTOOL=/workspace/tigerstore/.gosim/bin/gosim
cd internal/sim
for r in $(seq 1 "$ROUNDS"); do
  GOMAXPROCS=1 "$OUT/native.test" -test.run '^$' -test.bench '^BenchmarkFloor$' -test.benchtime 1x |
    grep -oE '[0-9]+ calls/core-s' | sed "s/^/round $r native /"
  SIM_SEED=$r SIM_SEEDS=1 ../../.gosim/bin/sim.test -test.run '^TestFloorUnderGosim$' -test.v |
    grep -oE '[0-9]+ calls answered.*: [0-9]+ calls per core-second$' | head -1 | sed "s/^/round $r gosim /"
  SIM_SEED=$r SIM_SEEDS=1 ../../.gosim/bin/sim.test -test.run '^TestCalm$' -test.v |
    grep -oE '[0-9]+ calls answered, [0-9.]+ s of CPU time in the load.*fixed$' | head -1 | sed "s/^/round $r calm /"
  SIM_SEED=$r SIM_SEEDS=1 ../../.gosim/bin/sim.test -test.run '^TestFaultedLoad$' -test.v |
    grep -oE '[0-9]+ calls answered, [0-9.]+ s of CPU time in the load.*fixed$' | head -1 | sed "s/^/round $r faulted /"
done

# Peak memory of the long seeds at this head, one at a time.
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

#!/usr/bin/env bash
# Floor measurements of tigerstore #500 on a Codex Cloud VM. Writes
# everything to $OUT/out.txt and the exit code to $OUT/exit.
set -uo pipefail
OUT=/tmp/m500
mkdir -p "$OUT"
exec >"$OUT/out.txt" 2>&1
export PATH=/workspace/.tigerstore-tools/go/bin:$PATH
export GOMODCACHE=/workspace/.tigerstore-tools/gopath/pkg/mod
export GOCACHE=/workspace/.tigerstore-tools/cache
cd /workspace/tigerstore
set -x
git rev-parse HEAD
go version
nproc; cat /sys/fs/cgroup/cpu.max; grep -m1 'model name' /proc/cpuinfo; free -g
go test -c -o "$OUT/sim.test" ./internal/sim
for p in 1 2 4; do
  GOMAXPROCS=$p "$OUT/sim.test" -test.run '^$' -test.bench '^BenchmarkFloor$' -test.benchtime 5x
done
SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^TestFloorUnderGosim$' -test.v | grep -E 'calls|PASS|FAIL|ok'
SIM_SEEDS=5 scripts/sim.sh ./internal/sim '^TestCalm$' -test.v | grep -E 'calls|PASS|FAIL|ok'
echo "$?" >"$OUT/exit"

#!/usr/bin/env bash
# Diagnostic experiment, NOT a Go 1.27 gosim port.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
exp=$root/experiments/gosim333
src=${GOSIM333_SOURCE:-/tmp/r333-gosim-translator}
bin=/tmp/r333-gosim-stubs
export GOMAXPROCS=2 GOFLAGS='-p=2 -mod=mod' GOTOOLCHAIN=go1.27.2
heavy() { flock /tmp/r333-heavy.lock "$@"; }
case ${1:-help} in
 prepare)
  git clone https://github.com/jellevandenhooff/gosim.git "$src"
  git -C "$src" checkout ffd3a613542675755e4cbf8186b5edaf404ed95c
  git -C "$src" apply "$exp/${PATCH:-runtime-explore.patch}"
  if [[ -n ${GOSIM333_ADAPTER:-} ]]; then python3 "$GOSIM333_ADAPTER" "$src" "$root"; fi
  (cd "$src"; heavy go build -o "$bin" ./cmd/gosim)
  cd "$root"
  go mod edit -require=github.com/jellevandenhooff/gosim@v0.0.0-20241210235323-ffd3a6135426 -replace=github.com/jellevandenhooff/gosim="$src"
  ;;
 translate)
  cd "$root"; heavy "$bin" translate ./internal/sim ./internal/node
  ;;
 compile)
  cd "$root/.gosim/translated/linux_arm64"
  heavy go test -vet=off -c -ldflags=-checklinkname=0 -tags=linkname -o /tmp/r333-sim.test ./github.com/djosh34/tigerstore/internal_/sim
  heavy go test -vet=off -c -ldflags=-checklinkname=0 -tags=linkname -o /tmp/r333-node.test ./github.com/djosh34/tigerstore/internal_/node
  ;;
 example)
  cd "$src"; heavy "$bin" test -v -seeds=1,1,2 -run='^TestGosim$' -logformat=raw ./examples
  ;;
 probe)
  cd "$root/experiments/gosim291"
  go mod edit -replace=github.com/jellevandenhooff/gosim="$src"
  heavy "$bin" test -v -seeds=1,1,2 -run='^TestDriverGate$' -logformat=raw .
  ;;
 seed|membership|cleaner)
  test=TestSeeds; executable=/tmp/r333-sim.test
  [[ $1 != membership ]] || test=TestMembershipFaults
  if [[ $1 == cleaner ]]; then test=TestConcurrentCleanerSimulation; executable=/tmp/r333-node.test; fi
  heavy timeout 30 "$executable" -test.v -test.run="^${test}$" -seeds=1 -logformat=raw
  ;;
 planted)
  cd "$root"
  cp "$exp/planted_wrapper.go.txt" internal/sim/gosim333_planted_test.go
  trap 'rm -f "$root/internal/sim/gosim333_planted_test.go"' EXIT
  GOSIM333_TAGS=planted heavy "$bin" translate ./internal/sim ./internal/node
  cd "$root/.gosim/translated/linux_arm64"
  heavy go test -vet=off -c -ldflags=-checklinkname=0 -tags=linkname -o /tmp/r333-planted.test ./github.com/djosh34/tigerstore/internal_/sim
  heavy timeout 30 /tmp/r333-planted.test -test.v -test.run='^TestGosim333Planted$' -seeds=1 -logformat=raw
  ;;
 *) echo 'usage: run.sh prepare|translate|compile|example|probe|seed|membership|cleaner|planted'; exit 2 ;;
esac

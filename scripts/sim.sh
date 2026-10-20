#!/usr/bin/env bash
# Runs simulator tests under gosim (docs/testing.md):
#
#   scripts/sim.sh <package> <test pattern> [test binary flags]
#
# as in SIM_SEED=17 SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestSeeds$'.
# TAGS=planted builds the planted bugs in. The script builds the gosim
# command from tools/gosim, translates the package's tests for gosim, and
# refuses a test binary that can reach a hook gosim does not implement and
# internal/sim/reachable.txt does not list. Then it runs the package's
# native test binary, in which each simulator test runs its seeds in gosim
# (internal/simrun). The flags, such as -test.v or -test.timeout 30m, go
# to that binary.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
pkg=$1
pattern=$2
shift 2
tags=${TAGS:-}
bin=$root/.gosim/bin
export GOSIMTOOL=$bin/gosim
mkdir -p "$bin"
go build -C "$root/tools/gosim" -o "$GOSIMTOOL" github.com/jellevandenhooff/gosim/cmd/gosim
cd "$root"
"$GOSIMTOOL" build-tests ${tags:+-tags "$tags"} -allow-unimplemented internal/sim/reachable.txt "$pkg"
test=$bin/$(basename "$pkg")${tags:+-$tags}.test
go test -c ${tags:+-tags "$tags"} -o "$test" "$pkg"
cd "$pkg"
exec "$test" -test.run "$pattern" "$@"

#!/usr/bin/env bash
set -euo pipefail
export GOMAXPROCS=2 GOFLAGS='-p=2 -mod=mod' GOTOOLCHAIN=go1.27.2
export GOSIM333_SOURCE=/tmp/r333-gosim-synctest
export GOSIM333_ADAPTER=$PWD/experiments/gosim333/synctest/apply.py
root=$PWD
out=$root/r333-results
mkdir -p "$out"
exec 3>"$out/status.tsv"
record() {
 local name=$1; shift
 printf '\nCOMMAND %q ' "$name" | tee -a "$out/commands.log"
 printf '%q ' "$@" | tee -a "$out/commands.log"
 printf '\n' | tee -a "$out/commands.log"
 set +e
 "$@" >"$out/$name.log" 2>&1
 last=$?
 set -e
 printf '%s\t%s\n' "$name" "$last" >&3
 printf '%s\t%s\n' "$name" "$last"
 tail -c 14000 "$out/$name.log"
}
go version >"$out/environment.txt"
uname -a >>"$out/environment.txt"
git rev-parse HEAD >>"$out/environment.txt"
record prepare bash experiments/gosim333/run.sh prepare
[[ $last == 0 ]] || exit "$last"
record translate bash experiments/gosim333/run.sh translate
[[ $last == 0 ]] || exit "$last"
git -C "$GOSIM333_SOURCE" add -N .
git -C "$GOSIM333_SOURCE" diff >"$out/gosim.patch"
git diff >"$out/tigerstore.patch"
cp go.mod "$out/tigerstore-go.mod"
record compile bash experiments/gosim333/run.sh compile
[[ $last == 0 ]] || exit "$last"
record example bash experiments/gosim333/run.sh example
record probe bash experiments/gosim333/run.sh probe
record seed-1a flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-sim.test -test.v '-test.run=^TestSeeds$' -seeds=1 -sim.seed=1 -sim.seeds=1 -logformat=raw -r333.output="$out/seed-1a"
if [[ $last != 0 ]]; then exit 0; fi
record seed-1b flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-sim.test -test.v '-test.run=^TestSeeds$' -seeds=1 -sim.seed=1 -sim.seeds=1 -logformat=raw -r333.output="$out/seed-1b"
record seed-2 flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-sim.test -test.v '-test.run=^TestSeeds$' -seeds=2 -sim.seed=1 -sim.seeds=1 -logformat=raw -r333.output="$out/seed-2"
# Separate force-checksum runs avoid changing the normal tigerstore recording runs.
for pair in 1a:1 1b:1 2:2; do
 record "events-${pair%:*}" flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-sim.test '-test.run=^TestSeeds$' -seeds="${pair#*:}" -sim.seed=1 -sim.seeds=1 -logformat=raw -force-checksum
 done
record membership flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-sim.test -test.v '-test.run=^TestMembershipFaults$' -seeds=1 -sim.seed=1 -sim.seeds=1 -logformat=raw
record cleaner flock /tmp/r333-heavy.lock timeout 300 /tmp/r333-node.test -test.v '-test.run=^TestConcurrentCleanerSimulation$/1$' -seeds=1 -logformat=raw
record planted bash experiments/gosim333/run.sh planted
python3 experiments/gosim333/synctest/compare.py "$out" >"$out/comparison.txt"
cat "$out/comparison.txt"

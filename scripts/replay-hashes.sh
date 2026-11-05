#!/usr/bin/env bash
# Runs a fixed set of simulator seeds under gosim and writes the hashes of
# every simulation to <out>, one line each, sorted (docs/testing.md):
#
#   scripts/replay-hashes.sh <out>
#
# A run is its commit, test and seed, so the file must be the same on
# every machine and architecture, and whatever else the environment
# holds; CI's replay jobs compare it. It needs the binaries of
# scripts/sim.sh ./internal/sim '^$'. A failed seed, or one with no
# hashes, fails the script; the file still has the hashes of the others.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=$1
export GOSIMTOOL=$root/.gosim/bin/gosim
cd "$root/internal/sim"
: >"$out"
status=0
# Each test, its first seed and the number of seeds. Each seed runs alone,
# as a replay does, so that a test's rules over its set do not apply and
# a failed run fails the script.
while read -r test first n; do
	for seed in $(seq "$first" $((first + n - 1))); do
		log=$(mktemp)
		if ! SIM_SEED=$seed SIM_SEEDS=1 "$root/.gosim/bin/sim.test" -test.run "^$test\$" -test.v -test.timeout 30m >"$log" 2>&1; then
			echo "$test seed $seed failed:"
			grep -E -- '^--- FAIL|^    --- FAIL|panic|replay:|^(ok|FAIL|PASS)$' "$log" | head -20 || true
			status=1
		fi
		# The driver logs each run's hashes, as "seed <seed>: trace ...",
		# and a double run's twice.
		hashes=$(grep -oE 'seed [0-9]+: trace [0-9a-f]+ history [0-9a-f]+( node[0-9]+ [0-9a-f]+)*' "$log" | sort -u || true)
		if [ -z "$hashes" ]; then
			echo "$test seed $seed left no hashes"
			status=1
		else
			echo "$hashes" | sed "s/^/$test /" >>"$out"
		fi
		rm -f "$log"
	done
done <<'EOF'
TestSeeds 1 8
TestMembershipFaults 1 4
TestKubernetesLoad 1 2
EOF
sort -o "$out" "$out"
cat "$out"
exit $status

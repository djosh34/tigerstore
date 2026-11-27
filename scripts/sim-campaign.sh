#!/usr/bin/env bash
# Run the planner's (at most four) process queues on one runner.
# A long seed's job contains exactly one queue of exactly one seed.
set -uo pipefail
export GOSIMTOOL="$GITHUB_WORKSPACE/.gosim/bin/gosim"

# A runner shut down for memory may never reach artifact upload (#775).
(while true; do
  echo "memory $(date -u +%T): $(free -m | awk '/^Mem:/ {print "used " $3 " MiB, available " $7 " MiB"}'); largest: $(ps -eo rss=,comm= --sort=-rss | head -3 | awk '{printf "%s %d MiB; ", $2, $1/1024}')" | tee -a "$RUNNER_TEMP/memory.log"
  sleep 5
done) &
memory=$!
trap 'kill "$memory" 2>/dev/null || true' EXIT

cd "$GITHUB_WORKSPACE/internal/sim" || exit 1
pids=()
procs=()
for ((p=0; p<$(jq length <<<"$PROCESSES"); p++)); do
  # A queue can cross a long seed's gap. Its spans run sequentially,
  # never concurrently, preserving the four-process limit per runner.
  (
    status=0
    while read -r seed n twice; do
      echo "process $p: seeds $seed to $((seed + n - 1)) of $TEST"
      SIM_SEED=$seed SIM_SEEDS=$n SIM_TWICE=$twice "$GITHUB_WORKSPACE/.gosim/bin/sim.test" -test.run "^$TEST\$" -test.v -test.timeout 340m || status=1
    done < <(jq -r --argjson p "$p" '.[$p][] | "\(.first) \(.seeds) \(.twice)"' <<<"$PROCESSES")
    exit "$status"
  ) >"$RUNNER_TEMP/seeds-$SHARD-$p.log" 2>&1 &
  pids+=($!)
  procs+=("$p")
done
status=0
for pid in "${pids[@]}"; do
  wait "$pid" || status=1
done
# Wait for every process before replaying any failures.
for p in "${procs[@]}"; do
  grep -E -- '^--- FAIL|^    --- FAIL| seeds: |took effect|^(ok|FAIL|PASS)$' "$RUNNER_TEMP/seeds-$SHARD-$p.log" | head -50 || true
  PKG=./internal/sim "$GITHUB_WORKSPACE/scripts/replay.sh" "$RUNNER_TEMP/seeds-$SHARD-$p.log" -gosim || status=1
  if ! grep -qE "^simrun: seed $TEST/[0-9]+ (PASS|FAIL|SKIP known-failure)\$" "$RUNNER_TEMP/seeds-$SHARD-$p.log"; then
    echo "process $p reported no seed of $TEST"
    status=1
  fi
done
exit "$status"

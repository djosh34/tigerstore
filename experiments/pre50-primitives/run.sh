#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
check_deadline() { test "$(date -u +%s)" -lt "$(date -u -d 2026-10-13T22:00:00Z +%s)"; }
check_deadline
ROOT="$PWD/.run"
mkdir -p "$ROOT"/{tmp,cache,results,data,cert,source}
export TMPDIR="$ROOT/tmp" GOCACHE="$ROOT/cache/build" GOMODCACHE="$ROOT/cache/mod" GOPATH="$ROOT/cache/gopath" GOTOOLCHAIN=local GOMAXPROCS=1
OUT="$ROOT/results"
pids=()
cleanup() {
 for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
 if [[ -d "$ROOT/cache" ]]; then chmod -R u+w "$ROOT/cache"; fi
 rm -rf "$ROOT/data" "$ROOT/cert" "$ROOT/tmp" "$ROOT/cache" "$ROOT/costs"
}
trap cleanup EXIT
cp ./*.go go.mod go.sum run.sh summarize.py README.md "$ROOT/source/"
cp ../../.github/workflows/pre50-primitives.yml "$ROOT/source/"
{
 date -u --iso-8601=seconds
 git rev-parse HEAD
 uname -a
 lscpu
 free -b
 df -B1 .
 findmnt -T . -o SOURCE,TARGET,FSTYPE,OPTIONS
 lsblk -o NAME,SIZE,ROTA,TYPE,MOUNTPOINTS
 go version
 go env
 env | grep -E '^(Image|GOMAXPROCS|GOTOOLCHAIN|GOCACHE|GOMODCACHE|GOPATH|TMPDIR|RUNNER_ARCH|RUNNER_OS)=' || true
 printf 'Initial caller affinity\n'
 taskset -pc $$
} > "$OUT/environment.txt"
[[ "$(findmnt -T . -n -o FSTYPE)" == ext4 ]]
! findmnt -T . -n -o OPTIONS | grep -Eq '(^|,)(nobarrier|barrier=0)(,|$)'
mapfile -t CPUs < <(python3 - <<'PY'
import os
for c in sorted(os.sched_getaffinity(0)): print(c)
PY
)
[[ ${#CPUs[@]} -ge 4 ]]
printf 'local/leader=%s follower1/rpc-server=%s follower2=%s rpc-client=%s\n' "${CPUs[0]}" "${CPUs[1]}" "${CPUs[2]}" "${CPUs[3]}" > "$OUT/layout.txt"
go mod verify > "$OUT/module-verify.txt"
go list -m all > "$OUT/modules.txt"
go vet ./... > "$OUT/vet.txt" 2>&1
go test ./... > "$OUT/tests.txt" 2>&1
go build -trimpath -o "$ROOT/costs" .
go version -m "$ROOT/costs" > "$OUT/binary-build-info.txt"
sha256sum "$ROOT/costs" > "$OUT/binary.sha256"
taskset -c "${CPUs[0]}" "$ROOT/costs" cert "$ROOT/cert"
check_deadline
timeout 240 taskset -c "${CPUs[0]}" "$ROOT/costs" local "$ROOT/cert" > "$OUT/local.jsonl" 2> "$OUT/local.stderr"
check_deadline
timeout 70 taskset -c "${CPUs[0]}" "$ROOT/costs" fsync "$ROOT/data" > "$OUT/fsync.jsonl" 2> "$OUT/fsync.stderr"
wait_ready() {
 local file="$1" pid="$2"
 for i in $(seq 1 200); do
  [[ -f "$file" ]] && return 0
  kill -0 "$pid" 2>/dev/null || { echo "process $pid died before ready" >&2; return 1; }
  sleep .05
 done
 echo "ready timeout" >&2; return 1
}
for rep in 1 2 3 4 5; do
 modes=(nondurable durable)
 [[ $((rep%2)) -eq 0 ]] && modes=(durable nondurable)
 for mode in "${modes[@]}"; do
  check_deadline
  dir="$ROOT/data/q-$mode-r$rep";mkdir -p "$dir"
  taskset -c "${CPUs[1]}" "$ROOT/costs" follower "$ROOT/cert" "$dir" 127.0.0.1:0 "$mode" "$rep" follower1 > "$OUT/q-$mode-r$rep-follower1.jsonl" 2> "$OUT/q-$mode-r$rep-follower1.stderr" &
  p1=$!;pids+=("$p1")
  taskset -c "${CPUs[2]}" "$ROOT/costs" follower "$ROOT/cert" "$dir" 127.0.0.1:0 "$mode" "$rep" follower2 > "$OUT/q-$mode-r$rep-follower2.jsonl" 2> "$OUT/q-$mode-r$rep-follower2.stderr" &
  p2=$!;pids+=("$p2")
  wait_ready "$dir/follower1.ready" "$p1"; wait_ready "$dir/follower2.ready" "$p2"
  { taskset -pc "$p1";taskset -pc "$p2"; } > "$OUT/q-$mode-r$rep-taskset.txt"
  a1=$(<"$dir/follower1.ready");a2=$(<"$dir/follower2.ready")
  timeout 45 taskset -c "${CPUs[0]}" "$ROOT/costs" quorum "$ROOT/cert" "$dir" "$a1" "$a2" "$mode" "$rep" > "$OUT/q-$mode-r$rep-leader.jsonl" 2> "$OUT/q-$mode-r$rep-leader.stderr"
  wait "$p1"; wait "$p2"; pids=()
  du -b "$dir"/* > "$OUT/q-$mode-r$rep-data-bytes.txt"
  rm -rf "$dir"
 done
 for concurrency in 1 64; do
  check_deadline
  name="rpc-c$concurrency-r$rep";dir="$ROOT/data/$name";mkdir -p "$dir"
  taskset -c "${CPUs[1]}" "$ROOT/costs" rpc-server "$ROOT/cert" "$dir" 127.0.0.1:0 "$rep" "$concurrency" "$name" > "$OUT/$name-server.jsonl" 2> "$OUT/$name-server.stderr" &
  p=$!;pids+=("$p");wait_ready "$dir/$name.ready" "$p"
  taskset -pc "$p" > "$OUT/$name-taskset.txt"
  address=$(<"$dir/$name.ready")
  timeout 40 taskset -c "${CPUs[3]}" "$ROOT/costs" rpc-client "$ROOT/cert" "$address" "$rep" "$concurrency" > "$OUT/$name-client.jsonl" 2> "$OUT/$name-client.stderr"
  cp "$dir/$name.result.json" "$OUT/$name-server-result.json"
  kill "$p";wait "$p" || true;pids=()
  rm -rf "$dir"
 done
done
findmnt -T . -o SOURCE,TARGET,FSTYPE,OPTIONS > "$OUT/mount-end.txt"
! findmnt -T . -n -o OPTIONS | grep -Eq '(^|,)(nobarrier|barrier=0)(,|$)'
sha256sum "$ROOT/source"/* > "$OUT/source.sha256"
python3 summarize.py "$OUT" > "$OUT/summary.md"
printf 'Finished %s\n' "$(date -u --iso-8601=seconds)"

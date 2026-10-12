#!/usr/bin/env bash
# Rebuild and rerun only before the requested deadline. No host settings change.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
export TMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" GOMODCACHE="$PWD/.gomodcache"
mkdir -p "$TMPDIR" "$GOCACHE" "$GOMODCACHE" build raw
check_time() {
  local now
  now=$(date -u +%s)
  if (( now >= $(date -u -d '2026-10-13T22:00:00Z' +%s) )); then
    printf 'Deadline reached, starting nothing.\n' >&2
    exit 1
  fi
  date -u +%Y-%m-%dT%H:%M:%SZ
}
check_time > raw/run-start.txt
{
  go version
  uname -a
  lscpu
  getconf CLK_TCK
  systemd-detect-virt || true
  free -b
  uptime
  ps -eo pid,psr,pcpu,comm --sort=-pcpu | head -n 30 || true
} > raw/host-before.txt
cp --remove-destination /proc/cpuinfo raw/cpuinfo.txt
cp --remove-destination /proc/meminfo raw/meminfo.txt
cp --remove-destination /proc/stat raw/proc-stat-before.txt
check_time
GOMAXPROCS=1 go build -o build/sketch . > raw/build.txt 2>&1
check_time
GOMAXPROCS=1 go vet ./... > raw/vet.txt 2>&1
check_time
taskset -c 6 env GOMAXPROCS=1 go test -v -count=1 ./... > raw/test.txt 2>&1
check_time
taskset -c 6 env GOMAXPROCS=1 go test -race -v -count=1 ./... > raw/test-race.txt 2>&1
check_time
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command exercise -seed 49 -reps 3 > raw/exercise-seed49.jsonl 2> raw/exercise-seed49.stderr
check_time
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command exercise -seed 50 -reps 3 > raw/exercise-seed50.jsonl 2> raw/exercise-seed50.stderr
check_time
vmstat -w 1 > raw/vmstat-bench.txt &
noise_pid=$!
trap 'kill "$noise_pid" 2>/dev/null || true; wait "$noise_pid" 2>/dev/null || true' EXIT
# 256 decimal MB requested; actual is rounded to complete 32768-byte echoes.
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command bench -seed 49 -reps 5 -ops 2000 -mb 256 > raw/benchmark.jsonl 2> raw/benchmark.stderr
kill "$noise_pid" 2>/dev/null || true
wait "$noise_pid" 2>/dev/null || true
trap - EXIT
{
  date -u +%Y-%m-%dT%H:%M:%SZ
  uptime
  free -b
  ps -eo pid,psr,pcpu,comm --sort=-pcpu | head -n 30 || true
} > raw/host-after.txt
cp --remove-destination /proc/stat raw/proc-stat-after.txt
sha256sum go.mod *.go run.sh > raw/source-sha256.txt
check_time > raw/run-end.txt
printf 'Raw results are under %s/raw. Remove build and owned caches before handoff.\n' "$PWD"

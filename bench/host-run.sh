#!/usr/bin/env bash
set -euo pipefail
cd /home/joshazimullah.linux/work_mounts/scratch/sol-multicore
unset GOMAXPROCS GOMEMLIMIT GOFLAGS
mkdir -p results/host
python3 bench/wait-quiet.py
export OUT=results/host DATA=/tmp/sol-multicore-data WARMUP=1s DURATION=3s HOST_QUIET=1
{
  date -u +%FT%TZ
  lscpu
  free -b
  findmnt -T /tmp -o SOURCE,FSTYPE,OPTIONS
  uname -a
  go version
  bin/etcd --version
  sha256sum bin/*
  git rev-parse HEAD
  echo 'server main 36eda3cd, no server/internal/module edits'
  git diff 36eda3cd -- cmd/tigerstore internal go.mod go.sum
  echo 'Full CPU affinity, no GOMAXPROCS or GOMEMLIMIT at runtime; no CPU limit. Scope MemoryMax=3G and MemorySwapMax=0.'
  echo 'Host windows: 1s warmup + 3s measurement, shorter than runner 3s+10s to fit retained MVCC history inside 3 GiB without swap.'
  cat /proc/self/cgroup
  grep Cpus_allowed_list /proc/self/status
  cg=$(awk -F: '$1==0 { print $3 }' /proc/self/cgroup)
  for name in memory.max memory.swap.max cpu.max; do echo "$name"; cat "/sys/fs/cgroup$cg/$name"; done
  vmstat 1 11
} > results/host/runner.txt
# Poll the scope rather than setting GOMEMLIMIT, which would change defaults.
cg=$(awk -F: '$1==0 { print $3 }' /proc/self/cgroup)
(
  while true; do
    printf '%s ' "$(date -u +%FT%TZ)"
    for name in memory.current memory.peak memory.swap.current; do printf '%s=' "$name"; tr '\n' ' ' < "/sys/fs/cgroup$cg/$name"; done
    echo
    sleep 1
  done
) > results/host/scope-memory.txt &
monitor=$!
trap 'kill "$monitor" 2>/dev/null || true' EXIT
bench/multicore.sh
cat "/sys/fs/cgroup$cg/memory.events" > results/host/scope-memory-events.txt
cat "/sys/fs/cgroup$cg/memory.peak" > results/host/scope-memory-peak.txt
printf 'host measurements complete\n'

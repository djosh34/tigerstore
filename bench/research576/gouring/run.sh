#!/usr/bin/env bash
set -euo pipefail
mkdir -p results
run_case() {
  mode=$1; n=$2; size=$3; rep=$4; procs=$5; trace=$6
  stem="results/$mode-n$n-b$size-p$procs-r$rep$trace"
  if [[ "$trace" == trace ]]; then
    taskset -c 0 strace -qq -f -c -o "$stem.strace" env GOMAXPROCS="$procs" ./gouring576 -mode="$mode" -reply="$size" >"$stem.server" 2>&1 &
  else
    taskset -c 0 env GOMAXPROCS="$procs" ./gouring576 -mode="$mode" -reply="$size" >"$stem.server" 2>&1 &
  fi
  pid=$!
  for i in $(seq 1 200); do
    if grep -q READY "$stem.server"; then break; fi
    if ! kill -0 "$pid" 2>/dev/null; then cat "$stem.server"; exit 1; fi
    sleep .05
  done
  d=2s; w=500ms
  if [[ "$trace" == trace ]]; then d=500ms; w=0; fi
  if ! timeout 25s taskset -c 3 env GOMAXPROCS=1 ./gouring576 -mode=client -reply="$size" -connections="$n" -duration="$d" -warmup="$w" >"$stem.client" 2>&1; then
    cat "$stem.client" "$stem.server"; kill "$pid" || true; exit 1
  fi
  wait "$pid" || { cat "$stem.server"; exit 1; }
  cat "$stem.server" "$stem.client"
}
for rep in 1 2 3; do
  for size in 256 4096 16384; do
    for n in 6 64; do
      for mode in net batch uring uringzc; do run_case "$mode" "$n" "$size" "$rep" 2 ''; done
    done
  done
done
for mode in net batch uring uringzc; do
  for rep in 1 2 3; do run_case "$mode" 6 256 "$rep" 1 ''; done
  for size in 256 4096 16384; do
    for n in 6 64; do run_case "$mode" "$n" "$size" 1 2 trace; done
  done
done

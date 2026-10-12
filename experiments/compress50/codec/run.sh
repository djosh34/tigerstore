#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo 'usage: ./run.sh /private/dump.json /results [cpu]' >&2
  exit 2
fi
# Use the first permitted CPU unless the caller supplies one.
allowed=$(taskset -pc $$)
allowed=${allowed##*: }
first=${allowed%%,*}
first=${first%%-*}
cpu=${3:-$first}
ulimit -c 0
go build -o codec-helper .
# Keep the entire process, including codec state, on one CPU. The binary has
# an internal 230-second budget; timeout also covers a slow library call.
exec taskset -c "$cpu" env GOMAXPROCS=1 timeout --signal=TERM --kill-after=2s 235s ./codec-helper -input "$1" -out "$2"

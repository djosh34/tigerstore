#!/usr/bin/env bash
# Research #575: the leader's allocations per op for each mixed-k8s kind
# alone (get,rev,list,update,create,delete), then the default mix.
set -euo pipefail
export CERTS=$PWD/certs ETCDCTL=$PWD/bin/etcdctl BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
export RESEARCH575_RATE=65536
mkdir -p results
{ uname -a; go version; git rev-parse HEAD; } > results/runner.txt
get() { curl --retry 2 -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$1/$2" -o "$3"; }
for kind in get:100,0,0,0,0,0 rev:0,100,0,0,0,0 list:0,0,100,0,0,0 update:0,0,0,100,0,0 create:0,0,0,0,100,0 delete:0,0,0,0,0,100 mix:18,10,2,50,12,8; do
 name=${kind%%:*} mix=${kind#*:}
 dir=$RUNNER_TEMP/data
 out=$PWD/results/$name
 mkdir -p "$out/markers"
 trap 'bench/cluster.sh stop "$dir"' EXIT
 bench/cluster.sh stop "$dir"
 bench/cluster.sh start tigerstore "$PWD/bin/tigerstore" "$dir" 0 1 2
 cp "$dir/placement.json" "$out/placement.json"
 leader=$(cat "$dir/leader")
 port=${leader##*:}
 RESEARCH575_MARKERS=$out/markers taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
 -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
 -pids "$(<"$dir/pids")" -workload mixed -value-size 1024 -keys 1000000 -conns 6 -streams 48 -watchers 3 -mix "$mix" \
 -warmup 5s -duration 15s -seed 1 -out "$out/result.json" >"$out/loadgen.log" 2>&1 &
 gen=$!
 for mark in 0 1; do
  until [ -f "$out/markers/$mark" ]; do
   if ! kill -0 "$gen" 2>/dev/null; then wait "$gen" || true; echo "$name: generator ended before marker" >&2; break 2; fi
   sleep 0.05
  done
  get "$leader" 'debug/pprof/allocs' "$out/$mark-$port.allocs"
  get "$leader" metrics "$out/$mark-$port.metrics"
  if [ "$mark" = 0 ]; then get "$leader" 'debug/pprof/profile?seconds=12' "$out/$port.cpu" & prof=$!; fi
 done
 wait "$prof" || true
 wait "$gen" || echo "$name: loadgen failed" >> results/failures.txt
 cp "$dir"/n*.log "$out/" || true
 bench/cluster.sh stop "$dir"
 rm -rf "$dir"
done

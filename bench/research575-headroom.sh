#!/usr/bin/env bash
# Research #575: leader CPU per mixed-k8s op as the leader's GC headroom
# grows with the value cache's size unchanged. Bench flags as bench/run.sh.
set -euo pipefail
export CERTS=$PWD/certs ETCDCTL=$PWD/bin/etcdctl BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
export GODEBUG=gctrace=1 RESEARCH575_RATE=524288
mkdir -p results
{ uname -a; go version; lscpu | grep -E 'Model name|^CPU\(s\)'; free -b; git rev-parse HEAD; } > results/runner.txt
get() { curl --retry 2 -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$1/$2" -o "$3"; }
run=0
for extra in ${POINTS:-0 1 3 0 1 3}; do
 run=$((run + 1))
 dir=$RUNNER_TEMP/data
 out=$PWD/results/$run-extra$extra
 mkdir -p "$out/markers" "$dir.limit"
 rm -f "$dir.limit"/*
 export RESEARCH575_LIMIT_DIR=$dir.limit
 trap 'bench/cluster.sh stop "$dir"' EXIT
 bench/cluster.sh stop "$dir"
 bench/cluster.sh start tigerstore "$PWD/bin/tigerstore" "$dir" 0 1 2
 cp "$dir/placement.json" "$out/placement.json"
 leaderpid=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["pids_leader_first"].split(",")[0])' "$dir/placement.json")
 leader=$(cat "$dir/leader")
 echo $((extra << 30)) > "$dir.limit/$leaderpid"
 RESEARCH575_MARKERS=$out/markers taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
 -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
 -pids "$(<"$dir/pids")" -workload mixed -value-size 1024 -keys 1000000 -conns 6 -streams 48 -watchers 3 \
 -warmup 5s -duration 30s -seed "$run" -out "$out/result.json" >"$out/loadgen.log" 2>&1 &
 gen=$!
 for mark in 0 1; do
  until [ -f "$out/markers/$mark" ]; do
   if ! kill -0 "$gen" 2>/dev/null; then wait "$gen"; echo 'generator ended before marker' >&2; exit 1; fi
   sleep 0.05
  done
  get "$leader" metrics "$out/$mark.metrics"
  if [ "$mark" = 0 ]; then get "$leader" 'debug/pprof/profile?seconds=25' "$out/leader.cpu" & prof=$!; fi
 done
 wait "$prof"
 wait "$gen"
 bench/cluster.sh check "$dir" || echo "leader changed" > "$out/leader-changed"
 cp "$dir"/n*.log "$out/"
 { ps -p "$(cat "$dir/pids")" -o pid,rss; free -b; } > "$out/resources.txt"
 bench/cluster.sh stop "$dir"
 rm -rf "$dir"
done

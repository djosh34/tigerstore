#!/usr/bin/env bash
set -euo pipefail
export CERTS=$PWD/certs ETCDCTL=$PWD/bin/etcdctl BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
mkdir -p results
uname -a > results/runner.txt
go version >> results/runner.txt
lscpu >> results/runner.txt
git rev-parse HEAD >> results/runner.txt
findmnt -T "$RUNNER_TEMP" >> results/runner.txt
get() { curl --retry 2 -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$1/$2" -o "$3"; }
for rate in 524288 65536; do
 export RESEARCH575_RATE=$rate
 dir=$RUNNER_TEMP/data-$rate
 out=$PWD/results/$rate
 mkdir -p "$out/markers"
 trap 'bench/cluster.sh stop "$dir"' EXIT
 bench/cluster.sh start tigerstore "$PWD/bin/tigerstore" "$dir" 0 1 2
 cp "$dir/placement.json" "$out/placement.json"
 read -r -a eps <<<"$(tr ',' ' ' <"$dir/endpoints")"
 cp "$dir/leader" "$out/leader"
 RESEARCH575_MARKERS=$out/markers taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
 -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
 -pids "$(<"$dir/pids")" -workload mixed -keys 1000000 -conns 6 -streams 96 \
 -warmup 5s -duration 40s -seed 1 -out "$out/result.json" >"$out/loadgen.log" 2>&1 &
 gen=$!
 for mark in 0 1; do
  until [ -f "$out/markers/$mark" ]; do
   if ! kill -0 "$gen" 2>/dev/null; then wait "$gen"; echo 'generator ended before marker' >&2; exit 1; fi
   sleep 0.05
  done
  jobs=()
  for ep in "${eps[@]}"; do
   get "$ep" 'debug/pprof/allocs' "$out/$mark-${ep##*:}.allocs" & jobs+=($!)
   get "$ep" metrics "$out/$mark-${ep##*:}.metrics" & jobs+=($!)
  done
  if [ "$mark" = 0 ]; then
   for ep in "${eps[@]}"; do
    get "$ep" 'debug/pprof/profile?seconds=35' "$out/${ep##*:}.cpu" & jobs+=($!)
   done
  fi
  for job in "${jobs[@]}"; do wait "$job"; done
 done
 wait "$gen"
 bench/cluster.sh check "$dir"
 cp "$dir"/n*.log "$out/"
 bench/cluster.sh stop "$dir"
 for ep in "${eps[@]}"; do
  port=${ep##*:}
  for sample in alloc_objects alloc_space; do
   go tool pprof -top -lines -nodecount=10000 -nodefraction=0 -sample_index="$sample" -base "$out/0-$port.allocs" "$out/1-$port.allocs" > "$out/$port-$sample.txt"
  done
 done
done

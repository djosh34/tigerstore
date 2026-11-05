#!/usr/bin/env bash
set -euo pipefail
bins=$(realpath bin)
export CERTS=$(realpath certs) ETCDCTL=$bins/etcdctl BENCH_GOMEMLIMIT=3GiB BENCH_FLAGS=--enable-pprof
out=$(realpath results)
dir=$RUNNER_TEMP/mixed-cluster
trap 'bench/cluster.sh stop "$dir"; bench/netem.sh off; sudo iptables -D OUTPUT -o lo -j R484 2>/dev/null || true; sudo iptables -F R484 2>/dev/null || true; sudo iptables -X R484 2>/dev/null || true' EXIT
sudo iptables -N R484
sudo iptables -A OUTPUT -o lo -j R484
sudo iptables -A R484 -p tcp -m multiport --ports 20011,20021,20031 -j RETURN
sudo iptables -A R484 -p tcp -m multiport --ports 20010,20020,20030 -j RETURN
for n in 1 2 3; do
 sudo iptables -I R484 1 -p tcp -m multiport --ports "$((20000+10*n))"
 sudo iptables -I R484 1 -p tcp -m multiport --ports "$((20001+10*n))"
done
for spec in ${R484_SPECS:-0:48 2:48 10:48 10:96}; do
 read -r rtt streams <<<"${spec/:/ }"
 bench/netem.sh on "$rtt"
 bench/netem.sh probe >"$out/probe-$rtt-$streams.txt"
 for rep in $(seq "${R484_REPS:-3}"); do
  run=$out/rtt-$rtt-streams-$streams-rep-$rep
  mkdir -p "$run"
  bench/cluster.sh start tigerstore "$bins/tigerstore" "$dir" 0 1 2
  cp "$dir/placement.json" "$run/placement.json"
  mapfile -t endpoints < <(tr ',' '\n' <"$dir/endpoints")
  leader=$(<"$dir/leader")
  follower=${endpoints[0]}; if [ "$follower" = "$leader" ]; then follower=${endpoints[1]}; fi
  profile=0
  if [ "$rep" = 1 ] && [ "$streams" = 48 ] && [ "$rtt" != 2 ]; then profile=1; fi
  (
   while [ ! -f "$run/start" ]; do sleep .05; done
   sudo iptables-save -c -t filter >"$run/wire-start.txt"
   cp /proc/diskstats "$run/diskstats-start.txt"
   for pid in $(tr ',' ' ' <"$dir/pids"); do cp "/proc/$pid/io" "$run/io-$pid-start.txt"; done
   curlpids=()
   if [ "$profile" = 1 ]; then
    for item in "leader $leader" "follower $follower"; do
     read -r role ep <<<"$item"
     curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$ep/debug/pprof/profile?seconds=9" -o "$run/$role.pprof" &
     curlpids+=("$!")
    done
   fi
   while [ ! -f "$run/stop" ]; do sleep .05; done
   sudo iptables-save -c -t filter >"$run/wire-stop.txt"
   cp /proc/diskstats "$run/diskstats-stop.txt"
   for pid in $(tr ',' ' ' <"$dir/pids"); do cp "/proc/$pid/io" "$run/io-$pid-stop.txt"; done
   for p in "${curlpids[@]}"; do wait "$p"; done
  ) &
  sampler=$!
  R484_DIR="$run" R484_PROFILE="$profile" taskset -c 3 "$bins/loadgen-research" \
   -endpoints "$(<"$dir/endpoints")" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
   -pids "$(<"$dir/pids")" -disk "$(basename "$(findmnt -T "$RUNNER_TEMP" -n -o SOURCE)")" \
   -workload mixed -keys 1000000 -conns 6 -streams "$streams" -warmup 3s -duration 10s -seed "$rep" -out "$run/result.json"
  wait "$sampler"
  bench/cluster.sh check "$dir"
  cp "$dir"/n*.log "$run/"
  ps -p "$(<"$dir/pids")" -o pid,rss,vsz >"$run/memory.txt"
  for role in leader follower generator; do
   if [ -f "$run/$role.pprof" ]; then
    binary=$bins/tigerstore; if [ "$role" = generator ]; then binary=$bins/loadgen-research; fi
    go tool pprof -top -nodecount=100 -nodefraction=0 "$binary" "$run/$role.pprof" >"$run/$role.top.txt"
    go tool pprof -top -cum -nodecount=100 -nodefraction=0 "$binary" "$run/$role.pprof" >"$run/$role.cum.txt"
   fi
  done
  jq -r --arg run "$(basename "$run")" '[ $run,.throughput,.server_cores_by_node,.client_cores,.server_cpu_us_per_op,.client_cpu_us_per_op,.disk_bytes_per_op,.latency_us.mean,.watch.events_per_s ]|@json' "$run/result.json" | tee -a "$out/summary.jsonl"
  bench/cluster.sh stop "$dir"
 done
 bench/netem.sh show >"$out/netem-$rtt-$streams.txt"
 bench/netem.sh off
done

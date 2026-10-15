#!/usr/bin/env bash
# fake-once.sh <name> <gen cpus> <keys> [extra loadgen flags]
# Mixed max against fakeetcd on CPUs ${FAKECPUS:-0-1}, generator on <gen cpus>.
set -euo pipefail
name=$1 gencpus=$2 keys=$3; shift 3
b=/tmp/b188; out=$b/fout; mkdir -p $out; c=$b/certs
GOGC=${FGOGC:-100} GOMEMLIMIT=${FML:-2500MiB} taskset -c ${FAKECPUS:-0-1} $b/bin/fakeetcd -listen 127.0.0.1:42000 -cert $c/node1.pem -key $c/node1-key.pem -cacert $c/ca.pem ${FAKEPROF:+-cpuprofile $out/$name.fake.pprof -profile-after $FAKEPROF} 2>$out/$name.fake.log &
fp=$!
trap 'kill $fp 2>/dev/null; wait $fp 2>/dev/null; true' EXIT
sleep 0.3
busy() { awk '/^cpu /{print $2+$3+$4+$7+$8, $2+$3+$4+$5+$6+$7+$8}' /proc/stat; }
status=0
read b0 t0 < <(busy)
taskset -c $gencpus ${LOADGEN:-$b/bin/loadgen} -endpoints 127.0.0.1:42000 \
  -cacert $c/ca.pem -cert $c/client.pem -key $c/client-key.pem \
  -pids $fp -label $name -workload mixed -value-size 1024 -watchers 3 \
  -warmup ${WARM:-2s} -duration ${DUR:-8s} -seed 1 -out $out/$name.json -keys $keys -conns 6 -streams 16 -preload-rate 0 \
  -cpuprofile $out/$name.pprof "$@" 2>$out/$name.stderr || status=$?
read b1 t1 < <(busy)
rss=$(ps -o rss= -p $fp)
tail -1 $out/$name.stderr
jq -c --arg st $status --arg rss "$rss" --argjson hostbusy "$(awk -v a=$b0 -v b=$b1 -v c=$t0 -v d=$t1 'BEGIN{printf "%.2f", (b-a)/(d-c)*8}')" \
 '{name:.label, status:$st, valid, conns, streams:.streams_per_conn, live_keys, tput:(.throughput|floor), p50:.latency_us.p50, client_cores:(.client_cores*100|floor/100), client_us_op:(.client_cpu_us_per_op*10|floor/10), fake_cores:(.server_cores_by_node[0]*100|floor/100), watch_eps:(.watch.events_per_s|floor), host_busy:$hostbusy, fake_rss_kb:$rss, errors}' $out/$name.json | tee -a $out/summary.jsonl

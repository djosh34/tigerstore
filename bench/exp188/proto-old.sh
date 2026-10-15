#!/usr/bin/env bash
# proto-old.sh <name> <gen cpus> <keys> [extra loadgen flags]
# Old loadgen against fakeetcd on FAKECPUS (default 4-7); outputs in /tmp/b188/proto/out.
set -euo pipefail
name=$1 gencpus=$2 keys=$3; shift 3
b=/tmp/b188; out=$b/proto/out; mkdir -p $out; c=$b/certs
FAKE=${FAKE:-$b/proto/bin/fakeetcd}
GOGC=off GOMEMLIMIT=800MiB taskset -c ${FAKECPUS:-4-7} $FAKE -listen 127.0.0.1:42000 -cert $c/node1.pem -key $c/node1-key.pem -cacert $c/ca.pem 2>$out/$name.fake.log &
fp=$!
trap 'kill $fp 2>/dev/null; wait $fp 2>/dev/null; true' EXIT
sleep 0.3
status=0
taskset -c $gencpus $b/bin/loadgen -endpoints 127.0.0.1:42000 \
  -cacert $c/ca.pem -cert $c/client.pem -key $c/client-key.pem \
  -pids $fp -label $name -workload mixed -value-size 1024 -watchers 3 \
  -warmup 2s -duration 8s -seed 1 -out $out/$name.json -keys $keys -conns 6 -streams 16 -preload-rate 0 \
  -cpuprofile $out/$name.pprof "$@" 2>$out/$name.stderr || status=$?
echo "load1=$(cut -d' ' -f1 /proc/loadavg)"
jq -c --arg st $status '{name:.label, status:$st, valid, tput:(.throughput|floor), p50:.latency_us.p50, p99:.latency_us.p99, client_cores:(.client_cores*100|floor/100), client_us_op:(.client_cpu_us_per_op*10|floor/10), fake_cores:(.server_cores_by_node[0]*100|floor/100), watch_eps:(.watch.events_per_s|floor), errors}' $out/$name.json

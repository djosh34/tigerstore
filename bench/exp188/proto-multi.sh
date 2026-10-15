#!/usr/bin/env bash
# proto-multi.sh <name> <instances> [extra loadgen2 flags]
# N loadgen2 instances (instance i on CPU i, disjoint keys, each watching
# every resource) against one fakeetcd on FAKECPUS (default 4-7).
set -euo pipefail
name=$1 n=$2; shift 2
b=/tmp/b188; out=$b/proto/out; mkdir -p $out; c=$b/certs
GOGC=off GOMEMLIMIT=${FML:-800MiB} taskset -c ${FAKECPUS:-4-7} $b/proto/bin/fakeetcd -listen 127.0.0.1:42000 -cert $c/node1.pem -key $c/node1-key.pem -cacert $c/ca.pem 2>$out/$name.fake.log &
fp=$!
trap 'kill $fp 2>/dev/null; wait $fp 2>/dev/null; true' EXIT
sleep 0.3
pids=()
for i in $(seq 0 $((n-1))); do
  taskset -c $i $b/proto/bin/loadgen2 -endpoint 127.0.0.1:42000 \
    -cacert $c/ca.pem -cert $c/client.pem -key $c/client-key.pem -pid $fp -label $name-i$i \
    -instance $i -watch-now -warmup 3s -out $out/$name-i$i.json "$@" >/dev/null 2>$out/$name-i$i.stderr &
  pids+=($!)
done
st=0
for p in "${pids[@]}"; do wait $p || st=1; done
echo "status=$st load1=$(cut -d' ' -f1 /proc/loadavg)"
for i in $(seq 0 $((n-1))); do
  jq -c '{l:.label,valid,ops:(.ops_per_s|floor),cores:(.gen_cores*100|floor/100),us:(.gen_us_per_op*100|floor/100),fake:(.fake_cores*100|floor/100),own_weps:(.watch_events_per_s|floor),foreign:.foreign_watch_events,p50:.latency_us.p50,checks:.check_failures}' $out/$name-i$i.json
done
jq -s -c '{cluster_ops:(map(.ops_per_s)|add|floor), gen_cores:(map(.gen_cores)|add*100|floor/100), fake_cores:(map(.fake_cores)|add/length*100|floor/100), valid:(map(.valid)|all)} | .gen_us_per_cluster_op=((.gen_cores/.cluster_ops*1e6*100|floor)/100) | .per_instance_us_per_cluster_op=((.gen_cores/'$n'/.cluster_ops*1e6*100|floor)/100)' $out/$name-i*.json

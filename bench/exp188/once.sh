#!/usr/bin/env bash
# once.sh <name> <gen cpus> <keys> [extra loadgen flags]
# Mirrors bench/run.sh once() for mixed-k8s max, with nodes on 0-1, 2-3, 4-5.
set -euo pipefail
name=$1 gencpus=$2 keys=$3; shift 3
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
b=/tmp/b188; out=$b/out; mkdir -p $out
export CERTS=$b/certs ETCDCTL=/usr/local/bin/etcdctl BENCH_GOMEMLIMIT=${GML:-1GiB} BENCH_PORT=${PORT:-41000}
dir=$b/data
$repo/bench/cluster.sh stop $dir
$repo/bench/cluster.sh start tigerstore $b/bin/tigerstore $dir 0-1 2-3 4-5
busy() { awk '/^cpu /{print $2+$3+$4+$7+$8, $2+$3+$4+$5+$6+$7+$8}' /proc/stat; }
read b0 t0 < <(busy); l0=$(cut -d' ' -f1 /proc/loadavg)
status=0
taskset -c $gencpus $b/bin/loadgen -endpoints "$(cat $dir/endpoints)" \
  -cacert $CERTS/ca.pem -cert $CERTS/client.pem -key $CERTS/client-key.pem \
  -pids "$(cat $dir/pids)" -label $name -workload mixed -value-size 1024 -watchers 3 \
  -warmup 3s -duration 10s -seed 1 -out $out/$name.json -keys $keys -conns 6 -streams 16 \
  -cpuprofile $out/$name.pprof "$@" 2>$out/$name.stderr || status=$?
read b1 t1 < <(busy); l1=$(cut -d' ' -f1 /proc/loadavg)
$repo/bench/cluster.sh check $dir || status=$?
$repo/bench/cluster.sh stop $dir
tail -2 $out/$name.stderr
jq -c --arg st $status --arg l0 $l0 --arg l1 $l1 --argjson hostbusy "$(awk -v a=$b0 -v b=$b1 -v c=$t0 -v d=$t1 'BEGIN{print (b-a)/(d-c)*8}')" \
 '{name:.label, status:$st, valid, live_keys, tput:(.throughput|floor), p99:.latency_us.p99, client_cores, client_us_op:.client_cpu_us_per_op, server_cores:.server_cores_by_node, watch_eps:(.watch.events_per_s|floor), hostbusy_cores_whole_run:$hostbusy, load:[$l0,$l1], errors}' $out/$name.json | tee -a $out/summary.jsonl

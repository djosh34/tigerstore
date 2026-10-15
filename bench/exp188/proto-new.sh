#!/usr/bin/env bash
# proto-new.sh <name> <gen cpus> [extra loadgen2 flags]
# loadgen2 against fakeetcd on FAKECPUS (default 4-7); outputs in /tmp/b188/proto/out.
set -euo pipefail
name=$1 gencpus=$2; shift 2
b=/tmp/b188; out=$b/proto/out; mkdir -p $out; c=$b/certs
GOGC=off GOMEMLIMIT=${FML:-800MiB} taskset -c ${FAKECPUS:-4-7} $b/proto/bin/fakeetcd -listen 127.0.0.1:42000 -cert $c/node1.pem -key $c/node1-key.pem -cacert $c/ca.pem ${FAKEPROF:+-cpuprofile $out/$name.fake.pprof -profile-after $FAKEPROF} 2>$out/$name.fake.log &
fp=$!
trap 'kill $fp 2>/dev/null; wait $fp 2>/dev/null; true' EXIT
sleep 0.3
status=0
taskset -c $gencpus ${GEN:-$b/proto/bin/loadgen2} -endpoint 127.0.0.1:42000 \
  -cacert $c/ca.pem -cert $c/client.pem -key $c/client-key.pem -pid $fp -label $name \
  -out $out/$name.json -cpuprofile $out/$name.pprof "$@" 2>$out/$name.stderr || status=$?
cat $out/$name.stderr | tail -3
echo "status=$status load1=$(cut -d' ' -f1 /proc/loadavg) fake_rss_kb=$(ps -o rss= -p $fp)"
jq -c '{label:.label, valid, workers, ops_per_s:(.ops_per_s|floor), gen_cores:(.gen_cores*100|floor/100), gen_us_per_op:(.gen_us_per_op*100|floor/100), fake_cores:(.fake_cores*100|floor/100), p50:.latency_us.p50, p99:.latency_us.p99, weps:(.watch_events_per_s|floor), gcs:.gc_cycles_window, preload_s, load1, check_failures}' $out/$name.json

#!/usr/bin/env bash
set -euo pipefail
mkdir -p results
# Null completion is intentionally not a store; the generator still validates
# protobuf shape, unique revisions and txn response operations, NOT CAS semantics.
for cpus in 0 0-2; do
 for wl in put txn; do
  for rep in 1 2 3; do
   out=results/null-${cpus}-${wl}-${rep}
   dir=/dev/shm/floor-null
   rm -rf "$dir"
   FLOOR_NULL=1 taskset -c "$cpus" bin/floor-server \
    --name n1 --data-dir "$dir" \
    --listen-client-urls https://127.0.0.1:20010 \
    --advertise-client-urls https://127.0.0.1:20010 \
    --listen-peer-urls https://127.0.0.1:20011 \
    --initial-advertise-peer-urls https://127.0.0.1:20011 \
    --initial-cluster n1=https://127.0.0.1:20011 \
    --cert-file certs/node1.pem --key-file certs/node1-key.pem \
    --trusted-ca-file certs/ca.pem --client-cert-auth \
    --peer-cert-file certs/node1.pem --peer-key-file certs/node1-key.pem \
    --peer-trusted-ca-file certs/ca.pem --enable-grpc-gateway=false \
    >"$out.log" 2>&1 &
   pid=$!
   trap 'kill "$pid" 2>/dev/null || true' EXIT
   sleep 3
   kill -0 "$pid"
   taskset -c 3 bin/loadgen -endpoints 127.0.0.1:20010 \
    -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem \
    -pids "$pid" -label "null-$cpus" -workload "$wl" -value-size 1024 \
    -keys 10000 -conns 6 -streams 256 -warmup 2s -duration 6s -seed "$rep" -out "$out.json"
   jq -e .valid "$out.json"
   kill "$pid"
   wait "$pid" || true
   trap - EXIT
  done
 done
done
python3 bench/ceiling-floor-report.py results | tee results/summary.md

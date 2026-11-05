#!/usr/bin/env bash
# Actual tigerstore listeners, not a guessed cipher preference. No workload.
set -euo pipefail
mkdir -p results
certs=$(realpath certs)
taskset -c 0 bin/tigerstore --name n1 --data-dir "$RUNNER_TEMP/tls-probe" \
 --listen-client-urls https://127.0.0.1:20010 --advertise-client-urls https://127.0.0.1:20010 \
 --listen-peer-urls https://127.0.0.1:20011 --initial-advertise-peer-urls https://127.0.0.1:20011 \
 --initial-cluster n1=https://127.0.0.1:20011 \
 --cert-file "$certs/node1.pem" --key-file "$certs/node1-key.pem" --trusted-ca-file "$certs/ca.pem" --client-cert-auth \
 --peer-cert-file "$certs/node1.pem" --peer-key-file "$certs/node1-key.pem" --peer-trusted-ca-file "$certs/ca.pem" >results/tigerstore-tls.log 2>&1 & pid=$!
trap 'kill "$pid"; wait "$pid" || true' EXIT
for i in $(seq 100); do
 if taskset -c 3 bin/netraw -tls -mode handshake -addr 127.0.0.1:20010 -alpn h2 >results/tigerstore-client-tls.json 2>results/tigerstore-client-tls.err; then break; fi
 sleep .1
done
test -s results/tigerstore-client-tls.json
taskset -c 1 bin/netraw -tls -mode handshake -addr 127.0.0.1:20011 -alpn tigerstore-peer/2 >results/tigerstore-peer-tls.json 2>results/tigerstore-peer-tls.err
{
 echo '## Actual tigerstore TLS negotiation (same certificates/flags as bench)'
 echo '```'
 git rev-parse HEAD
 cat results/tigerstore-{client,peer}-tls.json
 echo '```'
} >>"$GITHUB_STEP_SUMMARY"

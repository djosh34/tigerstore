#!/usr/bin/env bash
# Starts a 3-node cluster on this machine with the certificates of
# scripts/dev-certs.sh. Clients connect to https://127.0.0.1:2379, 22379
# and 12379, or to CLIENT_HOST in place of 127.0.0.1 (its IP must be in
# the certificates), peers to 127.0.0.1 on ports 2380, 22380 and 12380;
# TIGERSTORE is the binary (default ./tigerstore). OFFSET adds to every
# port. With OFFSET up to 2000 every port stays below Linux's ephemeral
# range (32768 on), where another process's outgoing connection can take
# a port after it was checked free and before the node binds it (#184).
# Data goes to ./data/node1..3, logs to ./data/node1..3.log. Each node
# may use MEMORY_BUDGET bytes (default a quarter of the host's memory).
# Arguments are passed to every node as extra flags, with
# @CLIENT@ replaced by the node's client port and @METRICS@ by that port
# plus 2.
# Stop it with: kill $(cat data/*.pid)
set -euo pipefail
bin=${TIGERSTORE:-./tigerstore}
certs=${CERTS:-certs}
off=${OFFSET:-0}
host=${CLIENT_HOST:-127.0.0.1}
client=($((2379 + off)) $((22379 + off)) $((12379 + off)))
peer=($((2380 + off)) $((22380 + off)) $((12380 + off)))
cluster=node1=https://127.0.0.1:${peer[0]},node2=https://127.0.0.1:${peer[1]},node3=https://127.0.0.1:${peer[2]}
budget=${MEMORY_BUDGET:-$(($(awk '/^MemTotal:/ {print $2}' /proc/meminfo) * 1024 / 4))}
mkdir -p data
for i in 1 2 3; do
	port=${client[$((i - 1))]}
	peerurl=https://127.0.0.1:${peer[$((i - 1))]}
	extra=("${@//@METRICS@/$((port + 2))}")
	"$bin" --name "node$i" --data-dir "data/node$i" \
		--initial-cluster "$cluster" \
		--listen-peer-urls "$peerurl" --initial-advertise-peer-urls "$peerurl" \
		--listen-client-urls "https://$host:$port" --advertise-client-urls "https://$host:$port" \
		--cert-file "$certs/node$i.pem" --key-file "$certs/node$i-key.pem" \
		--trusted-ca-file "$certs/ca.pem" --client-cert-auth \
		--peer-cert-file "$certs/node$i.pem" --peer-key-file "$certs/node$i-key.pem" \
		--peer-trusted-ca-file "$certs/ca.pem" \
		--memory-budget-bytes "$budget" "${extra[@]//@CLIENT@/$port}" \
		>"data/node$i.log" 2>&1 &
	echo $! >"data/node$i.pid"
done
echo "started 3 nodes; logs in data/node*.log"

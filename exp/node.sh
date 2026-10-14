#!/usr/bin/env bash
# Prototype E (#133) helper: restart one node of a bench/cluster.sh cluster
# with the same flags, or print a node's status.
#   exp/node.sh start <bin> <dir> <i> <cpus>
#   exp/node.sh status <dir> <i>     (prints raft index, applied index)
set -euo pipefail
port=${BENCH_PORT:-40000}
certs=${CERTS:?}
case $1 in
start)
	bin=$2 dir=$3 i=$4 cpus=$5
	initial=""
	for j in 1 2 3; do initial+="${initial:+,}n$j=https://127.0.0.1:$((port + 10 * j + 1))"; done
	client="https://127.0.0.1:$((port + 10 * i))"
	peer="https://127.0.0.1:$((port + 10 * i + 1))"
	GOMEMLIMIT=${BENCH_GOMEMLIMIT:-off} taskset -c "$cpus" "$bin" \
		--name "n$i" --data-dir "$dir/n$i" \
		--listen-client-urls "$client" --advertise-client-urls "$client" \
		--listen-peer-urls "$peer" --initial-cluster "$initial" --initial-cluster-token bench \
		--cert-file "$certs/node$i.pem" --key-file "$certs/node$i-key.pem" \
		--trusted-ca-file "$certs/ca.pem" --client-cert-auth \
		--peer-cert-file "$certs/node$i.pem" --peer-key-file "$certs/node$i-key.pem" \
		--peer-trusted-ca-file "$certs/ca.pem" >>"$dir/n$i.log" 2>&1 &
	echo $!
	;;
status)
	dir=$2 i=$3
	"${ETCDCTL:-etcdctl}" --endpoints "127.0.0.1:$((port + 10 * i))" --cacert "$certs/ca.pem" \
		--cert "$certs/client.pem" --key "$certs/client-key.pem" --command-timeout 2s endpoint status -w json |
		jq -r '.[0].Status | "\(.raftIndex // .raft_index) \(.raftAppliedIndex // .raft_applied_index)"'
	;;
esac

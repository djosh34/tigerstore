#!/usr/bin/env bash
# Runs the differential tests in difftest/ against a fresh server.
#
#   scripts/difftest.sh etcd          single-node etcd v3.7.2
#   scripts/difftest.sh etcd record   the same, and rewrites the recorded answers
#   scripts/difftest.sh tigerstore    3-node tigerstore built from this checkout
#
# ETCD is the etcd v3.7.2 binary, etcd on PATH by default. etcd runs on fixed
# ports, because its member ID comes from its URLs and the largest accepted
# request depends on the length of request IDs made from it.
set -euo pipefail
server=${1:?usage: scripts/difftest.sh etcd|tigerstore [record]}
record=${2:-}
work=$(mktemp -d)
stop() {
	local pids
	pids=$(cat "$work"/*.pid "$work"/data/*.pid 2>/dev/null || true)
	kill $pids 2>/dev/null || true
	# Wait until they are gone, so the ports are free for the next run.
	for _ in $(seq 100); do
		kill -0 $pids 2>/dev/null || break
		sleep 0.1
	done
	rm -rf "$work"
}
trap stop EXIT
# in_use reports whether something listens on a local port.
in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
args=(-count=1 -tags difftest ./difftest)
[ "$record" = record ] && args+=(-record)

case $server in
etcd)
	etcd=${ETCD:-etcd}
	"$etcd" --version | grep -q '^etcd Version: 3.7.2$' || {
		echo "$etcd is not etcd 3.7.2"
		exit 1
	}
	for port in 23790 23800; do
		if in_use $port; then
			echo "port $port is in use, so the tests would not reach a fresh etcd"
			exit 1
		fi
	done
	"$etcd" --name diff --data-dir "$work/etcd" \
		--listen-client-urls http://127.0.0.1:23790 --advertise-client-urls http://127.0.0.1:23790 \
		--listen-peer-urls http://127.0.0.1:23800 --initial-advertise-peer-urls http://127.0.0.1:23800 \
		--initial-cluster diff=http://127.0.0.1:23800 --log-level warn >"$work/etcd.log" 2>&1 &
	echo $! >"$work/etcd.pid"
	TIGERSTORE_DIFF_SERVER=etcd TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:23790 go test "${args[@]}" ||
		{ tail -50 "$work/etcd.log"; exit 1; }
	;;
tigerstore)
	go build -o "$work/tigerstore" ./cmd/tigerstore
	scripts/dev-certs.sh "$work/certs" >/dev/null 2>&1
	off=${OFFSET:-$((RANDOM % 2000 + 1))}
	for port in $((2379 + off)) $((22379 + off)) $((32379 + off)) $((2380 + off)) $((22380 + off)) $((32380 + off)); do
		if in_use $port; then
			echo "port $port is in use"
			exit 1
		fi
	done
	(cd "$work" && TIGERSTORE=./tigerstore CERTS=certs OFFSET=$off "$OLDPWD/scripts/local-cluster.sh")
	TIGERSTORE_DIFF_SERVER=tigerstore TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:$((2379 + off)) \
		TIGERSTORE_DIFF_CA=$work/certs/ca.pem TIGERSTORE_DIFF_CERT=$work/certs/client.pem TIGERSTORE_DIFF_KEY=$work/certs/client-key.pem \
		go test "${args[@]}" ||
		{ for f in "$work"/data/*.log; do echo "== $f"; tail -30 "$f"; done; exit 1; }
	;;
*)
	echo "unknown server $server"
	exit 1
	;;
esac

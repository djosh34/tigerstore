#!/usr/bin/env bash
# Starts or stops a 3-node cluster on 127.0.0.1, each node pinned to its own
# CPUs.
#
#   bench/cluster.sh start <target> <server binary> <dir> <cpus 1> <cpus 2> <cpus 3>
#   bench/cluster.sh stop <dir>
#
# <target> is etcd or tigerstore. Both get the same flags, ports, CPUs and
# disk: tigerstore is a drop-in etcd, so it takes etcd's flags. <cpus n> is
# a taskset CPU list such as 1 or 2-3. start writes the comma-separated
# client endpoints to <dir>/endpoints and the node pids to <dir>/pids, and
# returns once a write succeeds. stop kills the nodes and deletes <dir>.
#
# ETCDCTL names the etcdctl used for the readiness write (default: etcdctl
# next to the server binary, then on PATH). BENCH_PORT is the first port
# (default 40000).
set -euo pipefail

port=${BENCH_PORT:-40000}

start() {
	local target=$1 bin=$2 dir=$3
	shift 3
	local cpus=("$@")
	if [ "${#cpus[@]}" -ne 3 ]; then
		echo "need 3 CPU lists" >&2
		exit 2
	fi
	case $target in
	etcd | tigerstore) ;;
	*)
		echo "unknown target $target" >&2
		exit 2
		;;
	esac
	if [ -e "$dir" ]; then
		echo "$dir exists; stop the old cluster first" >&2
		exit 2
	fi
	mkdir -p "$dir"

	local initial="" endpoints=""
	for i in 1 2 3; do
		initial+="${initial:+,}n$i=http://127.0.0.1:$((port + 10 * i + 1))"
		endpoints+="${endpoints:+,}127.0.0.1:$((port + 10 * i))"
	done

	local pids=""
	for i in 1 2 3; do
		local client="http://127.0.0.1:$((port + 10 * i))"
		local peer="http://127.0.0.1:$((port + 10 * i + 1))"
		# The quota is etcd's suggested maximum, so long write runs do not
		# hit the 2 GiB default.
		taskset -c "${cpus[i - 1]}" "$bin" \
			--name "n$i" \
			--data-dir "$dir/n$i" \
			--listen-client-urls "$client" \
			--advertise-client-urls "$client" \
			--listen-peer-urls "$peer" \
			--initial-advertise-peer-urls "$peer" \
			--initial-cluster "$initial" \
			--initial-cluster-state new \
			--initial-cluster-token bench \
			--quota-backend-bytes $((8 << 30)) \
			--log-level warn \
			>"$dir/n$i.log" 2>&1 &
		pids+="${pids:+,}$!"
	done
	echo "$endpoints" >"$dir/endpoints"
	echo "$pids" >"$dir/pids"

	local etcdctl=${ETCDCTL:-$(dirname "$bin")/etcdctl}
	if [ ! -x "$etcdctl" ]; then
		etcdctl=etcdctl
	fi
	for _ in $(seq 100); do
		if "$etcdctl" --endpoints "$endpoints" --command-timeout 2s put /bench/ready 1 >/dev/null 2>&1; then
			return 0
		fi
		for pid in ${pids//,/ }; do
			if ! kill -0 "$pid" 2>/dev/null; then
				echo "a $target node exited; logs:" >&2
				tail -n 20 "$dir"/n*.log >&2
				exit 1
			fi
		done
		sleep 0.3
	done
	echo "$target cluster did not accept a write within 30s; logs:" >&2
	tail -n 20 "$dir"/n*.log >&2
	exit 1
}

stop() {
	local dir=$1
	if [ -f "$dir/pids" ]; then
		local pids pid
		pids=$(tr ',' ' ' <"$dir/pids")
		for pid in $pids; do
			kill "$pid" 2>/dev/null || true
		done
		for pid in $pids; do
			for _ in $(seq 100); do
				kill -0 "$pid" 2>/dev/null || break
				sleep 0.1
			done
			kill -9 "$pid" 2>/dev/null || true
		done
	fi
	rm -rf "$dir"
}

case ${1:-} in
start)
	shift
	start "$@"
	;;
stop)
	stop "$2"
	;;
*)
	echo "usage: $0 start <target> <bin> <dir> <cpus 1> <cpus 2> <cpus 3> | stop <dir>" >&2
	exit 2
	;;
esac

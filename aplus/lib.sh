#!/usr/bin/env bash
# Shared helpers for the A+ prototype measurements (#133). Sourced.
# Needs CERTS, BINS (with loadgen, etcdctl and the server binaries).
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(dirname "$here")
export ETCDCTL=${ETCDCTL:-$BINS/etcdctl}
port=${BENCH_PORT:-40000}

ctl() {
	"$ETCDCTL" --endpoints "$1" --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" --command-timeout 60s "${@:2}"
}

client_port() { echo $((port + 10 * $1)); }

# metric <node> <name>: one value of the node's /metrics, empty if down
metric() {
	curl -s -m 2 --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" \
		"https://127.0.0.1:$(client_port "$1")/metrics" | awk -v n="$2" '$1 == n {print $2}'
}

# node_of_endpoint <host:port>: node number
node_of_endpoint() { echo $(((${1##*:} - port) / 10)); }

# pid_of <dir> <node>
pid_of() { pgrep -f -- "--name n$2 --data-dir $1/n$2" | head -1; }

# limit <node> <pid> <bytes>: puts the process in a memory cgroup of its own
# (page cache included, no swap).
limit() {
	local cg=/sys/fs/cgroup/aplus-n$1
	sudo mkdir -p "$cg"
	echo "$3" | sudo tee "$cg/memory.max" >/dev/null
	echo 0 | sudo tee "$cg/memory.swap.max" >/dev/null || true
	echo "$2" | sudo tee "$cg/cgroup.procs" >/dev/null
}

# mem <dir> <node>: RSS of the node and its cgroup's usage
mem() {
	local pid
	pid=$(pid_of "$1" "$2")
	local rss anon file cur
	rss=$(awk '/^VmRSS/ {print $2 * 1024}' "/proc/$pid/status")
	anon=$(awk '/^RssAnon/ {print $2 * 1024}' "/proc/$pid/status")
	file=$(awk '/^RssFile/ {print $2 * 1024}' "/proc/$pid/status")
	cur=$(cat "/sys/fs/cgroup/aplus-n$2/memory.current" 2>/dev/null || echo none)
	peak=$(cat "/sys/fs/cgroup/aplus-n$2/memory.peak" 2>/dev/null || echo none)
	echo "mem node=$2 rss=$rss rss_anon=$anon rss_file=$file cgroup_current=$cur cgroup_peak=$peak disk=$(du -sb "$1/n$2" | cut -f1)"
}

# restart <dir> <node> [limit bytes]: kill -9 the node, run $DOWN_CMD if
# set, start it again with the same command line, and print the time to
# open the store, to answer /metrics, and to apply everything the leader
# had committed when the node started again.
restart() {
	local dir=$1 i=$2 lim=${3:-}
	local pid args
	pid=$(pid_of "$dir" "$i")
	mapfile -d '' args <"/proc/$pid/cmdline"
	local envs=()
	mapfile -d '' envs <"/proc/$pid/environ"
	local cpus
	cpus=$(taskset -pc "$pid" | awk '{print $NF}')
	kill -9 "$pid"
	while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
	if [ -n "${DOWN_CMD:-}" ]; then bash -c "$DOWN_CMD" || echo "down command failed" >&2; fi
	local leader target
	leader=$(node_of_endpoint "$(cat "$dir/leader")")
	target=$(metric "$leader" etcd_server_proposals_committed_total)
	local t0 t1="" t2=""
	t0=$(date +%s%N)
	env -i "${envs[@]}" "${args[@]}" >>"$dir/n$i.log" 2>&1 &
	local np=$!
	sed -i "s/\b$pid\b/$np/" "$dir/pids"
	if [ -n "$lim" ]; then limit "$i" "$np" "$lim"; fi
	taskset -apc "$cpus" "$np" >/dev/null 2>&1 || true
	for _ in $(seq 60000); do
		a=$(metric "$i" etcd_server_proposals_applied_total || true)
		if [ -n "$a" ] && [ -z "$t1" ]; then t1=$(date +%s%N); fi
		if [ -n "$a" ] && [ "$a" -ge "$target" ]; then t2=$(date +%s%N); break; fi
		if ! kill -0 "$np" 2>/dev/null; then echo "restarted node $i exited" >&2; tail -20 "$dir/n$i.log" >&2; return 1; fi
		sleep 0.01
	done
	echo "restart node=$i target_index=$target serving_s=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", (b-a)/1e9}') caught_up_s=$(awk -v a="$t0" -v b="$t2" 'BEGIN{printf "%.3f", (b-a)/1e9}')"
	grep -h 'aplus-open\|aplus-install' "$dir/n$i.log" | tail -2 || true
}

# compactor <endpoint> <every s> <keep revs>: compacts in the background
# to (revision - keep), as kube-apiserver compacts every 5 minutes.
compactor() {
	while sleep "$2"; do
		local rev
		rev=$(ctl "$1" get /bench/ready -w json 2>/dev/null | jq .header.revision) || continue
		if [ $((rev - $3)) -gt 1 ]; then ctl "$1" compact $((rev - $3)) >/dev/null 2>&1 || true; fi
	done
}

# stats <dir>: the last A+ counter line of each node
stats() {
	for i in 1 2 3; do
		echo "n$i $(grep -h aplus-stats "$1/n$i.log" | tail -1 | sed 's/.*aplus-stats //')"
	done
}

# io <dir>: bytes each node sent to storage
io() {
	for i in 1 2 3; do
		local p
		p=$(pid_of "$1" "$i")
		echo "io node=$i write_bytes=$(awk '/^write_bytes/ {print $2}' "/proc/$p/io")"
	done
}

lg() {
	"$BINS/loadgen" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" "$@"
}

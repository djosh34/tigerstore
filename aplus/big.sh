#!/usr/bin/env bash
# Items 3, 4 and 5 of #133: live values at least 3 times a per-node memory
# limit, resident memory, restart after kill -9, cold point Range and cold
# 500-key LIST.
#
#   aplus/big.sh <target> <memory limit bytes, 0 for none> <keys> <value size> <out dir>
#
# The limit is a cgroup per node (memory.max, page cache included, no
# swap). Before each measured load the page cache is dropped, so reads start
# cold. Range runs on one cluster (then the restart), LIST on a fresh one
# whose namespaces hold 500 keys each, so each LIST returns 500 values
# scattered over the log.
source "$(dirname "$0")/lib.sh"
set +e
target=$1 lim=$2 keys=$3 vsize=$4 out=$5
mkdir -p "$out"
data=${DATA:-/tmp/aplus-big}
gml=off
if [ "$lim" != 0 ]; then gml=$((lim * 6 / 10)); fi
drop='sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null'

start() {
	"$repo/bench/cluster.sh" stop "$data"
	pkill -9 -f -- "--data-dir $data/n" || true
	sleep 1
	BENCH_GOMEMLIMIT=$gml "$repo/bench/cluster.sh" start tigerstore "$BINS/$target" "$data" 0 1 2
	if [ "$lim" != 0 ]; then
		for i in 1 2 3; do limit "$i" "$(pid_of "$data" "$i")" "$lim"; done
	fi
}

run_load() { # <name> <workload> [env]
	local name=$1 wl=$2
	shift 2
	local L
	L=$(cat "$data/leader")
	env "$@" LOADGEN_AFTER_PRELOAD="$drop" "$BINS/loadgen" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
		-endpoints "$L" -pids "$(cat "$data/pids")" -label "$target" -workload "$wl" -value-size "$vsize" -keys "$keys" \
		-warmup 2s -duration 10s -conns 4 -streams 64 -out "$out/$name.json" 2>&1 | tail -3 || echo "$name loadgen failed"
	jq -c '{valid, throughput, ops, latency_us, errors}' "$out/$name.json" || true
}

{
	echo "## $target, limit $lim, keys $keys, value size $vsize, GOMEMLIMIT $gml"
	start
	t0=$(date +%s)
	run_load range range
	echo "preload+range took $(($(date +%s) - t0)) s"
	for i in 1 2 3; do mem "$data" "$i"; done
	stats "$data"
	io "$data"
	# Restart of a follower, then of the leader's peer again with no load.
	L=$(node_of_endpoint "$(cat "$data/leader")")
	f=$(((L % 3) + 1))
	restart "$data" "$f" "$( [ "$lim" != 0 ] && echo "$lim" )"
	sleep 5
	restart "$data" "$f" "$( [ "$lim" != 0 ] && echo "$lim" )"
	for i in 1 2 3; do mem "$data" "$i"; done
	cp "$data"/n*.log "$out/" 2>/dev/null || true
	for i in 1 2 3; do mv "$out/n$i.log" "$out/range-n$i.log"; done
	"$repo/bench/cluster.sh" stop "$data"

	start
	run_load list list LOADGEN_NS=$((keys / 500))
	echo list-done
	for i in 1 2 3; do mem "$data" "$i"; done
	stats "$data"
	cp "$data"/n1.log "$out/list-n1.log"
	"$repo/bench/cluster.sh" stop "$data"
} 2>&1 | tee "$out/big.txt"

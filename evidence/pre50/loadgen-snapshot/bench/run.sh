#!/usr/bin/env bash
# Runs one benchmark against each target store, back to back on this
# machine, and writes one JSON result per run.
#
#   bench/run.sh <benchmark> <out dir>
#
# <benchmark> is <workload>-<size>: workload put, txn, delete, range or
# watch, size 1k or 10k. Each repetition starts a fresh 3-node cluster per
# target and run, so no run sees another's data. Two kinds of run:
#
#   max   every stream busy (6 connections x 256 streams): throughput
#   rate  open-loop Poisson arrivals at half of etcd's median max
#         throughput: latency at the same load for every target
#
# Environment:
#   TARGETS     targets in order (default "etcd"; also "tigerstore")
#   LAYOUT      CPU lists for the generator and nodes 1 to 3 (default "0 1 2 3")
#   REPS        repetitions (default 5)
#   ETCD        etcd binary (default: etcd on PATH)
#   TIGERSTORE  tigerstore binary (default: tigerstore on PATH)
#   LOADGEN     loadgen binary (default: loadgen on PATH)
#   DATA        directory for cluster data, on the disk under test (default /tmp/bench-data)
set -euo pipefail

bench=$1
out=$2
read -r -a targets <<<"${TARGETS:-etcd}"
read -r -a layout <<<"${LAYOUT:-0 1 2 3}"
reps=${REPS:-5}
data=${DATA:-/tmp/bench-data}
loadgen=${LOADGEN:-loadgen}
here=$(dirname "$0")

workload=${bench%-*}
case ${bench##*-} in
1k) size=1024 ;;
10k) size=10240 ;;
*)
	echo "unknown size in $bench" >&2
	exit 2
	;;
esac
if [ "${#layout[@]}" -ne 4 ]; then
	echo "LAYOUT needs 4 CPU lists: generator, node 1, node 2, node 3" >&2
	exit 2
fi

# Delete removes each key once, so it needs a key per request: sized for up
# to about 45k deletes/s. The 10 KiB run is shorter to keep its preload, and
# the disk it fills, small.
keys=10000 warmup=3s duration=10s
if [ "$workload" = delete ]; then
	if [ "$size" = 1024 ]; then
		keys=600000
	else
		keys=250000 warmup=1s duration=4s
	fi
fi

binary() {
	case $1 in
	etcd) command -v "${ETCD:-etcd}" ;;
	tigerstore) command -v "${TIGERSTORE:-tigerstore}" ;;
	*)
		echo "unknown target $1" >&2
		return 2
		;;
	esac
}

# once <target> <mode> <rep> <loadgen flags...>
once() {
	local target=$1 mode=$2 rep=$3
	shift 3
	local dir=$data/$target
	"$here/cluster.sh" stop "$dir"
	"$here/cluster.sh" start "$target" "$(binary "$target")" "$dir" "${layout[@]:1}"
	local status=0
	taskset -c "${layout[0]}" "$loadgen" \
		-endpoints "$(cat "$dir/endpoints")" \
		-pids "$(cat "$dir/pids")" \
		-label "$target" \
		-workload "$workload" \
		-value-size "$size" \
		-watchers 3 \
		-warmup "$warmup" \
		-duration "$duration" \
		-seed "$rep" \
		-out "$out/$bench/$target-$mode-$rep.json" \
		"$@" || status=$?
	"$here/cluster.sh" stop "$dir"
	return "$status"
}

mkdir -p "$out/$bench"
for rep in $(seq "$reps"); do
	for target in "${targets[@]}"; do
		once "$target" max "$rep" -keys "$keys"
	done
done

# Half of etcd's median max throughput, or of the first target's without etcd.
ref=etcd
if [[ " ${targets[*]} " != *" etcd "* ]]; then
	ref=${targets[0]}
fi
rate=$(jq -s 'map(.throughput) | sort | .[length / 2 | floor] / 2 | floor' "$out/$bench/$ref"-max-*.json)
if [ "$workload" = delete ]; then
	keys=$(awk -v r="$rate" -v w="${warmup%s}" -v d="${duration%s}" 'BEGIN { print int(r * (w + d) * 1.2) + 10000 }')
fi
for rep in $(seq "$reps"); do
	for target in "${targets[@]}"; do
		once "$target" rate "$rep" -keys "$keys" -rate "$rate"
	done
done

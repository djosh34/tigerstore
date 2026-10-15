#!/usr/bin/env bash
# Runs one benchmark against each target store, back to back on this
# machine, and writes one JSON result per run.
#
#   bench/run.sh <benchmark> <out dir>
#
# <benchmark> is <workload>-<size>: workload put, txn, delete, range, list
# or watch, size 1k or 10k. Each repetition starts a fresh 3-node cluster per
# target and run, so no run sees another's data. Two kinds of run:
#
#   max   every stream busy (6 connections x 256 streams): throughput
#   rate  open-loop Poisson arrivals at half of etcd's median max
#         throughput: latency at the same load for every target
#
# Each repetition runs every target once, in TARGETS order on odd
# repetitions and reversed on even ones, so drift on the runner does not
# favour one target. Mixed drives all three endpoints; others use the leader.
# mixed-k8s uses the Kubernetes 1.37 dump sizes, not a fixed byte size.
# Preload has a separate 300 Txns/s arrival rate to limit setup bursts.
# Its mix is get/list/update/create/delete = 25/10/40/15/10, plus watches
# on each resource. MIX overrides it. A mixed rate-only job first runs one
# max calibration per store, then offers half of etcd's median max rate
# to all stores for REPS repetitions. MIXED_RATE can override that rate.
#
# Environment:
#   TARGETS     target names in order (default "etcd"); target <name> runs
#               the binary $BINS/<name>, as etcd if the name is etcd and as
#               tigerstore otherwise, so main and a candidate can run side
#               by side
#   BINS        directory of the server binaries and etcdctl (default ./bin)
#   CERTS       certificates made by scripts/dev-certs.sh (required)
#   LAYOUT      CPU lists for the generator, the leader and the two
#               followers (default "3 0 1 2")
#   MODES       which runs (default "max rate")
#   REPS        repetitions (default 5)
#   LOADGEN     loadgen binary (default: loadgen in BINS)
#   DATA        directory for cluster data, on the disk under test (default /tmp/bench-data)
#   DISK        device or partition of DATA in /proc/diskstats, such as
#               sda1; each result then has the bytes it wrote per operation
#   LAYOUTS     extra max runs of every tigerstore target, REPS each:
#               tmpfs puts all three nodes on TMPFS_DIR (no disk: the CPU
#               reference), alone puts the followers there and keeps the
#               leader on the disk; written as <target>-<layout>-<rep>.json
#   TMPFS_DIR   a directory on tmpfs for LAYOUTS (default /dev/shm/bench-data)
set -euo pipefail

bench=$1
out=$2
read -r -a targets <<<"${TARGETS:-etcd}"
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
default_modes="max rate"
if [ "$bench" = mixed-k8s ]; then default_modes=rate; fi
read -r -a modes <<<"${MODES:-$default_modes}"
reps=${REPS:-5}
data=${DATA:-/tmp/bench-data}
tmpfs=${TMPFS_DIR:-/dev/shm/bench-data}
read -r -a extra_layouts <<<"${LAYOUTS:-}"
bins=$(realpath "${BINS:-bin}")
loadgen=${LOADGEN:-$bins/loadgen}
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
export CERTS=$certs ETCDCTL=$bins/etcdctl
here=$(dirname "$0")

workload=${bench%-*}
case ${bench##*-} in
1k) size=1024 ;;
10k) size=10240 ;;
k8s)
	if [ "$workload" != mixed ]; then echo "k8s size needs mixed" >&2; exit 2; fi
	size=1024 # ignored by mixed; values come from the dump table
	;;
*)
	echo "unknown size in $bench" >&2
	exit 2
	;;
esac
if [ "${#layout[@]}" -ne 4 ]; then
	echo "LAYOUT needs 4 CPU lists: generator, leader, follower, follower" >&2
	exit 2
fi

# Delete removes each key once, so it needs a key per request, and a run
# that runs out is invalid: 600,000 keys over 1 + 4 s last up to 120k
# deletes/s. The 10 KiB run preloads fewer keys, to keep the disk it fills
# small, so it is shorter still: 300,000 keys over 1 + 2 s.
keys=10000 warmup=3s duration=10s
# A LIST asks for the first page of one namespace of 100: 1,000 keys
# each, pages of 500 (the loadgen default), so every answer holds a count
# of 1,000, 500 keys and "more". It does not page further. With 10 KiB
# values, 100 keys each keep the preload small.
# Each answer is about half a megabyte, so at max only 2 connections × 16
# streams are busy: with the default 1,536, etcd v3.7.2 lost its leader.
concurrency=()
if [ "$workload" = mixed ]; then
	keys=1000000
	concurrency=(-conns 6 -streams 16)
	if [ -n "${MIX+x}" ]; then concurrency+=(-mix "$MIX"); fi
	# ~1.47 GB values per replica before log/index overhead. Three nodes
	# run sequentially by target, never concurrently across targets. Keep
	# growing-log churn short on the 16 GB runner, without resizing values.
	export BENCH_GOMEMLIMIT=3GiB
fi
if [ "$workload" = list ]; then
	if [ "$size" = 1024 ]; then keys=100000; else keys=10000; fi
	concurrency=(-conns 2 -streams 16)
fi
if [ "$workload" = delete ]; then
	if [ "$size" = 1024 ]; then
		keys=600000 warmup=1s duration=4s
	else
		keys=300000 warmup=1s duration=2s
	fi
fi

kind() {
	if [ "$1" = etcd ]; then echo etcd; else echo tigerstore; fi
}

# order <rep> prints the targets in the order of repetition <rep>.
order() {
	if (($1 % 2 == 1)); then
		printf '%s\n' "${targets[@]}"
	else
		printf '%s\n' "${targets[@]}" | tac
	fi
}

# once <target> <mode> <rep> <loadgen flags...>
# Mode tmpfs or alone places the nodes as LAYOUTS says.
once() {
	local target=$1 mode=$2 rep=$3
	shift 3
	local dir=$data/$target place=()
	case $mode in
	tmpfs) place=(TMPFS_NODES="1 2 3") ;;
	alone) place=(TMPFS_NODES="2 3" LEAD=1) ;;
	esac
	"$here/cluster.sh" stop "$dir"
	env TMPFS_DIR="$tmpfs/$target" "${place[@]}" "$here/cluster.sh" start "$(kind "$target")" "$bins/$target" "$dir" "${layout[@]:1}"
	cp "$dir/placement.json" "$out/$bench/$target-$mode-$rep.placement"
	local status=0 endpoint_file=leader
	if [ "$workload" = mixed ]; then endpoint_file=endpoints; fi
	taskset -c "${layout[0]}" "$loadgen" \
		-endpoints "$(cat "$dir/$endpoint_file")" \
		-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
		-pids "$(cat "$dir/pids")" \
		-label "$target" \
		-workload "$workload" \
		-value-size "$size" \
		-watchers 3 \
		-warmup "$warmup" \
		-duration "$duration" \
		-seed "$rep" \
		${DISK:+-disk "$DISK"} \
		-out "$out/$bench/$target-$mode-$rep.json" \
		"$@" || status=$?
	if [ "$status" -eq 0 ]; then
		# The generator drove the leader on its CPUs only if the leader
		# did not change during the run.
		"$here/cluster.sh" check "$dir" || status=$?
	fi
	# Keep logs and a post-run memory/disk sample outside the CPU window.
	for node in 1 2 3; do
		cp "$dir/n$node.log" "$out/$bench/$target-$mode-$rep-n$node.log" || status=1
	done
	{
		echo "target=$target mode=$mode rep=$rep"
		ps -p "$(cat "$dir/pids")" -o pid,comm,rss,vsz
		free -b
		du -sb "$dir"
	} >"$out/$bench/$target-$mode-$rep.resources" || status=1
	"$here/cluster.sh" stop "$dir"
	return "$status"
}

if [ -e "$out/$bench" ]; then
	echo "$out/$bench exists; results of different runs must not mix" >&2
	exit 2
fi
mkdir -p "$out/$bench"
# A run that fails stops the benchmark: its result is invalid, and a
# comparison without it would be wrong.
if [[ " ${modes[*]} " == *" max "* ]]; then
	for rep in $(seq "$reps"); do
		for target in $(order "$rep"); do
			once "$target" max "$rep" -keys "$keys" "${concurrency[@]}"
		done
	done
fi

for mode in "${extra_layouts[@]}"; do
	for rep in $(seq "$reps"); do
		for target in $(order "$rep"); do
			if [ "$(kind "$target")" = tigerstore ]; then
				once "$target" "$mode" "$rep" -keys "$keys" "${concurrency[@]}"
			fi
		done
	done
done

if [ "$workload" = mixed ] && [[ " ${modes[*]} " == *" rate "* ]] && [[ " ${modes[*]} " != *" max "* ]]; then
	# One measured max point per store, with the same mixed concurrency.
	for target in $(order 1); do
		once "$target" max 1 -keys "$keys" "${concurrency[@]}"
	done
fi

if [[ " ${modes[*]} " == *" rate "* ]]; then
	# Half of etcd's median max throughput, or of the first target's
	# without etcd, so every target gets the same open-loop load.
	ref=etcd
	if [[ " ${targets[*]} " != *" etcd "* ]]; then
		ref=${targets[0]}
	fi
	if [[ " ${modes[*]} " == *" max "* ]] || [ "$workload" = mixed ]; then
		rate=$(jq -s 'map(.throughput) | sort | .[length / 2 | floor] / 2 | floor' "$out/$bench/$ref"-max-*.json)
	else
		echo "rate-only needs max results" >&2
		exit 2
	fi
	if [ "$workload" = mixed ] && [ -n "${MIXED_RATE+x}" ]; then rate=$MIXED_RATE; fi
	if [ "$workload" = delete ]; then
		keys=$(awk -v r="$rate" -v w="${warmup%s}" -v d="${duration%s}" 'BEGIN { print int(r * (w + d) * 1.2) + 10000 }')
	fi
	for rep in $(seq "$reps"); do
		for target in $(order "$rep"); do
			once "$target" rate "$rep" -keys "$keys" -rate "$rate" "${concurrency[@]}"
		done
	done
fi

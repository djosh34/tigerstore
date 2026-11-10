#!/usr/bin/env bash
# Runs one benchmark against each target store, back to back on this
# machine, and writes one JSON result per run.
#
#   bench/run.sh <benchmark> <out dir>
#
# <benchmark> is <workload>-<size>: workload put, txn, delete, range, list
# or watch, size 1k or 10k; or k8s, the replay of a recorded Kubernetes
# control plane (bench/recordings, loadgen's k8s workload). Each run starts
# a fresh 3-node cluster, so no run sees another's data. Each seed of SEEDS
# runs REPS times per target and mode; for k8s the seed draws the cluster
# shape and where each copy of it begins, so repeats of a seed measure the
# same workload. Three kinds of run:
#
#   max        every stream busy, from 6 connections x 256 streams, which
#              double while throughput climbs by 5% or more (loadgen
#              -ramp), but for delete and watch, which would run out of
#              keys or only follow the puts: throughput
#   rate       open-loop Poisson arrivals at half of etcd's median max
#              throughput: latency at the same load for every target
#   generator  one max run against cmd/nullstore, which answers every
#              call at once, on the nodes' CPUs: the generator's own
#              ceiling, written to <out dir>/generator-<benchmark>.json;
#              it runs alone, before any other mode
#
# Each repetition runs every target once, in TARGETS order on odd
# repetitions and reversed on even ones, so drift on the runner does not
# favour one target. k8s drives all three endpoints; others use the leader.
# k8s runs max only: 64 copies of the recorded cluster on 8 connections,
# its preload at 300 Txns/s. A run is <target>-<mode>-<seed>.<rep>.
#
# A run that fails writes its cause to <target>-<mode>-<rep>.failed, and
# the other runs still run: one store's failure must not hide the others'
# results. cmd/benchreport decides what a failure means. Where etcd has no
# max result, the rate is RATE_FALLBACK; without it no rate runs run, and
# <out dir>/<benchmark>/norate says why. The rate used goes to
# <out dir>/<benchmark>/rate.
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
#   SEEDS       space-separated seeds (default 1)
#   REPS        repetitions of each seed (default 3)
#   TRACES      k8s's recordings (default bench/recordings)
#   PROFILE_GENERATOR  set to anything: every run writes loadgen's CPU
#               profile of its measured window to <run>.loadgen.pprof
#   LOADGEN     loadgen binary (default: loadgen in BINS)
#   NULLSTORE   nullstore binary (default: nullstore in BINS)
#   DATA        directory for cluster data, on the disk under test (default /tmp/bench-data)
#   DISK        device or partition of DATA in /proc/diskstats, such as
#               sda1; each result then has the bytes it wrote per operation
#   LAYOUTS     extra max runs of every tigerstore target, REPS each:
#               tmpfs puts all three nodes on TMPFS_DIR (no disk: the CPU
#               reference), alone puts the followers there and keeps the
#               leader on the disk; written as <target>-<layout>-<rep>.json
#   TMPFS_DIR   a directory on tmpfs for LAYOUTS (default /dev/shm/bench-data)
#   RATE_FALLBACK  the rate where etcd has no max result (bench.yml passes
#               the first RTT's)
set -euo pipefail

bench=$1
out=$2
read -r -a targets <<<"${TARGETS:-etcd}"
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
read -r -a modes <<<"${MODES:-max rate}"
read -r -a seeds <<<"${SEEDS:-1}"
reps=${REPS:-3}
traces=$(realpath "${TRACES:-$(dirname "$0")/recordings}")
data=${DATA:-/tmp/bench-data}
tmpfs=${TMPFS_DIR:-/dev/shm/bench-data}
read -r -a extra_layouts <<<"${LAYOUTS:-}"
bins=$(realpath "${BINS:-bin}")
loadgen=${LOADGEN:-$bins/loadgen}
nullstore=${NULLSTORE:-$bins/nullstore}
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
export CERTS=$certs ETCDCTL=$bins/etcdctl
here=$(dirname "$0")

workload=${bench%-*}
case ${bench##*-} in
1k) size=1024 ;;
10k) size=10240 ;;
k8s)
	workload=k8s
	size=1024 # ignored: values have the recorded sizes
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
# that runs out is invalid: 600,000 keys over 0.5 + 3.5 s last up to 150k
# deletes/s. The 10 KiB run preloads fewer keys, to keep the disk it fills
# small, so it is shorter still: 300,000 keys over 0.5 + 1.5 s.
keys=10000 warmup=3s duration=10s watchers=3 ramp=(-ramp)
# A LIST asks for the first page of one namespace of 100: 1,000 keys
# each, pages of 500 (the loadgen default), so every answer holds a count
# of 1,000, 500 keys and "more". It does not page further. With 10 KiB
# values, 100 keys each keep the preload small.
# Each answer is about half a megabyte, so at max only 2 connections × 16
# streams are busy: with the default 1,536, etcd v3.7.2 lost its leader.
concurrency=()
# k8s's reads are linearizable and go to all three nodes, so at 10 ms RTT
# a call waits about 17 ms before any work, and a generator with N calls in
# flight does at most N / 17 ms. Its max runs start at 8 x 32 = 256 in
# flight and double from there; calls that wait on replies hold fewer.
if [ "$workload" = k8s ]; then
	# Writes to loopback cost the generator more than anything else, so
	# at max it holds requests back up to 200 µs to send many in a write.
	concurrency=(-conns 8 -streams 32 -copies 64 -traces "$traces" -coalesce "${COALESCE:-200us}")
	if [[ " ${modes[*]} " != *" generator "* ]]; then modes=(max); fi
	# Up to about 1 GB of values per replica, as many as the 64 copies'
	# starts hold, on the 16 GB runner.
	export BENCH_GOMEMLIMIT=3GiB
fi
# Three watchers of 10 KiB events take about 300 MB/s at max, more than
# the generator's one CPU reads: its streams fall behind, and tigerstore
# closes a stream 64 MiB behind. One watcher of 10 KiB takes about as many
# bytes as three of 1 KiB.
if [ "$workload" = watch ] && [ "$size" = 10240 ]; then watchers=1; fi
if [ "$workload" = watch ] || [ "$workload" = delete ]; then ramp=(); fi
if [ "$workload" = list ]; then
	if [ "$size" = 1024 ]; then keys=100000; else keys=10000; fi
	concurrency=(-conns 2 -streams 16)
fi
if [ "$workload" = delete ]; then
	if [ "$size" = 1024 ]; then
		keys=600000 warmup=0.5s duration=3.5s
	else
		keys=300000 warmup=0.5s duration=1.5s
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

# step <run> <name> <command...> runs one step of a run. If it fails, the
# step and its last line of errors go to <run>.failed as the cause.
step() {
	local run=$1 name=$2 status=0
	shift 2
	"$@" 2>"$run.stderr" || status=$?
	cat "$run.stderr" >&2
	if [ "$status" -ne 0 ]; then
		echo "$name exited $status: $(grep -v '^$' "$run.stderr" | tail -n 1)" >"$run.failed"
	fi
	rm "$run.stderr"
	return "$status"
}

# once <target> <mode> <seed> <rep> <loadgen flags...>
# Mode tmpfs or alone places the nodes as LAYOUTS says.
once() {
	local target=$1 mode=$2 seed=$3 rep=$4
	shift 4
	local dir=$data/$target place=() run=$out/$bench/$target-$mode-$seed.$rep
	case $mode in
	tmpfs) place=(TMPFS_NODES="1 2 3") ;;
	alone) place=(TMPFS_NODES="2 3" LEAD=1) ;;
	esac
	"$here/cluster.sh" stop "$dir"
	local endpoint_file=leader
	if [ "$workload" = k8s ]; then endpoint_file=endpoints; fi
	if step "$run" "cluster start" env TMPFS_DIR="$tmpfs/$target" "${place[@]}" "$here/cluster.sh" start "$(kind "$target")" "$bins/$target" "$dir" "${layout[@]:1}"; then
		cp "$dir/placement.json" "$run.placement"
		# The generator drove the leader on its CPUs only if the leader
		# did not change during the run.
		if step "$run" loadgen taskset -c "${layout[0]}" "$loadgen" \
			-endpoints "$(cat "$dir/$endpoint_file")" \
			-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
			-pids "$(cat "$dir/pids")" \
			-label "$target" \
			-workload "$workload" \
			-value-size "$size" \
			-watchers "$watchers" \
			-warmup "$warmup" \
			-duration "$duration" \
			-seed "$seed" \
			${DISK:+-disk "$DISK"} \
			-out "$run.json" \
			${PROFILE_GENERATOR:+-cpuprofile "$run.loadgen.pprof"} \
			"$@"; then
			step "$run" "leader check" "$here/cluster.sh" check "$dir" || true
		fi
	fi
	# Keep logs and a post-run memory/disk sample outside the CPU window.
	# After a failure some nodes may be gone.
	for node in 1 2 3; do
		if [ -f "$dir/n$node.log" ]; then cp "$dir/n$node.log" "$run-n$node.log"; fi
	done
	{
		echo "target=$target mode=$mode seed=$seed rep=$rep"
		ps -p "$(cat "$dir/pids")" -o pid,comm,rss,vsz || true
		free -b
		du -sb "$dir"
	} >"$run.resources"
	"$here/cluster.sh" stop "$dir"
}

# generator runs loadgen against nullstore on the nodes' CPUs, as the max
# runs drive a cluster. k8s has none: nullstore does not answer all of a
# recording's calls, and cmd/benchreport puts loadgen's CPU per op in the
# k8s runs against the target rate instead.
generator() {
	local ports=() pid status=0 keys=$keys
	for i in 1 2 3; do ports+=("127.0.0.1:$((${BENCH_PORT:-20000} + 10 * i))"); done
	# A LIST answers a page of 500 of the 1,000 keys of a namespace, as
	# list does.
	local values=(-value-size "$size" -list-keys 1000)
	# nullstore deletes faster than the stores' budget of keys lasts.
	if [ "$workload" = delete ]; then keys=$((keys * 10)); fi
	taskset -c "$(IFS=,; echo "${layout[*]:1}")" "$nullstore" -listen "$(IFS=,; echo "${ports[*]}")" \
		-cert "$certs/node1.pem" -key "$certs/node1-key.pem" "${values[@]}" &
	pid=$!
	local up=""
	for _ in $(seq 100); do
		if (exec 3<>"/dev/tcp/127.0.0.1/${ports[2]##*:}") 2>/dev/null; then
			up=yes
			break
		fi
		sleep 0.1
	done
	if [ -z "$up" ]; then
		echo "nullstore did not listen within 10s" >&2
		kill "$pid" || true
		return 1
	fi
	taskset -c "${layout[0]}" "$loadgen" \
		-endpoints "${ports[0]}" \
		-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
		-pids "$pid" -label nullstore -workload "$workload" -value-size "$size" \
		-watchers "$watchers" -warmup "$warmup" -duration "$duration" -seed 1 \
		-out "$out/generator-$bench.json" -keys "$keys" "${concurrency[@]}" "${ramp[@]}" || status=$?
	kill "$pid"
	wait "$pid" || true
	return "$status"
}

if [[ " ${modes[*]} " == *" generator "* ]]; then
	mkdir -p "$out"
	if [ "$workload" != k8s ]; then generator; fi
	exit 0
fi

# runs <mode> <loadgen flags...> runs mode for every seed, REPS times
# each, every target once a repetition, the order turning each time.
# A layout mode runs tigerstore targets only.
runs() {
	local mode=$1 n=0
	shift
	for seed in "${seeds[@]}"; do
		for rep in $(seq "$reps"); do
			n=$((n + 1))
			for target in $(order "$n"); do
				if [ "$mode" = max ] || [ "$mode" = rate ] || [ "$(kind "$target")" = tigerstore ]; then
					once "$target" "$mode" "$seed" "$rep" "$@"
				fi
			done
		done
	done
}

if [ -e "$out/$bench" ]; then
	echo "$out/$bench exists; results of different runs must not mix" >&2
	exit 2
fi
mkdir -p "$out/$bench"
if [[ " ${modes[*]} " == *" max "* ]]; then
	runs max -keys "$keys" "${concurrency[@]}" "${ramp[@]}"
fi

for mode in "${extra_layouts[@]}"; do
	runs "$mode" -keys "$keys" "${concurrency[@]}"
done

if [[ " ${modes[*]} " == *" rate "* ]]; then
	# Half of etcd's median max throughput, or of the first target's
	# without etcd, so every target gets the same open-loop load.
	ref=etcd
	if [[ " ${targets[*]} " != *" etcd "* ]]; then
		ref=${targets[0]}
	fi
	maxes=()
	for f in "$out/$bench/$ref"-max-*.json; do
		if [ -f "$f" ] && [ ! -f "${f%.json}.failed" ]; then maxes+=("$f"); fi
	done
	if [ "${#maxes[@]}" -gt 0 ]; then
		rate=$(jq -s 'map(.throughput) | sort | (if length % 2 == 0 then (.[length / 2 - 1] + .[length / 2]) / 2 else .[length / 2 | floor] end) / 2 | floor' "${maxes[@]}")
	elif [ -n "${RATE_FALLBACK:-}" ]; then
		echo "$ref has no max result: the rate is RATE_FALLBACK, $RATE_FALLBACK" >&2
		rate=$RATE_FALLBACK
	else
		echo "$ref has no max result here or at the first RTT" | tee "$out/$bench/norate" >&2
		exit 0
	fi
	echo "$rate" >"$out/$bench/rate"
	if [ "$workload" = delete ]; then
		keys=$(awk -v r="$rate" -v w="${warmup%s}" -v d="${duration%s}" 'BEGIN { print int(r * (w + d) * 1.2) + 10000 }')
	fi
	runs rate -keys "$keys" -rate "$rate" "${concurrency[@]}"
fi

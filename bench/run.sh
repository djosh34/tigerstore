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
# Each repetition runs every target once, in TARGETS order on odd
# repetitions and reversed on even ones, so drift on the runner does not
# favour one target. The generator drives the leader only.
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
#   PROFILE     if 1, after the measured runs, one more max run per target
#               records the leader with perf over the measurement window
#               only, into <out dir>/profiles: the perf data and a report
#               of the hottest functions. These runs are not measured.
set -euo pipefail

bench=$1
out=$2
read -r -a targets <<<"${TARGETS:-etcd}"
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
read -r -a modes <<<"${MODES:-max rate}"
reps=${REPS:-5}
data=${DATA:-/tmp/bench-data}
bins=$(realpath "${BINS:-bin}")
loadgen=${LOADGEN:-$bins/loadgen}
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
export CERTS=$certs ETCDCTL=$bins/etcdctl
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
	echo "LAYOUT needs 4 CPU lists: generator, leader, follower, follower" >&2
	exit 2
fi

# Delete removes each key once, so it needs a key per request, and a run
# that runs out is invalid: 600,000 keys over 1 + 4 s last up to 120k
# deletes/s. The 10 KiB run preloads fewer keys, to keep the disk it fills
# small, so it is shorter still: 300,000 keys over 1 + 2 s.
keys=10000 warmup=3s duration=10s
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
# With profile set to a path prefix, the run is profiled instead of
# measured: its result goes to <profile>.loadgen, not with the others.
once() {
	local target=$1 mode=$2 rep=$3
	shift 3
	local dir=$data/$target
	local result=$out/$bench/$target-$mode-$rep.json perf=() perfpid=
	"$here/cluster.sh" stop "$dir"
	"$here/cluster.sh" start "$(kind "$target")" "$bins/$target" "$dir" "${layout[@]:1}"
	if [ -n "$profile" ]; then
		result=$profile.loadgen
		rm -f "$dir.ctl"
		mkfifo "$dir.ctl"
		# Off until the generator turns it on for the measurement window.
		taskset -c "${layout[0]}" perf record -D -1 --control "fifo:$dir.ctl" -F 999 -g \
			-p "$(cut -d, -f1 "$dir/pids")" -o "$profile.data" 2>"$profile.log" &
		perfpid=$!
		perf=(-perf-ctl "$dir.ctl")
	else
		cp "$dir/placement.json" "$out/$bench/$target-$mode-$rep.placement"
	fi
	local status=0
	taskset -c "${layout[0]}" "$loadgen" \
		-endpoints "$(cat "$dir/leader")" \
		-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
		-pids "$(cat "$dir/pids")" \
		-label "$target" \
		-workload "$workload" \
		-value-size "$size" \
		-watchers 3 \
		-warmup "$warmup" \
		-duration "$duration" \
		-seed "$rep" \
		-out "$result" \
		"${perf[@]}" \
		"$@" || status=$?
	if [ -n "$perfpid" ]; then
		# perf exits with 130 after SIGINT once it wrote its data.
		kill -INT "$perfpid"
		wait "$perfpid" || true
		grep -q '^\[ perf record: Captured and wrote' "$profile.log" || status=1
		rm -f "$dir.ctl"
	fi
	if [ "$status" -eq 0 ]; then
		# The generator drove the leader on its CPUs only if the leader
		# did not change during the run.
		"$here/cluster.sh" check "$dir" || status=$?
	fi
	"$here/cluster.sh" stop "$dir"
	return "$status"
}

if [ -e "$out/$bench" ]; then
	echo "$out/$bench exists; results of different runs must not mix" >&2
	exit 2
fi
mkdir -p "$out/$bench"
profile=
# A run that fails stops the benchmark: its result is invalid, and a
# comparison without it would be wrong.
if [[ " ${modes[*]} " == *" max "* ]]; then
	for rep in $(seq "$reps"); do
		for target in $(order "$rep"); do
			once "$target" max "$rep" -keys "$keys"
		done
	done
fi

if [[ " ${modes[*]} " == *" rate "* ]]; then
	# Half of etcd's median max throughput, or of the first target's
	# without etcd, so every target gets the same open-loop load.
	ref=etcd
	if [[ " ${targets[*]} " != *" etcd "* ]]; then
		ref=${targets[0]}
	fi
	rate=$(jq -s 'map(.throughput) | sort | .[length / 2 | floor] / 2 | floor' "$out/$bench/$ref"-max-*.json)
	if [ "$workload" = delete ]; then
		keys=$(awk -v r="$rate" -v w="${warmup%s}" -v d="${duration%s}" 'BEGIN { print int(r * (w + d) * 1.2) + 10000 }')
	fi
	for rep in $(seq "$reps"); do
		for target in $(order "$rep"); do
			once "$target" rate "$rep" -keys "$keys" -rate "$rate"
		done
	done
fi

if [ "${PROFILE:-}" = 1 ]; then
	mkdir -p "$out/profiles"
	for target in "${targets[@]}"; do
		profile=$out/profiles/$bench-$target
		once "$target" max 1 -keys "$keys"
		perf report -i "$profile.data" --stdio --no-children --sort symbol -g none --percent-limit 0.3 >"$profile.txt" 2>/dev/null
	done
fi

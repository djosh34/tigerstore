#!/usr/bin/env bash
# Takes CPU profiles of all three nodes while a workload runs at max on the
# disk, as the bench runs it: back to back 5 s profiles of every node until
# the generator ends, a 2 s execution trace of each node early on, and one
# heap profile of each node at the end.
#
#   bench/profile-disk.sh <tigerstore binary> <benchmark> <out dir>
#
# <benchmark> is put-1k, txn-1k or mixed-k8s. Environment as in
# bench/run.sh: BINS, CERTS, LAYOUT, DATA, DISK.
set -euo pipefail
bin=$(realpath "$1")
bench=$2
out=$3
bins=$(realpath "${BINS:-bin}")
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
export CERTS=$certs ETCDCTL=$bins/etcdctl BENCH_FLAGS=--enable-pprof
here=$(dirname "$0")
dir=${DATA:-/tmp/bench-data}/profile
mkdir -p "$out"
wl=${bench%-*}
flags=(-keys 10000 -conns 6 -streams 512 -value-size 1024 -warmup 3s -duration 30s)
endpoint_file=leader
if [ "$wl" = mixed ]; then
	flags=(-keys 1000000 -conns 6 -streams 96 -warmup 3s -duration 40s)
	endpoint_file=endpoints
	export BENCH_GOMEMLIMIT=3GiB
fi
trap 'cp "$dir"/n*.log "$out/" 2>/dev/null; "$here/cluster.sh" stop "$dir"' EXIT
"$here/cluster.sh" stop "$dir"
"$here/cluster.sh" start tigerstore "$bin" "$dir" "${layout[@]:1}"
read -r -a eps <<<"$(tr ',' ' ' <"$dir/endpoints")"
leader=$(cat "$dir/leader")
taskset -c "${layout[0]}" "$bins/loadgen" -endpoints "$(cat "$dir/$endpoint_file")" \
	-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
	-pids "$(cat "$dir/pids")" -workload "$wl" ${DISK:+-disk "$DISK"} \
	-out "$out/$bench.json" "${flags[@]}" &
gen=$!
get() {
	curl -fsS --cacert "$certs/ca.pem" --cert "$certs/client.pem" --key "$certs/client-key.pem" "https://$1/debug/pprof/$2" -o "$3" || true
}
snap() {
	for ep in "${eps[@]}"; do
		get "$ep" ../../metrics "$out/$bench-metrics-$1-${ep##*:}.txt"
	done
	if [ -n "${DISK:-}" ]; then { date +%s.%N; grep " $DISK " /proc/diskstats; } >"$out/$bench-diskstats-$1.txt"; fi
}
i=0
while kill -0 "$gen" 2>/dev/null; do
	if [ "$i" = 2 ]; then snap start; fi
	if [ "$i" = 4 ]; then
		# An execution trace of every node: what each goroutine waits on.
		for ep in "${eps[@]}"; do
			get "$ep" "trace?seconds=2" "$out/$bench-trace-${ep##*:}.out" &
		done
		wait "${!}"
		sleep 2.5
	fi
	i=$((i + 1))
	curls=()
	for ep in "${eps[@]}"; do
		role=f
		if [ "$ep" = "$leader" ]; then role=l; fi
		get "$ep" "profile?seconds=5" "$out/$bench-$(printf %03d "$i")-$role-${ep##*:}.pprof" &
		curls+=($!)
	done
	wait "${curls[@]}"
	snap last
done
for ep in "${eps[@]}"; do
	get "$ep" heap "$out/$bench-heap-${ep##*:}.pprof"
done
wait "$gen"

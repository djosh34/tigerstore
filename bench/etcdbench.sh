#!/usr/bin/env bash
# Runs etcd's own benchmark tool (tools/benchmark of etcd v3.7.2) against
# each target store, on the clusters bench/cluster.sh starts, as a second
# view next to cmd/loadgen. One fresh cluster per target, test and
# repetition; the tool runs on the generator's CPU.
#
#   bench/etcdbench.sh <benchmark binary> <out dir>
#
# Environment as in bench/run.sh: TARGETS, BINS, CERTS, LAYOUT, REPS, DATA.
set -euo pipefail

tool=$(realpath "$1")
out=$2
read -r -a targets <<<"${TARGETS:-etcd}"
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
reps=${REPS:-3}
data=${DATA:-/tmp/bench-data}
bins=$(realpath "${BINS:-bin}")
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
export CERTS=$certs ETCDCTL=$bins/etcdctl
here=$(dirname "$0")

# The tests of etcd's performance documentation, with 1 KiB values.
tests=(
	"put --key-size=8 --sequential-keys --total=200000 --val-size=1024"
	"txn-put --key-size=8 --key-space-size=100000 --total=200000 --val-size=1024"
	"range k --consistency=l --total=200000"
	"range k --consistency=s --total=200000"
)

if [ -e "$out" ]; then
	echo "$out exists" >&2
	exit 2
fi
mkdir -p "$out"
for rep in $(seq "$reps"); do
	for target in "${targets[@]}"; do
		kind=tigerstore
		if [ "$target" = etcd ]; then kind=etcd; fi
		for i in "${!tests[@]}"; do
			dir=$data/$target
			"$here/cluster.sh" stop "$dir"
			"$here/cluster.sh" start "$kind" "$bins/$target" "$dir" "${layout[@]:1}"
			# The key the range tests read, with a 1 KiB value.
			"$ETCDCTL" --endpoints "$(cat "$dir/leader")" --cacert "$certs/ca.pem" --cert "$certs/client.pem" --key "$certs/client-key.pem" \
				put k "$(head -c 1024 /dev/zero | tr '\0' v)" >/dev/null
			read -r -a t <<<"${tests[i]}"
			taskset -c "${layout[0]}" "$tool" --endpoints "$(cat "$dir/leader")" \
				--cacert "$certs/ca.pem" --cert "$certs/client.pem" --key "$certs/client-key.pem" \
				--conns 100 --clients 1000 "${t[@]}" >"$out/$target-$i-$rep.txt" 2>&1
			# The tool reports failed requests in an error distribution and
			# leaves them out of its statistics; such a run is invalid.
			if grep -q 'Error distribution' "$out/$target-$i-$rep.txt" || ! grep -q 'Requests/sec' "$out/$target-$i-$rep.txt"; then
				echo "$target ${tests[i]} repetition $rep failed:" >&2
				tail -n 20 "$out/$target-$i-$rep.txt" >&2
				exit 1
			fi
			"$here/cluster.sh" check "$dir"
			"$here/cluster.sh" stop "$dir"
		done
	done
done
# One line per target and test: requests/s and p99 of every repetition.
for i in "${!tests[@]}"; do
	for target in "${targets[@]}"; do
		echo "${tests[i]%% *} $(cut -d' ' -f2- <<<"${tests[i]}") | $target | $(grep -h 'Requests/sec' "$out/$target-$i"-*.txt | awk '{printf "%.0f ", $2}') | $(grep -h ' 99% in' "$out/$target-$i"-*.txt | awk '{printf "%.1f ", $3*1000}')"
	done
done | tee "$out/summary.txt"

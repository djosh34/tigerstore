#!/usr/bin/env bash
# Long Put runs with cleaning (#146): what the disk wrote against what the
# log and the packs hold, per target. Throwaway.
#   exp146/journal.sh <out dir>
set -euo pipefail
out=$1
mkdir -p "$out"
: "${CERTS:?}" "${BINS:?}" "${DATA:?}" "${DISK:?}"
export CERTS ETCDCTL=$BINS/etcdctl
budget=${BUDGET:-3221225472}
rate=${RATE:-20000}
dur=${DUR:-600s}
scrape() {
	local dir=$1 f=$2 ep
	: >"$f"
	for ep in $(tr ',' ' ' <"$dir/endpoints"); do
		curl -s --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$ep/metrics" |
			grep -E '^(etcd_disk_wal_write_bytes_total|tigerstore_cleaned_bytes_total|tigerstore_disk_bytes) ' | sed "s|^|$ep |" >>"$f"
	done
}
sectors() { awk -v d="$DISK" '$3 == d {print $10}' /proc/diskstats; }
for rep in 1 2; do
	for target in baseline candidate; do
		name=$target-$rep dir=$DATA/$target
		BENCH_FLAGS="--disk-budget-bytes $budget" bench/cluster.sh start tigerstore "$BINS/$target" "$dir" 0 1 2
		scrape "$dir" "$out/$name.m0"
		s0=$(sectors)
		taskset -c 3 "$BINS/loadgen" -endpoints "$(cat "$dir/leader")" \
			-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
			-pids "$(cat "$dir/pids")" -workload put -value-size 1024 -keys 200000 -rate "$rate" \
			-warmup 0s -duration "$dur" -disk "$DISK" -seed "$rep" -out "$out/$name.json" || echo "$name loadgen failed" | tee -a "$out/failed"
		s1=$(sectors)
		scrape "$dir" "$out/$name.m1"
		echo "$((s1 - s0))" >"$out/$name.sectors"
		ls -l "$dir"/n*/wal >"$out/$name.files"
		for n in 1 2 3; do cp "$dir/n$n.log" "$out/$name-n$n.log"; done
		bench/cluster.sh stop "$dir"
		sync
	done
done

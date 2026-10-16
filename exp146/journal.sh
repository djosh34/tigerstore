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
rate=${RATE:-15000}
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
# jbd2 reports per journal how many transactions it committed and their
# average size in blocks; their product is the journal's blocks.
journal() { cat /proc/fs/jbd2/"$DISK"-*/info; }
for rep in 1 2; do
	for target in baseline candidate; do
		name=$target-$rep dir=$DATA/$target
		BENCH_FLAGS="--disk-budget-bytes $budget" bench/cluster.sh start tigerstore "$BINS/$target" "$dir" 0 1 2
		scrape "$dir" "$out/$name.m0"
		# Compact every 10 s to the revision of 10 s before, as
		# kube-apiserver does every 5 minutes, so cleaning has garbage.
		(
			prev=0
			while sleep 10; do
				rev=$("$ETCDCTL" --endpoints "$(cat "$dir/leader")" --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" endpoint status -w json | jq '.[0].Status.header.revision')
				if [ "$prev" -gt 0 ]; then
					"$ETCDCTL" --endpoints "$(cat "$dir/leader")" --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" compact "$prev" >/dev/null || echo "$name compact $prev failed" >>"$out/failed"
				fi
				prev=$rev
			done
		) &
		compactor=$!
		s0=$(sectors)
		journal >"$out/$name.jbd0"
		taskset -c 3 "$BINS/loadgen" -endpoints "$(cat "$dir/leader")" \
			-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
			-pids "$(cat "$dir/pids")" -workload put -value-size 1024 -keys 200000 -rate "$rate" \
			-warmup 0s -duration "$dur" -disk "$DISK" -seed "$rep" -out "$out/$name.json" || echo "$name loadgen failed" | tee -a "$out/failed"
		s1=$(sectors)
		journal >"$out/$name.jbd1"
		kill "$compactor"
		scrape "$dir" "$out/$name.m1"
		echo "$((s1 - s0))" >"$out/$name.sectors"
		ls -l "$dir"/n*/wal >"$out/$name.files"
		for n in 1 2 3; do cp "$dir/n$n.log" "$out/$name-n$n.log"; done
		bench/cluster.sh stop "$dir"
		sync
	done
done

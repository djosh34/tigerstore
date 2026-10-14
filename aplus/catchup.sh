#!/usr/bin/env bash
# Item 6 of #133: a follower down past the RAM window catches up.
#
#   aplus/catchup.sh <target> <keys> <down seconds> <out dir>
#
# Preloads keys, kills a follower, overwrites random keys at full speed for
# the down time, compacts to the current revision (kube-apiserver compacts
# every 5 minutes), then restarts the follower and times it until it has
# applied what the leader committed. Bytes sent: A+ counts snapshot bytes
# on the leader; for main the missed entries are the growth of the
# leader's log while the follower was down.
source "$(dirname "$0")/lib.sh"
set +e
target=$1 keys=$2 down=$3 out=$4
mkdir -p "$out"
data=${DATA:-/tmp/aplus-catchup}
{
	echo "## $target, keys $keys, down ${down}s"
	"$repo/bench/cluster.sh" stop "$data"
	"$repo/bench/cluster.sh" start tigerstore "$BINS/$target" "$data" 0 1 2
	L=$(cat "$data/leader")
	ln=$(node_of_endpoint "$L")
	f=$(((ln % 3) + 1))
	lg -endpoints "$L" -workload range -keys "$keys" -warmup 0s -duration 1s -out "$out/preload.json" >/dev/null 2>&1 || true
	echo "leader n$ln, follower n$f"
	echo "before: leader wal $(du -sb "$data/n$ln/wal" | cut -f1) $(grep -h aplus-stats "$data/n$ln.log" | tail -1 | grep -o 'snapsent=[0-9]* snaps=[0-9]*')"
	export DOWN_CMD="source $here/lib.sh; lg -endpoints $L -workload put -keys $keys -warmup 0s -duration ${down}s -out $out/down.json >/dev/null 2>&1 || true; du -sb $data/n$ln/wal | cut -f1 > $data/walmid; r=\$(ctl $L get /bench/ready -w json | jq .header.revision); ctl $L compact \$r >/dev/null; echo compacted at \$r"
	restart "$data" "$f"
	sleep 3
	echo "after: leader wal before compaction-free growth: mid $(cat "$data/walmid") $(grep -h aplus-stats "$data/n$ln.log" | tail -1 | grep -o 'snapsent=[0-9]* snaps=[0-9]* snapbuilt=[0-9]*')"
	jq -c '{throughput, ops}' "$out/down.json" || true
	grep -h 'aplus-snap\|aplus-install\|aplus-open' "$data"/n*.log || true
	stats "$data"
	for i in 1 2 3; do mem "$data" "$i"; done
	cp "$data"/n*.log "$out/"
	"$repo/bench/cluster.sh" stop "$data"
} 2>&1 | tee "$out/catchup.txt"

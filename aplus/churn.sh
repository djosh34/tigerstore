#!/usr/bin/env bash
# Item 7 of #133: cleaning write amplification under churn.
#
#   aplus/churn.sh <target> <keys> <rate, 0 for max> <seconds> <compact every s> <keep revs> <out dir>
#
# Preloads keys, then overwrites random keys for the given time while a
# compactor keeps the newest <keep revs> revisions. Samples disk use and
# the A+ counters every 10 s.
source "$(dirname "$0")/lib.sh"
set +e
target=$1 keys=$2 rate=$3 secs=$4 every=$5 keep=$6 out=$7
mkdir -p "$out"
data=${DATA:-/tmp/aplus-churn}
{
	echo "## $target, keys $keys, rate $rate, ${secs}s, compact every ${every}s keeping $keep revisions"
	"$repo/bench/cluster.sh" stop "$data"
	BENCH_GOMEMLIMIT=${GML:-off} "$repo/bench/cluster.sh" start tigerstore "$BINS/$target" "$data" 0 1 2
	L=$(cat "$data/leader")
	lg -endpoints "$L" -workload range -keys "$keys" -warmup 0s -duration 1s -out "$out/preload.json" >/dev/null 2>&1 || true
	compactor "$L" "$every" "$keep" &
	cpid=$!
	(
		while sleep 10; do
			echo "sample t=$(date +%s) du=$(du -sb "$data/n1" | cut -f1),$(du -sb "$data/n2" | cut -f1),$(du -sb "$data/n3" | cut -f1) rss=$(ps -o rss= -p "$(cat "$data/pids")" | tr '\n' ',')"
			stats "$data" | head -1
		done
	) &
	spid=$!
	lg -endpoints "$L" -pids "$(cat "$data/pids")" -workload put -keys "$keys" -rate "$rate" -warmup 0s -duration "${secs}s" -conns 4 -streams 64 -out "$out/churn.json" 2>&1 | tail -2 || true
	kill "$cpid" "$spid" 2>/dev/null || true
	sleep 3
	jq -c '{valid, throughput, ops, latency_us, errors}' "$out/churn.json" || true
	echo "final revision $(ctl "$L" get /bench/ready -w json | jq .header.revision)"
	for i in 1 2 3; do mem "$data" "$i"; done
	stats "$data"
	io "$data"
	ls -la "$data/n1/wal" | head -40
	cp "$data"/n*.log "$out/"
	"$repo/bench/cluster.sh" stop "$data"
} 2>&1 | tee "$out/churn.txt"

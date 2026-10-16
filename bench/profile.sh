#!/usr/bin/env bash
# Measures what each step of a request costs on the leader's one CPU: runs
# a workload at max with all three nodes on tmpfs (no disk wait), takes a
# CPU profile of the leader in the middle of the window, and splits the
# leader's CPU per operation by step.
#
#   bench/profile.sh <tigerstore binary> <workload> <out dir>
#
# Environment as in bench/run.sh: BINS (loadgen, etcdctl), CERTS, LAYOUT.
# Workloads: put, txn, range, list (1 KiB values).
set -euo pipefail
bin=$(realpath "$1")
wl=$2
out=$3
bins=$(realpath "${BINS:-bin}")
certs=$(realpath "${CERTS:?CERTS must name a dev-certs.sh directory}")
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
export CERTS=$certs ETCDCTL=$bins/etcdctl BENCH_FLAGS=--enable-pprof TMPFS_NODES="1 2 3" TMPFS_DIR=${TMPFS_DIR:-/dev/shm/bench-profile}
here=$(dirname "$0")
dir=$(mktemp -d)/c
mkdir -p "$out"
flags=(-keys 10000)
if [ "$wl" = list ]; then flags=(-keys 100000 -conns 2 -streams 16); fi
trap 'cp "$dir"/n*.log "$out/" 2>/dev/null; "$here/cluster.sh" stop "$dir"' EXIT
"$here/cluster.sh" start tigerstore "$bin" "$dir" "${layout[@]:1}"
leader=$(cat "$dir/leader")
taskset -c "${layout[0]}" "$bins/loadgen" -endpoints "$leader" \
	-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
	-pids "$(cat "$dir/pids")" -workload "$wl" -value-size 1024 -warmup 2s -duration 10s \
	-out "$out/$wl.json" "${flags[@]}" &
gen=$!
sleep 4
curl -fsS --cacert "$certs/ca.pem" --cert "$certs/client.pem" --key "$certs/client-key.pem" \
	"https://$leader/debug/pprof/profile?seconds=8" -o "$out/$wl.pprof"
wait "$gen"

# Steps by the goroutine they run on, then by what they call. Each share
# is of the leader's sampled CPU, and the cost is that share of the
# leader's CPU per operation that loadgen measured.
us=$(jq '.server_cpu_us_per_op_by_node[0]' "$out/$wl.json")
# With every node shown, "accounting for" is all the samples that pass the
# filters.
sampled() {
	go tool pprof -top -nodecount=1000000 -nodefraction=0 -unit=ms "$@" "$bin" "$out/$wl.pprof" 2>/dev/null |
		sed -n 's/^Showing nodes accounting for \([0-9.]*\)\(ms\)\{0,1\},.*/\1/p'
}
total=$(sampled)
counted=0
step() {
	local name=$1 focus=$2 ignore=${3:-^$} s
	s=$(sampled -focus "$focus" -ignore "$ignore")
	counted=$(awk -v a="$counted" -v b="${s:-0}" 'BEGIN {print a + b}')
	row "$name" "${s:-0}"
}
row() {
	awk -v n="$1" -v s="$2" -v t="$total" -v u="$us" 'BEGIN {printf "| %s | %.1f%% | %.2f |\n", n, 100 * s / t, u * s / t}'
}
{
	echo "$wl: $(jq '.throughput | floor' "$out/$wl.json") ops/s, leader $us us/op, $total ms sampled"
	echo "| Step | Share | us/op |"
	echo "|---|---:|---:|"
	step "client TLS read and decrypt" 'server.\(\*conn\).readLoop' 'onFrame|readFrame'
	step "HTTP/2 frames and HPACK" 'server.\(\*conn\).onFrame|server.\(\*conn\).readFrame' 'admit|flush'
	step "request decode and batch" 'server.\(\*conn\).(admit|flush)'
	step "committer: Raft" 'node.\(\*Node\).commit' 'core.\(\*Node\).(apply|runApply|serveRead|SubmitAll|handle)|encodeBatch|Complete|completeAll'
	step "committer: checks and queue" 'core.\(\*Node\).SubmitAll|node.\(\*Node\).handle'
	step "committer: entry encode" 'core.encodeBatch'
	step "committer: apply and reads" 'core.\(\*Node\).(runApply|serveRead)'
	step "committer: completions" 'node.completeAll|server.\(\*conn\).Complete'
	step "log write and sync" 'node.\(\*Node\).(persist|syncGroups)'
	step "peer send" 'peer.\(\*sender\)'
	step "peer receive" 'peer.\(\*TCP\).*receive|peer.*readLoop|peer.\(\*receiver\)'
	step "reply encode" 'server.\(\*conn\).complete(Waiting)?$|server.\(\*conn\).complete\b' 'tls'
	step "reply TLS encrypt and write" 'server.\(\*conn\).writeLoop' 'completeWaiting'
	step "garbage collection" 'runtime.gcBgMarkWorker|runtime.bgsweep|runtime.bgscavenge'
	row "everything else" "$(awk -v t="$total" -v c="$counted" 'BEGIN {print t - c}')"
} | tee "$out/$wl.steps.md"

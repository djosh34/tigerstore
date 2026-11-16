#!/usr/bin/env bash
# Measure only (measure/602-perf, never merged): a perf profile of the
# leader of a k8s max run, as the bench runs it with no netem, from when
# the leader answers 10,000 replies/s for 30 s.
#
#   bench/perf-leader.sh <tigerstore binary> <label> <out dir>
set -euo pipefail
bin=$(realpath "$1")
label=$2
out=$3
bins=$(realpath "${BINS:-bin}")
certs=$(realpath "${CERTS:?}")
read -r -a layout <<<"${LAYOUT:-3 0 1 2}"
export CERTS=$certs ETCDCTL=$bins/etcdctl BENCH_GOMEMLIMIT=3GiB
here=$(dirname "$0")
dir=${DATA:-/tmp/bench-data}/perf
mkdir -p "$out"
trap 'for f in "$dir"/n*.log; do cp "$f" "$out/$label-$(basename "$f")"; done 2>/dev/null; "$here/cluster.sh" stop "$dir"' EXIT
"$here/cluster.sh" stop "$dir"
"$here/cluster.sh" start tigerstore "$bin" "$dir" "${layout[@]:1}"
pid=$(cut -d, -f1 <"$dir/pids")
leader=$(cat "$dir/leader")
n=$(grep -n "$leader" <(tr ',' '\n' <"$dir/endpoints") | cut -d: -f1 || true)
taskset -c "${layout[0]}" "$bins/loadgen" -endpoints "$(cat "$dir/endpoints")" \
	-cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
	-pids "$(cat "$dir/pids")" -workload k8s \
	-out "$out/$label.json" -conns 3 -streams 128 -copies 64 -traces "$(realpath "${TRACES:-$here/recordings}")" -seed "${SEED:-1}" -warmup 3s -duration 40s &
gen=$!
for _ in $(seq 600); do
	if grep -hE "replies/s [0-9]{5,}" "$dir"/n*.log >/dev/null 2>&1; then break; fi
	sleep 1
done
sudo perf record -F 1999 -g -p "$pid" -o "$out/$label.perf.data" -- sleep 30 2>&1 | tail -2
wait "$gen"
sudo chmod a+r "$out/$label.perf.data"
sudo perf report -i "$out/$label.perf.data" --stdio --no-children --sort symbol --percent-limit 0.2 2>/dev/null | grep -v "^$" | grep -E "^ +[0-9]" >"$out/$label.flat.txt" || true
sudo perf report -i "$out/$label.perf.data" --stdio --children --sort symbol -g none --percent-limit 0.5 2>/dev/null | grep -E "^ +[0-9]" >"$out/$label.cum.txt" || true
echo "leader pid $pid, endpoint $leader"
head -40 "$out/$label.flat.txt"

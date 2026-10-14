#!/usr/bin/env bash
# Question (#134): during main's own max-mode bench (put-1k or txn-1k, the
# flags of bench/run.sh), what does the disk do, how many bytes per write
# reach the log and the device, and what does main reach when the disk is
# not shared or not there?
#
#   exp/134/benchdisk.sh <put|txn> <out dir>
#
# Layouts, all with the bench's pinning (generator 3, leader 0, followers 1 2):
#   shared  all three data dirs on the runner disk, as bench.yml runs
#   tmpfs   all three on /dev/shm: no disk, fsync is free
#   alone   the leader on the runner disk, both followers on /dev/shm
# Binaries: main (76952348 as is) and instr (main plus persist-timing.patch).
set -euo pipefail
wl=$1
out=$2
mkdir -p "$out"
here=$(cd "$(dirname "$0")" && pwd)
: "${CERTS:?}" "${BINS:?}" "${DISKDIR:?}" "${DEV:?}"
export CERTS ETCDCTL=$BINS/etcdctl
shm=/dev/shm/b134

once() {
	local layout=$1 bin=$2 rep=$3 name="$1-$2-$3"
	local dir=$DISKDIR/c tries=0
	while :; do
		tries=$((tries + 1))
		rm -rf "$dir" "$shm"
		mkdir -p "$shm"
		unset NODEDIR_1 NODEDIR_2 NODEDIR_3
		case $layout in
		tmpfs) export NODEDIR_1=$shm/n1 NODEDIR_2=$shm/n2 NODEDIR_3=$shm/n3 ;;
		alone) export NODEDIR_2=$shm/n2 NODEDIR_3=$shm/n3 ;;
		esac
		"$here/cluster.sh" start tigerstore "$BINS/$bin" "$dir" 0 1 2
		# alone needs node 1, the one on disk, to lead.
		if [ "$layout" != alone ] || [ "$(cat "$dir/leader")" = 127.0.0.1:40010 ]; then break; fi
		"$here/cluster.sh" stop "$dir"
		if [ "$tries" -ge 12 ]; then
			echo "node 1 never led" >&2
			return 1
		fi
	done
	echo "$name tries=$tries leader=$(cat "$dir/leader")" | tee -a "$out/runs.txt"
	cp "$dir/placement.json" "$out/$name.placement"
	taskset -c 3 "$BINS/walstat" sample "$DEV" 100 "$(cat "$dir/pids")" "$out/$name.csv" &
	local sampler=$!
	date +%s%N >"$out/$name.t0"
	taskset -c 3 "$BINS/loadgen" \
		-endpoints "$(cat "$dir/leader")" \
		-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
		-pids "$(cat "$dir/pids")" -label "$name" -workload "$wl" -value-size 1024 \
		-watchers 3 -warmup 3s -duration 10s -seed "$rep" -keys 10000 \
		-out "$out/$name.json" || echo "$name LOADGEN FAILED" | tee -a "$out/runs.txt"
	date +%s%N >"$out/$name.t1"
	sleep 0.3
	kill "$sampler"
	"$here/cluster.sh" check "$dir" || echo "$name LEADER CHANGED" | tee -a "$out/runs.txt"
	local i
	for i in 1 2 3; do
		local ndv="NODEDIR_$i"
		"$BINS/walstat" count "${!ndv:-$dir/n$i}" >"$out/$name.wal-n$i.json"
		cp "$dir/n$i.log" "$out/$name.n$i.log"
	done
	du -sb "$dir" "$shm" >"$out/$name.du" || true
	"$here/cluster.sh" stop "$dir"
	rm -rf "$shm"
	sync
	sleep 2
}

for rep in 1 2 3 4 5; do
	once shared main "$rep"
	if [ "$rep" -le 3 ]; then
		if ((rep % 2)); then
			once tmpfs main "$rep"
			once alone main "$rep"
			once shared instr "$rep"
			once alone instr "$rep"
		else
			once alone instr "$rep"
			once shared instr "$rep"
			once alone main "$rep"
			once tmpfs main "$rep"
		fi
	fi
done

#!/usr/bin/env bash
# Measures what the shared disk gives to synced appends: one writer, as
# cmd/ceiling measures it, and three at once, as three nodes write their
# logs, at 1 MiB and 4 MiB a sync, into new files that grow. It
# prints the MiB/s the writers got together and the bytes the device wrote
# per byte asked.
#
#   bench/disk-probe.sh <dir on the disk> <device in /proc/diskstats>
set -euo pipefail
dir=$1
dev=$2
mib=600
sectors() { awk -v d="$dev" '$3 == d {print $10}' /proc/diskstats; }
run() {
	local writers=$1 group=$2 i
	rm -f "$dir"/probe-*
	sync
	local s0 t0 t1 s1 count=$((mib / group))
	s0=$(sectors)
	t0=$(date +%s.%N)
	for i in $(seq "$writers"); do
		dd if=/dev/zero of="$dir/probe-$i" bs="${group}M" count="$count" oflag=dsync conv=notrunc status=none &
	done
	wait
	t1=$(date +%s.%N)
	s1=$(sectors)
	awk -v w="$writers" -v g="$group" -v n=$((writers * count * group)) -v s="$((s1 - s0))" -v t0="$t0" -v t1="$t1" \
		'BEGIN {printf "%d writer(s), %d MiB a sync: %6.1f MiB/s, device wrote %.3f bytes per byte\n", w, g, n / (t1 - t0), s * 512 / (n * 1048576)}'
}
for round in 1 2; do
	for case in "1 4" "3 1" "3 4" "1 1"; do run $case; done
done
rm -f "$dir"/probe-*

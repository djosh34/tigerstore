#!/usr/bin/env bash
# Measures what the disk writes for 1 MiB appends that are each synced, as
# three nodes write their logs at once: into new files that grow, into
# files fallocate made, and into files written before. For each it prints
# the MiB/s the writers got and the bytes the device wrote per byte asked.
#
#   bench/disk-probe.sh <dir on the disk> <device in /proc/diskstats>
set -euo pipefail
dir=$1
dev=$2
mib=400
sectors() { awk -v d="$dev" '$3 == d {print $10}' /proc/diskstats; }
run() {
	local kind=$1 i
	rm -f "$dir"/probe-*
	for i in 1 2 3; do
		case $kind in
		fallocate) fallocate -l "${mib}M" "$dir/probe-$i" ;;
		written)
			dd if=/dev/zero of="$dir/probe-$i" bs=1M count="$mib" status=none
			sync "$dir/probe-$i"
			;;
		esac
	done
	sync
	local s0 t0 t1 s1
	s0=$(sectors)
	t0=$(date +%s.%N)
	for i in 1 2 3; do
		dd if=/dev/zero of="$dir/probe-$i" bs=1M count="$mib" oflag=dsync conv=notrunc status=none &
	done
	wait
	t1=$(date +%s.%N)
	s1=$(sectors)
	awk -v k="$kind" -v n=$((3 * mib)) -v s="$((s1 - s0))" -v t0="$t0" -v t1="$t1" \
		'BEGIN {printf "%-10s %6.1f MiB/s asked, device wrote %.3f bytes per byte\n", k, n / (t1 - t0), s * 512 / (n * 1048576)}'
}
for kind in grow fallocate written grow fallocate written; do run "$kind"; done
rm -f "$dir"/probe-*

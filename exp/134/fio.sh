#!/usr/bin/env bash
# Question (#134): what is the runner disk's own write ceiling, with no
# filesystem log format in the way: O_DIRECT sequential bandwidth at queue
# depth 1 to 32, small-write IOPS, three writers, flush every 16 MiB, and the
# sustained rate of buffered writes with no fsync once page cache is full.
#
#   exp/134/fio.sh <dir on the disk> <out dir>
set -euo pipefail
dir=$1
out=$2
mkdir -p "$out"
f="--directory=$dir --time_based --runtime=15 --size=4G --group_reporting --output-format=json"
run() {
	local name=$1
	shift
	fio --name="$name" $f "$@" >"$out/$name.json"
	jq -r --arg n "$name" '.jobs[0].write | "\($n) \(.bw_bytes/1048576|floor) MiB/s \(.iops|floor) IOPS clat p50 \(.clat_ns.percentile."50.000000"/1000|floor) us p99 \(.clat_ns.percentile."99.000000"/1000|floor) us"' "$out/$name.json" | tee -a "$out/summary.txt"
	rm -f "$dir"/"$name"*
	sync
	sleep 2
}
for qd in 1 4 16 32; do
	run "seq1m-direct-qd$qd" --rw=write --bs=1M --direct=1 --ioengine=libaio --iodepth=$qd
done
for qd in 1 8; do
	run "seq4m-direct-qd$qd" --rw=write --bs=4M --direct=1 --ioengine=libaio --iodepth=$qd
done
run seq1m-direct-qd16-3jobs --rw=write --bs=1M --direct=1 --ioengine=libaio --iodepth=16 --numjobs=3
run seq1m-direct-qd16-fdatasync16 --rw=write --bs=1M --direct=1 --ioengine=libaio --iodepth=16 --fdatasync=16
run seq64k-direct-qd32 --rw=write --bs=64k --direct=1 --ioengine=libaio --iodepth=32
run rand4k-direct-qd32 --rw=randwrite --bs=4k --direct=1 --ioengine=libaio --iodepth=32
run seq4k-direct-qd32 --rw=write --bs=4k --direct=1 --ioengine=libaio --iodepth=32
# Buffered, no fsync, 8 GiB: the first seconds land in page cache, the tail
# runs at the rate writeback reaches the device.
fio --name=bufsustain --directory="$dir" --rw=write --bs=1M --size=8G --ioengine=psync \
	--write_bw_log="$out/bufsustain" --log_avg_msec=1000 --end_fsync=1 --output-format=json >"$out/bufsustain.json"
jq -r '.jobs[0].write | "bufsustain 8G incl end fsync \(.bw_bytes/1048576|floor) MiB/s"' "$out/bufsustain.json" | tee -a "$out/summary.txt"
rm -f "$dir"/bufsustain*

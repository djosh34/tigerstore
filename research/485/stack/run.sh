#!/usr/bin/env bash
set -euo pipefail
out=${1:-research/485/stack/results}
mkdir -p "$out"
{
 echo "run=$GITHUB_RUN_ID sha=$GITHUB_SHA runner=$RUNNER_NAME arch=$RUNNER_ARCH"
 go version
 nproc
 lscpu
 free -b
 df -h
 findmnt -T "$RUNNER_TEMP" -o TARGET,SOURCE,FSTYPE,OPTIONS
 uname -a
} > "$out/hardware.txt"
# Enable the filesystem barriers before measuring write+fsync, as bench.yml does.
sudo mount -o remount,barrier=1 /
findmnt -T "$RUNNER_TEMP" -o TARGET,SOURCE,FSTYPE,OPTIONS > "$out/disk-mount.txt"
if findmnt -T "$RUNNER_TEMP" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'; then exit 1; fi
python3 research/485/stack/overlay.py
for mode in native purego; do
 tags=()
 if [ "$mode" = purego ]; then tags=(-tags=purego); fi
 go test -c "${tags[@]}" -o "$out/$mode.test" ./research/485/stack
 "$out/$mode.test" -test.run='^$' -test.bench='Benchmark(Peer|Certificates|Handshake|Client|SHA256|AESGCM)$' -test.benchtime=1s -test.count=5 -test.cpu=2 -test.timeout=12m | tee "$out/$mode.txt"
 if [ "$mode" = native ]; then
  "$out/$mode.test" -test.run='^$' -test.bench='BenchmarkMemory$' -test.benchtime=1s -test.count=5 -test.cpu=4 -test.timeout=3m | tee "$out/memory.txt"
 fi
 stubtags=exp485stub
 if [ "$mode" = purego ]; then stubtags=exp485stub,purego; fi
 go test -c -overlay=research/485/stack/overlay.json -tags="$stubtags" -o "$out/$mode-stub.test" ./research/485/stack
 EXP485_STUB=1 "$out/$mode-stub.test" -test.run='^$' -test.bench='Benchmark(Client|WatchEvent1KiB)$' -test.benchtime=1s -test.count=5 -test.cpu=2 -test.timeout=5m | tee "$out/$mode-stub.txt"
done
rm "$out/"*.test
# Each fio test is repeated five times, time-based, single job. Read and
# random-read tests use the file actually written by seqwrite (not a hole).
file="$RUNNER_TEMP/exp485-fio"
fio --name=prepare --filename="$file" --size=1G --rw=write --bs=1M --direct=1 --ioengine=sync --end_fsync=1 --output-format=json --output="$out/fio-prepare.json"
for rep in 1 2 3 4 5; do
 for kind in seqwrite seqread fsync randread; do
  common=(--name="$kind" --filename="$file" --size=1G --time_based=1 --runtime=5 --ramp_time=1 --numjobs=1 --group_reporting=1 --output-format=json --output="$out/fio-$kind-$rep.json")
  case "$kind" in
   seqwrite) args=(--rw=write --bs=1M --direct=1 --ioengine=sync --iodepth=1 --end_fsync=1);;
   seqread) args=(--rw=read --bs=1M --direct=1 --ioengine=sync --iodepth=1);;
   fsync) args=(--rw=write --bs=4k --direct=0 --ioengine=sync --iodepth=1 --fsync=1);;
   randread) args=(--rw=randread --bs=4k --direct=1 --ioengine=libaio --iodepth=32);;
  esac
  fio "${common[@]}" "${args[@]}"
 done
done
rm -f "$file"
python3 research/485/stack/report.py "$out" | tee "$out/report.md"
cat "$out/report.md" >> "$GITHUB_STEP_SUMMARY"

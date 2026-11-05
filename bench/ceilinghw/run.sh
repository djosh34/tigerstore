#!/usr/bin/env bash
set -euo pipefail
out=$(realpath "${1:-results}")
data="$RUNNER_TEMP/ceiling-data"
mkdir -p "$out" "$data"
trap 'bench/netem.sh off; jobs -pr | xargs -r kill' EXIT
{
  git rev-parse HEAD; uname -a; go version; lscpu; cat /proc/cpuinfo; free -b
  findmnt -T "$data" -o TARGET,SOURCE,FSTYPE,OPTIONS; lsblk -O; df -h "$data"
  for f in /sys/block/sda/queue/* /sys/devices/system/cpu/cpu0/cpufreq/*; do
    [ -f "$f" ] && { echo "$f"; cat "$f" 2>/dev/null || true; }
  done
  sysctl net.ipv4.tcp_wmem net.ipv4.tcp_rmem net.core.wmem_max net.core.rmem_max net.ipv4.tcp_congestion_control
  sudo dmidecode -t system || true
  curl -fsS --noproxy '*' --connect-timeout 2 --max-time 4 -H Metadata:true 'http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01' || true
} >"$out/runner.txt" 2>&1
for n in 0 1 2; do dd if=/dev/zero of="$data/pre.$n" bs=4M count=256 conv=fdatasync status=none; done
# Prewritten files are shared between the fio and total-cycle measurements,
# but never between concurrent writers. Fio phase percentiles are NOT summed.
for direct in 0 1; do
 for sync in fdatasync fsync; do
  for place in pre append; do
   for writers in 1 3; do
    for bs in 4096 16384 65536 262144 1048576 4194304; do
     label="d${direct}-${sync}-${place}-w${writers}-b${bs}"
     rm -f "$data"/append.*
     extra=(--filename_format="$place.\$jobnum" --size=1G --allow_file_create=0)
     if [ "$place" = append ]; then extra=(--filename_format='append.$jobnum' --size=8G --allow_file_create=1 --create_on_open=1 --file_append=1 --fallocate=none); fi
     fio --name=durable --directory="$data" --rw=write --ioengine=psync --iodepth=1 --direct="$direct" --bs="$bs" --numjobs="$writers" --cpus_allowed=0-2 --cpus_allowed_policy=split --time_based=1 --runtime=4 --"$sync"=1 --percentile_list=50:99:99.9 --output-format=json --output="$out/fio-$label.json" "${extra[@]}"
     rm -f "$data"/append.*
     ap=0; [ "$place" = append ] && ap=1
     fs=0; [ "$sync" = fsync ] && fs=1
     bin/raw disk "$writers" "$data" "$bs" "$direct" "$ap" "$fs" >"$out/cycle-$label.jsonl"
    done
   done
  done
 done
done
for rw in write read; do
 for qd in 1 4; do
  fio --name=sequential --filename="$data/pre.0" --allow_file_create=0 --rw="$rw" --ioengine=libaio --iodepth="$qd" --direct=1 --bs=1M --size=1G --time_based=1 --runtime=6 --output-format=json --output="$out/fio-seq-$rw-qd$qd.json"
 done
done
taskset -c 3 bin/diskbench -dir "$data" | tee "$out/diskbench.jsonl"
for mode in copy write; do for cores in 1 4; do bin/raw memory "$cores" "$mode" >"$out/memory-$mode-$cores.json"; done; done
for cpu in 0 1 2 3; do taskset -c "$cpu" sysbench cpu --threads=1 --time=4 --cpu-max-prime=20000 run >"$out/cpu-$cpu.txt"; done
for procs in 1 2; do GOMAXPROCS=$procs taskset -c 0 bin/primitives.test -test.run='^$' -test.bench=. -test.benchtime=300ms -test.count=3 >"$out/primitives-p$procs.txt"; done
for rtt in off 0 2 10; do
 if [ "$rtt" = off ]; then bench/netem.sh off; else bench/netem.sh on "$rtt"; fi
 for port in 20011 20010; do
  for cpus in '0 1' '3 0'; do
   read -r client server <<<"$cpus"
   for secure in plain tls; do
    tls=(); [ "$secure" = tls ] && tls=(-tls)
    tag="r$rtt-p$port-c$client-s$server-$secure"
    GOMAXPROCS=2 taskset -c "$server" bin/netraw -server -addr "127.0.0.1:$port" "${tls[@]}" >"$out/server-$tag.txt" 2>&1 & pid=$!
    sleep .2
    GOMAXPROCS=2 taskset -c "$client" bin/netraw -addr "127.0.0.1:$port" "${tls[@]}" 2>"$out/cipher-$tag.txt" >"$out/ping-$tag.json"
    kill "$pid"; wait "$pid" || true
   done
  done
 done
 # Streaming on exactly the peer port, both CPU pairings, 1/2 streams.
 for cpus in '0 1' '3 0'; do
  read -r client server <<<"$cpus"
  for streams in 1 2; do
   tag="r$rtt-c$client-s$server-n$streams"
   taskset -c "$server" iperf3 -s -1 -B 127.0.0.1 -p 20011 >"$out/iperf-server-$tag.txt" 2>&1 & pid=$!
   sleep .2
   taskset -c "$client" iperf3 -c 127.0.0.1 -p 20011 -P "$streams" -t 4 -J >"$out/iperf-$tag.json"
   wait "$pid"
   GOMAXPROCS=2 taskset -c "$server" bin/netraw -server -tls -mode stream -addr 127.0.0.1:20011 >"$out/tls-server-$tag.txt" 2>&1 & pid=$!
   sleep .2
   /usr/bin/time -v env GOMAXPROCS=2 taskset -c "$client" bin/netraw -tls -mode stream -addr 127.0.0.1:20011 -streams "$streams" -duration 4s >"$out/tls-stream-$tag.json" 2>"$out/tls-time-$tag.txt"
   kill "$pid"; wait "$pid" || true
  done
 done
 # UDP measures delivered datagrams, not TCP GSO aggregates; qdisc counters
 # and process/host CPU distinguish queue drops, CPU limits, and delay.
 for size in 64 1024; do
  tag="r$rtt-b$size"
  cat /proc/stat >"$out/stat-before-$tag.txt"
  taskset -c 1 iperf3 -s -1 -B 127.0.0.1 -p 20011 >"$out/udp-server-$tag.txt" 2>&1 & pid=$!
  sleep .2
  taskset -c 0 iperf3 -c 127.0.0.1 -p 20011 -u -b 0 -l "$size" -t 3 -J >"$out/udp-$tag.json"
  wait "$pid"
  cat /proc/stat >"$out/stat-after-$tag.txt"
 done
 bench/netem.sh show >"$out/netem-$rtt.txt"
done
bench/netem.sh off
python3 bench/ceilinghw/report.py "$out" >"$out/summary.md"
cat "$out/summary.md" >>"$GITHUB_STEP_SUMMARY"

#!/usr/bin/env bash
# #484, fresh pinned cluster per point, three reps, TLS, same lo netem as bench.
set -euo pipefail
export CERTS=$(realpath certs) ETCDCTL=$(realpath bin/etcdctl)
export BENCH_FLAGS=--enable-pprof GAP_TIMING=1 GAP_BLOCK=1
root=${DATA:?} out=${OUT:-results}
mkdir -p "$out"
cleanup() { bench/cluster.sh stop "$root/c"; bench/netem.sh off; }
trap cleanup EXIT
once() {
 local wl=$1 place=$2 rtt=$3 variant=$4 rep=$5
 local dest=$out/$wl-$place-$rtt-$variant-$rep
 mkdir -p "$dest"
 export TMPFS_DIR=/dev/shm/ceiling-gap TMPFS_NODES=""
 if [ "$place" = tmpfs ]; then export TMPFS_NODES="1 2 3"; fi
 local bin=bin/gap streams=256
 export GAP_TIMING=1 GAP_BLOCK=1 LEAD="" BENCH_FLAGS=--enable-pprof
 case "$variant" in
 wide) bin=bin/wide ;;
 depth8) bin=bin/depth8 ;;
 streams512) streams=512 ;;
 streams1024) streams=1024 ;;
 pure|purerate) bin=bin/pure; export GAP_TIMING=0 GAP_BLOCK=0 ;;
 alone) export TMPFS_NODES="2 3" LEAD=1 ;;
 unsafe) export BENCH_FLAGS="--enable-pprof --unsafe-no-fsync" ;;
 esac
 bench/netem.sh on "$rtt"
 bench/cluster.sh start tigerstore "$(realpath "$bin")" "$root/c" 0 1 2
 cp "$root/c/placement.json" "$dest/placement.json"
 local leader=$(<"$root/c/leader") follower
 follower=$(tr ',' '\n' <"$root/c/endpoints" | grep -v -F "$leader" | head -1)
 local curl=(curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem")
 local rate=0
 if [ "$variant" = rate ] || [ "$variant" = purerate ]; then rate=2500; fi
 local trace=""
 if [ "${GAP_BPF:-0}" = 1 ]; then
  read -r -a ids <<<"$(tr ',' ' ' <"$root/c/pids")"
  sudo bpftrace -q -o "$dest/syscalls.txt" bench/ceiling-gap.bt "${ids[@]}" >"$dest/bpftrace.log" 2>&1 &
  trace=$!
  for _ in $(seq 50); do
   grep -q READY "$dest/syscalls.txt" 2>/dev/null && break
   kill -0 "$trace" 2>/dev/null || break
   sleep 0.1
  done
 fi
 taskset -c 3 bin/loadgen -endpoints "$leader" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
  -pids "$(<"$root/c/pids")" -workload "$wl" -value-size 1024 -keys 10000 -warmup 3s -duration 14s \
  -seed "$rep" -conns 6 -streams "$streams" -rate "$rate" -disk "$DISK" -out "$dest/result.json" 2>"$dest/loadgen.log" &
 local gen=$!
 sleep 5
 local block=""
 if [ "${GAP_BPF:-0}" = 1 ] && [ "$variant" = max ]; then
  sudo blktrace -d /dev/sda -w 8 -o "$dest/block" >"$dest/blktrace.log" 2>&1 &
  block=$!
 fi
 taskset -c 3 python3 bench/ceiling-gap-sample.py "$dest" "$root/c" "$CERTS" "$DISK" >"$dest/sample.log" 2>&1 &
 local sample=$!
 "${curl[@]}" "https://$leader/debug/pprof/profile?seconds=8" -o "$dest/leader.pprof" &
 local lp=$!
 "${curl[@]}" "https://$follower/debug/pprof/profile?seconds=8" -o "$dest/follower.pprof" &
 local fp=$!
 wait "$gen"; wait "$sample"; wait "$lp"; wait "$fp"
 if [ -n "$trace" ]; then wait "$trace" || true; fi
 if [ -n "$block" ]; then
  wait "$block" || true
  sudo blkparse -i "$dest/block" -f '%a %S %n %d\n' -o "$dest/block.txt" >"$dest/blkparse.log" 2>&1 || true
  sudo debugfs -R 'stat <8>' /dev/sda1 >"$dest/journal.txt" 2>&1 || true
  cat /sys/class/block/sda1/start >"$dest/partition-start.txt"
 fi
 bench/cluster.sh check "$root/c"
 bench/netem.sh show >"$dest/netem.txt"
 cp "$root/c"/n*.log "$dest/"
 for d in $(<"$root/c/datadirs"); do du -ab "$d"; done >"$dest/files.txt"
 bench/cluster.sh stop "$root/c"
 jq -r --arg cfg "$(basename "$dest")" '[ $cfg, (.throughput|floor), (.latency_us.p50/1000), .server_cores_by_node, .client_cores, .disk_bytes_per_op ] | @json' "$dest/result.json"
}
if [ "${GAP_PHASE:-full}" = pipeline ]; then
 for rep in 1 2 3; do
  for wl in put txn; do
   for rtt in 0 10; do
    for variant in max depth8; do once "$wl" disk "$rtt" "$variant" "$rep"; done
   done
  done
 done
 bench/netem.sh off
 python3 bench/ceiling-gap-report.py "$out" >"$out/summary.md"
 cat "$out/summary.md" >>"$GITHUB_STEP_SUMMARY"
 exit 0
fi
if [ "${GAP_PHASE:-full}" = detail ]; then
 for rep in 1 2 3; do
  for wl in put txn; do
   for variant in max alone unsafe streams1024 purerate; do once "$wl" disk 0 "$variant" "$rep"; done
  done
 done
 bench/netem.sh off
 python3 bench/ceiling-gap-report.py "$out" >"$out/summary.md"
 cat "$out/summary.md" >>"$GITHUB_STEP_SUMMARY"
 exit 0
fi
# Alternate layout/RTT order each repetition to expose drift.
for rep in 1 2 3; do
 rtts="0 2 10"; places="disk tmpfs"
 if [ "$rep" = 2 ]; then rtts="10 2 0"; places="tmpfs disk"; fi
 for wl in put txn; do
  for rtt in $rtts; do
   for place in $places; do
    once "$wl" "$place" "$rtt" max "$rep"
    once "$wl" "$place" "$rtt" rate "$rep"
   done
  done
  for place in disk tmpfs; do
   for variant in streams512 streams1024 wide; do once "$wl" "$place" 10 "$variant" "$rep"; done
   once "$wl" "$place" 0 pure "$rep"
  done
 done
done
bench/netem.sh off
python3 bench/ceiling-gap-report.py "$out" >"$out/summary.md"
cat "$out/summary.md" >>"$GITHUB_STEP_SUMMARY"

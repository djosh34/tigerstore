#!/usr/bin/env bash
# Research #576: system calls, bytes per call and context switches per
# mixed-k8s op on each node, counted with bpftrace over a fixed window
# inside the loadgen's measurement window. A second clean run takes a CPU
# profile of the leader without the tracer.
set -euo pipefail
export BINS="$PWD/bin" CERTS="$PWD/certs" ETCDCTL="$PWD/bin/etcdctl" BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
out="$PWD/results"
dir="$RUNNER_TEMP/mixed576"
window=${WINDOW:-20}
trap 'bench/cluster.sh stop "$dir"' EXIT
for mode in counted clean; do
  bench/cluster.sh stop "$dir"
  bench/cluster.sh start tigerstore "$PWD/bin/tigerstore" "$dir" 0 1 2
  leader=$(<"$dir/leader")
  pids=$(<"$dir/pids")
  read -r L F1 F2 <<<"${pids//,/ }" # cluster.sh places the leader first.
  echo "mode=$mode leader=$leader pids=$pids" | tee "$out/$mode-placement.txt"
  marker="$RUNNER_TEMP/marker-$mode-"
  rm -f "${marker}0" "${marker}1"
  RESEARCH576_MARKER="$marker" taskset -c 3 bin/loadgen \
    -endpoints "$(<"$dir/endpoints")" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
    -pids "$pids" -disk "$(basename "$(findmnt -T "$RUNNER_TEMP" -n -o SOURCE)")" \
    -workload mixed -keys 1000000 -conns 6 -streams 48 -watchers 3 -seed 1 \
    -warmup 3s -duration 40s -out "$out/$mode.json" >"$out/$mode-gen.log" 2>&1 &
  gen=$!
  while [ ! -f "${marker}0" ]; do kill -0 "$gen" || { wait "$gen"; exit 1; }; sleep 0.05; done
  for p in $L $F1 $F2; do
    sudo ls -l "/proc/$p/fd" >"$out/$mode-fd-$p.txt" 2>&1 || true
    grep ctxt "/proc/$p/status" >"$out/$mode-ctxt-start-$p.txt"
    cp "/proc/$p/stat" "$out/$mode-stat-start-$p.txt"
  done
  sudo ss -tnpi >"$out/$mode-ss.txt" 2>&1 || true
  if [ "$mode" = counted ]; then
    G=$gen
    cat >"$RUNNER_TEMP/count.bt" <<EOF
BEGIN { @t0 = nsecs; }
tracepoint:raw_syscalls:sys_enter /pid == $L || pid == $F1 || pid == $F2 || pid == $G/ { @calls[pid, args.id] = count(); }
tracepoint:syscalls:sys_enter_write /pid == $L || pid == $F1 || pid == $F2/ { @wfd[pid, args.fd] = count(); @fd[tid] = args.fd; }
tracepoint:syscalls:sys_exit_write /@fd[tid] != 0/ { @wbytes[pid, @fd[tid]] = sum(args.ret); @wsize[pid] = hist(args.ret); delete(@fd[tid]); }
tracepoint:syscalls:sys_enter_read /pid == $L || pid == $F1 || pid == $F2/ { @rfd[pid, args.fd] = count(); @rd[tid] = args.fd; }
tracepoint:syscalls:sys_exit_read /@rd[tid] != 0/ { @rbytes[pid, @rd[tid]] = sum(args.ret > 0 ? args.ret : 0); @rsize[pid] = hist(args.ret); if (args.ret < 0) { @rerr[pid, args.ret] = count(); } delete(@rd[tid]); }
tracepoint:syscalls:sys_exit_pwrite64 /pid == $L || pid == $F1 || pid == $F2/ { @pwbytes[pid] = sum(args.ret); @pwsize[pid] = hist(args.ret); }
tracepoint:sched:sched_switch /pid == $L || pid == $F1 || pid == $F2 || pid == $G/ { @switch[pid] = count(); }
tracepoint:sched:sched_wakeup /args.pid != 0/ { @wakeups_by_waker[pid] = count(); }
interval:s:$window { printf("elapsed_ns %llu\n", nsecs - @t0); exit(); }
EOF
    sudo bpftrace "$RUNNER_TEMP/count.bt" >"$out/syscounts.txt" 2>"$out/bpftrace.log" || true
    echo "L=$L F1=$F1 F2=$F2 G=$G" >>"$out/syscounts.txt"
  else
    curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$leader/debug/pprof/profile?seconds=30" -o "$out/leader.pprof"
  fi
  for p in $L $F1 $F2; do
    grep ctxt "/proc/$p/status" >"$out/$mode-ctxt-end-$p.txt" || true
    cp "/proc/$p/stat" "$out/$mode-stat-end-$p.txt" || true
  done
  wait "$gen"
  cp "$dir"/n*.log "$out/" || true
  bench/cluster.sh check "$dir" | tee "$out/$mode-check.txt"
  bench/cluster.sh stop "$dir"
done
go tool pprof -top bin/tigerstore results/leader.pprof > results/profile-top.txt
go tool pprof -top -cum bin/tigerstore results/leader.pprof > results/profile-cum.txt

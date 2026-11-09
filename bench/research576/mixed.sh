#!/usr/bin/env bash
set -euo pipefail
export BINS="$PWD/bin" CERTS="$PWD/certs" ETCDCTL="$PWD/bin/etcdctl" BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
out="$PWD/results"
dir="$RUNNER_TEMP/mixed576"
trap 'bench/cluster.sh stop "$dir"' EXIT
for mode in clean counted; do
  bench/cluster.sh stop "$dir"
  bench/cluster.sh start tigerstore "$PWD/bin/tigerstore" "$dir" 0 1 2
  leader=$(<"$dir/leader")
  pids=$(<"$dir/pids")
  pid=${pids%%,*} # cluster.sh places the leader first.
  echo "mode=$mode leader=$leader pid=$pid pids=$pids" | tee "$out/$mode-placement.txt"
  marker="$RUNNER_TEMP/marker-$mode-"
  rm -f "${marker}0" "${marker}1"
  RESEARCH576_MARKER="$marker" taskset -c 3 bin/loadgen \
    -endpoints "$(<"$dir/endpoints")" -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
    -pids "$pids" -disk "$(basename "$(findmnt -T "$RUNNER_TEMP" -n -o SOURCE)")" \
    -workload mixed -keys 1000000 -conns 6 -streams 48 -warmup 3s -duration 40s -out "$out/$mode.json" >"$out/$mode-gen.log" 2>&1 &
  gen=$!
  if [ "$mode" = counted ]; then
    cat > "$RUNNER_TEMP/count.bt" <<EOF
tracepoint:syscalls:sys_enter_openat /pid == $gen/ {
  if (str(args.filename) == "${marker}0") { @active = 1; }
  if (str(args.filename) == "${marker}1") { @active = 0; }
}
tracepoint:raw_syscalls:sys_enter /pid == $pid && @active/ {
  @calls[args.id] = count();
}
EOF
    sudo bpftrace "$RUNNER_TEMP/count.bt" >"$out/syscounts.txt" 2>"$out/bpftrace.log" &
    tracer=$!
    sleep 2
    if ! kill -0 "$tracer" 2>/dev/null; then tracer=""; fi
  fi
  while [ ! -f "${marker}0" ]; do kill -0 "$gen" || { wait "$gen"; exit 1; }; sleep 0.05; done
  if [ "$mode" = clean ]; then
    curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://$leader/debug/pprof/profile?seconds=30" -o "$out/leader.pprof" &
    profiler=$!
    cp "/proc/$pid/stat" "$out/leader-start.stat"
    wait "$profiler"
  elif [ -z "$tracer" ]; then
    sudo strace -f -c -S calls -p "$pid" -o "$out/strace.txt" 2>"$out/strace.log" &
    tracer=$!
  fi
  while [ ! -f "${marker}1" ]; do kill -0 "$gen" || break; sleep 0.05; done
  cp "/proc/$pid/stat" "$out/$mode-end.stat"
  if [ "$mode" = counted ] && [ -n "$tracer" ]; then sudo kill -INT "$tracer" || true; wait "$tracer" || true; fi
  wait "$gen"
  cp "$dir"/n*.log "$out/" || true
  bench/cluster.sh check "$dir" | tee "$out/$mode-check.txt"
  bench/cluster.sh stop "$dir"
done
go tool pprof -top bin/tigerstore results/leader.pprof > results/profile-top.txt
go tool pprof -top -cum bin/tigerstore results/leader.pprof > results/profile-cum.txt

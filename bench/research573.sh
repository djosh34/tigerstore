#!/usr/bin/env bash
set -euo pipefail
export CERTS=$PWD/certs ETCDCTL=$PWD/bin/etcdctl BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
export RESEARCH573_BOUNDARY=$PWD/bench/research573-boundary.sh
mkdir -p results
cp /usr/bin/true bin/research573-start
cp /usr/bin/true bin/research573-stop
trap 'bench/cluster.sh stop "$RUNNER_TEMP/research573-cluster"' EXIT
# Two independent normal profiles, then a separate BPF-instrumented run.
for mode in profile1 profile2 trace; do
 dir=$RUNNER_TEMP/research573-cluster
 export OUT=$PWD/results/$mode
 mkdir -p "$OUT"
 bench/cluster.sh stop "$dir"
 bench/cluster.sh start tigerstore "$PWD/bin/candidate" "$dir" 0 1 2
 cp "$dir/placement.json" "$OUT/placement.json"
 leader=$(<"$dir/leader")
 follower=$(tr ',' '\n' < "$dir/endpoints" | grep -vx "$leader" | head -1)
 export RESEARCH_EPS="$leader $follower"
 pids=$(<"$dir/pids")
 lp=${pids%%,*}
 allpids=$(tr ',' ' ' < "$dir/pids")
 # Find the selected follower PID (endpoint order is node order).
 fp=$(pgrep -f "bin/candidate --name n$(( (${follower##*:} - 20000) / 10 )) " | head -1)
 printf 'leader %s %s\nfollower %s %s\n' "$leader" "$lp" "$follower" "$fp" > "$OUT/roles.txt"
 bpfpid=""
 if [ "$mode" = trace ]; then
  cat > "$OUT/trace.bt" <<EOF
tracepoint:sched:sched_process_exec /str(args->filename) == "$PWD/bin/research573-start"/ { @active = 1; }
tracepoint:sched:sched_process_exec /str(args->filename) == "$PWD/bin/research573-stop"/ { @active = 0; exit(); }
tracepoint:raw_syscalls:sys_enter /@active && (pid == $lp || pid == $fp)/ { @syscalls[pid, args->id] = count(); @sysstack[pid, ustack(24)] = count(); }
uprobe:$PWD/bin/candidate:runtime.memmove /@active && (pid == $lp || pid == $fp)/ { @copybytes[pid, ustack(24)] = sum(reg("r2")); @copycalls[pid] = count(); }
EOF
  sudo env BPFTRACE_MAP_KEYS_MAX=16384 bpftrace "$OUT/trace.bt" > "$OUT/bpf.txt" 2> "$OUT/bpf.stderr" &
  bpfpid=$!
  sleep 5
  # Failed BPF is preserved and does not discard otherwise useful profiles.
 fi
 taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
  -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
  -pids "$pids" -workload mixed -keys 1000000 -conns 6 -streams 96 \
  -warmup 5s -duration 40s -seed 1 -out "$OUT/result.json" > "$OUT/loadgen.stdout" 2> "$OUT/loadgen.stderr" &
 gen=$!
 while [ ! -f "$OUT/phase-0" ]; do kill -0 "$gen"; sleep 0.2; done
 if [ "$mode" != trace ]; then
  read -ra eps <<< "$RESEARCH_EPS"
  curls=()
  for i in 0 1; do
   curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://${eps[i]}/debug/pprof/profile?seconds=30" -o "$OUT/node-$i.cpu" &
   curls+=($!)
  done
  for p in "${curls[@]}"; do wait "$p"; done
 fi
 wait "$gen"
 if [ -n "$bpfpid" ]; then wait "$bpfpid" || true; fi
 bench/cluster.sh check "$dir"
 cp "$dir"/n*.log "$OUT/"
 for pid in $allpids; do cp "/proc/$pid/io" "$OUT/io-$pid.txt"; done
 bench/cluster.sh stop "$dir"
done

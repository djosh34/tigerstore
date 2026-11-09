#!/usr/bin/env bash
# Research #573, round 2: where the leader's and a follower's CPU, syscalls
# and copies go under bench mixed-k8s at max (6 x 96, as the ramp picks).
#   sys:  pprof CPU + allocs, gctrace, and cheap BPF: syscalls with bytes
#         and stacks, kernel stacks per CPU, user<->kernel copy bytes, faults
#   copy: memmove uprobe with the caller from the link register (slow)
set -euo pipefail
export CERTS=$PWD/certs ETCDCTL=$PWD/bin/etcdctl BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
export RESEARCH573_BOUNDARY=$PWD/bench/research573-boundary.sh
mkdir -p results
cp /usr/bin/true bin/research573-start
cp /usr/bin/true bin/research573-stop
trap 'bench/cluster.sh stop "$RUNNER_TEMP/research573-cluster"' EXIT
sudo bpftrace -l 'kprobe:__arch_copy_*' > results/kprobes.txt 2>&1 || true
sudo bpftrace -l 'kprobe:*copy*iter*' >> results/kprobes.txt 2>&1 || true
for mode in ${MODES:-sys copy}; do
	dir=$RUNNER_TEMP/research573-cluster
	if [ "$mode" = mix ]; then
		# The leader's CPU per op with LIST at 0% and 6% of calls, the
		# difference taken from updates, twice each in alternating order.
		for rep in 1 2; do
			for mix in 18,10,0,52,12,8 18,10,6,46,12,8 18,10,2,50,12,8; do
				out=$PWD/results/mix/$mix-$rep
				mkdir -p "$out"
				bench/cluster.sh stop "$dir"
				bench/cluster.sh start tigerstore "$PWD/bin/candidate" "$dir" 0 1 2
				taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
					-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
					-pids "$(<"$dir/pids")" -workload mixed -keys 1000000 -conns 6 -streams 96 -watchers 3 \
					-mix "$mix" -warmup 3s -duration 20s -seed "$rep" -out "$out/result.json" >"$out/loadgen.stdout" 2>"$out/loadgen.stderr"
				bench/cluster.sh check "$dir"
				bench/cluster.sh stop "$dir"
			done
		done
		continue
	fi
	export OUT=$PWD/results/$mode
	mkdir -p "$OUT"
	bench/cluster.sh stop "$dir"
	if [ "$mode" = sys ]; then export GODEBUG=gctrace=1; else unset GODEBUG; fi
	bench/cluster.sh start tigerstore "$PWD/bin/candidate" "$dir" 0 1 2
	unset GODEBUG
	cp "$dir/placement.json" "$OUT/placement.json"
	leader=$(<"$dir/leader")
	follower=$(tr ',' '\n' <"$dir/endpoints" | grep -vx "$leader" | head -1)
	export RESEARCH_EPS="$leader $follower"
	pids=$(<"$dir/pids")
	lp=${pids%%,*}
	allpids=$(tr ',' ' ' <"$dir/pids")
	fp=$(pgrep -f "bin/candidate --name n$(((${follower##*:} - 20000) / 10)) " | head -1)
	export RESEARCH_PIDS="$lp $fp"
	printf 'leader %s %s\nfollower %s %s\n' "$leader" "$lp" "$follower" "$fp" >"$OUT/roles.txt"
	for p in $allpids; do taskset -pc "$p" >>"$OUT/roles.txt"; done
	gate='@active && (pid == '$lp' || pid == '$fp')'
	start="tracepoint:sched:sched_process_exec /str(args->filename) == \"$PWD/bin/research573-start\"/ { @active = 1; }"
	stop="tracepoint:sched:sched_process_exec /str(args->filename) == \"$PWD/bin/research573-stop\"/ { @active = 0; exit(); }"
	bpf=()
	if [ "$mode" = sys ]; then
		cat >"$OUT/sys.bt" <<EOF
$start
$stop
tracepoint:raw_syscalls:sys_enter /$gate/ { @syscalls[pid, args->id] = count(); @sysstack[pid, args->id, ustack(20)] = count(); }
tracepoint:raw_syscalls:sys_exit /$gate && args->ret > 0/ { @sysbytes[pid, args->id] = sum(args->ret); }
profile:hz:499 /@active && cpu <= 2/ { @kprof[cpu, pid, comm, kstack(14)] = count(); @cpuprof[cpu, pid] = count(); }
EOF
		cat >"$OUT/kcopy.bt" <<EOF
$start
$stop
kprobe:__arch_copy_to_user /$gate/ { @k2u[pid, kstack(10)] = sum(arg2); @k2ucalls[pid] = count(); }
kprobe:__arch_copy_from_user /$gate/ { @u2k[pid, kstack(10)] = sum(arg2); @u2kcalls[pid] = count(); }
software:minor-faults:1 /$gate/ { @minflt[pid] = count(); }
software:major-faults:1 /$gate/ { @majflt[pid] = count(); }
EOF
		sudo env BPFTRACE_MAX_MAP_KEYS=200000 bpftrace "$OUT/sys.bt" >"$OUT/sys.txt" 2>"$OUT/sys.stderr" &
		bpf+=($!)
		sudo env BPFTRACE_MAX_MAP_KEYS=200000 bpftrace "$OUT/kcopy.bt" >"$OUT/kcopy.txt" 2>"$OUT/kcopy.stderr" &
		bpf+=($!)
	else
		cat >"$OUT/copy.bt" <<EOF
$start
$stop
uprobe:$PWD/bin/candidate:runtime.memmove /$gate/ { @bytes[pid, usym(reg("r30")), ustack(9)] = sum(reg("r2")); @calls[pid, usym(reg("r30"))] = count(); @total[pid] = sum(reg("r2")); @ncalls[pid] = count(); }
EOF
		sudo env BPFTRACE_MAX_MAP_KEYS=200000 bpftrace "$OUT/copy.bt" >"$OUT/copy.txt" 2>"$OUT/copy.stderr" &
		bpf+=($!)
	fi
	sleep 8
	taskset -c 3 bin/loadgen -endpoints "$(<"$dir/endpoints")" \
		-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
		-pids "$pids" -workload mixed -keys 1000000 -conns 6 -streams 96 -watchers 3 \
		-warmup 5s -duration 40s -seed 1 -out "$OUT/result.json" >"$OUT/loadgen.stdout" 2>"$OUT/loadgen.stderr" &
	gen=$!
	while [ ! -f "$OUT/phase-0" ]; do
		kill -0 "$gen"
		sleep 0.2
	done
	if [ "$mode" = sys ]; then
		read -ra eps <<<"$RESEARCH_EPS"
		curls=()
		for i in 0 1; do
			curl -fsS --cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem" "https://${eps[i]}/debug/pprof/profile?seconds=30" -o "$OUT/node-$i.cpu" &
			curls+=($!)
		done
		for p in "${curls[@]}"; do wait "$p"; done
	fi
	wait "$gen"
	for p in "${bpf[@]}"; do wait "$p" || true; done
	bench/cluster.sh check "$dir"
	cp "$dir"/n*.log "$OUT/"
	bench/cluster.sh stop "$dir"
done

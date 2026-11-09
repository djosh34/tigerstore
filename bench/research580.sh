#!/usr/bin/env bash
# Research #580: mixed-k8s ablations on one runner. Each variant removes one
# part of today's leader work with the same server code (407cf5ec plus a
# research-only SIGUSR1 that turns its GC off), so the drop in leader CPU per
# op is what an architecture that removes that part could save at most.
#   base    as the bench: client TLS, all three endpoints, default mix
#   nogc    base, the leader's GC off for the window
#   plain   client traffic without TLS (peers keep TLS)
#   follow  clients only on the two followers: the leader serves no client
#   nolist  no LIST in the mix (its 2% go to GET)
#   bold    plain + follow + nogc together
set -euo pipefail
export BINS="$PWD/bin" CERTS="$PWD/certs" ETCDCTL="$PWD/bin/etcdctl" BENCH_FLAGS=--enable-pprof BENCH_GOMEMLIMIT=3GiB
export RESEARCH580_GCSIGNAL=1
out="$PWD/results"
reps=${REPS:-3}
read -r -a variants <<<"${VARIANTS:-base nogc plain follow nolist bold}"
dir="$RUNNER_TEMP/c580"
disk=$(basename "$(findmnt -T "$RUNNER_TEMP" -n -o SOURCE)")
trap 'bench/cluster.sh stop "$dir" || true' EXIT
mkdir -p "$out"
n=${#variants[@]}
for rep in $(seq 1 "$reps"); do
	for k in $(seq 0 $((n - 1))); do
		v=${variants[$(((k + rep - 1) % n))]}
		scheme=https mix=18,10,2,50,12,8 nogc=0 follow=0
		case $v in
		nogc) nogc=1 ;;
		plain) scheme=http ;;
		follow) follow=1 ;;
		nolist) mix=20,10,0,50,12,8 ;;
		bold) scheme=http follow=1 nogc=1 ;;
		esac
		tag=$v-$rep
		bench/cluster.sh stop "$dir" || true
		BENCH_CLIENT_SCHEME=$scheme bench/cluster.sh start tigerstore "$BINS/tigerstore" "$dir" 0 1 2
		leader=$(<"$dir/leader")
		pids=$(<"$dir/pids")
		lpid=${pids%%,*}
		eps=$(<"$dir/endpoints")
		if [ "$follow" = 1 ]; then
			eps=$(tr ',' '\n' <<<"$eps" | grep -vx "$leader" | paste -sd,)
		fi
		tls=(-cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem")
		curlt=(--cacert "$CERTS/ca.pem" --cert "$CERTS/client.pem" --key "$CERTS/client-key.pem")
		if [ "$scheme" = http ]; then tls=() curlt=(); fi
		echo "$tag leader=$leader lpid=$lpid pids=$pids endpoints=$eps mix=$mix scheme=$scheme nogc=$nogc" | tee "$out/$tag.placement"
		marker="$RUNNER_TEMP/m580-$tag-"
		rm -f "${marker}0" "${marker}1"
		RESEARCH580_MARKER="$marker" taskset -c 3 "$BINS/loadgen" -endpoints "$eps" "${tls[@]}" \
			-pids "$pids" -disk "$disk" -workload mixed -mix "$mix" -keys "${KEYS:-1000000}" -conns 6 -streams 96 \
			-warmup 3s -duration "${DURATION:-12s}" -seed "$rep" -out "$out/$tag.json" >"$out/$tag-gen.log" 2>&1 &
		gen=$!
		while [ ! -f "${marker}0" ]; do kill -0 "$gen" 2>/dev/null || break; sleep 0.05; done
		if [ "$nogc" = 1 ]; then kill -USR1 "$lpid"; fi
		grep -E '^(VmRSS|VmHWM)' "/proc/$lpid/status" >"$out/$tag.mem-start" || true
		if [ "$rep" = 1 ]; then
			curl -fsS "${curlt[@]}" "$scheme://$leader/debug/pprof/profile?seconds=10" -o "$out/$tag.pprof" || true
		fi
		while [ ! -f "${marker}1" ]; do kill -0 "$gen" 2>/dev/null || break; sleep 0.05; done
		grep -E '^(VmRSS|VmHWM)' "/proc/$lpid/status" >"$out/$tag.mem-end" || true
		wait "$gen" || echo "$tag loadgen failed" | tee -a "$out/failures"
		cp "$dir/n1.log" "$out/$tag-n1.log"; cp "$dir/n2.log" "$out/$tag-n2.log"; cp "$dir/n3.log" "$out/$tag-n3.log"
		BENCH_CLIENT_SCHEME=$scheme bench/cluster.sh check "$dir" | tee "$out/$tag.check" || echo "$tag leader changed" | tee -a "$out/failures"
		BENCH_CLIENT_SCHEME=$scheme bench/cluster.sh stop "$dir"
		jq -c --arg tag "$tag" '{tag: $tag, ops: .ops, tput: .throughput, leader: .leader_cpu_us_per_op, nodes: .server_cpu_us_per_op_by_node, cores: .server_cores_by_node, client: .client_cpu_us_per_op, errors: .errors}' "$out/$tag.json" | tee -a "$out/summary.jsonl"
	done
done
for p in "$out"/*.pprof; do
	go tool pprof -top "$BINS/tigerstore" "$p" >"${p%.pprof}.top.txt" 2>/dev/null || true
done

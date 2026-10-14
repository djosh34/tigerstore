#!/usr/bin/env bash
# Prototype E (#133): the data-past-RAM measurements, on one runner.
#
#   exp/scale.sh <target> <memlimit> <out dir> [churn]
#
# <target> is a binary in $BINS (baseline = main, candidate = E).
# <memlimit> is a cgroup memory.max per node (e.g. 512M) or "none".
# Steps: preload KEYS keys of VSIZE bytes with a cold point Range run,
# memory per node, a cold LIST run, restart of a killed follower and of a
# killed leader, catch-up of a follower that was down while VSIZE * N
# more bytes were written, and with "churn" a long overwrite run with
# compaction every 5 s, a disk budget, and a follower down in the middle.
set -uo pipefail
target=$1 mem=$2 out=$3 churn=${4:-}
bins=$(realpath "${BINS:-bin}")
certs=$(realpath "${CERTS:?}")
export CERTS=$certs ETCDCTL=$bins/etcdctl
here=$(dirname "$(realpath "$0")")
dir=${DATA:-/tmp/scale}/$target
keys=${KEYS:-820000} vsize=${VSIZE:-2048} ns=${NS:-1640}
layout=(3 0 1 2)
port=${BENCH_PORT:-40000}
mkdir -p "$out"
log() { echo "$(date +%T.%N | cut -c1-12) $*" | tee -a "$out/steps.log"; }
ctl() { "$ETCDCTL" --cacert "$certs/ca.pem" --cert "$certs/client.pem" --key "$certs/client-key.pem" --command-timeout 3s "$@"; }
ep() { echo "127.0.0.1:$((port + 10 * $1))"; }
pidof_node() { pgrep -f -- "--name n$1 --data-dir $dir/n$1 " || true; }
status() { ctl --endpoints "$(ep "$1")" endpoint status -w json 2>/dev/null | jq -r '.[0].Status | "\(.leader) \(.header.member_id // .header.memberId) \(.raftAppliedIndex // .raft_applied_index) \(.header.revision)"'; }
leader_i() {
	for i in 1 2 3; do
		read -r l m _ _ <<<"$(status "$i")" || continue
		if [ -n "${l:-}" ] && [ "$l" = "$m" ]; then echo "$i"; return; fi
	done
}
applied() { status "$1" | awk '{print $3}'; }
cg() {
	# cg <i> <pid>: puts a node into its own memory cgroup.
	[ "$mem" = none ] && return
	local g=/sys/fs/cgroup/e117/n$1
	sudo mkdir -p "$g"
	echo +memory | sudo tee /sys/fs/cgroup/e117/cgroup.subtree_control >/dev/null
	echo "$mem" | sudo tee "$g/memory.max" >/dev/null
	echo 0 | sudo tee "$g/memory.swap.max" >/dev/null
	echo "$2" | sudo tee "$g/cgroup.procs" >/dev/null
}
memreport() {
	# memreport <label>
	for i in 1 2 3; do
		local p
		p=$(pidof_node "$i")
		local line="mem $1 n$i pid=${p:-dead}"
		if [ -n "$p" ]; then
			line+=" $(grep -E '^(VmRSS|VmHWM|RssAnon|RssFile)' /proc/"$p"/status | tr -s ' \n' ' ')"
		fi
		if [ "$mem" != none ]; then
			local g=/sys/fs/cgroup/e117/n$i
			line+=" cg_current=$(cat $g/memory.current) cg_peak=$(cat $g/memory.peak 2>/dev/null) anon=$(awk '/^anon /{print $2}' $g/memory.stat) file=$(awk '/^file /{print $2}' $g/memory.stat) oom_kill=$(awk '/oom_kill /{print $2}' $g/memory.events)"
		fi
		line+=" disk=$(du -sb "$dir/n$i" 2>/dev/null | cut -f1)"
		log "$line"
	done
}
startnode() {
	# startnode <i> <cpu>
	local p
	p=$(BENCH_PORT=$port "$here/node.sh" start "$bins/$target" "$dir" "$1" "$2")
	cg "$1" "$p"
	echo "$p"
}
iowchar() { awk '/^wchar/{print $2}' /proc/"$1"/io; }
lg() {
	# lg <endpoint> <out name> <loadgen flags...>
	local e=$1 name=$2
	shift 2
	local pids
	pids=$(for i in 1 2 3; do pidof_node "$i"; done | paste -sd,)
	taskset -c "${layout[0]}" "$bins/loadgen" -endpoints "$e" -cacert "$certs/ca.pem" -cert "$certs/client.pem" -key "$certs/client-key.pem" \
		-pids "$pids" -label "$target" -value-size "$vsize" -keys "$keys" -namespaces "$ns" -seed 1 -out "$out/$name.json" "$@" 2>&1 | tail -3 | tee -a "$out/steps.log"
}
waitcatch() {
	# waitcatch <i> <t0>: waits until node i applied what the leader has.
	local i=$1 t0=$2 l la a
	for _ in $(seq 6000); do
		l=$(leader_i)
		if [ -n "$l" ] && [ "$l" != "$i" ]; then
			la=$(applied "$l")
			a=$(applied "$i")
			if [ -n "$a" ] && [ -n "$la" ] && [ "$a" -ge "$la" ]; then
				log "n$i caught up to $a (leader n$l at $la) $(awk -v a="$(date +%s.%N)" -v b="$t0" 'BEGIN{printf "%.2f", a-b}') s after start"
				return 0
			fi
		fi
		sleep 0.1
	done
	log "n$i did not catch up"
	return 1
}
waitserve() {
	local i=$1 t0=$2
	for _ in $(seq 6000); do
		if [ -n "$(applied "$i")" ]; then
			log "n$i serving $(awk -v a="$(date +%s.%N)" -v b="$t0" 'BEGIN{printf "%.2f", a-b}') s after start"
			return 0
		fi
		sleep 0.05
	done
	return 1
}

pgrep -f -- "--data-dir $dir/n" | xargs -r kill -9
sudo rm -rf "$dir"
mkdir -p "$(dirname "$dir")"
log "== $target memlimit=$mem keys=$keys vsize=$vsize"
"$here/../bench/cluster.sh" start tigerstore "$bins/$target" "$dir" "${layout[@]:1}" || { log "cluster did not start"; exit 1; }
for i in 1 2 3; do cg "$i" "$(pidof_node "$i")"; done
L=$(leader_i)
log "leader n$L"

# 1. Preload and cold point Range (the loadgen checks every value).
log "preload $keys keys and point Range"
lg "$(ep "$L")" range -workload range -warmup 2s -duration 20s
memreport after-preload
if [ "$mem" != none ] && [ -z "$(pidof_node "$L")" ]; then
	log "leader died"
	memreport dead
	tail -n 5 "$dir"/n*.log >>"$out/steps.log"
	cp "$dir"/n*.log "$out/" 2>/dev/null
	exit 0
fi
grep -h "E bytes" "$dir/n$L.log" | tail -1 | tee -a "$out/steps.log"

# 2. Cold LIST of whole namespaces.
log "LIST"
lg "$(ep "$L")" list -workload list -skip-preload -page 500 -conns 2 -streams 16 -warmup 2s -duration 20s
memreport after-list
grep -h "E bytes" "$dir/n$L.log" | tail -1 | tee -a "$out/steps.log"

# 3. Restart after kill -9: a follower, then the leader.
F=$((L % 3 + 1))
log "kill -9 follower n$F"
kill -9 "$(pidof_node "$F")"
sleep 1
t0=$(date +%s.%N)
startnode "$F" "${layout[2]}" >/dev/null
waitserve "$F" "$t0"
waitcatch "$F" "$t0"
grep -h "E recovery" "$dir/n$F.log" | tail -1 | tee -a "$out/steps.log"
memreport after-follower-restart
log "kill -9 leader n$L"
kill -9 "$(pidof_node "$L")"
sleep 1
t0=$(date +%s.%N)
startnode "$L" "${layout[1]}" >/dev/null
waitserve "$L" "$t0"
waitcatch "$L" "$t0"
grep -h "E recovery" "$dir/n$L.log" | tail -1 | tee -a "$out/steps.log"
memreport after-leader-restart
sleep 2
L=$(leader_i)
log "leader now n$L"
# Pin the new leader as the generator expects: leader CPU 0, the others 1 and 2.
c=1
for i in 1 2 3; do
	if [ "$i" = "$L" ]; then taskset -apc 0 "$(pidof_node "$i")" >/dev/null; else taskset -apc $c "$(pidof_node "$i")" >/dev/null; c=$((c + 1)); fi
done

# 4. Catch-up of a follower that was down past the RAM window.
F=$((L % 3 + 1))
log "catch-up: kill -9 follower n$F, then $((CATCHUP_S)) s of puts"
fcpu=$(taskset -pc "$(pidof_node "$F")" | awk '{print $NF}')
kill -9 "$(pidof_node "$F")"
lg "$(ep "$L")" catchup-load -workload put -warmup 0s -duration "${CATCHUP_S}s"
sleep 2
lp=$(pidof_node "$L")
w0=$(iowchar "$lp")
s0=$(grep -h "E bytes" "$dir/n$L.log" | tail -1)
t0=$(date +%s.%N)
startnode "$F" "$fcpu" >/dev/null
waitcatch "$F" "$t0"
w1=$(iowchar "$lp")
log "catch-up: leader wrote $((w1 - w0)) bytes (wchar) while n$F caught up"
sleep 6
grep -h "E snapshot" "$dir"/n*.log | tail -4 | tee -a "$out/steps.log"
grep -h "E bytes" "$dir/n$L.log" | tail -1 | tee -a "$out/steps.log"
memreport after-catchup

if [ "$churn" = churn ]; then
	# 5. Cleaning under churn: restart every node with a disk budget, then
	# overwrite keys at random with a compaction every 5 s, as
	# kube-apiserver does every 5 min, and take a follower down for a
	# while in the middle.
	log "churn: restart with budget $BUDGET"
	for i in 1 2 3; do kill "$(pidof_node "$i")"; done
	sleep 3
	for i in 1 2 3; do
		pgrep -f -- "--data-dir $dir/n$i " | xargs -r kill -9
		TS_E_DISK_BUDGET=$BUDGET startnode "$i" "${layout[i]}" >/dev/null
	done
	sleep 5
	L=$(leader_i)
	c=1
	for i in 1 2 3; do
		if [ "$i" = "$L" ]; then taskset -apc 0 "$(pidof_node "$i")" >/dev/null; else taskset -apc $c "$(pidof_node "$i")" >/dev/null; c=$((c + 1)); fi
	done
	log "churn leader n$L"
	for i in 1 2 3; do grep -h "E bytes" "$dir/n$i.log" | tail -1 | sed "s/^/before churn n$i /" | tee -a "$out/steps.log"; done
	(
		prev=""
		while sleep 5; do
			r=$(ctl --endpoints "$(ep "$L")" endpoint status -w json 2>/dev/null | jq '.[0].Status.header.revision')
			[ -n "$prev" ] && ctl --endpoints "$(ep "$L")" compact "$prev" >/dev/null 2>&1
			prev=$r
			[ -f "$out/churn.stop" ] && break
		done
	) &
	cpid=$!
	F=$((L % 3 + 1))
	fcpu=$(taskset -pc "$(pidof_node "$F")" | awk '{print $NF}')
	(
		sleep $((CHURN_S / 3))
		log "churn: kill -9 follower n$F"
		kill -9 "$(pidof_node "$F")"
		sleep $((CHURN_S / 3))
		t0=$(date +%s.%N)
		lp=$(pidof_node "$L")
		w0=$(iowchar "$lp")
		TS_E_DISK_BUDGET=$BUDGET startnode "$F" "$fcpu" >/dev/null
		waitcatch "$F" "$t0"
		log "churn catch-up: leader wrote $(($(iowchar "$lp") - w0)) bytes (wchar), with client load running"
	) &
	fpid=$!
	(
		# Disk guard: three nodes share the runner's disk.
		while sleep 1; do
			[ -f "$out/churn.stop" ] && break
			free=$(df -B1 --output=avail "$dir" | tail -1)
			if [ "$free" -lt $((2 << 30)) ]; then
				log "disk guard: $free bytes free, stopping the churn load"
				pkill -f -- "-out $out/churn.json"
				break
			fi
		done
	) &
	gpid=$!
	lg "$(ep "$L")" churn -workload put -warmup 0s -duration "${CHURN_S}s"
	wait "$fpid"
	touch "$out/churn.stop"
	wait "$cpid" "$gpid"
	sleep 6
	for i in 1 2 3; do grep -h "E bytes" "$dir/n$i.log" | tail -1 | sed "s/^/after churn n$i /" | tee -a "$out/steps.log"; done
	grep -h "E clean" "$dir/n$L.log" | tail -3 | tee -a "$out/steps.log"
	grep -h "E snapshot" "$dir"/n*.log | tail -4 | tee -a "$out/steps.log"
	log "revision $(status "$L" | awk '{print $4}')"
	memreport after-churn
fi

for i in 1 2 3; do kill "$(pidof_node "$i")" 2>/dev/null; done
sleep 3
for i in 1 2 3; do grep -h "E bytes" "$dir/n$i.log" | tail -1 | sed "s/^/final n$i /" >>"$out/steps.log"; done
cp "$dir"/n*.log "$out/" 2>/dev/null
pgrep -f -- "--data-dir $dir/n" | xargs -r kill -9
sudo rm -rf "$dir"
log "done"

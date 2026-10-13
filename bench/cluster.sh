#!/usr/bin/env bash
# Starts or stops a 3-node cluster on 127.0.0.1 with client and peer TLS,
# the leader pinned to its own CPUs.
#
#   bench/cluster.sh start <kind> <server binary> <dir> <leader cpus> <cpus> <cpus>
#   bench/cluster.sh check <dir>
#   bench/cluster.sh stop <dir>
#
# <kind> is etcd or tigerstore. Both get the same flags, ports, certificates
# and disk: tigerstore is a drop-in etcd, so it takes etcd's flags. A CPU
# list is a taskset list such as 1 or 2-3. Once a write succeeds, start
# asks for the leader and pins every thread of the leader's process to the
# first list and the followers to the others. It writes the leader's
# client endpoint to <dir>/leader, all endpoints to <dir>/endpoints, the
# node pids (leader first) to <dir>/pids, and the placement to
# <dir>/placement.json. check fails if the leader or the term changed
# since start. stop kills the nodes and deletes <dir>.
#
# CERTS is a directory made by scripts/dev-certs.sh (required). ETCDCTL
# names the etcdctl (default: etcdctl next to the server binary, then on
# PATH). BENCH_PORT is the first port (default 20000, outside runner ephemeral range).
set -euo pipefail

port=${BENCH_PORT:-20000}
metrics_port=${SHAPE_METRICS_PORT:-24000}
etcdctl=${ETCDCTL:-etcdctl}
certs=${CERTS:-}

# ctl runs etcdctl against $endpoints with the client certificate.
ctl() {
	"$etcdctl" --endpoints "$endpoints" --cacert "$certs/ca.pem" \
		--cert "$certs/client.pem" --key "$certs/client-key.pem" --command-timeout 2s "$@"
}

# status prints the leader's endpoint and the term, and fails unless every
# node names the same single leader.
status() {
	local st leader
	st=$(ctl endpoint status -w json)
	# etcdctl versions name fields in snake or camel case.
	leader=$(jq -r 'map(select(.Status.leader == (.Status.header.member_id // .Status.header.memberId))) | if length == 1 then .[0].Endpoint else "" end' <<<"$st")
	if [ -z "$leader" ] || [ "$(jq '[.[].Status.leader] | unique | length' <<<"$st")" -ne 1 ]; then
		echo "no single leader: $st" >&2
		return 1
	fi
	echo "${leader#https://} $(jq '[.[].Status | (.raft_term // .raftTerm)] | max' <<<"$st")"
}

# place finds the leader, pins every thread of each node to its CPUs, the
# leader to the first list, and records where everything is.
place() {
	local kind=$1 dir=$2 endpoints=$3 pids=$4
	shift 4
	local cpus=("$@") leader term st
	read -r -a pid <<<"${pids//,/ }"
	read -r -a ep <<<"${endpoints//,/ }"
	st=$(status) || exit 1
	read -r leader term <<<"$st"
	local order=() i
	for i in 0 1 2; do
		if [ "${ep[i]}" = "$leader" ]; then
			order=("$i" "${order[@]}")
		else
			order+=("$i")
		fi
	done
	local placed=""
	for i in 0 1 2; do
		taskset -apc "${cpus[i]}" "${pid[order[i]]}" >/dev/null
		placed+="${placed:+,}${pid[order[i]]}"
	done
	echo "$leader" >"$dir/leader"
	echo "$term" >"$dir/term"
	echo "$placed" >"$dir/pids"
	jq -n --arg kind "$kind" --arg leader "$leader" --arg term "$term" --arg pids "$placed" --arg cpus "${cpus[*]}" \
		'{kind: $kind, leader: $leader, term: $term, pids_leader_first: $pids, cpus_leader_first: $cpus}' >"$dir/placement.json"
}

start() {
	local kind=$1 bin=$2 dir=$3
	shift 3
	local cpus=("$@")
	if [ "${#cpus[@]}" -ne 3 ]; then
		echo "need 3 CPU lists" >&2
		exit 2
	fi
	case $kind in
	etcd | tigerstore) ;;
	*)
		echo "unknown kind $kind" >&2
		exit 2
		;;
	esac
	if [ -e "$dir" ]; then
		echo "$dir exists; stop the old cluster first" >&2
		exit 2
	fi
	mkdir -p "$dir"

	certs=${CERTS:?CERTS must name a dev-certs.sh directory}
	local initial="" endpoints=""
	for i in 1 2 3; do
		initial+="${initial:+,}n$i=https://127.0.0.1:$((port + 10 * i + 1))"
		endpoints+="${endpoints:+,}127.0.0.1:$((port + 10 * i))"
	done

	local pids=""
	for i in 1 2 3; do
		local client="https://127.0.0.1:$((port + 10 * i))"
		local peer="https://127.0.0.1:$((port + 10 * i + 1))"
		# etcd-only flags: tigerstore always verifies peer certificates,
		# has no quota and logs little. The quota is etcd's suggested
		# maximum, so long write runs do not hit etcd's 2 GiB default.
		local extra=()
		if [ "$kind" = etcd ]; then
			extra=(--initial-advertise-peer-urls "$peer" --initial-cluster-state new
				--quota-backend-bytes $((8 << 30)) --log-level warn --peer-client-cert-auth
				--listen-metrics-urls "http://127.0.0.1:$((metrics_port + i))" --enable-pprof)
		fi
		shape_debug="127.0.0.1:$((metrics_port + i))"
		if [ "${SHAPE_OBSERVE:-1}" = 0 ]; then shape_debug=""; fi
		GOMAXPROCS=2 SHAPE_DEBUG="$shape_debug" taskset -c "${cpus[i - 1]}" "$bin" \
			--name "n$i" \
			--data-dir "$dir/n$i" \
			--listen-client-urls "$client" \
			--advertise-client-urls "$client" \
			--listen-peer-urls "$peer" \
			--initial-cluster "$initial" \
			--initial-cluster-token bench \
			--cert-file "$certs/node$i.pem" --key-file "$certs/node$i-key.pem" \
			--trusted-ca-file "$certs/ca.pem" --client-cert-auth \
			--peer-cert-file "$certs/node$i.pem" --peer-key-file "$certs/node$i-key.pem" \
			--peer-trusted-ca-file "$certs/ca.pem" \
			"${extra[@]}" \
			>"$dir/n$i.log" 2>&1 &
		pids+="${pids:+,}$!"
	done
	echo "$endpoints" >"$dir/endpoints"
	echo "$pids" >"$dir/pids"

	etcdctl=${ETCDCTL:-$(dirname "$bin")/etcdctl}
	if [ ! -x "$etcdctl" ]; then
		etcdctl=etcdctl
	fi
	for _ in $(seq 100); do
		if ctl put /bench/ready 1 >/dev/null 2>&1; then
			place "$kind" "$dir" "$endpoints" "$pids" "${cpus[@]}"
			return 0
		fi
		for pid in ${pids//,/ }; do
			if ! kill -0 "$pid" 2>/dev/null; then
				echo "a $kind node exited; logs:" >&2
				tail -n 20 "$dir"/n*.log >&2
				exit 1
			fi
		done
		sleep 0.3
	done
	echo "$kind cluster did not accept a write within 30s; logs:" >&2
	tail -n 20 "$dir"/n*.log >&2
	exit 1
}

check() {
	local dir=$1 leader term st
	endpoints=$(cat "$dir/endpoints")
	st=$(status) || return 1
	read -r leader term <<<"$st"
	if [ "$leader" != "$(cat "$dir/leader")" ] || [ "$term" != "$(cat "$dir/term")" ]; then
		echo "the leader changed during the run: $(cat "$dir/leader") term $(cat "$dir/term"), now $leader term $term" >&2
		return 1
	fi
}

stop() {
	local dir=$1
	if [ -f "$dir/pids" ]; then
		local pids pid
		pids=$(tr ',' ' ' <"$dir/pids")
		for pid in $pids; do
			kill "$pid" 2>/dev/null || true
		done
		for pid in $pids; do
			for _ in $(seq 100); do
				kill -0 "$pid" 2>/dev/null || break
				sleep 0.1
			done
			kill -9 "$pid" 2>/dev/null || true
		done
	fi
	rm -rf "$dir"
}

case ${1:-} in
start)
	shift
	start "$@"
	;;
check)
	check "$2"
	;;
stop)
	stop "$2"
	;;
*)
	echo "usage: $0 start <kind> <bin> <dir> <leader cpus> <cpus> <cpus> | check <dir> | stop <dir>" >&2
	exit 2
	;;
esac

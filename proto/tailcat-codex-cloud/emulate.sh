#!/usr/bin/env bash
# Emulates two Codex Cloud task VMs as network namespaces with the egress
# Codex documents: no route out at all, only an HTTP CONNECT proxy (here on
# 10.99.{0,1}.1:8088, standing in for proxy:8088). No UDP leaves either
# namespace, and the two cannot reach each other. Through sudo. (#630)
#
#   emulate.sh up | down | run <ns> <cmd...>
#
# run puts HTTPS_PROXY in the command's environment and a private
# XDG_CONFIG_HOME so saved tailcat keys don't leak between namespaces.
set -euo pipefail
out=${OUT:-$(dirname "$0")/out}

case ${1:-} in
up)
	for i in 0 1; do
		ns=tc$i
		sudo ip netns add $ns
		sudo ip link add $ns-h type veth peer name $ns-n
		sudo ip link set $ns-n netns $ns
		sudo ip addr add 10.99.$i.1/24 dev $ns-h
		sudo ip link set $ns-h up
		sudo ip -n $ns addr add 10.99.$i.2/24 dev $ns-n
		sudo ip -n $ns link set $ns-n up
		sudo ip -n $ns link set lo up
		# no default route: nothing but the proxy is reachable
		nohup "$out/connectproxy" -listen 10.99.$i.1:8088 >"$out/proxy-$ns.log" 2>&1 &
	done
	;;
down)
	pkill -f "$out/connectproxy" || true
	for i in 0 1; do sudo ip netns del tc$i 2>/dev/null || true; sudo ip link del tc$i-h 2>/dev/null || true; done
	;;
run)
	ns=$2
	shift 2
	i=${ns#tc}
	mkdir -p "$out/cfg-$ns"
	exec sudo ip netns exec "$ns" sudo -u "$USER" env HOME="$HOME" PATH="$PATH" \
		HTTPS_PROXY=http://10.99.$i.1:8088 HTTP_PROXY=http://10.99.$i.1:8088 \
		XDG_CONFIG_HOME="$out/cfg-$ns" "$@"
	;;
*)
	echo "usage: $0 up|down|run <ns> <cmd...>" >&2
	exit 2
	;;
esac

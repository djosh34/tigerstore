#!/usr/bin/env bash
# Delays node-to-node traffic of bench/cluster.sh on lo, and leaves client
# traffic alone, so the bench measures the stores with latency between
# nodes (#395, #400). Through sudo.
#
#   bench/netem.sh on <rtt ms>
#   bench/netem.sh probe
#   bench/netem.sh show
#   bench/netem.sh off
#
# Prototype for #524: delay at ingress, not at the sending socket's
# egress. tc-netem(8) warns that egress netem interacts with TCP Small
# Queues. Redirect only packets to/from peer ports into an IFB, so clients
# stay undelayed. On lo both directions pass ingress, each delayed by half
# the RTT. DIAG524_EGRESS=1 restores the original harness for comparison.
# probe times a TCP connect to peer and client ports without a store.
set -euo pipefail

port=${BENCH_PORT:-20000}

case ${1:-} in
on)
	rtt=${2:?on needs an RTT in ms}
	half=$(awk -v r="$rtt" 'BEGIN { printf "%.3fms", r / 2 }')
	"$0" off
	if [ "${DIAG524_EGRESS:-0}" != 1 ]; then
		sudo modprobe ifb
		sudo ip link add ifb524 type ifb
		sudo ip link set ifb524 up
		sudo tc qdisc add dev ifb524 root netem delay "$half" limit 1000000
		sudo tc qdisc add dev lo clsact
		for n in 1 2 3; do
			for dir in dport sport; do
				sudo tc filter add dev lo ingress protocol ip prio 1 u32 match ip "$dir" $((port + 10 * n + 1)) 0xffff action mirred egress redirect dev ifb524
			done
		done
		exit 0
	fi
	sudo tc qdisc add dev lo root handle 1: prio bands 2 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
	sudo tc qdisc add dev lo parent 1:1 handle 10: pfifo limit 100000
	sudo tc qdisc add dev lo parent 1:2 handle 20: netem delay "$half" limit 1000000
	for n in 1 2 3; do
		for dir in dport sport; do
			sudo tc filter add dev lo parent 1: protocol ip prio 1 u32 match ip "$dir" $((port + 10 * n + 1)) 0xffff flowid 1:2
		done
	done
	;;
probe)
	python3 -I - "$((port + 21))" "$((port + 20))" <<'PY'
import socket, statistics, sys, time
for p in map(int, sys.argv[1:]):
    srv = socket.create_server(("127.0.0.1", p))
    times = []
    for _ in range(20):
        t = time.perf_counter()
        c = socket.create_connection(("127.0.0.1", p))
        times.append((time.perf_counter() - t) * 1000)
        c.close()
        srv.accept()[0].close()
    srv.close()
    print(f"port {p}: median connect {statistics.median(times):.3f} ms")
PY
	;;
show)
	sudo tc -s qdisc show dev lo
	sudo tc filter show dev lo
	sudo tc filter show dev lo ingress 2>/dev/null || true
	sudo tc -s qdisc show dev ifb524 2>/dev/null || true
	;;
off)
	sudo tc qdisc del dev lo root 2>/dev/null || true
	sudo tc qdisc del dev lo clsact 2>/dev/null || true
	sudo ip link del ifb524 2>/dev/null || true
	;;
*)
	echo "usage: $0 on <rtt ms> | probe | show | off" >&2
	exit 2
	;;
esac

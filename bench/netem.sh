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
# on puts a prio qdisc at the root of lo: packets to or from a peer port
# (BENCH_PORT + 10 * n + 1 for nodes 1 to 3) go to band 2, a netem qdisc
# that delays each packet by half the RTT; every other packet goes to band
# 1, a plain FIFO. On lo both directions of a connection leave through the
# same root, so a round trip between two nodes is delayed by the whole RTT.
# The stores dial from ephemeral ports, so each packet matches one port.
# The netem limit is high so the queue never drops at the bench's rates.
# on also starts every TCP receive buffer at 1 MiB instead of 128 KiB,
# keeping the kernel's minimum and maximum: with a delay on lo, autotuning
# could leave a peer connection's window near 107 KiB, which caps it at
# about 10 MB/s at 10 ms RTT. A follower behind such a link fell behind
# the others, and the writes it took timed out after 7 s, in etcd's runs
# as in tigerstore's (#524).
# RTT 0 installs the same qdiscs with no delay, so every RTT has the same
# path. probe prints the median TCP connect time, one round trip, to the
# peer and the client port of node 2 while nothing else listens there.
set -euo pipefail

port=${BENCH_PORT:-20000}

case ${1:-} in
on)
	rtt=${2:?on needs an RTT in ms}
	half=$(awk -v r="$rtt" 'BEGIN { printf "%.3fms", r / 2 }')
	read -r low _ high < <(sysctl -n net.ipv4.tcp_rmem)
	sudo sysctl -q -w net.ipv4.tcp_rmem="$low 1048576 $high"
	sudo tc qdisc del dev lo root 2>/dev/null || true
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
	;;
off)
	sudo tc qdisc del dev lo root 2>/dev/null || true
	;;
*)
	echo "usage: $0 on <rtt ms> | probe | show | off" >&2
	exit 2
	;;
esac

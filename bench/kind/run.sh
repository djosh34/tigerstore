#!/usr/bin/env bash
# Kubernetes-level comparison (#146): kube-burner's create, update and
# mass-delete churn on a one-node kind cluster whose API server uses each
# store in turn as external etcd: a 3-node TLS cluster on the host, started
# by bench/cluster.sh as the benchmarks start it. REPS repetitions,
# alternating the order; a fresh store and kind cluster for every run.
#
#   bench/kind/run.sh <etcd binary> <tigerstore binary> <out dir>
#
# KUBE_BURNER names kube-burner, ITER the create iterations (namespaces),
# CERTS a dev-certs.sh directory made with CERT_IPS=172.30.0.1.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
bins=$(mktemp -d)
cp "$1" "$bins/etcd"
cp "$2" "$bins/tigerstore"
cp "$(dirname "$1")/etcdctl" "$bins/etcdctl"
out=$3
iter=${ITER:-1000}
reps=${REPS:-3}
certs=$(realpath "${CERTS:?}")
gateway=172.30.0.1
port=${BENCH_PORT:-20000}
export CERTS=$certs ETCDCTL=$bins/etcdctl BENCH_LISTEN_IP=$gateway
mkdir -p "$out"
work=$(mktemp -d)
cp "$here"/*.yaml "$work/"
sed -i "s/ITER/$iter/" "$work/churn.yaml"
endpoints=""
for i in 1 2 3; do endpoints+="${endpoints:+, }https://$gateway:$((port + 10 * i))"; done
sed -i "s|CERTS|$certs|; s|ENDPOINTS|$endpoints|" "$work/cluster.yaml"
# kind takes an existing network named kind; this one has a known gateway
# for the stores to listen on.
docker network create kind --subnet 172.30.0.0/16 --gateway "$gateway" >/dev/null
for rep in $(seq "$reps"); do
	order="etcd tigerstore"
	if ((rep % 2 == 0)); then order="tigerstore etcd"; fi
	for store in $order; do
		dir=$work/data-$store
		"$here/../cluster.sh" start "$store" "$bins/$store" "$dir" 0 1 2
		kind create cluster --name bench --config "$work/cluster.yaml" --wait 5m >"$out/$store-$rep.kind.log" 2>&1 || {
			kind export logs --name bench "$out/$store-$rep-kind-logs" >/dev/null 2>&1 || true
			exit 1
		}
		# The kind node's CPU and memory every 10 s, to see what failed.
		while sleep 10; do docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' bench-control-plane; done >"$out/$store-$rep.stats" 2>&1 &
		sampler=$!
		start=$(date +%s.%N)
		(cd "$work" && "$KUBE_BURNER" init -c churn.yaml --uuid "$store-$rep" --timeout 30m) >"$out/$store-$rep.burner.log" 2>&1 || true
		end=$(date +%s.%N)
		kill "$sampler"
		# kube-burner logs a failed request and goes on; such a run is
		# invalid. Keep what shows why.
		if grep -q 'level=error' "$out/$store-$rep.burner.log"; then
			echo "$store $rep: invalid, kube-burner logged errors" | tee -a "$out/summary.txt"
			{
				docker ps -a
				docker inspect bench-control-plane --format '{{json .State}}'
				free -m
				sudo dmesg | tail -100
			} >"$out/$store-$rep.failure" 2>&1
			kind export logs --name bench "$out/$store-$rep-kind-logs" >/dev/null 2>&1 || true
		fi
		kubectl get --raw /metrics | grep -E '^etcd_request_duration_seconds_(sum|count)' >"$out/$store-$rep.apiserver" || true
		echo "$store $rep total $(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.1f", b-a}') s" | tee -a "$out/summary.txt"
		grep -E "took" "$out/$store-$rep.burner.log" | sed -E "s/.*msg=\"([^\"]*)\".*/$store $rep \1/" | tee -a "$out/summary.txt"
		# The API server's view of the store: mean latency per operation.
		awk -v n="$store $rep" '/_sum/ {match($0, /operation="[^"]*"/); o = substr($0, RSTART + 11, RLENGTH - 12); s[o] += $2}
			/_count/ {match($0, /operation="[^"]*"/); o = substr($0, RSTART + 11, RLENGTH - 12); c[o] += $2}
			END {printf "%s apiserver to store, mean ms (requests):", n; for (o in c) if (c[o] >= 100) printf " %s %.2f (%d)", o, 1000 * s[o] / c[o], c[o]; print ""}' \
			"$out/$store-$rep.apiserver" | tee -a "$out/summary.txt"
		kind delete cluster --name bench >/dev/null 2>&1
		"$here/../cluster.sh" check "$dir" || echo "$store $rep: the leader changed" | tee -a "$out/summary.txt"
		cp "$dir/n1.log" "$out/$store-$rep-n1.log"
		"$here/../cluster.sh" stop "$dir"
	done
done

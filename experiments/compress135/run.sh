#!/usr/bin/env bash
# Question (#135): does compressing Kubernetes values pay off for tigerstore?
# Builds a kind 1.37 cluster, runs workload.py through kube-apiserver while
# recording every etcd write with etcdctl watch, dumps the live keys, deletes
# the cluster and measures. Raw values stay in $PRIVATE and are deleted;
# only aggregates go to results/.
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")"
OUT="$PWD/results"
mkdir -p "$OUT"
PRIVATE=$(mktemp -d)
cleanup() {
  [[ -n "${WATCH_PID:-}" ]] && kill "$WATCH_PID" 2>/dev/null || true
  kind delete cluster --name compression >/dev/null 2>&1 || true
  rm -rf "$PRIVATE"
}
trap cleanup EXIT
{
  date -u; git rev-parse HEAD; go version; uname -a; lscpu; free -b; df -h /tmp; lsblk -o NAME,SIZE,ROTA,MODEL || true
  printf 'Runner image: %s %s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
} > "$OUT/environment.txt"
dpkg -s libzstd-dev >/dev/null 2>&1 || sudo apt-get install -y -q libzstd-dev >/dev/null
dpkg -s libzstd-dev | grep -i '^version' >> "$OUT/environment.txt"
ARCH=$(uname -m)
case "$ARCH" in aarch64) ARCH=arm64;; x86_64) ARCH=amd64;; *) exit 1;; esac
curl -fsSL --retry 3 "https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-$ARCH" -o "$PRIVATE/kind"
chmod +x "$PRIVATE/kind"
export PATH="$PRIVATE:$PATH"
kind version >> "$OUT/environment.txt"
cat > "$PRIVATE/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    maxPods: 250
YAML
kind create cluster --name compression --config "$PRIVATE/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --wait 120s
kubectl version -o json > "$OUT/kubernetes-version.json"
ETCD="kubectl -n kube-system exec etcd-compression-control-plane -- etcdctl --endpoints=https://127.0.0.1:2379 --cacert=/etc/kubernetes/pki/etcd/ca.crt --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt --key=/etc/kubernetes/pki/etcd/healthcheck-client.key"
kubectl -n kube-system exec etcd-compression-control-plane -- etcd --version >> "$OUT/environment.txt"
docker exec compression-control-plane crictl pull registry.k8s.io/pause:3.10.1
# Every write from here on, in revision order. This file can hold credentials.
$ETCD watch --prefix '' -w json > "$PRIVATE/trace.json" &
WATCH_PID=$!
sleep 2
python3 workload.py | tee "$OUT/workload.jsonl"
$ETCD get '' --prefix --write-out=json > "$PRIVATE/snap.json"
sleep 3
kill "$WATCH_PID"; wait "$WATCH_PID" 2>/dev/null || true; WATCH_PID=
ls -l "$PRIVATE"/*.json | awk '{print $5, $9}' | sed "s#$PRIVATE/##" >> "$OUT/environment.txt"
kind delete cluster --name compression
cd bench
go build -o "$PRIVATE/bench" .
go version -m "$PRIVATE/bench" >> "$OUT/environment.txt"
CPU=$(python3 -c 'import os; print(max(os.sched_getaffinity(0)))')
mkdir -p "$PRIVATE/scratch"
printf 'Benchmark CPU %s (taskset), GOMAXPROCS=1, cluster deleted before timings\n' "$CPU" >> "$OUT/environment.txt"
GOMAXPROCS=1 taskset -c "$CPU" timeout 1500 "$PRIVATE/bench" -snap "$PRIVATE/snap.json" -trace "$PRIVATE/trace.json" -scratch "$PRIVATE/scratch" -reps 5 | tee "$OUT/report.txt"

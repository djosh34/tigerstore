#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
DEADLINE=1791928800
if (( $(date -u +%s) >= DEADLINE )); then echo 'Deadline reached'; exit 1; fi
OUT="$PWD/results"
mkdir -p "$OUT"
PRIVATE=$(mktemp -d)
cleanup() {
  kind delete cluster --name compression >/dev/null 2>&1 || true
  rm -rf "$PRIVATE"
}
trap cleanup EXIT
{
  date -u
  git rev-parse HEAD
  go version
  uname -a
  lscpu
  free -b
  printf 'Runner image: %s %s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
} > "$OUT/environment.txt"
ARCH=$(uname -m)
case "$ARCH" in aarch64) ARCH=arm64;; x86_64) ARCH=amd64;; *) exit 1;; esac
curl -fsSL --retry 3 "https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-$ARCH" -o "$PRIVATE/kind"
chmod +x "$PRIVATE/kind"
export PATH="$PRIVATE:$PATH"
kind version >> "$OUT/environment.txt"
# The default kind kubeadm etcd is stock, with its ordinary storage encoding.
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
kubectl -n kube-system get pod etcd-compression-control-plane -o jsonpath='{.spec.containers[0].image}' >> "$OUT/environment.txt"
printf '\n' >> "$OUT/environment.txt"
kubectl -n kube-system exec etcd-compression-control-plane -- etcd --version >> "$OUT/environment.txt"
# Ensure the tiny workload image is present before scheduling 120 replicas.
docker exec compression-control-plane crictl pull registry.k8s.io/pause:3.10.1
python3 workload.py | tee "$OUT/workload.jsonl"
kubectl -n kube-system exec etcd-compression-control-plane -- etcdctl \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt \
  --key=/etc/kubernetes/pki/etcd/healthcheck-client.key \
  get '' --prefix --write-out=json > "$PRIVATE/dump.json"
# This file can include bootstrap tokens and service-account signing material.
# Never print or upload it, or a trained dictionary containing its fragments.
sha256sum "$PRIVATE/dump.json" | cut -d ' ' -f 1 > "$OUT/dump.sha256"
kind delete cluster --name compression
cd codec
go test ./...
go build -o "$PRIVATE/compressbench" .
go version -m "$PRIVATE/compressbench" >> "$OUT/environment.txt"
CPU=$(python3 -c 'import os; print(min(os.sched_getaffinity(0)))')
printf 'Compression CPU: %s; GOMAXPROCS=1; cluster deleted before timings\n' "$CPU" >> "$OUT/environment.txt"
# Interface supplied by the benchmark module.
GOMAXPROCS=1 taskset -c "$CPU" "$PRIVATE/compressbench" -input "$PRIVATE/dump.json" -out "$OUT"

#!/usr/bin/env bash
# Actions only. Reuse #135's application fleet and capture real stored
# protobuf and CRD JSON values. The dump stays private to this runner.
set -euo pipefail
out=$1
private=$(mktemp -d)
cleanup() {
  kind delete cluster --name compression
  rm -rf "$private"
}
curl -fsSL --retry 3 https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-arm64 -o "$private/kind"
chmod +x "$private/kind"
export PATH="$private:$PATH"
trap cleanup EXIT
cat > "$private/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    maxPods: 250
YAML
kind create cluster --name compression --config "$private/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --wait 120s
docker exec compression-control-plane crictl pull registry.k8s.io/pause:3.10.1
python3 -B bench/compression-workload.py
kubectl -n kube-system exec etcd-compression-control-plane -- etcdctl \
  --endpoints=https://127.0.0.1:2379 --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt --key=/etc/kubernetes/pki/etcd/healthcheck-client.key \
  get '' --prefix --write-out=json > "$out"
chmod 600 "$out"

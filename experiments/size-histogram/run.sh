#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")"
DEADLINE=1791928800
before_deadline() {
  if (( $(date -u +%s) >= DEADLINE )); then
    echo 'Deadline reached'
    exit 1
  fi
}
before_deadline
umask 077
OUT="$PWD/results"
mkdir -p "$OUT"
PRIVATE=$(mktemp -d)
export KUBECONFIG="$PRIVATE/kubeconfig"
cleanup() {
  kind delete cluster --name compression >/dev/null 2>&1 || true
  rm -rf "$PRIVATE"
}
trap cleanup EXIT
{
  date -u +%FT%TZ
  git rev-parse HEAD
  go version
  uname -a
  lscpu
  free -b
  printf 'Runner image: %s %s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
} > "$OUT/environment.txt"
test "$(uname -m)" = aarch64
before_deadline
GO111MODULE=off go test ./export
GO111MODULE=off CGO_ENABLED=0 go build -trimpath -o "$PRIVATE/size-export" ./export
python3 -m unittest discover -s . -p 'test_*.py'
before_deadline
curl -fsSL --retry 3 https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-arm64 -o "$PRIVATE/kind"
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
before_deadline
kind create cluster --name compression --config "$PRIVATE/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --wait 120s
kubectl version -o json > "$OUT/kubernetes-version.json"
{
  kubectl -n kube-system get pod etcd-compression-control-plane -o jsonpath='{.spec.containers[0].image}'
  printf '\n'
  kubectl -n kube-system exec etcd-compression-control-plane -- etcd --version
} >> "$OUT/environment.txt"
# Install through docker exec so the running node's mount namespace owns the file.
docker exec -i compression-control-plane bash -c \
  'umask 077; cat > /usr/local/bin/size-export; chmod 700 /usr/local/bin/size-export' < "$PRIVATE/size-export"
printf '%s' '{"count":1,"kvs":[{"key":"L3JlZ2lzdHJ5L3BvZHMvbi9w","value":"YWJj"}]}' \
  | docker exec -i compression-control-plane /usr/local/bin/size-export > "$PRIVATE/export-check.csv"
printf 'resource,value_bytes\npods,3\n' > "$PRIVATE/expected.csv"
cmp "$PRIVATE/export-check.csv" "$PRIVATE/expected.csv"
docker exec compression-control-plane crictl pull registry.k8s.io/pause:3.10.1
before_deadline
# The exact compression pre-work workload. Stdout contains counters only.
python3 ../compress50/workload.py | tee "$OUT/workload.jsonl"
before_deadline
ETCD_ID=$(docker exec compression-control-plane crictl ps --name etcd -q)
[[ "$ETCD_ID" =~ ^[a-f0-9]+$ ]]
# Raw JSON stays in a pipe and process memory INSIDE the disposable kind node.
# Only resource names and byte lengths cross docker exec's stdout boundary.
# No shell tracing, dump file, value hash or container log export.
docker exec compression-control-plane bash -o pipefail -c '
  ulimit -c 0
  crictl exec "$1" etcdctl \
    --endpoints=https://127.0.0.1:2379 \
    --cacert=/etc/kubernetes/pki/etcd/ca.crt \
    --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt \
    --key=/etc/kubernetes/pki/etcd/healthcheck-client.key \
    get "" --prefix --write-out=json | /usr/local/bin/size-export
' -- "$ETCD_ID" > "$PRIVATE/lengths.csv"
python3 histogram.py "$PRIVATE/lengths.csv" "$OUT/histogram.csv"
date -u +%FT%TZ >> "$OUT/environment.txt"
# Length rows are safe, but only the smaller aggregate histogram is uploaded.
rm -f "$PRIVATE/lengths.csv"
kind delete cluster --name compression

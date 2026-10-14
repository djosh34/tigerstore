#!/usr/bin/env bash
# Question (#136): which values does kube-apiserver read from etcd?
# kind 1.37 with etcd at debug log level (one "request stats" line per unary
# request, values redacted by etcd), plus a watch with prev_kv reduced to keys,
# revisions and sizes. Raw values never touch the disk outside etcd.
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")"
OUT="$PWD/results"
mkdir -p "$OUT"
PRIVATE=$(mktemp -d)
cleanup() {
  [[ -n "${WATCH_PID:-}" ]] && kill "$WATCH_PID" 2>/dev/null || true
  kind delete cluster --name cache >/dev/null 2>&1 || true
  rm -rf "$PRIVATE"
}
trap cleanup EXIT
{ date -u; git rev-parse HEAD; uname -a; nproc; free -b; printf 'Runner image: %s %s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"; } > "$OUT/environment.txt"
curl -fsSL --retry 3 "https://kind.sigs.k8s.io/dl/v0.33.0/kind-linux-arm64" -o "$PRIVATE/kind"
chmod +x "$PRIVATE/kind"
export PATH="$PRIVATE:$PATH"
cat > "$PRIVATE/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    maxPods: 250
    containerLogMaxSize: 4Gi
    containerLogMaxFiles: 2
YAML
kind create cluster --name cache --config "$PRIVATE/kind.yaml" \
  --image kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5 --wait 120s
kubectl version -o json > "$OUT/kubernetes-version.json"
N=cache-control-plane
docker exec $N cat /etc/kubernetes/manifests/kube-apiserver.yaml | grep -- ' - --' > "$OUT/apiserver-flags.txt"
docker exec $N sed -i 's/^    - etcd$/    - etcd\n    - --log-level=debug/' /etc/kubernetes/manifests/etcd.yaml
docker exec $N grep -n -- 'log-level' /etc/kubernetes/manifests/etcd.yaml >> "$OUT/environment.txt"
sleep 20
for i in $(seq 60); do kubectl get --raw /readyz >/dev/null 2>&1 && break; sleep 2; done
kubectl -n kube-system get pods -o wide >> "$OUT/environment.txt"
ETCD="kubectl -n kube-system exec -i etcd-$N -- etcdctl --endpoints=https://127.0.0.1:2379 --cacert=/etc/kubernetes/pki/etcd/ca.crt --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt --key=/etc/kubernetes/pki/etcd/healthcheck-client.key"
$ETCD version >> "$OUT/environment.txt"
docker exec $N crictl pull registry.k8s.io/pause:3.10.1 >/dev/null
echo "{\"trace_start\": $(date +%s.%N)}" > "$OUT/marks.jsonl"
$ETCD get '' --prefix -w json | python3 watchsizes.py > "$OUT/snapshot.jsonl"
REV=$(head -1 "$OUT/snapshot.jsonl" | python3 -c 'import json,sys; print(json.load(sys.stdin)["snap_rev"])')
( $ETCD watch --prefix '' --prev-kv --rev $((REV + 1)) -w json | python3 watchsizes.py > "$OUT/writes.jsonl" ) &
WATCH_PID=$!
sleep 2
echo "{\"workload_start\": $(date +%s.%N)}" >> "$OUT/marks.jsonl"
python3 workload.py | tee -a "$OUT/marks.jsonl" || echo "{\"workload_failed\": 1}" >> "$OUT/marks.jsonl"
echo "{\"churn_start\": $(date +%s.%N)}" >> "$OUT/marks.jsonl"
CHURN_SECONDS=${CHURN_SECONDS:-300} bash churn.sh | tee -a "$OUT/marks.jsonl"
echo "{\"idle_start\": $(date +%s.%N)}" >> "$OUT/marks.jsonl"
sleep 60
echo "{\"trace_end\": $(date +%s.%N)}" >> "$OUT/marks.jsonl"
kubectl get --raw /metrics | grep -E '^(apiserver_storage_objects|etcd_requests_total|apiserver_watch_cache_|apiserver_cache_list_total|apiserver_storage_list_)' > "$OUT/apiserver-metrics.txt" || true
docker exec $N sh -c 'curl -s http://127.0.0.1:2381/metrics' | grep -E '^(etcd_debugging_mvcc_watch|etcd_mvcc_|grpc_server_handled_total)' > "$OUT/etcd-metrics.txt" || true
pkill -f 'etcdctl --endpoints' || true
kill "$WATCH_PID" 2>/dev/null || true; WATCH_PID=
# etcd redacts Put and Txn values in these lines; the check below fails the job if any value text slipped in.
docker exec $N sh -c 'cat /var/log/pods/kube-system_etcd-*/etcd/*.log' | grep '"request stats"' > "$PRIVATE/stats.log" || true
if grep -q 'value:"' "$PRIVATE/stats.log"; then echo "value text in log, not uploading"; exit 1; fi
wc -l "$PRIVATE/stats.log" >> "$OUT/environment.txt"
gzip -c "$PRIVATE/stats.log" > "$OUT/requests.log.gz"
gzip -f "$OUT/writes.jsonl" "$OUT/snapshot.jsonl"
ls -l "$OUT" >> "$OUT/environment.txt"

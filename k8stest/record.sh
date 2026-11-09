#!/usr/bin/env bash
# Records the etcd calls of one cluster shape (k8stest/shapes.json) on a
# Kubernetes cluster that k8stest/boot.sh started with RECORD set:
#
#   RECORD=<log> bash k8stest/boot.sh
#   bash k8stest/record.sh <shape> <seconds>
#
# kwok v0.8.0 runs the shape's fake nodes, and the shape's pods go to them:
# the two real workers get a NoSchedule taint. load.py sets the shape up,
# the cluster settles for a minute, then load.py churns it for <seconds>.
# The window of the recording is the churn: its start and end, in ns since
# the Unix epoch, go to window-start and window-end in the results. Then
# the recorder stops, which closes its log. Run only on a disposable
# Actions runner.
set -euo pipefail
root=${K8S_ROOT:?}
shape=$1
seconds=$2
results=$root/results
export PATH="$root/bin:$PATH"
nodes=$(jq -er --arg s "$shape" '.[$s].nodes' k8stest/shapes.json)

for f in kwok.yaml stage-fast.yaml; do
  curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/$f" -o "$results/$f"
done
sha256sum "$results/kwok.yaml" "$results/stage-fast.yaml" | tee "$results/kwok.sha256"
kubectl apply -f "$results/kwok.yaml"
kubectl apply -f "$results/stage-fast.yaml"
kubectl -n kube-system rollout status deployment/kwok-controller --timeout=300s
kubectl get stages -o yaml >"$results/kwok-stages.yaml"
kubectl taint nodes tigerstore-worker tigerstore-worker2 bench=real:NoSchedule

python3 - "$nodes" <<'PY' | kubectl create -f -
import json, sys
items = []
for i in range(int(sys.argv[1])):
    name = f"kwok-{i:04d}"
    items.append({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "annotations": {"kwok.x-k8s.io/node": "fake"},
                     "labels": {"kubernetes.io/hostname": name, "kubernetes.io/os": "linux",
                                "kubernetes.io/arch": "arm64", "type": "kwok"}},
        "status": {"capacity": {"cpu": "32", "memory": "128Gi", "pods": "110"},
                   "allocatable": {"cpu": "32", "memory": "128Gi", "pods": "110"}},
    })
print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}))
PY
kubectl wait --for=condition=Ready node -l type=kwok --timeout=300s

python3 k8stest/load.py setup "$shape"
kubectl get --raw /metrics >"$results/apiserver-metrics-setup.txt"
sleep 60
date +%s%N >"$results/window-start"
python3 k8stest/load.py churn "$shape" "$seconds" 1 "$results/churn.json"
date +%s%N >"$results/window-end"
kubectl get --raw /metrics >"$results/apiserver-metrics-end.txt"
kubectl get nodes --no-headers | wc -l >"$results/nodes-count.txt"
kubectl get pods -A --no-headers | wc -l >"$results/pods-count.txt"

pid=$(cat "$root/etcdrec.pid")
kill -TERM "$pid"
for _ in $(seq 60); do
  if ! kill -0 "$pid" 2>/dev/null; then break; fi
  sleep 1
done
if kill -0 "$pid" 2>/dev/null; then
  echo "etcdrec did not stop within 60s" >&2
  exit 1
fi

#!/usr/bin/env bash
# Diagnostic errors are recorded and fail this step; artifact upload still runs.
set -uo pipefail
root=${K8S_ROOT:?}
mkdir -p "$root/results"
failed=0
record() {
  local file=$1
  shift
  if ! timeout --kill-after=5s 60s "$@" >"$root/results/$file" 2>&1; then
    echo "failed to collect $file" >&2
    failed=1
  fi
}
record pods.txt kubectl get pods -A -o wide
record events.txt kubectl get events -A --sort-by=.metadata.creationTimestamp
record namespaces.json kubectl get namespaces -o json
record apiserver.log kubectl -n kube-system logs -l component=kube-apiserver --tail=-1
record controller-manager.log kubectl -n kube-system logs -l component=kube-controller-manager --tail=-1
record status-after.json etcdctl endpoint status -w json
record docker-stats.txt docker stats --no-stream
record memory.txt free -b
record disk.txt df -B1 "$root"
record kernel.log sudo dmesg
record kind-export.log kind export logs --name tigerstore "$root/results/kind-logs"
exit "$failed"

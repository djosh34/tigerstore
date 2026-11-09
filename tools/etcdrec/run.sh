#!/usr/bin/env bash
set -euo pipefail
root=${REC_ROOT:?}; export PATH="$root/bin:$PATH"
mark() { python3 - "$root/results/windows.jsonl" "$1" "$2" <<'PY'
import json,sys,time
with open(sys.argv[1],'a') as f: f.write(json.dumps({'scenario':sys.argv[2],'phase':sys.argv[3],'t':time.time()})+'\n')
PY
}
mark idle start
sleep "${IDLE_SECONDS:-600}"
mark idle end
kubectl get --raw /metrics > "$root/results/apiserver-metrics-idle-end.txt"
if [ "${RUN_LOAD:-true}" = false ]; then exit 0; fi
# Upstream load lifecycle, with real-network/SLO/Prometheus measurements, DNS
# checks, scheduler microbenchmark and separate pod-startup microbenchmark
# omitted. The actual create / scale-and-update / delete modules are unchanged.
load="$root/perf/clusterloader2/testing/load"
cp "$load/config.yaml" "$root/results/upstream-load-config.yaml"
python3 - "$load" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1]);stub='''steps:
- name: omitted kwok-inapplicable measurement or microbenchmark
  measurements:
  - Identifier: KwokSkip
    Method: Sleep
    Params:
      duration: 1s
'''
for path in ['modules/measurements.yaml','modules/dns-k8s-hostnames.yaml','modules/scheduler-throughput/config.yaml','modules/pod-startup-latency.yaml']:
 (p/path).write_text(stub)
PY
# Record exact variant and overrides as artifacts, not just prose.
cp -r "$load" "$root/results/load-variant"
cat > "$root/results/load-overrides.yaml" <<'YAML'
PODS_PER_NODE: 30
CL2_ENABLE_PVS: false
CL2_LOAD_TEST_THROUGHPUT: 10
CL2_DELETE_TEST_THROUGHPUT: 10
CL2_DEFAULT_QPS: 10
CL2_CREATE_OPERATION_TIMEOUT_BUFFER: 120
CL2_UPDATE_OPERATION_TIMEOUT_BUFFER: 120
CL2_DELETE_OPERATION_TIMEOUT_BUFFER: 120
YAML
# The file contains rendered config and sanitized kubeconfig; no private certs
# are intentionally copied into artifacts.
mark load start
set +e
timeout --signal=INT --kill-after=60s 35m clusterloader2 \
 --kubeconfig "$HOME/.kube/config" --provider local --nodes "${NODES:-100}" \
 --testconfig "$load/config.yaml" --testoverrides "$root/results/load-overrides.yaml" \
 --report-dir "$root/cl2-report" > "$root/results/clusterloader2.log" 2>&1
rc=$?
set -e
mark load end
echo "$rc" > "$root/results/clusterloader2-exit-code.txt"
# Copy only structured result files, not kubeconfigs or pod objects with values.
mkdir -p "$root/results/cl2-report"
find "$root/cl2-report" -maxdepth 1 -type f \( -name '*.json' -o -name '*.xml' -o -name '*.txt' \) -exec cp '{}' "$root/results/cl2-report/" \; || true
kubectl get pods -A -o wide > "$root/results/pods-load-end.txt"
exit "$rc"

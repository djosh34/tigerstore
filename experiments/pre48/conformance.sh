#!/usr/bin/env bash
# Throwaway full-selection discovery. A timeout or incomplete inventory is not a pass.
set -euo pipefail
test "$(date -u +%s)" -lt "$(date -u -d 2026-10-13T22:00:00Z +%s)"
root="$PWD/.pre48"
export PATH="$root/bin:$PATH" TMPDIR="$root/tmp"
mkdir -p "$root/results/conformance"
arch=$(uname -m)
case "$arch" in aarch64) arch=arm64;; x86_64) arch=x86_64;; *) exit 2;; esac
curl -fsSL --retry 2 --max-time 60 "https://github.com/kubernetes-sigs/hydrophone/releases/download/v0.8.0/hydrophone_Linux_$arch.tar.gz" -o "$root/tmp/hydrophone.tgz"
curl -fsSL --retry 2 --max-time 30 'https://github.com/kubernetes-sigs/hydrophone/releases/download/v0.8.0/hydrophone_0.8.0_checksums.txt' -o "$root/tmp/hydrophone-checksums.txt"
expected=$(awk -v name="hydrophone_Linux_$arch.tar.gz" '$2==name {print $1}' "$root/tmp/hydrophone-checksums.txt")
test -n "$expected"
echo "$expected  $root/tmp/hydrophone.tgz" | sha256sum -c -
tar -xzf "$root/tmp/hydrophone.tgz" -C "$root/bin" hydrophone
hydrophone --version > "$root/results/conformance/tool-version.txt"
sha256sum "$root/bin/hydrophone" > "$root/results/conformance/tool-sha256.txt"
start=$(date -u +%FT%TZ)
t0=$(date +%s.%N)
set +e
timeout -k 20s 10m hydrophone --conformance --skip='' --parallel=4 \
  --conformance-image=registry.k8s.io/conformance:v1.37.0 \
  --startup-timeout=2m --disable-progress-status \
  --extra-ginkgo-args='--timeout=8m,--seed=48,--json-report=/tmp/results/ginkgo.json' \
  --output-dir="$root/results/conformance" \
  > "$root/results/conformance/hydrophone.log" 2>&1
rc=$?
set -e
t1=$(date +%s.%N)
python3 - "$start" "$t0" "$t1" "$rc" > "$root/results/conformance/timing.json" <<'PY'
import json,sys
print(json.dumps(dict(start_utc=sys.argv[1],elapsed_seconds=float(sys.argv[3])-float(sys.argv[2]),hydrophone_exit_code=int(sys.argv[4]),suite_timeout_seconds=480,outer_timeout_seconds=600,parallel=4,focus=r'\[Conformance\]',skip='',discovery_only=True),indent=2))
PY
# Hydrophone fetches reports and removes its pod on ordinary suite completion.
# If its outer timeout fired, retrieve the files directly while the sidecar lives.
if kubectl -n conformance get pod e2e-conformance-test >/dev/null 2>&1; then
  kubectl -n conformance get pod e2e-conformance-test -o json > "$root/results/conformance/pod.json" || true
  kubectl -n conformance logs e2e-conformance-test -c conformance-container > "$root/results/conformance/pod.log" 2>&1 || true
  timeout 30s kubectl -n conformance cp -c output-container e2e-conformance-test:/tmp/results/. "$root/results/conformance/" || true
fi
python3 experiments/pre48/conformance_summary.py "$root/results/conformance"
# Do not turn the discovery job into a false conformance gate. Read summary.json.
exit 0

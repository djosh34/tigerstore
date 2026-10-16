#!/usr/bin/env bash
# All conformance specs, including Serial, Slow and Disruptive. No retries.
set -euo pipefail
root=${K8S_ROOT:?}
case $(uname -m) in
  aarch64) arch=arm64 ;;
  x86_64) arch=x86_64 ;;
  *) exit 1 ;;
esac
archive="hydrophone_Linux_$arch.tar.gz"
curl -fsSL --retry 3 "https://github.com/kubernetes-sigs/hydrophone/releases/download/v0.8.0/$archive" -o "$root/$archive"
curl -fsSL --retry 3 https://github.com/kubernetes-sigs/hydrophone/releases/download/v0.8.0/hydrophone_0.8.0_checksums.txt -o "$root/hydrophone.sha256"
expected=$(awk -v file="$archive" '$2==file {print $1}' "$root/hydrophone.sha256")
test -n "$expected"
echo "$expected  $root/$archive" | sha256sum -c
tar -xzf "$root/$archive" -C "$root/bin" hydrophone
out="$root/results/conformance"
mkdir -p "$out"
hydrophone --version > "$out/tool-version.txt"
sha256sum "$root/bin/hydrophone" > "$out/tool-sha256.txt"
status=0
timeout --kill-after=30s 130m hydrophone --conformance --skip='' --parallel=4 \
  --conformance-image=registry.k8s.io/conformance:v1.37.0 \
  --startup-timeout=5m --disable-progress-status \
  --extra-ginkgo-args='--timeout=120m,--seed=148' \
  --output-dir="$out" 2>&1 | tee "$out/hydrophone.log" || status=$?
echo "$status" > "$out/exit-code.txt"
# A failed suite still gets a strict inventory report. A missing report,
# timeout, selected skip or missing spec is never a pass.
python3 k8stest/conformance_results.py "$out" "$status"

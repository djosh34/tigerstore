#!/usr/bin/env bash
set -euo pipefail
[[ $(date -u +%s) -lt $(date -u -d '2026-10-13T22:00:00Z' +%s) ]]
: "${RUNNER_TEMP:?Run on a disposable GitHub runner}" "${OUT:?}" "${BIN:?}" "${STORE:?}"
mkdir -p "$BIN" "$OUT"
arch=$(uname -m); asset_arch=$arch
case "$arch" in aarch64) arch=arm64; asset_arch=arm64;; x86_64) arch=amd64;; *) exit 1;; esac
curl -fLSs --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch" -o "$BIN/kind"
curl -fLSs --retry 3 "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-$arch.sha256sum" -o "$OUT/kind.sha256sum"
expected=$(awk '{print $1}' "$OUT/kind.sha256sum"); echo "$expected  $BIN/kind" | sha256sum -c
curl -fLSs --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl" -o "$BIN/kubectl"
expected=$(curl -fLSs --retry 3 "https://dl.k8s.io/release/v1.37.0/bin/linux/$arch/kubectl.sha256"); echo "$expected  $BIN/kubectl" | sha256sum -c
curl -fLSs --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$RUNNER_TEMP/etcd.tgz"
curl -fLSs --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/SHA256SUMS -o "$OUT/etcd-SHA256SUMS"
expected=$(awk -v file="etcd-v3.7.2-linux-$arch.tar.gz" '$2==file {print $1}' "$OUT/etcd-SHA256SUMS")
echo "$expected  $RUNNER_TEMP/etcd.tgz" | sha256sum -c
tar -xzf "$RUNNER_TEMP/etcd.tgz" -C "$BIN" --strip-components=1 "etcd-v3.7.2-linux-$arch/etcd" "etcd-v3.7.2-linux-$arch/etcdctl"
asset="kube-burner-V2.8.6-linux-$asset_arch.tar.gz"
curl -fLSs --retry 3 "https://github.com/kube-burner/kube-burner/releases/download/v2.8.6/$asset" -o "$RUNNER_TEMP/$asset"
curl -fLSs --retry 3 https://github.com/kube-burner/kube-burner/releases/download/v2.8.6/kube-burner-checksums.txt -o "$OUT/kube-burner-checksums.txt"
expected=$(awk -v file="$asset" '$2==file {print $1}' "$OUT/kube-burner-checksums.txt")
echo "$expected  $RUNNER_TEMP/$asset" | sha256sum -c
tar -xzf "$RUNNER_TEMP/$asset" -C "$BIN" kube-burner
chmod +x "$BIN/"*
export PATH="$BIN:$PATH"
if [[ "$STORE" == tigerstore ]]; then
  (cd product && test "$(git rev-parse HEAD)" = "$TIGERSTORE_REF" && git diff --exit-code && CGO_ENABLED=0 GOFLAGS=-p=2 GOMEMLIMIT=2GiB go build -trimpath -o "$BIN/tigerstore" ./cmd/tigerstore)
fi
sudo mount -o remount,barrier=1 /
test "$(findmnt -T "$RUNNER_TEMP" -n -o FSTYPE)" = ext4
! findmnt -T "$RUNNER_TEMP" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
{
  date -u; printf 'store=%s rep=%s tigerstore_ref=%s experiment_ref=%s\n' "$STORE" "$REP" "$TIGERSTORE_REF" "$(git rev-parse HEAD)"
  printf 'ImageOS=%s ImageVersion=%s\n' "${ImageOS:-}" "${ImageVersion:-}"
  uname -a; lscpu; free -b; df -B1 "$RUNNER_TEMP"; findmnt -T "$RUNNER_TEMP"; docker version
  go version; kind version; kubectl version --client; etcd --version; kube-burner version
  sha256sum "$BIN/"*
  if [[ "$STORE" == tigerstore ]]; then go version -m "$BIN/tigerstore"; fi
} > "$OUT/versions.txt"
# All stores, kind nodes, kube-burner and controllers share the runner CPUs.
# No pinning, injected delays, unsafe fsync flags or product instrumentation.
docker pull alpine:3.22
docker pull kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
docker pull registry.k8s.io/pause:3.10
docker image inspect alpine:3.22 registry.k8s.io/pause:3.10 > "$OUT/images.json"

#!/usr/bin/env bash
set -euo pipefail
if [[ $(date -u +%s) -ge $(date -u -d 2026-10-13T22:00:00Z +%s) ]]; then
  echo 'deadline reached; refusing to start' >&2
  exit 1
fi
script=$(cd "$(dirname "$0")" && pwd)
version=${1:?pass v0.35.0, v0.36.0 or v0.37.0}
root=${2:-$script/.work/$version}
mkdir -p "$root"
root=$(cd "$root" && pwd)
export TMPDIR="$root/tmp" GOCACHE="$root/cache/build" GOPATH="$root/cache/go" GOMODCACHE="$root/cache/mod" GOTOOLCHAIN=local
mkdir -p "$TMPDIR" "$GOCACHE" "$GOPATH" "$GOMODCACHE" "$root/tools" "$root/evidence"
arch=$(uname -m)
case $arch in aarch64|arm64) arch=arm64;; x86_64) arch=amd64;; *) exit 1;; esac
if [[ ${DOWNLOAD_GO:-1} == 1 ]]; then
  curl --fail --location --retry 3 "https://go.dev/dl/go1.27.2.linux-$arch.tar.gz" -o "$root/tools/go.tar.gz"
  sha256sum "$root/tools/go.tar.gz" > "$root/evidence/go-tar.sha256"
  tar -xzf "$root/tools/go.tar.gz" -C "$root/tools"
  export PATH="$root/tools/go/bin:$PATH"
fi
curl --fail --location --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$root/tools/etcd.tar.gz"
sha256sum "$root/tools/etcd.tar.gz" > "$root/evidence/etcd-tar.sha256"
tar -xzf "$root/tools/etcd.tar.gz" -C "$root/tools"
export STOCK_ETCD="$root/tools/etcd-v3.7.2-linux-$arch/etcd" STORAGE_TEST_FACTORY="$script/stock-factory.py"
{ date -u +%FT%TZ; uname -a; go version; "$STOCK_ETCD" --version; lscpu; df -h "$root"; } > "$root/evidence/environment.txt"
module=$(python3 "$script/prepare.py" "$version" "$root")
cd "$module"
gofmt -w pkg/storage/etcd3/testserver/test_server.go pkg/storage/etcd3/store_test.go
# Build timing is distinct from execution timing. This also proves no original
# embedded etcd implementation remains in the selected package dependencies.
/usr/bin/time -p -o "$root/evidence/build.time" go test -run '^$' -count=1 ./pkg/storage/etcd3/... ./pkg/storage/testing > "$root/evidence/build.log" 2>&1
go list -deps ./pkg/storage/etcd3/... ./pkg/storage/testing > "$root/evidence/dependencies.txt"
go test -list . ./pkg/storage/etcd3/... ./pkg/storage/testing > "$root/evidence/test-list.txt"
set +e
/usr/bin/time -p -o "$root/evidence/test.time" go test -json -count=1 -timeout=10m ./pkg/storage/etcd3/... ./pkg/storage/testing > "$root/evidence/tests.jsonl" 2> "$root/evidence/test.stderr"
result=$?
set -e
python3 "$script/summarize.py" "$root/evidence"
# Keep only evidence and original downloaded source archives. No build output,
# tool binaries, data directories, module copies or caches survive the run.
rm -rf "$root/tmp" "$root/cache" "$root/tools" "$module"
exit "$result"

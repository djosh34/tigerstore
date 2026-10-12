#!/usr/bin/env bash
set -euo pipefail
if [[ $(date -u +%s) -ge $(date -u -d 2026-10-13T22:00:00Z +%s) ]]; then exit 1; fi
script=$(cd "$(dirname "$0")" && pwd)
root="$script/.work"
mkdir -p "$root"/{tmp,cache/build,cache/go,cache/mod,tools,evidence}
export TMPDIR="$root/tmp" GOCACHE="$root/cache/build" GOPATH="$root/cache/go" GOMODCACHE="$root/cache/mod" GOTOOLCHAIN=local GOFLAGS=-modcacherw
arch=$(uname -m); case $arch in aarch64) arch=arm64;; x86_64) arch=amd64;; esac
curl --fail --location --retry 3 "https://go.dev/dl/go1.27.2.linux-$arch.tar.gz" -o "$root/tools/go.tar.gz"
tar -xzf "$root/tools/go.tar.gz" -C "$root/tools"
export PATH="$root/tools/go/bin:$PATH"
curl --fail --location --retry 3 "https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-$arch.tar.gz" -o "$root/tools/etcd.tar.gz"
tar -xzf "$root/tools/etcd.tar.gz" -C "$root/tools"
sha256sum "$root/tools/"*.tar.gz > "$root/evidence/tools.sha256"
export STOCK_ETCD="$root/tools/etcd-v3.7.2-linux-$arch/etcd" STORAGE_TEST_FACTORY="$script/../pre-48-storage/stock-factory.py" STORAGE_TEST_DATA_DIR="$root/tmp/data" STORAGE_TEST_PROGRESS_INTERVAL=0s PROBE_EVIDENCE="$root/evidence"
{ date -u +%FT%TZ; go version; "$STOCK_ETCD" --version; uname -a; lscpu; } > "$root/evidence/environment.txt"
cd "$script"
/usr/bin/time -p -o "$root/evidence/build.time" go build -mod=mod -o "$root/tools/probe" . > "$root/evidence/build.log" 2>&1
go list -m all > "$root/evidence/modules.txt"
cp go.sum "$root/evidence/go.sum"
/usr/bin/time -p -o "$root/evidence/probe.time" "$root/tools/probe" > "$root/evidence/wire.jsonl" 2> "$root/evidence/probe.stderr"
rm -rf "$root/tmp" "$root/cache" "$root/tools"

#!/usr/bin/env bash
set -euo pipefail
repo=$PWD
work=$RUNNER_TEMP/review97
mkdir -p "$work"
go build -o "$work/tigerstore" ./cmd/tigerstore
go build -o "$work/factory" ./experiments/review97/factory
scripts/dev-certs.sh "$work/certs"
curl -fsSL -o "$work/etcd.tgz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
echo "5e3ff99d428fabfcefdc95348f6560d1efe835f7b2d1ab3334fe0831076966b7  $work/etcd.tgz" | sha256sum -c
tar xzf "$work/etcd.tgz" -C "$work"
export REVIEW_ETCD="$work/etcd-v3.7.2-linux-arm64/etcd"
dir=$(go mod download -json k8s.io/apiserver@v0.37.0 | jq -r .Dir)
module="$work/apiserver"
cp -r "$dir" "$module"
chmod -R u+w "$module"
go run ./apiservertest/prepare "$module"
export TIGERSTORE="$work/tigerstore" STORAGE_TEST_FACTORY="$work/factory" STORAGE_TEST_CERTS="$work/certs"
export STORAGE_TEST_CA="$work/certs/ca.pem" STORAGE_TEST_CERT="$work/certs/client.pem" STORAGE_TEST_KEY="$work/certs/client-key.pem"
mkdir -p "$repo/experiments/review97/results"
findmnt -T "$work" > "$repo/experiments/review97/results/disk.txt"
lscpu > "$repo/experiments/review97/results/cpu.txt"
cd "$module"
# First invocation deliberately matches the original cold TestCompact run.
# The other packages still build concurrently, as in the PR's original job.
set +e
go test -json -count=1 -timeout=5m -run '^TestCompact$' ./pkg/storage/etcd3/... ./pkg/storage/testing > "$repo/experiments/review97/results/cold.jsonl" 2>&1
cold=$?
# Then repeat against the compiled packages with identical upstream assertions.
go test -json -count=30 -timeout=5m -run '^TestCompact$' ./pkg/storage/etcd3/... ./pkg/storage/testing > "$repo/experiments/review97/results/warm.jsonl" 2>&1
warm=$?
set -e
printf 'backend=%s cold=%d warm=%d\n' "$REVIEW_BACKEND" "$cold" "$warm" | tee "$repo/experiments/review97/results/exit.txt"
# This is evidence collection, not a correctness gate. All test exits are recorded.

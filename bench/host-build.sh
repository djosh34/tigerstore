#!/usr/bin/env bash
set -euo pipefail
cd /home/joshazimullah.linux/work_mounts/scratch/sol-multicore
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB
unset GOMAXPROCS
export GOCACHE=/tmp/sol-multicore-build-cache
export GOTMPDIR=/tmp/sol-multicore-go-tmp
mkdir -p bin "$GOCACHE" "$GOTMPDIR"
go test -short ./cmd/loadgen
go build -o bin/loadgen ./cmd/loadgen
go build -o bin/tigerstore ./cmd/tigerstore
curl -fsSL -o /tmp/sol-multicore-etcd.tgz https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
echo '5e3ff99d428fabfcefdc95348f6560d1efe835f7b2d1ab3334fe0831076966b7  /tmp/sol-multicore-etcd.tgz' | sha256sum -c
mkdir -p /tmp/sol-multicore-release
tar xzf /tmp/sol-multicore-etcd.tgz -C /tmp/sol-multicore-release
cp /tmp/sol-multicore-release/etcd-v3.7.2-linux-arm64/{etcd,etcdctl} bin/
scripts/dev-certs.sh certs
rm -rf "$GOCACHE" "$GOTMPDIR" /tmp/sol-multicore-release /tmp/sol-multicore-etcd.tgz
printf 'build complete\n'

#!/usr/bin/env bash
# Runs the k8s.io/apiserver etcd3 storage tests of one Kubernetes version
# against tigerstore: a fresh 3-node TLS cluster for every RunEtcd call.
#
#   apiservertest/run.sh <k8s.io/apiserver version, e.g. v0.37.0> [go test flags...]
#
# It builds tigerstore and the factory, makes certificates, copies the
# module from the module cache, replaces its embedded-etcd test server
# with the adapter (apiservertest/prepare) and runs the tests of
# pkg/storage/etcd3 and pkg/storage/testing unchanged, and fails if one is
# skipped. WORK keeps the copy and the test output (default: a temporary
# directory).
set -euo pipefail

version=${1:?usage: apiservertest/run.sh <version> [go test flags...]}
shift
repo=$(cd "$(dirname "$0")/.." && pwd)
work=${WORK:-$(mktemp -d)}
mkdir -p "$work"
work=$(cd "$work" && pwd)

go build -o "$work/tigerstore" "$repo/cmd/tigerstore"
go build -o "$work/factory" "$repo/apiservertest/factory"
"$repo/scripts/dev-certs.sh" "$work/certs" >/dev/null

dir=$(cd "$repo" && go mod download -json "k8s.io/apiserver@$version" | jq -r .Dir)
module="$work/apiserver-$version"
rm -rf "$module"
cp -r "$dir" "$module"
chmod -R u+w "$module"
(cd "$repo" && go run ./apiservertest/prepare "$module")
gofmt -l "$module/pkg/storage/etcd3/testserver" "$module/pkg/storage/etcd3/store_test.go" | (! grep .)

export TIGERSTORE="$work/tigerstore" STORAGE_TEST_FACTORY="$work/factory" STORAGE_TEST_CERTS="$work/certs"
export STORAGE_TEST_CA="$work/certs/ca.pem" STORAGE_TEST_CERT="$work/certs/client.pem" STORAGE_TEST_KEY="$work/certs/client-key.pem"
cd "$module"
# The adapter must not leave the embedded etcd server in the tests.
if go list -deps -test ./pkg/storage/etcd3/... ./pkg/storage/testing | grep -q '^go.etcd.io/etcd/server/v3'; then
	echo "the storage tests still import go.etcd.io/etcd/server/v3" >&2
	exit 1
fi
# Compile every test binary first, so that linking does not compete with
# the first tests for the disk and the CPU.
go test -count=1 -run '^$' ./pkg/storage/etcd3/... ./pkg/storage/testing >/dev/null
go test -count=1 -timeout=20m -v "$@" ./pkg/storage/etcd3/... ./pkg/storage/testing | tee "$work/test-$version.log" | grep -E -- '^(--- |ok|FAIL|panic)|^\s+--- FAIL'
# #48 allows no skipped test.
if grep -q -- '--- SKIP' "$work/test-$version.log"; then
	echo "a test was skipped:" >&2
	grep -- '--- SKIP' "$work/test-$version.log" >&2
	exit 1
fi

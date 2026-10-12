#!/usr/bin/env bash
# Starts a local 3-node cluster and runs etcdctl against it over TLS with
# client certificates. Needs etcdctl on PATH. Fails on the first wrong
# answer.
set -euo pipefail
work=$(mktemp -d)
trap 'kill $(cat "$work"/data/*.pid 2>/dev/null) 2>/dev/null || true; rm -rf "$work"' EXIT
go build -o "$work/tigerstore" ./cmd/tigerstore
scripts/dev-certs.sh "$work/certs" >/dev/null 2>&1
off=${OFFSET:-$((RANDOM % 2000 + 1))}
(cd "$work" && TIGERSTORE=./tigerstore CERTS=certs OFFSET=$off "$OLDPWD/scripts/local-cluster.sh")

ctl() {
	etcdctl --endpoints=https://127.0.0.1:$((2379 + off)),https://127.0.0.1:$((22379 + off)),https://127.0.0.1:$((32379 + off)) \
		--cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
		--command-timeout=5s "$@"
}
expect() {
	local want=$1
	shift
	local got
	got=$(ctl "$@")
	if [ "$got" != "$want" ]; then
		echo "etcdctl $*: got [$got], want [$want]"
		for f in "$work"/data/*.log; do echo "== $f"; tail -20 "$f"; done
		exit 1
	fi
}

for _ in $(seq 50); do
	ctl put smoke ready >/dev/null 2>&1 && break
	sleep 0.2
done
expect OK put foo bar
expect $'foo\nbar' get foo
expect OK put foo baz
expect $'foo\nbaz' get foo --consistency=s
expect 1 del foo
expect "" get foo
ctl put a 1 >/dev/null
ctl put b 2 >/dev/null
expect $'a\n1\nb\n2' get a c
expect 2 del a c
if etcdctl --endpoints=https://127.0.0.1:$((2379 + off)) --cacert "$work/certs/ca.pem" \
	--command-timeout=3s get foo >/dev/null 2>&1; then
	echo "a client without a client certificate was served"
	exit 1
fi
echo "smoke test passed"

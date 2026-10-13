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
# Watch from the revision after a marker write, so the puts below cannot
# race the watch's start. etcdctl runs in the background directly, so that
# kill reaches it.
rev=$(ctl put w0 marker -w json | grep -o '"revision":[0-9]*' | cut -d: -f2)
etcdctl --endpoints=https://127.0.0.1:$((2379 + off)) \
	--cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	watch w --prefix --rev=$((rev + 1)) >"$work/watch.out" 2>&1 &
watcher=$!
ctl put w1 x >/dev/null
ctl put w2 y >/dev/null
ctl del w1 >/dev/null
for _ in $(seq 50); do
	[ "$(wc -l <"$work/watch.out")" -ge 8 ] && break
	sleep 0.2
done
kill "$watcher"
want=$'PUT\nw1\nx\nPUT\nw2\ny\nDELETE\nw1'
if [ "$(cat "$work/watch.out")" != "$want" ]; then
	echo "etcdctl watch: got [$(cat "$work/watch.out")], want [$want]"
	exit 1
fi
# Leases, with the keepalive stream, and compaction.
lease=$(ctl lease grant 60 | awk '{print $2}')
ctl lease keep-alive --once "$lease" | grep -q "keepalived with TTL(60)" || {
	echo "lease keep-alive did not renew $lease"
	exit 1
}
expect OK put --lease="$lease" leased v
ctl lease timetolive "$lease" --keys | grep -q leased || {
	echo "lease timetolive does not list the leased key"
	exit 1
}
ctl lease revoke "$lease" | grep -q revoked
expect "" get leased
rev=$(ctl put c0 marker -w json | grep -o '"revision":[0-9]*' | cut -d: -f2)
ctl compaction "$rev" | grep -q "compacted revision $rev"
ctl member list | grep -q started
# endpoint health reads a key and then lists the alarms on every node.
ctl endpoint health
expect "" alarm list
# endpoint status reads Maintenance.Status from every node; the table
# divides by the database size, so a zero size would crash etcdctl.
ctl endpoint status -w table
ctl endpoint status -w json | grep -q '"version":"3.7.2"' || {
	echo "endpoint status does not report version 3.7.2"
	exit 1
}
version=$(curl -sf --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	"https://127.0.0.1:$((2379 + off))/version")
if [ "$version" != '{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}' ]; then
	echo "/version: got [$version]"
	exit 1
fi
health=$(curl -sf --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	"https://127.0.0.1:$((2379 + off))/health")
if [ "$health" != '{"health":"true","reason":""}' ]; then
	echo "/health: got [$health]"
	exit 1
fi
# A probe that does not offer HTTP/2 gets /health over HTTP/1.1.
health=$(curl -sf --http1.1 --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	"https://127.0.0.1:$((2379 + off))/health")
if [ "$health" != '{"health":"true","reason":""}' ]; then
	echo "/health over HTTP/1.1: got [$health]"
	exit 1
fi
if etcdctl --endpoints=https://127.0.0.1:$((2379 + off)) --cacert "$work/certs/ca.pem" \
	--command-timeout=3s get foo >/dev/null 2>&1; then
	echo "a client without a client certificate was served"
	exit 1
fi
echo "smoke test passed"

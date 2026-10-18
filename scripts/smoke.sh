#!/usr/bin/env bash
# Starts a local 3-node cluster and runs etcdctl against it over TLS with
# client certificates. Needs etcdctl on PATH. Fails on the first wrong
# answer. With KEEP set, a failed run leaves its logs there.
set -euo pipefail
work=$(mktemp -d)
# keep copies the logs of a failed run to $KEEP, if set, for the
# workflow to upload.
keep() {
	if [ "$1" != 0 ] && [ -n "${KEEP:-}" ]; then
		mkdir -p "$KEEP"
		(cd "$work" && find . -name '*.log' -exec cp --parents {} "$KEEP" \;)
	fi
}
trap 'status=$?; kill $(cat "$work"/data/*.pid 2>/dev/null) 2>/dev/null || true; keep "$status"; rm -rf "$work"' EXIT
go build -o "$work/tigerstore" ./cmd/tigerstore
scripts/dev-certs.sh "$work/certs" >/dev/null 2>&1
off=${OFFSET:-$((RANDOM % 2000 + 1))}
(cd "$work" && TIGERSTORE=./tigerstore CERTS=certs OFFSET=$off "$OLDPWD/scripts/local-cluster.sh")

ctl() {
	etcdctl --endpoints=https://127.0.0.1:$((2379 + off)),https://127.0.0.1:$((22379 + off)),https://127.0.0.1:$((12379 + off)) \
		--cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
		--command-timeout=5s "$@"
}
# has runs a command and greps its whole output for pattern. A pipe into
# grep -q would fail with SIGPIPE under pipefail when grep exits before
# the command's last write.
has() {
	local pattern=$1
	shift
	local out
	out=$("$@")
	grep -q -- "$pattern" <<<"$out"
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
# A serializable read may reach a follower that has not applied the put
# yet, as etcd allows; it must show it within 5 s.
for _ in $(seq 25); do
	got=$(ctl get foo --consistency=s)
	[ "$got" = $'foo\nbaz' ] && break
	sleep 0.2
done
if [ "$got" != $'foo\nbaz' ]; then
	echo "etcdctl get foo --consistency=s: got [$got] after 5 s, want [foo baz]"
	exit 1
fi
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
has "keepalived with TTL(60)" ctl lease keep-alive --once "$lease" || {
	echo "lease keep-alive did not renew $lease"
	exit 1
}
expect OK put --lease="$lease" leased v
has leased ctl lease timetolive "$lease" --keys || {
	echo "lease timetolive does not list the leased key"
	exit 1
}
has revoked ctl lease revoke "$lease"
expect "" get leased
rev=$(ctl put c0 marker -w json | grep -o '"revision":[0-9]*' | cut -d: -f2)
has "compacted revision $rev" ctl compaction "$rev"
has started ctl member list
# endpoint health reads a key and then lists the alarms on every node.
ctl endpoint health
expect "" alarm list
# endpoint status reads Maintenance.Status from every node; the table
# divides by the database size, so a zero size would crash etcdctl.
ctl endpoint status -w table
has '"version":"3.7.2"' ctl endpoint status -w json || {
	echo "endpoint status does not report version 3.7.2"
	exit 1
}
# etcdctl checks that the snapshot ends with the SHA-256 of what comes
# before it, after a multiple of 512 bytes.
has "Snapshot saved at" etcdctl --endpoints=https://127.0.0.1:$((2379 + off)) \
	--cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	snapshot save "$work/snap.db"
# tigerstore snapshot restore writes it into a new data directory.
restored=$("$work/tigerstore" snapshot restore "$work/snap.db" --name node1 --data-dir "$work/restored" \
	--initial-cluster node1=https://127.0.0.1:1,node2=https://127.0.0.1:2,node3=https://127.0.0.1:3 2>&1)
grep -q "restored the snapshot" <<<"$restored"
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
# Prometheus scrapes /metrics; a put was made above.
metrics=$(curl -sf --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	"https://127.0.0.1:$((2379 + off))/metrics")
if ! grep -q '^grpc_server_handled_total{grpc_code="OK",grpc_method="Put"' <<<"$metrics" || ! grep -q '^etcd_server_has_leader 1$' <<<"$metrics"; then
	echo "/metrics: got [$metrics]"
	exit 1
fi
# A probe that does not offer HTTP/2 gets /health over HTTP/1.1.
health=$(curl -sf --http1.1 --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
	"https://127.0.0.1:$((2379 + off))/health")
if [ "$health" != '{"health":"true","reason":""}' ]; then
	echo "/health over HTTP/1.1: got [$health]"
	exit 1
fi
# The gRPC gateway speaks JSON, with keys and values base64 encoded:
# gw, set to 1, reads back.
gw() {
	curl -sf --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" \
		-X POST -d "$2" "https://127.0.0.1:$((2379 + off))$1"
}
gw /v3/kv/put '{"key":"Z3c=","value":"MQ=="}' >/dev/null
range=$(gw /v3/kv/range '{"key":"Z3c="}')
if ! grep -q '"value":"MQ=="' <<<"$range"; then
	echo "/v3/kv/range: got [$range]"
	exit 1
fi
if etcdctl --endpoints=https://127.0.0.1:$((2379 + off)) --cacert "$work/certs/ca.pem" \
	--command-timeout=3s get foo >/dev/null 2>&1; then
	echo "a client without a client certificate was served"
	exit 1
fi
# etcd's gRPC proxy, which tigerstore runs, keeps the keys of its
# --namespace under that prefix. It dials with a certificate without a
# CommonName, as etcd requires.
proxy=$((2383 + off))
"$work/tigerstore" grpc-proxy start --endpoints=127.0.0.1:$((2379 + off)) --listen-addr=127.0.0.1:$proxy \
	--cacert "$work/certs/ca.pem" --cert "$work/certs/proxy.pem" --key "$work/certs/proxy-key.pem" \
	--namespace=/ns/ >"$work/proxy.log" 2>&1 &
echo $! >"$work/data/proxy.pid"
pctl() {
	etcdctl --endpoints=127.0.0.1:$proxy --command-timeout=5s "$@"
}
for _ in $(seq 50); do
	pctl put viaproxy 1 >/dev/null 2>&1 && break
	sleep 0.2
done
expect $'/ns/viaproxy\n1' get /ns/viaproxy
proxied=$(pctl get viaproxy)
if [ "$proxied" != $'viaproxy\n1' ]; then
	echo "get through the gRPC proxy: got [$proxied]"
	tail -20 "$work/proxy.log"
	exit 1
fi
# etcd's TCP gateway, which tigerstore runs, passes TLS through to the
# cluster.
gateway=$((2384 + off))
"$work/tigerstore" gateway start --endpoints=127.0.0.1:$((2379 + off)),127.0.0.1:$((22379 + off)) \
	--listen-addr=127.0.0.1:$gateway >"$work/gateway.log" 2>&1 &
echo $! >"$work/data/gateway.pid"
for _ in $(seq 50); do
	etcdctl --endpoints=https://127.0.0.1:$gateway --cacert "$work/certs/ca.pem" --cert "$work/certs/client.pem" \
		--key "$work/certs/client-key.pem" --command-timeout=2s put viagateway 1 >/dev/null 2>&1 && break
	sleep 0.2
done
expect $'viagateway\n1' get viagateway
# etcd's v3 discovery: three new nodes find each other through the
# cluster above, which serves as the discovery service.
ctl put /_etcd/registry/smoke/_config/size 3 >/dev/null
mkdir -p "$work/disc"
# disc starts node d$1 of that cluster.
disc() {
	local i=$1 c=$((2390 + 10 * $1 + off))
	"$work/tigerstore" --name "d$i" --data-dir "$work/disc/d$i" \
		--listen-peer-urls "https://127.0.0.1:$((c + 1))" --initial-advertise-peer-urls "https://127.0.0.1:$((c + 1))" \
		--listen-client-urls "https://127.0.0.1:$c" --advertise-client-urls "https://127.0.0.1:$c" \
		--cert-file "$work/certs/node$i.pem" --key-file "$work/certs/node$i-key.pem" \
		--trusted-ca-file "$work/certs/ca.pem" --client-cert-auth \
		--peer-cert-file "$work/certs/node$i.pem" --peer-key-file "$work/certs/node$i-key.pem" \
		--peer-trusted-ca-file "$work/certs/ca.pem" \
		--discovery-endpoints "127.0.0.1:$((2379 + off))" --discovery-token smoke \
		--discovery-cacert "$work/certs/ca.pem" --discovery-cert "$work/certs/client.pem" \
		--discovery-key "$work/certs/client-key.pem" >>"$work/disc/d$i.log" 2>&1 &
	echo $! >"$work/data/disc$i.pid"
}
for i in 1 2 3; do
	disc "$i"
done
members=""
for _ in $(seq 100); do
	members=$(etcdctl --endpoints="https://127.0.0.1:$((2400 + off))" --cacert "$work/certs/ca.pem" \
		--cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" --command-timeout=2s member list 2>/dev/null) &&
		[ "$(grep -c ', started, d[123], ' <<<"$members")" = 3 ] && break
	sleep 0.2
done
if [ "$(grep -c ', started, d[123], ' <<<"$members")" != 3 ]; then
	echo "the cluster from discovery has members [$members]"
	for f in "$work"/disc/*.log; do echo "== $f"; tail -20 "$f"; done
	exit 1
fi
# A restart takes the members from the data directory; the discovery
# cluster is full by now.
kill "$(cat "$work/data/disc1.pid")"
while kill -0 "$(cat "$work/data/disc1.pid")" 2>/dev/null; do sleep 0.1; done
disc 1
for _ in $(seq 100); do
	etcdctl --endpoints="https://127.0.0.1:$((2400 + off))" --cacert "$work/certs/ca.pem" \
		--cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" --command-timeout=2s put afterrestart 1 >/dev/null 2>&1 && break
	sleep 0.2
done
if ! etcdctl --endpoints="https://127.0.0.1:$((2400 + off))" --cacert "$work/certs/ca.pem" \
	--cert "$work/certs/client.pem" --key "$work/certs/client-key.pem" --command-timeout=2s get afterrestart | grep -q '^1$'; then
	echo "d1 did not serve after its restart"
	tail -20 "$work/disc/d1.log"
	exit 1
fi
echo "smoke test passed"

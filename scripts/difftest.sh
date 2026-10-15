#!/usr/bin/env bash
# Runs the differential tests in difftest/ against fresh servers.
#
#   scripts/difftest.sh etcd          single-node etcd v3.7.2
#   scripts/difftest.sh etcd record   the same, and rewrites etcd's recorded answers
#   scripts/difftest.sh tigerstore    3-node tigerstore built from this checkout
#   scripts/difftest.sh tigerstore record
#                                     the same, and rewrites tigerstore's answers to
#                                     the steps it does not match yet
#
# ONLY, a regular expression, runs only the tests whose names match it.
#
# Each test in suites runs on a fresh server started with the flags next to
# it, on top of the ones below; @CERTS@ in a flag is the directory of the
# certificates and keys, which the tests get in TIGERSTORE_DIFF_CERTS,
# @METRICS@ the port of --listen-metrics-urls, which they get in
# TIGERSTORE_DIFF_METRICS, and @CLIENT@ the server's client port. Servers
# run in the work directory, where a unix socket URL puts its socket; the
# tests get it in TIGERSTORE_DIFF_WORK.
#
# ETCD is the etcd v3.7.2 binary, etcd on PATH by default. etcd runs on fixed
# ports, because its member ID comes from its peer URL and the largest
# accepted request depends on the length of request IDs made from it. Like
# tigerstore, it serves clients over TLS with --client-cert-auth: etcd then
# puts the client certificate's name into every write's header, and the size
# limit counts it (#99).
set -euo pipefail
server=${1:?usage: scripts/difftest.sh etcd|tigerstore [record]}
record=${2:-}
suites=(
	"TestBoundaries|"
	"TestSweep|"
	"TestQuota|--quota-backend-bytes 1048576"
	"TestAuth|"
	"TestAuthJWT|--auth-token jwt,priv-key=@CERTS@/jwt.pem,sign-method=RS256"
	"TestAutoCompaction|--auto-compaction-mode periodic --auto-compaction-retention 2s"
	"TestDebug|--enable-pprof --metrics extensive --listen-metrics-urls http://127.0.0.1:@METRICS@"
	"TestProxy|"
	"TestGateway|"
	"TestTLS|--cipher-suites TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 --client-crl-file @CERTS@/crl.der"
	"TestTLSStrict|--tls-min-version TLS1.3 --client-cert-allowed-hostname localhost"
	"TestUnix|--listen-client-urls https://127.0.0.1:@CLIENT@,unixs://localhost:@CLIENT@"
)
work=$(mktemp -d)
# stop ends every server this script started and waits until they are
# gone, so the ports are free for the next one.
stop() {
	local pids
	pids=$(cat "$work"/*.pid "$work"/data/*.pid 2>/dev/null || true)
	kill $pids 2>/dev/null || true
	for _ in $(seq 100); do
		kill -0 $pids 2>/dev/null || break
		sleep 0.1
	done
	rm -f "$work"/*.pid "$work"/data/*.pid
}
trap 'stop; rm -rf "$work"' EXIT
# in_use reports whether something listens on a local port.
in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
# alive fails unless every server this script started still runs, so the
# tests cannot have talked to another process that held the port.
alive() {
	local f
	for f in "$work"/*.pid "$work"/data/*.pid; do
		[ -e "$f" ] || continue
		if ! kill -0 "$(cat "$f")" 2>/dev/null; then
			echo "the server of $f exited during the tests"
			exit 1
		fi
	done
}
# proxy starts, with binary $1, the gRPC proxy of TestProxy, with
# --namespace /ns/, in front of the server's client URL $2 on port $3, or
# the TCP gateway of TestGateway on port $4.
proxy() {
	case $test in
	TestProxy)
		"$1" grpc-proxy start --endpoints="$2" --listen-addr="127.0.0.1:$3" --namespace=/ns/ \
			--cacert "$work/certs/ca.pem" --cert "$work/certs/proxy.pem" --key "$work/certs/proxy-key.pem" >"$work/proxy.log" 2>&1 &
		;;
	TestGateway)
		"$1" gateway start --endpoints="$2" --listen-addr="127.0.0.1:$4" >"$work/proxy.log" 2>&1 &
		;;
	*)
		return 0
		;;
	esac
	echo $! >"$work/proxy.pid"
}
args=(-count=1 -tags difftest ./difftest)
[ "$record" = record ] && args+=(-record)

case $server in
etcd)
	etcd=${ETCD:-etcd}
	"$etcd" --version | grep -q '^etcd Version: 3.7.2$' || {
		echo "$etcd is not etcd 3.7.2"
		exit 1
	}
	scripts/dev-certs.sh "$work/certs" >/dev/null
	;;
tigerstore)
	go build -o "$work/tigerstore" ./cmd/tigerstore
	scripts/dev-certs.sh "$work/certs" >/dev/null 2>&1
	;;
*)
	echo "unknown server $server"
	exit 1
	;;
esac

# The key of the JWT suite.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$work/certs/jwt.pem" 2>/dev/null
# The client certificates of the TLS suites: revoked, named client but in
# crl.der, and other, named other for other.example only.
(
	cd "$work/certs"
	for cert in "revoked|/CN=client|IP:127.0.0.1,DNS:localhost" "other|/CN=other|DNS:other.example"; do
		IFS='|' read -r name subj san <<<"$cert"
		openssl ecparam -name prime256v1 -genkey -noout -out "$name-key.pem"
		openssl req -new -key "$name-key.pem" -subj "$subj" -out "$name.csr"
		printf 'subjectAltName=%s\nextendedKeyUsage=clientAuth\n' "$san" >"$name.ext"
		openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
			-days 30 -extfile "$name.ext" -out "$name.pem" 2>/dev/null
	done
	printf '[ca]\ndefault_ca = dev\n[dev]\ndatabase = index.txt\ncrlnumber = crlnumber\ndefault_md = sha256\ndefault_crl_days = 30\n' >ca.cnf
	: >index.txt
	echo 01 >crlnumber
	openssl ca -config ca.cnf -keyfile ca-key.pem -cert ca.pem -revoke revoked.pem 2>/dev/null
	openssl ca -config ca.cnf -keyfile ca-key.pem -cert ca.pem -gencrl -out crl.pem 2>/dev/null
	# etcd reads a CRL in DER.
	openssl crl -in crl.pem -outform DER -out crl.der
)

for suite in "${suites[@]}"; do
	test=${suite%%|*}
	[[ $test =~ ${ONLY:-.} ]] || continue
	read -r -a flags <<<"${suite#*|}"
	flags=("${flags[@]//@CERTS@/$work/certs}")
	echo "== $test ${flags[*]}"
	case $server in
	etcd)
		for port in 23790 23791 23792 23793 23800; do
			if in_use $port; then
				echo "port $port is in use, so the tests would not reach a fresh etcd"
				exit 1
			fi
		done
		rm -rf "$work/etcd"
		etcdflags=("${flags[@]//@METRICS@/23791}")
		(cd "$work" && exec "$etcd" --name diff --data-dir "$work/etcd" \
			--listen-client-urls https://127.0.0.1:23790 --advertise-client-urls https://127.0.0.1:23790 \
			--cert-file "$work/certs/node1.pem" --key-file "$work/certs/node1-key.pem" \
			--trusted-ca-file "$work/certs/ca.pem" --client-cert-auth \
			--listen-peer-urls http://127.0.0.1:23800 --initial-advertise-peer-urls http://127.0.0.1:23800 \
			--initial-cluster diff=http://127.0.0.1:23800 --log-level warn "${etcdflags[@]//@CLIENT@/23790}") >"$work/etcd.log" 2>&1 &
		echo $! >"$work/etcd.pid"
		proxy "$etcd" 127.0.0.1:23790 23792 23793
		TIGERSTORE_DIFF_SERVER=etcd TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:23790 TIGERSTORE_DIFF_METRICS=127.0.0.1:23791 \
			TIGERSTORE_DIFF_CERTS=$work/certs TIGERSTORE_DIFF_WORK=$work \
			TIGERSTORE_DIFF_PROXY=127.0.0.1:23792 TIGERSTORE_DIFF_GATEWAY=127.0.0.1:23793 \
			TIGERSTORE_DIFF_CA=$work/certs/ca.pem TIGERSTORE_DIFF_CERT=$work/certs/client.pem TIGERSTORE_DIFF_KEY=$work/certs/client-key.pem \
			go test "${args[@]}" -run "^$test\$" ||
			{ tail -n 50 "$work/etcd.log" "$work"/proxy.log; exit 1; }
		;;
	tigerstore)
		off=${OFFSET:-$((RANDOM % 2000 + 1))}
		for port in $((2379 + off)) $((22379 + off)) $((32379 + off)) $((2380 + off)) $((22380 + off)) $((32380 + off)) $((2381 + off)) $((22381 + off)) $((32381 + off)) $((2383 + off)) $((2384 + off)); do
			if in_use $port; then
				echo "port $port is in use"
				exit 1
			fi
		done
		rm -rf "$work/data"
		(cd "$work" && TIGERSTORE=./tigerstore CERTS=certs OFFSET=$off "$OLDPWD/scripts/local-cluster.sh" "${flags[@]}")
		proxy "$work/tigerstore" 127.0.0.1:$((2379 + off)) $((2383 + off)) $((2384 + off))
		TIGERSTORE_DIFF_SERVER=tigerstore TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:$((2379 + off)) TIGERSTORE_DIFF_METRICS=127.0.0.1:$((2381 + off)) \
			TIGERSTORE_DIFF_CERTS=$work/certs TIGERSTORE_DIFF_WORK=$work \
			TIGERSTORE_DIFF_PROXY=127.0.0.1:$((2383 + off)) TIGERSTORE_DIFF_GATEWAY=127.0.0.1:$((2384 + off)) \
			TIGERSTORE_DIFF_CA=$work/certs/ca.pem TIGERSTORE_DIFF_CERT=$work/certs/client.pem TIGERSTORE_DIFF_KEY=$work/certs/client-key.pem \
			go test "${args[@]}" -run "^$test\$" ||
			{ for f in "$work"/data/*.log "$work"/proxy.log; do [ -e "$f" ] && echo "== $f" && tail -30 "$f"; done; exit 1; }
		;;
	esac
	alive
	stop
done

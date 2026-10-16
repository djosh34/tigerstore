#!/usr/bin/env bash
# Differential fuzzing against etcd v3.7.2 (#148). Starts a fresh etcd
# v3.7.2 and a fresh 3-node tigerstore side by side, both with client TLS
# and the certificates of scripts/dev-certs.sh, and runs the difftest's
# TestDiffFuzz against both for DURATION (default 60s) from seed SEED
# (default: from the clock), then TestDiffFuzzFound, which replays every
# saved mismatch in difftest/testdata/fuzz.
#
#   scripts/difffuzz.sh
#
# ETCD is the etcd v3.7.2 binary, etcd on PATH by default. A mismatch is
# shrunk, saved to FUZZ_OUT (default difftest/testdata/fuzz) as a named
# case, and fails the run. BUILD_FLAGS go to the build of tigerstore, for
# example to build in a planted bug. With RACE set, tigerstore and the
# tests are built with -race and stop at their first race report, and a
# report in a node's log fails the run.
set -euo pipefail
etcd=${ETCD:-etcd}
"$etcd" --version | grep -q '^etcd Version: 3.7.2$' || {
	echo "$etcd is not etcd 3.7.2"
	exit 1
}
mkdir -p "${FUZZ_OUT:-difftest/testdata/fuzz}"
out=$(cd "${FUZZ_OUT:-difftest/testdata/fuzz}" && pwd)
work=$(mktemp -d)
stop() {
	local pids
	pids=$(cat "$work"/*.pid "$work"/data/*.pid 2>/dev/null || true)
	kill $pids 2>/dev/null || true
	for _ in $(seq 100); do
		kill -0 $pids 2>/dev/null || break
		sleep 0.1
	done
}
trap 'stop; rm -rf "$work"' EXIT
in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
off=${OFFSET:-$((RANDOM % 2000 + 1))}
for port in 23790 23800 $((2379 + off)) $((22379 + off)) $((12379 + off)) $((2380 + off)) $((22380 + off)) $((12380 + off)); do
	if in_use $port; then
		echo "port $port is in use"
		exit 1
	fi
done
# shellcheck disable=SC2086
go build ${RACE:+-race} ${BUILD_FLAGS:-} -o "$work/tigerstore" ./cmd/tigerstore
[ -n "${RACE:-}" ] && export GORACE=halt_on_error=1
scripts/dev-certs.sh "$work/certs" >/dev/null 2>&1
(cd "$work" && exec "$etcd" --name fuzz --data-dir "$work/etcd" \
	--listen-client-urls https://127.0.0.1:23790 --advertise-client-urls https://127.0.0.1:23790 \
	--cert-file "$work/certs/node1.pem" --key-file "$work/certs/node1-key.pem" \
	--trusted-ca-file "$work/certs/ca.pem" --client-cert-auth \
	--listen-peer-urls http://127.0.0.1:23800 --initial-advertise-peer-urls http://127.0.0.1:23800 \
	--initial-cluster fuzz=http://127.0.0.1:23800 --log-level warn) >"$work/etcd.log" 2>&1 &
echo $! >"$work/etcd.pid"
(cd "$work" && TIGERSTORE=./tigerstore CERTS=certs OFFSET=$off "$OLDPWD/scripts/local-cluster.sh")
TIGERSTORE_DIFF_ETCD=127.0.0.1:23790 TIGERSTORE_DIFF_ENDPOINT=127.0.0.1:$((2379 + off)) \
	TIGERSTORE_DIFF_CA=$work/certs/ca.pem TIGERSTORE_DIFF_CERT=$work/certs/client.pem TIGERSTORE_DIFF_KEY=$work/certs/client-key.pem \
	go test -count=1 -tags difftest ${RACE:+-race} -timeout 0 ./difftest -run '^TestDiffFuzz(Found)?$' -v \
	-args -difffuzz.duration="${DURATION:-60s}" -difffuzz.seed="${SEED:-0}" -difffuzz.out="$out" |
	grep -v -E '^=== RUN|^\s+--- PASS: TestDiffFuzzFound/' ||
	{
		for f in "$work"/etcd.log "$work"/data/*.log; do [ -e "$f" ] && echo "== $f" && tail -30 "$f"; done
		exit 1
	}
stop
if [ -n "${RACE:-}" ]; then
	for f in "$work"/data/*.log; do
		if grep -q 'WARNING: DATA RACE' "$f"; then
			echo "$f has a data race report:"
			grep -A60 'WARNING: DATA RACE' "$f" | head -120
			exit 1
		fi
	done
fi

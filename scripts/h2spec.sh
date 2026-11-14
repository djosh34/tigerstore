#!/usr/bin/env bash
# Runs h2spec, the HTTP/2 conformance tester, against one tigerstore node,
# over TLS and over HTTP/2 without TLS:
#
#   TIGERSTORE=<binary> H2SPEC=<binary> scripts/h2spec.sh <work dir>
#
# The node takes clients without certificates and allows 100 streams per
# connection, which h2spec's case 5.1.2 opens one past. A case that fails
# over either and is not listed in scripts/h2spec-known.txt fails the
# script; so does a listed case that passes over both, so the list stays
# true. Each run's report is <work dir>/h2spec-<tls|h2c>.txt, and the
# node's log <work dir>/node-<tls|h2c>.log.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mkdir -p "${1:?work dir}" && cd "$1" && pwd)
tigerstore=${TIGERSTORE:?the tigerstore binary}
h2spec=${H2SPEC:?the h2spec binary}
"$root/scripts/dev-certs.sh" "$work/certs" >/dev/null 2>&1
known=$(sed -e '/^#/d' -e '/^$/d' "$root/scripts/h2spec-known.txt" | cut -f1 | sort)
status=0 all=""
for mode in tls h2c; do
	scheme=https flags=(-t -k)
	if [ "$mode" = h2c ]; then scheme=http flags=(); fi
	client=$((23900 + RANDOM % 50)) peer=$((23960 + RANDOM % 30))
	rm -rf "$work/data-$mode"
	"$tigerstore" --name n1 --data-dir "$work/data-$mode" \
		--listen-client-urls "$scheme://127.0.0.1:$client" --advertise-client-urls "$scheme://127.0.0.1:$client" \
		--listen-peer-urls "https://127.0.0.1:$peer" --initial-advertise-peer-urls "https://127.0.0.1:$peer" \
		--initial-cluster "n1=https://127.0.0.1:$peer" \
		--cert-file "$work/certs/node1.pem" --key-file "$work/certs/node1-key.pem" \
		--peer-cert-file "$work/certs/node1.pem" --peer-key-file "$work/certs/node1-key.pem" \
		--peer-trusted-ca-file "$work/certs/ca.pem" --memory-budget-bytes $((512 << 20)) \
		--max-concurrent-streams 100 >"$work/node-$mode.log" 2>&1 &
	pid=$!
	for _ in $(seq 100); do
		curl -sfk "$scheme://127.0.0.1:$client/health" >/dev/null && break
		sleep 0.1
	done
	"$h2spec" -h 127.0.0.1 -p "$client" "${flags[@]}" -o 3 -j "$work/h2spec-$mode.xml" >"$work/h2spec-$mode.txt" 2>&1 || true
	kill "$pid"
	wait "$pid" || true
	# The JUnit report names each case by its section and description, and
	# holds a failure or error element for each that failed.
	failed=$(python3 -B - "$work/h2spec-$mode.xml" <<-'EOF'
		import sys, xml.etree.ElementTree as ET
		for suite in ET.parse(sys.argv[1]).getroot().iter("testsuite"):
		    for case in suite.iter("testcase"):
		        if case.find("failure") is not None or case.find("error") is not None:
		            print(f"{suite.get('package')}: {case.get('classname')}")
		EOF
	)
	echo "h2spec over $mode: $(grep -c . <<<"$failed" || true) cases failed"
	all+=$failed$'\n'
done
all=$(sort -u <<<"$all" | sed '/^$/d')
new=$(comm -23 <(echo "$all") <(echo "$known") | sed '/^$/d')
fixed=$(comm -13 <(echo "$all") <(echo "$known") | sed '/^$/d')
if [ -n "$new" ]; then
	echo "failed and not in scripts/h2spec-known.txt:"
	echo "$new"
	status=1
fi
if [ -n "$fixed" ]; then
	echo "in scripts/h2spec-known.txt but passed:"
	echo "$fixed"
	status=1
fi
exit "$status"

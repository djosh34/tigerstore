#!/usr/bin/env bash
# Prints the command that runs each failed test of a go test log again.
#
#   scripts/replay.sh <log> [go test flags, such as -race] [-- test args]
#
# A test that failed and has no failed subtest is printed with its
# package, and so is a test still running when its binary timed out. The
# flags go into every line. The test args, such as the -sim.seed and
# -sim.seeds of the job, go after -args of a line that has no args of its
# own, as a test that failed as a whole over its seeds needs them; with
# them, such a test is printed too, besides its failed subtests. A log
# of a test binary run directly has no package line; PKG then names the
# package, as in PKG=./internal/sim.
#
# The simulator's tests print their own replay lines, with the seed flags
# and the planted bug; those are printed with the flags, and a simulator
# test with no line of its own gets the generic one. A seed of a
# simulator test that timed out gets its seed flags here. A subtest named
# after a test function runs as that test.
set -euo pipefail
log=$1
shift
flags=()
args=()
while [ $# -gt 0 ]; do
	if [ "$1" = -- ]; then
		shift
		args=("$@")
		break
	fi
	flags+=("$1")
	shift
done
awk -v flags="${flags[*]:-}" -v args="${args[*]:-}" -v pkgarg="${PKG:-}" '
function quote(s) {
	gsub(/[][().*+?^$|\\{}]/, "\\\\&", s)
	return "^" s "$"
}
function gotest() {
	return "go test " (flags == "" ? "" : flags " ")
}
# The first pass collects the -run patterns of the replay lines the
# tests printed themselves.
FNR == NR {
	if (match($0, /replay: go test .*-run '\''[^'\'']*'\''/)) {
		p = substr($0, RSTART, RLENGTH)
		sub(/.*-run '\''/, "", p)
		sub(/'\''$/, "", p)
		own[p] = 1
	}
	next
}
/^ *--- FAIL: / {
	failed[++n] = $3
	timedOut[n] = 0
	next
}
# A test binary that timed out lists the tests that were still running.
/^[ \t]*running tests:/ {
	running = 1
	next
}
running && /^[ \t]+Test[^ ]* \(/ {
	failed[++n] = $1
	timedOut[n] = 1
	next
}
{
	running = 0
}
function flush(pkg, i, j, k, p, first, leaf, parts, pattern, extra, sim) {
	sim = pkg ~ /internal\/sim\/?$/
	for (i = 1; i <= n; i++) {
		leaf = 1
		for (j = 1; j <= n; j++) {
			if (index(failed[j], failed[i] "/") == 1) {
				leaf = 0
			}
		}
		# A test that failed as a whole, as over its seeds, runs again
		# with the args of the job too.
		if (!leaf && (args == "" || index(failed[i], "/") > 0)) {
			continue
		}
		k = split(failed[i], parts, "/")
		first = 1
		for (p = k; p > 1; p--) {
			if (parts[p] ~ /^Test/) {
				first = p
				break
			}
		}
		pattern = quote(parts[first])
		for (p = first + 1; p <= k; p++) {
			pattern = pattern "/" quote(parts[p])
		}
		if (sim && !timedOut[i] && (pattern in own)) {
			continue
		}
		extra = ""
		if (sim && first < k && parts[first] ~ /^Test(Seeds|Linearizable|MembershipFaults)$/ && parts[first + 1] ~ /^[0-9]+$/) {
			extra = " -args -sim.seed=" parts[first + 1] " -sim.seeds=1"
		} else if (args != "") {
			extra = " -args " args
		}
		printf "replay: %s-count=1 -run '\''%s'\'' %s%s\n", gotest(), pattern, pkg, extra
	}
	n = 0
}
/^(FAIL|ok)[ \t]+[^ \t]+\// {
	if ($1 == "FAIL") {
		flush($2)
	}
	n = 0
}
END {
	if (n > 0 && pkgarg != "") {
		flush(pkgarg)
	}
}
' "$log" "$log"
grep -h 'replay: go test' "$log" | sed 's/^ *[a-z_]*\.go:[0-9]*: //' | sort -u | sed "s/^replay: go test /replay: go test ${flags[*]:+${flags[*]} }/" || true

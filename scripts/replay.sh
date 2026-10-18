#!/usr/bin/env bash
# Prints the command that runs each failed test of a go test log again.
#
#   scripts/replay.sh <log> [go test flags, such as -race]
#
# A test that failed and has no failed subtest is printed with its
# package, and so is a test still running when its binary timed out. The
# simulator's failed tests print their own replay lines, with the seed
# flags and the planted bug, so for internal/sim those lines are printed
# instead; a seed of a simulator test that timed out gets its seed flags
# here.
set -euo pipefail
log=$1
shift
awk -v flags="$*" '
function quote(s) {
	gsub(/[][().*+?^$|\\{}]/, "\\\\&", s)
	return "^" s "$"
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
/^(FAIL|ok)[ \t]+[^ \t]+\// {
	pkg = $2
	sim = pkg ~ /\/internal\/sim$/
	for (i = 1; $1 == "FAIL" && i <= n; i++) {
		leaf = 1
		for (j = 1; j <= n; j++) {
			if (index(failed[j], failed[i] "/") == 1) {
				leaf = 0
			}
		}
		if (!leaf || sim && !timedOut[i]) {
			continue
		}
		k = split(failed[i], parts, "/")
		pattern = quote(parts[1])
		for (p = 2; p <= k; p++) {
			pattern = pattern "/" quote(parts[p])
		}
		args = ""
		if (sim && parts[1] ~ /^Test(Seeds|Linearizable|MembershipFaults)$/ && parts[2] ~ /^[0-9]+$/) {
			args = " -args -sim.seed=" parts[2] " -sim.seeds=1"
		}
		printf "replay: go test %s-count=1 -run '\''%s'\'' %s%s\n", (flags == "" ? "" : flags " "), pattern, pkg, args
	}
	n = 0
}
' "$log"
grep -h 'replay: go test' "$log" | sed 's/^ *[a-z_]*\.go:[0-9]*: //' | sort -u || true

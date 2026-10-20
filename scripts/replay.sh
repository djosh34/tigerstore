#!/usr/bin/env bash
# Prints the command that runs each failed test of a go test log again.
#
#   scripts/replay.sh <log> [-gosim] [go test flags, such as -race]
#
# A test that failed and has no failed subtest is printed with its
# package, and so is a test still running when its binary timed out. The
# flags go into every go test line. A log of a test binary run directly,
# as scripts/sim.sh does, has no package line; PKG then names the
# package, as in PKG=./internal/sim.
#
# -gosim, before the flags, says the log is of scripts/sim.sh, whose
# failed tests run again with scripts/sim.sh. The simulator's tests print
# their own replay lines, with the variables that set their seeds and
# planted bug: scripts/sim.sh lines from runs under gosim, go test lines
# from native runs under -race. Those are printed as they are, with the
# flags in a go test line, and a test with no line of its own gets the
# generic one. A seed of a simulator test that timed out gets its seed
# variables here. A subtest named after a test function runs as that
# test.
set -euo pipefail
log=$1
shift
gosim=0
if [ "${1:-}" = -gosim ]; then
	gosim=1
	shift
fi
flags=("$@")
awk -v flags="${flags[*]:-}" -v pkgarg="${PKG:-}" -v gosim="$gosim" '
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
	if (match($0, /replay: .*(go test|scripts\/sim\.sh) .*'\''[^'\'']*'\''/)) {
		p = substr($0, RSTART, RLENGTH)
		sub(/'\''$/, "", p)
		sub(/.*'\''/, "", p)
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
function flush(pkg, i, j, k, p, first, leaf, parts, pattern, env) {
	for (i = 1; i <= n; i++) {
		leaf = 1
		for (j = 1; j <= n; j++) {
			if (index(failed[j], failed[i] "/") == 1) {
				leaf = 0
			}
		}
		if (!leaf) {
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
		if (!timedOut[i] && (pattern in own)) {
			continue
		}
		env = ""
		# A subtest named after a number is a seed (simrun.Seeds); a test
		# with fixed seeds of its own ignores the variables.
		if (first < k && parts[first + 1] ~ /^[0-9]+$/) {
			env = "SIM_SEED=" parts[first + 1] " SIM_SEEDS=1 "
		}
		if (gosim) {
			printf "replay: %sscripts/sim.sh %s '\''%s'\''\n", env, pkg, pattern
		} else {
			printf "replay: %s%s-count=1 -run '\''%s'\'' %s\n", env, gotest(), pattern, pkg
		}
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
grep -hE 'replay: .*(go test|scripts/sim\.sh) ' "$log" | sed 's/^ *[a-z_]*\.go:[0-9]*: //' | sort -u | sed "s/^\(replay: [^']*\)go test /\1go test ${flags[*]:+${flags[*]} }/" || true

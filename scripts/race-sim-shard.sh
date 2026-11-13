#!/usr/bin/env bash
# Prints the -run pattern of the simulator's tests that race-sim's shard
# SHARD of SHARDS runs:
#
#   go test -race -run "$(scripts/race-sim-shard.sh 0 4)" ./internal/sim
#
# Every test goes to exactly one shard. The tests are dealt out longest
# first, each to the shard with the least time so far, by the times
# below. Those are seconds under -race on GitHub's arm runner, from
# runs 43384098979 and 43384101963 (#602). A test not listed counts 10 s, about
# what one took on average. A new long test should join the list.
set -euo pipefail
shard=${1:-}
shards=${2:-}
if ! [[ $shard =~ ^[0-9]+$ && $shards =~ ^[1-9][0-9]*$ ]] || [ "$shard" -ge "$shards" ]; then
	echo "usage: $0 <shard> <shards>, with 0 <= shard < shards" >&2
	exit 2
fi
here=$(cd "$(dirname "$0")/.." && pwd)
listed=$(cd "$here" && go test -list '.*' ./internal/sim)
# go test -run matches tests, fuzz targets and examples alike.
tests=$(grep -E '^(Test|Fuzz|Example)' <<<"$listed" | sort || true)
if [ -z "$tests" ]; then
	echo "$0: no tests listed in ./internal/sim" >&2
	exit 1
fi
awk -v shard="$shard" -v shards="$shards" '
BEGIN {
	split("TestLoneSegmentHeader 1278 TestAllLinkMasks 984 TestMembershipFaults 572 " \
		"TestPowerLossAfterKill 342 TestSeeds 295 TestEndWaitsForCatchUp 238 " \
		"TestSnapshotFromCorruptLeader 187 TestKubeCacheSlots 179 TestHalfOpenLink 178 " \
		"TestGrayFsyncOutlastsHealing 174 TestCleanerSettles 170 TestSlowFetch 161 " \
		"TestFailedSyncThenRestart 158 TestFailedCutThenRestart 153 " \
		"TestStalledLeaderIsReplaced 132 TestLinearizable 128 TestPastRAMRecovery 127 " \
		"TestNewestVersion 116 TestMembershipColdRestart 105 TestFormedClusterAnswers 100", t, " ")
	for (i = 1; i in t; i += 2) secs[t[i]] = t[i + 1]
}
{ name[NR] = $1; w[NR] = ($1 in secs) ? secs[$1] : 10 }
END {
	# Longest first, then by name, so that every shard deals alike.
	for (i = 1; i <= NR; i++) order[i] = i
	for (i = 2; i <= NR; i++) {
		for (j = i; j > 1; j--) {
			a = order[j - 1]; b = order[j]
			if (w[a] > w[b] || w[a] == w[b] && name[a] < name[b]) break
			order[j - 1] = b; order[j] = a
		}
	}
	for (s = 0; s < shards; s++) load[s] = 0
	pattern = ""
	for (i = 1; i <= NR; i++) {
		k = order[i]
		least = 0
		for (s = 1; s < shards; s++) if (load[s] < load[least]) least = s
		load[least] += w[k]
		if (least == shard) pattern = pattern (pattern == "" ? "" : "|") name[k]
	}
	printf "^(%s)$\n", pattern
}' <<<"$tests"

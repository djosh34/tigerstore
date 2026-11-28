#!/usr/bin/env bash
# The part of a Codex Cloud campaign (scripts/codex-campaign.mjs) that
# runs in the VM, from the commit under test:
#
#   scripts/codex-campaign-vm.sh start '<plan>'
#   /workspace/tigerstore/scripts/codex-campaign-vm.sh report
#
# start runs the plan detached, prints "started", its pid and the plan's
# SHA-256, and returns at once; report prints how far it got. The plan is
# JSON: a list of jobs, each a test and its process queues of spans
# [first seed, seeds, seeds run twice], run one job after the other, the
# queues of a job at once:
#
#   [{"test":"TestCampaign","processes":[[[1,60,0],[62,10,0]],[[72,70,0]]]}]
#
# Each span of a queue runs as one native test binary, as sim-campaign.sh
# does on Actions. There is no -test.timeout: a queue may run for hours,
# and every seed has its own real-time backstop (SIM_WALL in
# docs/testing.md). There is no SIM_OUT either: a failure is replayed
# from its seed and commit, and the files of many failed runs could fill
# the VM's disk.
#
# report prints one line per fact, which the host script parses. A
# model turn keeps only the last 64 KiB of a command's output, so the
# failures come first, grouped by cause, the counts last, and the last
# line says how many lines there are, so a cut report shows:
#   cause <test> <seeds> <error>          failed seeds of one error, its
#                                         numbers aside; seeds as 1-3,48
#   replay <test> <seed> <command>        when it is not the plain one
#   known <test> <seed> <issue>           skipped: in known-failures.txt
#   campaign <test> <counts>              TestCampaign's count lines
#   build <line>                          the build log's end, if no seed ran
#   commit <sha>
#   state running|done <exit code>
#   elapsed <seconds since the seeds started>
#   cpu <core-seconds the VM used since the seeds started>
#   reported <test> <first>-<last>,...    seeds with a driver verdict
#   end <lines>
set -uo pipefail
dir=${CODEX_CAMPAIGN_DIR:-/tmp/campaign}
root=$(cd "$(dirname "$0")/.." && pwd)

cpu_seconds() {
	awk '/^usage_usec/ {printf "%d\n", $2 / 1000000}' /sys/fs/cgroup/cpu.stat
}

case ${1:-} in
start)
	if [ -e "$dir" ]; then
		echo "$dir exists: a campaign already ran in this VM" >&2
		exit 1
	fi
	mkdir -p "$dir"
	jq -e 'type == "array"' <<<"$2" >/dev/null || exit 1
	printf '%s\n' "$2" >"$dir/plan.json"
	setsid nohup "$0" run >"$dir/run.log" 2>&1 </dev/null &
	echo "started $! $(printf '%s' "$2" | sha256sum | cut -d ' ' -f 1)"
	;;
run)
	# The environment's setup put Go 1.27.2 and a module and build cache
	# here (#476).
	tools=/workspace/.tigerstore-tools
	export PATH=$tools/go/bin:$PATH GOMODCACHE=$tools/gopath/pkg/mod GOCACHE=$tools/cache GOTOOLCHAIN=local
	# As sim-campaign.yml: tests format a 512 MiB log ring (#602). The
	# native test binary runs each seed with the gosim command sim.sh
	# builds; without it the tests skip.
	export ETCD_UNSAFE_LOG_RING_BYTES=536870912 GOSIMTOOL=$root/.gosim/bin/gosim
	cd "$root" || exit 1
	if ! scripts/sim.sh ./internal/sim '^$' >"$dir/build.log" 2>&1; then
		echo 1 >"$dir/exit"
		exit 1
	fi
	date +%s >"$dir/started"
	cpu_seconds >"$dir/cpu-start"
	status=0
	for ((j = 0; j < $(jq length "$dir/plan.json"); j++)); do
		test=$(jq -r ".[$j].test" "$dir/plan.json")
		pids=()
		for ((p = 0; p < $(jq ".[$j].processes | length" "$dir/plan.json"); p++)); do
			(
				cd internal/sim || exit 1
				s=0
				while read -r seed n twice; do
					echo "process $p: seeds $seed to $((seed + n - 1)) of $test"
					SIM_SEED=$seed SIM_SEEDS=$n SIM_TWICE=$twice "$root/.gosim/bin/sim.test" -test.run "^$test\$" -test.v -test.timeout 0 || s=1
				done < <(jq -r ".[$j].processes[$p][] | \"\(.[0]) \(.[1]) \(.[2])\"" "$dir/plan.json")
				exit "$s"
			) >"$dir/seeds-$j-$p.log" 2>&1 &
			pids+=($!)
		done
		for pid in "${pids[@]}"; do
			wait "$pid" || status=1
		done
	done
	cpu_seconds >"$dir/cpu-end"
	date +%s >"$dir/ended"
	echo "$status" >"$dir/exit"
	;;
report)
	cd "$root" || exit 1
	out=$(mktemp)
	{
		for ((j = 0; j < $(jq length "$dir/plan.json"); j++)); do
			test=$(jq -r ".[$j].test" "$dir/plan.json")
			cat "$dir"/seeds-"$j"-*.log 2>/dev/null | awk -v test="$test" '
			# A failed seed'"'"'s error: the driver'"'"'s own line about it, as
			# a run that played out differently the second time, or else the
			# first message of the seed that passing seeds do not print.
			$0 ~ "^=== RUN   " test "/[0-9]+$" {
				split($3, a, "/")
				cur = a[2] + 0
				driver[cur] = inner[cur] = ""
			}
			/^    native\.go:[0-9]+: / && driver[cur] == "" {
				msg = $0
				sub(/^    native\.go:[0-9]+: /, "", msg)
				if (msg ~ /(failed in gosim with seed|played out differently|left no result|saved no image|is not the image|in gosim with seed [0-9]+:)/ && msg !~ /: test failed$/) {
					driver[cur] = msg
				}
			}
			/^ +[a-z_]+\.go:[0-9]+: / && inner[cur] == "" {
				msg = $0
				sub(/^ +[a-z_]+\.go:[0-9]+: /, "", msg)
				if (msg !~ /^(the gosim run of|=== RUN|Using pre-built|revision witness used|campaign:|trace of seed|the nodes at the end|hashes|replay:|the run is saved|seed [0-9]+ real-time backstop|image of seed|fork [0-9]+ hashes|set SIM_OUT)/ &&
					msg !~ /^seed [0-9]+: .* on [0-9] voters, [0-9]+ clients \(/ &&
					msg !~ / failed in gosim with seed [0-9]+: test failed$/) {
					inner[cur] = msg
				}
			}
			# A replay with more than the seed (a second run, a fork) wins.
			/^ +([a-z_]+\.go:[0-9]+: )?replay: / {
				r = $0
				sub(/^.*replay: /, "", r)
				if (!(cur in replay) || r ~ /SIM_(TWICE|FORK|IMAGE)/) {
					replay[cur] = r
				}
			}
			$0 ~ "^simrun: seed " test "/[0-9]+ FAIL$" {
				split($3, a, "/")
				x = a[2] + 0
				e = driver[x] != "" ? driver[x] : inner[x]
				if (e == "") {
					e = "(no message; see the replay)"
				}
				e = substr(e, 1, 300)
				key = e
				sub(/^seed [0-9]+: /, "", key)
				gsub(/[0-9]+(\.[0-9]+)?/, "N", key)
				if (!(key in error)) {
					error[key] = e
					order[++causes] = key
				}
				# Seeds of a cause as ranges of consecutive seeds.
				if (key in last && x == last[key] + 1) {
					tail[key] = "-" x
				} else {
					seeds[key] = seeds[key] (key in last ? tail[key] "," : "") x
					tail[key] = ""
				}
				last[key] = x
				if (x in replay && replay[x] ~ /SIM_(TWICE|FORK|IMAGE)/ && ++replays <= 200) {
					extra[replays] = "replay " test " " x " " replay[x]
				}
			}
			/known failure: seed [0-9]+ of [^ ]+ fails because of / {
				match($0, /seed [0-9]+ of [^ ]+ fails because of [^ ]+/)
				split(substr($0, RSTART, RLENGTH), k, " ")
				if (k[4] == test && !((k[2], k[8]) in known)) {
					known[k[2], k[8]] = 1
					print "known " test " " k[2] " " k[8]
				}
			}
			/^ +campaign_test\.go:[0-9]+: campaign: / && !/by cost: fresh 0, aged 0, long 0 of 0$/ {
				c = $0
				sub(/^.*campaign: /, "", c)
				print "campaign " test " " c
			}
			END {
				for (i = 1; i <= causes; i++) {
					print "cause " test " " seeds[order[i]] tail[order[i]] " " error[order[i]]
				}
				for (i = 1; i <= replays && i <= 200; i++) {
					print extra[i]
				}
			}'
		done
		if [ ! -e "$dir/started" ]; then
			# Still building, or the build failed.
			tail -n 20 "$dir/build.log" 2>/dev/null | cut -c 1-300 | sed 's/^/build /'
		fi
		echo "commit $(git rev-parse HEAD)"
		if [ -e "$dir/exit" ]; then
			echo "state done $(cat "$dir/exit")"
		else
			echo "state running"
		fi
		if [ -e "$dir/started" ]; then
			now=$(cat "$dir/ended" 2>/dev/null || date +%s)
			echo "elapsed $((now - $(cat "$dir/started")))"
			echo "cpu $(($(cat "$dir/cpu-end" 2>/dev/null || cpu_seconds) - $(cat "$dir/cpu-start")))"
		fi
		for ((j = 0; j < $(jq length "$dir/plan.json"); j++)); do
			test=$(jq -r ".[$j].test" "$dir/plan.json")
			# Only the driver's verdict counts as reported (sim-campaign.py).
			ranges=$(cat "$dir"/seeds-"$j"-*.log 2>/dev/null |
				sed -nE "s#^simrun: seed $test/([0-9]+) (PASS|FAIL|SKIP known-failure)\$#\\1#p" | sort -n -u |
				awk 'NR == 1 || $1 != last + 1 { if (NR > 1) out = out (start == last ? start : start "-" last) ","; start = $1 } { last = $1 } END { if (NR) print out (start == last ? start : start "-" last) }')
			if [ -n "$ranges" ]; then
				echo "reported $test $ranges"
			fi
		done
	} >"$out"
	cat "$out"
	echo "end $(($(wc -l <"$out") + 1))"
	rm -f "$out"
	;;
*)
	echo "usage: $0 start <plan> | run | report" >&2
	exit 2
	;;
esac

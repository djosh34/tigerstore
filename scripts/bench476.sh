#!/usr/bin/env bash
# The runner bench of #476: the same tests on a GitHub runner and in a
# Codex Cloud VM. Run from the repository root:
#
#   scripts/bench476.sh tools   # sysbench (apt, no root) and fio 3.39 (source)
#   scripts/bench476.sh micro   # CPU, memory bandwidth, fsync rate, sequential write
#   scripts/bench476.sh seeds   # cold build, then seeds 1 to 400 of TestSeeds,
#                               # in one process and in four of 100
#   scripts/bench476.sh kube    # seed 202611040014001 of TestKubernetesLoad, SIM_LONG=1
#   scripts/bench476.sh public  # the settings of RunsOn's public runner bench
#                               # (runs-on-demo/benchmark-public, bin/hw-bench.py)
#
# Each result is a line "R <name> <value> <unit>". TOOLS (default
# /tmp/bench476) holds the tools and logs; WORK (default /tmp) is where
# fio writes.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tools=${TOOLS:-/tmp/bench476}
work=${WORK:-/tmp}
mkdir -p "$tools"
export LD_LIBRARY_PATH=$tools/pk/usr/lib/$(uname -m)-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
sysbench=$tools/pk/usr/bin/sysbench
fio=$tools/fio/fio
r() { echo "R $*"; }

info() {
	r arch "$(uname -m)" -
	r nproc "$(nproc)" cpus
	r cpu_max "$(cat /sys/fs/cgroup/cpu.max 2>/dev/null | tr ' ' /)" quota/period
	r mem_max "$(cat /sys/fs/cgroup/memory.max 2>/dev/null)" bytes
	r mem_total "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" KiB
	r model "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //; s/ /_/g')" -
	r kernel "$(uname -r)" -
	r disk "$(df -h "$work" | awk 'NR==2 {print $1"_"$2}')" -
}

case ${1:-} in
tools)
	d=$tools/apt
	mkdir -p "$d/lists/partial" "$d/cache/archives/partial" "$tools/deb"
	a="-o Dir::State::Lists=$d/lists -o Dir::Cache=$d/cache -o Debug::NoLocking=1"
	apt-get $a update >/dev/null 2>&1
	# Debian 13 and Ubuntu 24.04 both have sysbench 1.0.20; its LuaJIT
	# library has another package name on each.
	for p in sysbench libluajit-5.1-2 libluajit2-5.1-2 libaio1t64; do
		(cd "$tools/deb" && apt-get $a download "$p" >/dev/null 2>&1) || true
	done
	for f in "$tools"/deb/*.deb; do dpkg -x "$f" "$tools/pk"; done
	"$sysbench" --version
	if [ ! -x "$fio" ]; then
		curl -fsSL https://github.com/axboe/fio/archive/refs/tags/fio-3.39.tar.gz | tar -xz -C "$tools"
		mv "$tools/fio-fio-3.39" "$tools/fio"
		(cd "$tools/fio" && ./configure >/dev/null && make -j"$(nproc)" >/dev/null 2>&1)
	fi
	"$fio" --version
	;;
micro)
	info
	for i in 1 2 3; do
		for t in 1 4; do
			r "cpu_t$t" "$("$sysbench" cpu --cpu-max-prime=20000 --threads=$t --time=20 run | awk '/events per second/ {print $4}')" events/s
			r "mem_t$t" "$("$sysbench" memory --memory-block-size=1M --memory-total-size=1000G --threads=$t --time=20 run | sed -n 's/.*(\([0-9.]*\) MiB\/sec).*/\1/p')" MiB/s
		done
		f=$work/bench476-fio
		mkdir -p "$f"
		"$fio" --name=fsync --directory="$f" --rw=write --bs=4k --size=256m --fsync=1 --ioengine=psync \
			--time_based --runtime=30 --output-format=json >"$tools/fsync-$i.json"
		r fsync_iops "$(jq '.jobs[0].write.iops' "$tools/fsync-$i.json")" writes+fsyncs/s
		r fsync_p99 "$(jq '.jobs[0].sync.lat_ns.percentile["99.000000"] / 1000' "$tools/fsync-$i.json")" us
		rm -rf "$f"/*
		"$fio" --name=seq --directory="$f" --rw=write --bs=1M --size=4g --ioengine=psync --end_fsync=1 \
			--output-format=json >"$tools/seq-$i.json"
		r seq_write "$(jq '.jobs[0].write.bw / 1024' "$tools/seq-$i.json")" MiB/s
		rm -rf "$f"
	done
	;;
seeds)
	info
	export GOCACHE=$tools/gocache
	rm -rf "$GOCACHE"
	go mod download && go -C tools/gosim mod download
	s=$SECONDS
	scripts/sim.sh ./internal/sim '^$' >"$tools/build.log" 2>&1 || { tail -20 "$tools/build.log"; exit 1; }
	r build $((SECONDS - s)) s
	export GOSIMTOOL=$root/.gosim/bin/gosim
	cd internal/sim
	s=$SECONDS
	SIM_SEED=1 SIM_SEEDS=400 ../../.gosim/bin/sim.test -test.run '^TestSeeds$' -test.v -test.timeout 90m >"$tools/seeds-1.log" 2>&1
	st=$?
	r seeds_1proc $((SECONDS - s)) s
	r seeds_1proc_exit $st -
	r seeds_hashes "$(grep -o 'hashes: .*' "$tools/seeds-1.log" | sha256sum | cut -c1-16)" sha256
	r seeds_hash_lines "$(grep -c 'hashes: ' "$tools/seeds-1.log")" seeds
	s=$SECONDS
	pids=()
	for p in 0 1 2 3; do
		SIM_SEED=$((1 + p * 100)) SIM_SEEDS=100 ../../.gosim/bin/sim.test -test.run '^TestSeeds$' -test.v -test.timeout 90m >"$tools/seeds-4-$p.log" 2>&1 &
		pids+=($!)
	done
	st=0
	for p in "${pids[@]}"; do wait "$p" || st=1; done
	r seeds_4proc $((SECONDS - s)) s
	r seeds_4proc_exit $st -
	grep -E -- '^--- FAIL|^    --- FAIL' "$tools"/seeds-*.log | head -20
	;;
kube)
	info
	export GOCACHE=$tools/gocache
	scripts/sim.sh ./internal/sim '^$' >"$tools/build.log" 2>&1 || { tail -20 "$tools/build.log"; exit 1; }
	s=$SECONDS
	OUT=$tools FIRST=202611040014001 MINUTES=1 scripts/kube-sim.sh | tee "$tools/kube.out"
	st=${PIPESTATUS[0]}
	r kube $((SECONDS - s)) s
	r kube_exit "$st" -
	r kube_peak "$(sed -n 's/.*peak \([0-9]*\) MiB.*/\1/p' "$tools/kube.out")" MiB
	r kube_stats "$(grep -o 'Messages:[0-9]* Dropped:[0-9]* Crashes:[0-9]*' "$tools/seed-202611040014001.log" | head -1 | tr ' ' ,)" -
	;;
public)
	info
	for i in 1 2 3; do
		for t in 1 4; do
			r "pub_cpu_t$t" "$("$sysbench" cpu --threads=$t --time=20 run | awk '/events per second/ {print $4}')" events/s
		done
		for op in read write; do
			r "pub_mem_$op" "$("$sysbench" memory --memory-oper=$op --threads=4 --time=15 run | sed -n 's/.*(\([0-9.]*\) MiB\/sec).*/\1/p')" MiB/s
		done
		f=$work/bench476-fio
		mkdir -p "$f"
		# RunsOn uses libaio; this fio has psync, so the sequential
		# write runs at queue depth 1 instead of 32.
		"$fio" --name=sync --directory="$f" --rw=write --bs=4k --size=2g --direct=1 --fdatasync=1 --ioengine=psync \
			--time_based --runtime=20 --ramp_time=2 --output-format=json >"$tools/pub-sync-$i.json" 2>"$tools/pub-sync-$i.err"
		r pub_sync_iops "$(jq '.jobs[0].write.iops' "$tools/pub-sync-$i.json" 2>/dev/null || head -c 200 "$tools/pub-sync-$i.err" | tr ' ' _)" IOPS
		rm -rf "$f"/*
		"$fio" --name=seq --directory="$f" --rw=write --bs=1M --size=2g --direct=1 --ioengine=psync \
			--time_based --runtime=20 --ramp_time=2 --output-format=json >"$tools/pub-seq-$i.json" 2>"$tools/pub-seq-$i.err"
		r pub_seq_write "$(jq '.jobs[0].write.bw / 1024' "$tools/pub-seq-$i.json" 2>/dev/null || head -c 200 "$tools/pub-seq-$i.err" | tr ' ' _)" MiB/s
		rm -rf "$f"
	done
	;;
*)
	echo "usage: $0 tools|micro|seeds|kube" >&2
	exit 2
	;;
esac

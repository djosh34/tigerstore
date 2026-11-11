#!/usr/bin/env bash
# THROWAWAY PROTOTYPE (#630). Question: can two GitHub Actions runners
# reach each other over tailcat, and at what throughput/latency, direct
# or DERP?
#
# One node of an N-node tailcat mesh on a GitHub-hosted runner. Nodes
# meet through artifacts of the same workflow run: each publishes its
# tailcat *client public key*, then starts `tailcat serve --allow=<the
# other nodes' public keys>` and publishes its address. Because of
# --allow, the address alone does not let anyone else in, so the public
# artifacts leak nothing usable. The address is never printed to the log.
#
#   tailcat-node.sh <step>     steps: info keys serve clients stability linger
#
# Env: N (this node), NODES, RUN, R (owner/repo), GH_TOKEN, X (exchange
# dir, uploaded by the workflow), O (results dir, uploaded), S (private).
set -euo pipefail

: "${N:?}" "${NODES:?}" "${RUN:?}" "${R:?}"
X=${X:-$PWD/xchg} O=${O:-$PWD/out} S=${S:-$HOME/.tcpriv}
mkdir -p "$X" "$O" "$S"
TC=${TC:-$HOME/go/bin/tailcat}
here=$(cd "$(dirname "$0")" && pwd)

log() { echo "[$(date -u +%T)] node$N: $*" >&2; }

arts() { gh api "repos/$R/actions/runs/$RUN/artifacts?per_page=100" --jq '.artifacts[].name' 2>/dev/null || true; }

# wait_art name... : waits until every named artifact exists in this run.
wait_art() {
	local deadline=$((SECONDS + ${WAIT:-1200})) have missing
	while :; do
		have=$(arts)
		missing=()
		for a in "$@"; do grep -qx "$a" <<<"$have" || missing+=("$a"); done
		[ ${#missing[@]} -eq 0 ] && return 0
		if [ $SECONDS -gt $deadline ]; then log "timed out waiting for ${missing[*]}"; return 1; fi
		sleep 5
	done
}

# get_art name dir
get_art() {
	local id
	id=$(gh api "repos/$R/actions/runs/$RUN/artifacts?per_page=100" --jq ".artifacts[] | select(.name==\"$1\") | .id" | head -1)
	mkdir -p "$2"
	gh api "repos/$R/actions/artifacts/$id/zip" >"$S/$1.zip"
	unzip -o -q "$S/$1.zip" -d "$2"
}

peers_above() { local j; for ((j = N + 1; j <= NODES; j++)); do echo "$j"; done; }
others() { local j; for ((j = 1; j <= NODES; j++)); do [ "$j" = "$N" ] || echo "$j"; done; }

case $1 in
info)
	imds=$(curl -s -m 3 -H Metadata:true "http://169.254.169.254/metadata/instance/compute?api-version=2021-02-01" || true)
	pubip=$(curl -s -m 5 https://api.ipify.org || true)
	python3 -I - "$O/info.json" "$imds" "$pubip" <<'PY'
import json, os, platform, subprocess, sys
out, imds, pubip = sys.argv[1:]
try:
    m = json.loads(imds)
    imds = {k: m.get(k) for k in ("location", "vmSize", "zone", "azEnvironment")}
except Exception:
    imds = {"error": (imds or "no IMDS reply")[:200]}
lscpu = subprocess.run(["lscpu"], capture_output=True, text=True).stdout
model = next((l.split(":", 1)[1].strip() for l in lscpu.splitlines() if l.startswith("Model name")), "?")
ips = subprocess.run(["hostname", "-I"], capture_output=True, text=True).stdout.split()
json.dump({
    "node": int(os.environ["N"]), "runner": os.environ.get("RUNNER_NAME"),
    "label": os.environ.get("LABEL"), "arch": platform.machine(), "cores": os.cpu_count(),
    "cpu": model, "mem_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1),
    "imds": imds, "public_ip": pubip, "local_ips": ips,
}, open(out, "w"), indent=1)
PY
	cat "$O/info.json"
	;;
keys)
	"$TC" genkey --client --key=client-default 2>/dev/null | grep -o 'nodekey:[0-9a-f]*' >"$X/pub"
	log "client key $(cut -c1-20 "$X/pub")..."
	;;
serve)
	allow=()
	for j in $(others); do wait_art "pub-$j"; get_art "pub-$j" "$S/pub-$j"; allow+=("$(cat "$S/pub-$j/pub")"); done
	allow_csv=$(IFS=,; echo "${allow[*]}")
	iperf3 -s -p 5202 -D --logfile "$S/iperf3.log"
	nohup python3 -I "$here/echo.py" server 7000 >"$S/echo.log" 2>&1 &
	TAILCAT_ADDR_FILE=$S/addr nohup "$TC" --verbose serve --key=new --allow="$allow_csv" perf,5202,7000 >"$S/serve.out" 2>"$S/serve.log" &
	echo $! >"$S/serve.pid"
	for _ in $(seq 120); do [ -s "$S/addr" ] && break; sleep 0.5; done
	[ -s "$S/addr" ] || { log "serve did not print an address"; sed -E 's/tc[A-Za-z0-9_-]{40,}/<addr>/g' "$S/serve.log" | tail -30; exit 1; }
	cp "$S/addr" "$X/addr"
	log "serving perf,5202,7000 to ${#allow[@]} allowed peers (address not printed)"
	;;
clients)
	if [ "$N" -gt 1 ]; then wait_art "done-$((N - 1))"; fi
	for j in $(peers_above); do
		wait_art "addr-$j"; get_art "addr-$j" "$S/addr-$j"
		A=$(cat "$S/addr-$j/addr")
		d=$O/to$j; mkdir -p "$d"
		log "→ node$j: ping until direct"
		t0=$(date +%s.%N)
		"$TC" ping --until-direct --timeout=30s "$A" >"$d/ping-until-direct.txt" 2>&1 || true
		echo "elapsed $(echo "$(date +%s.%N) - $t0" | bc)s" >>"$d/ping-until-direct.txt"
		cat "$d/ping-until-direct.txt"
		direct=no; grep -q 'via [0-9\[]' "$d/ping-until-direct.txt" && direct=yes
		echo "$direct" >"$d/direct"
		if [ $direct = yes ]; then
			for t in "tcp1:" "tcp4:--parallel=4" "tcp1rev:--reverse" "tcp4rev:--parallel=4 --reverse" "udp:--udp --bitrate=200M"; do
				name=${t%%:*}; args=${t#*:}
				log "→ node$j: tailcat perf $name"
				# shellcheck disable=SC2086
				"$TC" --json perf $args --time=10s --timeout=30s "$A" >"$d/perf-$name.json" 2>"$d/perf-$name.err" || log "perf $name failed: $(tail -1 "$d/perf-$name.err")"
			done
		else
			log "→ node$j: no direct path; skipping throughput tests (tailcat refuses them on Tailscale's shared DERP)"
		fi
		"$TC" forward "$A" 15202:5202 17000:7000 >"$d/forward.out" 2>"$d/forward.log" &
		fpid=$!
		for _ in $(seq 60); do grep -q 17000 "$d/forward.log" 2>/dev/null && break; sleep 0.5; done
		if [ $direct = yes ]; then
			for t in "iperf1:-P 1" "iperf4:-P 4" "iperf1rev:-P 1 -R" "iperf8:-P 8"; do
				name=${t%%:*}; args=${t#*:}
				log "→ node$j: iperf3 $name over tailcat forward"
				# shellcheck disable=SC2086
				timeout 40 iperf3 -c 127.0.0.1 -p 15202 -t 10 $args -J >"$d/$name.json" 2>&1 || log "iperf3 $name failed"
			done
		fi
		log "→ node$j: echo RTT"
		python3 -I "$here/echo.py" rtt 17000 3000 64 >"$d/echo-64.json" || true
		python3 -I "$here/echo.py" rtt 17000 1000 4096 >"$d/echo-4k.json" || true
		cat "$d/echo-64.json"
		kill $fpid 2>/dev/null || true
	done
	;;
stability)
	deps=(); for ((j = 1; j <= NODES; j++)); do deps+=("done-$j"); done
	wait_art "${deps[@]}"
	pids=()
	for j in $(peers_above); do
		(
			A=$(cat "$S/addr-$j/addr")
			d=$O/to$j; mkdir -p "$d"
			lp=$((18000 + j))
			"$TC" --verbose forward "$A" "$lp:7000" >"$d/stab-forward.out" 2>"$d/stab-forward.raw" &
			fpid=$!
			for _ in $(seq 60); do grep -q "$lp" "$d/stab-forward.raw" 2>/dev/null && break; sleep 0.5; done
			(
				end=$((SECONDS + ${STAB_SECS:-300}))
				while [ $SECONDS -lt $end ]; do
					echo "$(date -u +%T) $(timeout 20 "$TC" ping "$A" 2>&1 | tail -1)"
					sleep 15
				done
			) >"$d/stab-paths.txt" &
			ppid=$!
			python3 -I "$here/echo.py" stab "$lp" "${STAB_SECS:-300}" >"$d/stab-echo.json" || true
			wait $ppid || true
			kill $fpid 2>/dev/null || true
			sleep 1
			# Keep only path-related magicsock lines; redact anything address-shaped.
			grep -Ei 'derp|disco|endpoint|now using|path|switch|magicsock' "$d/stab-forward.raw" |
				sed -E 's/tc[A-Za-z0-9_-]{40,}/<addr>/g' | tail -400 >"$d/stab-forward-paths.log" || true
			rm -f "$d/stab-forward.raw"
		) &
		pids+=($!)
	done
	for p in "${pids[@]}"; do wait "$p" || true; done
	;;
linger)
	deps=(); for ((j = 1; j <= NODES; j++)); do deps+=("fin-$j"); done
	wait_art "${deps[@]}" || true
	kill "$(cat "$S/serve.pid")" 2>/dev/null || true
	sed -E 's/tc[A-Za-z0-9_-]{40,}/<addr>/g' "$S/serve.log" | grep -Ei 'derp|disco|now using|endpoint|magicsock' | tail -300 >"$O/serve-paths.log" || true
	;;
*)
	echo "unknown step $1" >&2; exit 2 ;;
esac

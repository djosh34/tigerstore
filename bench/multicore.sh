#!/usr/bin/env bash
# Closed loop, 8 TLS connections, N/8 streams each. No affinity changes.
set -euo pipefail
cd "$(dirname "$0")/.."
unset GOMAXPROCS GOMEMLIMIT GOFLAGS
export NO_PIN=1
export CERTS=$(realpath "${CERTS:-certs}")
export ETCDCTL=$(realpath bin/etcdctl)
bins=$(realpath bin)
out=${OUT:-results}
data=${DATA:?DATA must be a disk directory}
workloads=${WORKLOADS:-put txn range}
clients=${CLIENTS:-64 256 1024}
reps=${REPS:-3}
deadline=1791928800 # 2026-10-13T22:00:00Z
mkdir -p "$out"
cleanup() { bench/cluster.sh stop "$data/cluster"; }
trap cleanup EXIT
for workload in $workloads; do
  for n in $clients; do
    for rep in $(seq "$reps"); do
      targets='etcd tigerstore'
      if ((rep % 2 == 0)); then targets='tigerstore etcd'; fi
      for target in $targets; do
        if (($(date -u +%s) >= deadline)); then echo 'deadline reached' >&2; exit 3; fi
        name="$workload-$n-$target-$rep"
        if [[ -e "$out/$name.json" ]]; then echo "refusing overwrite $name" >&2; exit 2; fi
        attempt=0
        while true; do
        attempt=$((attempt+1))
        if ((attempt > 20)); then echo 'host stayed noisy' >&2; exit 1; fi
        cleanup
        if [ "${HOST_QUIET:-0}" = 1 ]; then python3 bench/wait-quiet.py; fi
        # Dummy CPU lists satisfy cluster.sh's interface; NO_PIN skips both taskset calls.
        bench/cluster.sh start "$target" "$bins/$target" "$data/cluster" all all all
        cp "$data/cluster/placement.json" "$out/$name.placement.json"
        {
          date -u +%FT%TZ
          for pid in $(tr ',' ' ' < "$data/cluster/pids"); do
            echo "pid $pid"
            grep -E 'Cpus_allowed_list|Mems_allowed_list|Threads' "/proc/$pid/status"
            tr '\0' '\n' < "/proc/$pid/environ" | grep -E '^GO(MAXPROCS|MEMLIMIT|FLAGS)=' || true
            cat "/proc/$pid/cgroup"
          done
          echo 'generator inherits same full affinity and unset GOMAXPROCS/GOMEMLIMIT'
          grep Cpus_allowed_list /proc/self/status
        } > "$out/$name.environment.txt"
        "$bins/loadgen" -endpoints "$(< "$data/cluster/leader")" \
          -cacert "$CERTS/ca.pem" -cert "$CERTS/client.pem" -key "$CERTS/client-key.pem" \
          -pids "$(< "$data/cluster/pids")" -label "$target" -workload "$workload" \
          -value-size 1024 -keys 10000 -conns 8 -streams "$((n/8))" \
          -rate 0 -warmup "${WARMUP:-3s}" -duration "${DURATION:-10s}" -seed "$rep" -out "$out/$name.json"
        bench/cluster.sh check "$data/cluster"
        cleanup
        if [ "${HOST_QUIET:-0}" = 1 ] && ! python3 - "$out/$name.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
background = r['machine_cores'] - sum(r['server_cores_by_node']) - r['client_cores']
print('Measured background cores:', round(background, 3), flush=True)
sys.exit(0 if background < 0.8 else 1)
PY
        then
          mkdir -p "$out/excluded"
          for f in "$out/$name".*; do mv "$f" "$out/excluded/$(basename "$f").attempt-$attempt"; done
          echo 'Discarding noisy host window; retrying same store/rep' >&2
          continue
        fi
        break
        done
      done
    done
  done
done

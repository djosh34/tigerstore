#!/usr/bin/env bash
set -euxo pipefail
root=/home/joshazimullah.linux/work_mounts/scratch/pre-50-next
cd "$root/repo"
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB GOMAXPROCS=2
export HOST=1 MODE=primary GENERATOR=lean TOTAL_PUTS=60000 PYTHONDONTWRITEBYTECODE=1
export BIN_DIR="$root/host-lean/bin" WORK_DIR=/tmp/pre-50-next-host-lean-8f27a1
python3 - <<'PY'
from pathlib import Path
p = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::')[1].lstrip('/')
print('cgroup', p)
for name in ['memory.max', 'memory.swap.max', 'memory.current', 'memory.events']:
 print(name, (p / name).read_text())
assert (p / 'memory.max').read_text().strip() == '1073741824'
assert (p / 'memory.swap.max').read_text().strip() == '0'
PY
for clients in 64 256; do
  test "$(( $(date -u +%s) + 120 ))" -lt "$(date -ud 2026-10-13T22:00:00Z +%s)"
  CLIENTS="$clients" OUT="$root/host-lean/c$clients" python3 experiments/pre50next/run.py
  python3 experiments/pre50next/summarize.py "$root/host-lean/c$clients"
done
python3 - <<'PY'
from pathlib import Path
p = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::')[1].lstrip('/')
for name in ['memory.max', 'memory.current', 'memory.events']:
 print(name, (p / name).read_text())
PY
rm -rf "$WORK_DIR"

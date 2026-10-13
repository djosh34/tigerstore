#!/usr/bin/env bash
set -euo pipefail
[[ $(date -u +%s) -lt $(date -u -d '2026-10-13T22:00:00Z' +%s) ]]
: "${RUNNER_TEMP:?}" "${STORE:?}" "${REP:?}" "${TIGERSTORE_REF:?}"
export OUT="$PWD/.burner-results/$STORE-$RUNNER_ARCH-$REP"
export PRIVATE="$RUNNER_TEMP/burner-private" BIN="$RUNNER_TEMP/burner-bin"
export KUBECONFIG="$PRIVATE/kubeconfig" PATH="$BIN:$PATH"
mkdir -p "$OUT" "$PRIVATE" "$BIN"
script="$PWD/experiments/kube-burner"
start=$(date +%s.%N)
# shellcheck disable=SC2317 # The EXIT trap calls this function.
cleanup() {
  rc=$?
  trap - EXIT
  set +e
  python3 "$script/collect.py" "$OUT/diagnostics" > "$OUT/collect.log" 2>&1
  end=$(date +%s.%N)
  python3 - "$OUT" "$start" "$end" "$rc" <<'PY'
import json, os, sys
from pathlib import Path
out, start, end, rc = sys.argv[1:]
Path(out, 'result.json').write_text(json.dumps(dict(store=os.environ['STORE'], rep=int(os.environ['REP']), arch=os.environ['RUNNER_ARCH'], tigerstore_ref=os.environ['TIGERSTORE_REF'], experiment_ref=os.environ['GITHUB_SHA'], exit=int(rc), total_seconds=float(end)-float(start)), indent=2)+'\n')
PY
  kind delete cluster --name burner >> "$OUT/cleanup.log" 2>&1
  docker rm -f external-etcd-1 external-etcd-2 external-etcd-3 >> "$OUT/cleanup.log" 2>&1
  # Runner destruction handles root-owned store files; do not upload their contents.
  exit "$rc"
}
trap cleanup EXIT
bash -x "$script/setup.sh" > "$OUT/setup.log" 2>&1
bash -x "$script/boot.sh" > "$OUT/boot.log" 2>&1
if [[ ${BOOT_ONLY:-0} == 1 ]]; then exit 0; fi
python3 "$script/metrics.py" snapshot "$OUT/before"
set +e
python3 "$script/workload.py" --out "$OUT/workload" > "$OUT/workload.log" 2>&1
rc=$?
set -e
python3 "$script/metrics.py" snapshot "$OUT/after"
python3 "$script/metrics.py" diff "$OUT/before" "$OUT/after" "$OUT/metrics.json"
exit "$rc"

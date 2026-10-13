#!/usr/bin/env bash
# Run on Actions only. No database, network faults, or product builds.
set -euo pipefail
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'GitHub Actions required' >&2; exit 2; }
base=$(cd "$(dirname "$0")" && pwd)
saved=$(realpath "${1:?prepared saved directory}")
case_id=${2:?manifest case ID 0..7}
out=$(realpath -m "${3:?output directory}")
deadline=$(date -ud 2026-10-13T22:00:00Z +%s)
before_deadline() { (( $(date -u +%s) < deadline )); }
before_deadline || { echo 'Deadline reached' >&2; exit 2; }
mkdir -p "$out"
python3 - "$saved" "$case_id" "$out" <<'PY'
import json,pathlib,sys
root=pathlib.Path(sys.argv[1]); m=json.loads((root/'manifest.json').read_text())[int(sys.argv[2])]
path=pathlib.Path(sys.argv[3]); (path/'input.json').write_text(json.dumps(m,indent=2)+'\n')
(path/'source-directory.txt').write_text(str(root/m['directory'])+'\n')
PY
src=$(<"$out/source-directory.txt")
# Resolve dependencies once. This does not compile or build product code.
# Every analyzer invocation below uses a separate 10g JVM.
before_deadline || exit 2
(cd "$base/offline"; lein classpath) > "$out/classpath.txt"
cp=$(<"$out/classpath.txt")
before_deadline || exit 2
timeout --signal=TERM --kill-after=10s 120s java -Xmx10g -cp "$cp" clojure.main -m offline.selftest > "$out/selftest.log" 2>&1
before_deadline || exit 2
java -Xmx10g -cp "$cp" clojure.main -m offline.recheck keys "$src/history.edn" > "$out/keys.txt"
python3 - "$out" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); keys=[int(k) for k in (p/'keys.txt').read_text().split()]
assert keys == json.loads((p/'input.json').read_text())['keys'], 'Full-history keys differ from saved key files'
PY
# The independent checker equivalence is checked again in each child JVM.
while read -r key; do
  mkdir -p "$out/$key"
  resolved=false
  for algorithm in wgl linear competition; do
    if ! before_deadline; then
      printf '%s\n' deadline > "$out/$key/$algorithm.status"
      break
    fi
    seconds=${WGL_SECONDS:-120}
    [[ $algorithm == wgl ]] || seconds=${ALTERNATE_SECONDS:-180}
    started=$(date -u +%FT%TZ)
    set +e
    timeout --signal=TERM --kill-after=10s "${seconds}s" \
      java -Xmx10g -Djava.awt.headless=true -cp "$cp" clojure.main -m offline.recheck \
      check "$src/history.edn" "$src/independent/$key/history.edn" "$key" "$algorithm" \
      "$out/$key/$algorithm.edn" > "$out/$key/$algorithm.log" 2>&1
    rc=$?
    set -e
    printf '%s\n' "$rc" > "$out/$key/$algorithm.status"
    printf 'start=%s end=%s key=%s algorithm=%s rc=%s\n' "$started" "$(date -u +%FT%TZ)" "$key" "$algorithm" "$rc"
    # A verdict requires both a clean exit and its written result.
    if [[ ( $rc == 0 || $rc == 1 ) && -s "$out/$key/$algorithm.edn" ]]; then
      resolved=true
      break
    fi
  done
  # All keys continue even after one false/unknown verdict.
  printf '%s\n' "$resolved" > "$out/$key/resolved.txt"
done < "$out/keys.txt"
python3 "$base/summarize-offline.py" "$out"

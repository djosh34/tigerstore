#!/usr/bin/env bash
set -euo pipefail
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
# Never remount or run a measurement on the agent host.
[[ ${GITHUB_ACTIONS:-false} == true && $(uname -m) == aarch64 ]]
BASELINE=eb8a20eda7b1436e3319b924753469f2d8c0ee99
CANDIDATE=$(git rev-parse "${CANDIDATE_REF:-HEAD}^{commit}")
EXTRA=${EXTRA_BASELINE_REF:-}
if [[ -n $EXTRA ]]; then EXTRA=$(git rev-parse "$EXTRA^{commit}"); fi
OUT=$PWD/evidence/wholebatch/c${CLIENTS:?}
PRIVATE=$RUNNER_TEMP/wholebatch
mkdir -p "$OUT" "$PRIVATE/bin" "$PRIVATE/baseline" "$PRIVATE/candidate"
trap 'rm -rf "$PRIVATE"' EXIT
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB
export CANDIDATE EXTRA BASELINE OUT
python3 - <<'PY'
import json, os
from pathlib import Path
variants = [dict(name='main', binary='baseline', ref=os.environ['BASELINE'], frontend='', batch_mode=''),
            dict(name='etcd', binary='etcd', ref='v3.7.2', frontend='', batch_mode='')]
for mode in ('rpc', 'admit', 'full'):
    variants.append(dict(name=mode, binary='candidate', ref=os.environ['CANDIDATE'], frontend='wholebatch', batch_mode=mode))
if os.environ['EXTRA']:
    variants.insert(1, dict(name='native', binary='native', ref=os.environ['EXTRA'],
                           frontend=os.environ.get('EXTRA_BASELINE_FRONTEND', 'native'), batch_mode=''))
Path(os.environ['OUT'], 'variants.json').write_text(json.dumps(variants, indent=2))
PY
{
 date -u +%FT%TZ
 git rev-parse HEAD
 printf 'main %s\ncandidate %s\nextra %s\n' "$BASELINE" "$CANDIDATE" "$EXTRA"
 sha256sum experiments/wholebatch/{instrument.py,observe.go.txt,osfs.go.txt,load/*.go}
 uname -a
 lscpu
 free -b
 lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
 df -B1 "$PWD" "$PRIVATE"
 findmnt -T "$PRIVATE"
 env | grep -E '^(ImageOS|ImageVersion)='
 go version
} > "$OUT/environment.txt"
check_time
sudo mount -o remount,barrier=1 /
findmnt -T "$PRIVATE" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$OUT/mount.txt"
test "$(findmnt -T "$PRIVATE" -n -o FSTYPE)" = ext4
! findmnt -T "$PRIVATE" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
check_time
curl --fail --location --retry 3 -o "$PRIVATE/etcd.tar.gz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
tar -xzf "$PRIVATE/etcd.tar.gz" -C "$PRIVATE"
cp "$PRIVATE/etcd-v3.7.2-linux-arm64/etcd" "$PRIVATE/etcd-v3.7.2-linux-arm64/etcdctl" "$PRIVATE/bin/"
echo '17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d  '"$PRIVATE/bin/etcd" | sha256sum -c -
git diff --exit-code
git status --short > "$OUT/build-status.txt"
check_time
git archive "$BASELINE" | tar -x -C "$PRIVATE/baseline"
git archive "$CANDIDATE" | tar -x -C "$PRIVATE/candidate"
declare -A refs=([baseline]="$BASELINE" [candidate]="$CANDIDATE" [native]="$EXTRA")
sources=(baseline candidate)
mkdir -p "$OUT/instrumentation"
cp experiments/wholebatch/{instrument.py,observe.go.txt,osfs.go.txt} "$OUT/instrumentation/"
if [[ -n $EXTRA ]]; then
 mkdir -p "$PRIVATE/native"
 git archive "$EXTRA" | tar -x -C "$PRIVATE/native"
 sources+=(native)
fi
for source in "${sources[@]}"; do
 check_time
 python3 experiments/wholebatch/instrument.py "$PRIVATE/$source"
 # Record the complete non-behavioral source overlay, in addition to its hash.
 diff -u <(git show "${refs[$source]}:cmd/tigerstore/main.go") "$PRIVATE/$source/cmd/tigerstore/main.go" > "$OUT/$source-observe.diff" || [[ $? == 1 ]]
 check_time
 (cd "$PRIVATE/$source" && go build -o "$PRIVATE/bin/$source" ./cmd/tigerstore)
done
check_time
PYTHONDONTWRITEBYTECODE=1 python3 experiments/wholebatch/test_tooling.py
check_time
go test -short -parallel 2 ./experiments/wholebatch/load
check_time
go build -o "$PRIVATE/bin/load" ./experiments/wholebatch/load
{
 "$PRIVATE/bin/etcd" --version
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 for source in "${sources[@]}" load; do go version -m "$PRIVATE/bin/$source"; done
} > "$OUT/versions.txt"
check_time
bash scripts/dev-certs.sh "$PRIVATE/certs"
check_time
timeout --kill-after=5s 700s taskset -c 3 python3 experiments/wholebatch/run.py
check_time
python3 experiments/wholebatch/summarize.py "$OUT"

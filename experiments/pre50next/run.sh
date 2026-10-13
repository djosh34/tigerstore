#!/usr/bin/env bash
set -euo pipefail
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
# HOST uses run.py directly with downloaded binaries. This script is runner-only.
[[ ${GITHUB_ACTIONS:-false} == true && ${HOST:-0} != 1 && $(uname -m) == aarch64 ]]
[[ ${CLIENTS:?} == 64 || $CLIENTS == 256 ]]
[[ ${MODE:-primary} == primary || ${MODE:-primary} == profile ]]
export MODE=${MODE:-primary}
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB GOMAXPROCS=2
export OUT=$PWD/evidence/pre50next/$MODE/c$CLIENTS
export WORK_DIR=$RUNNER_TEMP/pre50next-$MODE-c$CLIENTS-${GITHUB_RUN_ATTEMPT:-1}
export BIN_DIR=$WORK_DIR/bin CERT_DIR=$WORK_DIR/certs
[[ ! -e $WORK_DIR && ! -e $OUT ]]
mkdir -p "$OUT" "$BIN_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT
# Save build/download argv, including expanded paths and immutable refs.
exec 19>"$OUT/setup-commands.log"
export BASH_XTRACEFD=19
set -x
{
 date -u +%FT%TZ
 git rev-parse HEAD
 uname -a
 lscpu
 free -b
 lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
 df -B1 "$PWD" "$WORK_DIR"
 findmnt -T "$WORK_DIR"
 env | grep -E '^(ImageOS|ImageVersion)=' || true
 go version
} > "$OUT/environment.txt"
check_time
sudo mount -o remount,barrier=1 /
findmnt -T "$WORK_DIR" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$OUT/mount.txt"
test "$(findmnt -T "$WORK_DIR" -n -o FSTYPE)" = ext4
! findmnt -T "$WORK_DIR" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
check_time
curl --fail --location --retry 3 --max-time 90 -o "$WORK_DIR/etcd.tar.gz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
check_time
tar -xzf "$WORK_DIR/etcd.tar.gz" -C "$WORK_DIR"
cp "$WORK_DIR/etcd-v3.7.2-linux-arm64/etcd" "$WORK_DIR/etcd-v3.7.2-linux-arm64/etcdctl" "$BIN_DIR/"
echo '17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d  '"$BIN_DIR/etcd" | sha256sum -c -
git diff --exit-code
git status --short > "$OUT/build-status.txt"
cp experiments/pre50next/refs.json "$OUT/refs.json"
mkdir "$OUT/instrumentation"
cp experiments/pre50next/{instrument.py,observe.go.txt,osfs.go.txt} "$OUT/instrumentation/"
sha256sum experiments/pre50next/{instrument.py,observe.go.txt,osfs.go.txt,run.py,run.sh,load/*.go} > "$OUT/harness-sha256.txt"
python3 - <<'PY' > "$WORK_DIR/build-refs.tsv"
import json, os, re
refs = json.load(open('experiments/pre50next/refs.json'))
items = [('baseline', refs['base'])]
if os.environ['MODE'] == 'primary':
    assert len(refs['candidates']) <= 3
    items += [(c['name'], c['ref']) for c in refs['candidates']]
seen = {'etcd', 'etcdctl', 'load'}
for name, ref in items:
    assert re.fullmatch(r'[a-z][a-z0-9_-]*', name) and name not in seen
    assert re.fullmatch(r'[0-9a-f]{40}', ref), 'use immutable source SHAs'
    seen.add(name)
    print(name, ref, sep='\t')
PY
while IFS=$'\t' read -r name ref; do
 check_time
 mkdir "$WORK_DIR/$name"
 git archive "$ref" | tar -x -C "$WORK_DIR/$name"
 python3 experiments/pre50next/instrument.py "$WORK_DIR/$name"
 diff -u <(git show "$ref:cmd/tigerstore/main.go") "$WORK_DIR/$name/cmd/tigerstore/main.go" > "$OUT/$name-observe.diff" || [[ $? == 1 ]]
 tar -czf "$OUT/$name-instrumented-source.tar.gz" -C "$WORK_DIR/$name" .
 check_time
 (cd "$WORK_DIR/$name" && go build -o "$BIN_DIR/$name" ./cmd/tigerstore)
done < "$WORK_DIR/build-refs.tsv"
check_time
PYTHONDONTWRITEBYTECODE=1 python3 experiments/pre50next/test_harness.py
check_time
go test -short -parallel 2 -timeout 1m ./experiments/pre50next/load
check_time
go build -o "$BIN_DIR/load" ./experiments/pre50next/load
python3 - <<'PY'
import hashlib, json, os
from pathlib import Path
refs = json.load(open('experiments/pre50next/refs.json'))
bin = Path(os.environ['BIN_DIR'])
manifest = dict(base=refs['base'], candidates=refs['candidates'],
                harness=os.popen('git rev-parse HEAD').read().strip(),
                sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in bin.iterdir()})
(bin/'binaries.json').write_text(json.dumps(manifest, indent=2)+'\n')
PY
{
 "$BIN_DIR/etcd" --version
 "$BIN_DIR/etcdctl" version
 sha256sum "$BIN_DIR/"*
 for binary in "$BIN_DIR/"*; do [[ $binary == *.json ]] || go version -m "$binary"; done
} > "$OUT/versions.txt"
# Archive before measurements, so failure still leaves reusable symbol binaries.
check_time
tar -czf "$OUT/binaries.tar.gz" -C "$BIN_DIR" .
check_time
bash scripts/dev-certs.sh "$CERT_DIR"
check_time
python3 experiments/pre50next/run.py
# CPU capture is complete before starting offline Go tools. No compilation.
while IFS= read -r -d '' profile; do
 check_time
 stem=${profile%-cpu.pb.gz}
 for kind in raw top-flat top-cumulative; do
  check_time
  case $kind in
   raw) args=(-raw);;
   top-flat) args=(-top);;
   top-cumulative) args=(-top -cum);;
  esac
  go tool pprof "${args[@]}" "$BIN_DIR/baseline" "$profile" > "$stem-cpu-$kind.txt"
 done
done < <(find "$OUT" -name '*-cpu.pb.gz' -print0)
check_time
python3 experiments/pre50next/summarize.py "$OUT"
check_time
python3 experiments/pre50next/profile_manifest.py "$OUT"

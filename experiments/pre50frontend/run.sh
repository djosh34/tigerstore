#!/usr/bin/env bash
set -euo pipefail
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
[[ ${GITHUB_ACTIONS:-} == true ]] || { echo 'No local measurements' >&2; exit 1; }
[[ $(uname -m) == aarch64 && ${CLIENTS:?} =~ ^(16|64|256)$ ]]
BASE=eb8a20eda7b1436e3319b924753469f2d8c0ee99
EXPERIMENT=$PWD/experiments/pre50frontend
PRIVATE=${RUNNER_TEMP:?}/pre50frontend
# Inputs override committed config, so this also works before dispatch registration.
config_exports=$(python3 - <<'PY'
import json, os, re, shlex
j = json.load(open('experiments/pre50frontend/refs.json'))
assert j['main_ref'] == 'eb8a20eda7b1436e3319b924753469f2d8c0ee99'
for env, key in [('NATIVE_REF','native_ref'), ('TUNED_REF','tuned_ref'), ('MODE','mode'), ('SELECTED_VARIANT','selected_variant')]:
    value = os.environ.get(env) or j.get(key) or ''
    if key.endswith('_ref'):
        assert re.fullmatch('[0-9a-f]{40}', value), f'{key} must be a committed immutable SHA'
    elif key == 'mode':
        assert value in ('primary', 'profile', 'smoke')
    elif value:
        assert value in ('a','b','c')
    print(f'export {env}={shlex.quote(value)}')
PY
)
eval "$config_exports"
[[ ${NATIVE_REF:-} =~ ^[0-9a-f]{40}$ && ${TUNED_REF:-} =~ ^[0-9a-f]{40}$ ]]
[[ ${MODE:-} =~ ^(primary|profile|smoke)$ ]]
[[ $MODE != profile || ${SELECTED_VARIANT:-} =~ ^[abc]$ ]]
OUT=$EXPERIMENT/results/$MODE/c$CLIENTS
mkdir -p "$OUT" "$PRIVATE/bin"
trap 'rm -rf "$PRIVATE"' EXIT
check_time
for ref in "$BASE"; do
  if ! git cat-file -e "$ref^{commit}"; then
    check_time
    git fetch --no-tags origin "$ref"
  fi
  [[ $(git rev-parse "$ref^{commit}") == "$ref" ]]
  git merge-base --is-ancestor "$BASE" "$ref"
done
# Architect stopped native variants before any measured windows completed.
python3 - "$OUT/sources.json" "$BASE" "$(git rev-parse HEAD)" <<'PY'
import json, sys
json.dump(dict(zip(['a','experiment'], sys.argv[2:])), open(sys.argv[1],'w'), indent=2)
PY
{
  date -u +%FT%TZ
  uname -a
  lscpu
  free -b
  lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
  df -B1 "$PWD" "$PRIVATE"
  env | grep -E '^(ImageOS|ImageVersion|GOFLAGS|GOMEMLIMIT)='
  check_time
  go version
  git status --short
} > "$OUT/environment.txt"
# Keep the original barrier guarantee. Fail on a filesystem where it cannot be checked.
check_time
sudo mount -o remount,barrier=1 /
findmnt -T "$PRIVATE" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$OUT/mount.txt"
[[ $(findmnt -T "$PRIVATE" -n -o FSTYPE) == ext4 ]]
! findmnt -T "$PRIVATE" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
check_time
curl --fail --location --retry 3 --max-time 90 -o "$PRIVATE/etcd.tar.gz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
tar -xzf "$PRIVATE/etcd.tar.gz" -C "$PRIVATE"
cp "$PRIVATE/etcd-v3.7.2-linux-arm64/etcd" "$PRIVATE/etcd-v3.7.2-linux-arm64/etcdctl" "$PRIVATE/bin/"
printf '%s  %s\n' 17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d "$PRIVATE/bin/etcd" | sha256sum -c -
refs=("$BASE")
variants=(a)
for i in 0; do
  check_time
  source=$PRIVATE/${variants[$i]}-source
  mkdir -p "$source"
  git archive "${refs[$i]}" | tar -x -C "$source"
  (cd "$source" && patch --batch --fuzz=0 -p1 < "$EXPERIMENT/observability.patch")
  cp "$EXPERIMENT/observation/cmd/observe.go" "$source/cmd/tigerstore/pre50frontend.go"
  cp "$EXPERIMENT/observation/osfs/counters.go" "$source/internal/osfs/pre50frontend.go"
  # Save exactly which observation bytes went into every variant.
  sha256sum "$source/cmd/tigerstore/pre50frontend.go" "$source/internal/osfs/pre50frontend.go" >> "$OUT/instrumentation-sha256.txt"
  check_time
  (cd "$source" && go build -tags pre50frontend -o "$PRIVATE/bin/${variants[$i]}" ./cmd/tigerstore)
done
check_time
go build -o "$PRIVATE/bin/load" ./experiments/pre50frontend/load
check_time
go test ./experiments/pre50frontend/load -parallel 2 -count=1
{
  "$PRIVATE/bin/etcd" --version
  "$PRIVATE/bin/etcdctl" version
  sha256sum "$PRIVATE/bin/"* "$PRIVATE/etcd.tar.gz" "$EXPERIMENT/observability.patch"
  for variant in a load; do
    check_time
    go version -m "$PRIVATE/bin/$variant"
  done
} > "$OUT/versions.txt"
check_time
bash scripts/dev-certs.sh "$PRIVATE/certs"
check_time
timeout --kill-after=5s 600s taskset -c 3 python3 "$EXPERIMENT/run.py"
check_time
if [[ $MODE != smoke ]]; then
  python3 "$EXPERIMENT/summarize.py" "$OUT"
fi
if [[ $MODE == profile ]]; then
  check_time
  python3 "$EXPERIMENT/attribute.py" "$OUT" > "$OUT/attribution.json"
fi

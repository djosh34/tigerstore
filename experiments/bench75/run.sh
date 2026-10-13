#!/usr/bin/env bash
set -euo pipefail
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
resolve_commit() {
 if ! git rev-parse --verify "$1^{commit}" >/dev/null 2>&1; then
  git fetch --no-tags origin "$1" >/dev/null || return
 fi
 git rev-parse --verify "$1^{commit}"
}
BASELINE=$(resolve_commit "${BASELINE_REF:?}")
CANDIDATE=$(resolve_commit "${CANDIDATE_REF:?}")
SECOND=${SECOND_CANDIDATE_REF:-}
SOURCES=(baseline candidate)
if [[ -n "$SECOND" ]]; then
 SECOND=$(resolve_commit "$SECOND")
 SOURCES+=(second)
fi
THIRD=${THIRD_CANDIDATE_REF:-}
if [[ -n "$THIRD" ]]; then
 THIRD=$(resolve_commit "$THIRD")
 SOURCES+=(third)
fi
export PROFILE_STORE=${SOURCES[-1]}
export WORKLOAD=${WORKLOAD:-put}
export BENCH_OUT=$PWD/evidence/bench75/run-${GITHUB_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
OUT=$BENCH_OUT/$WORKLOAD-c${CLIENTS:?}
PRIVATE=$RUNNER_TEMP/bench75
[[ ! -e "$OUT" && ! -e "$PRIVATE" ]]
mkdir -p "$OUT" "$PRIVATE/bin" "$PRIVATE/baseline" "$PRIVATE/candidate"
trap 'rm -rf "$PRIVATE"' EXIT
{
 date -u +%FT%TZ
 printf 'baseline %s\ncandidate %s\nsecond %s\nthird %s\nexperiment %s\n' "$BASELINE" "$CANDIDATE" "$SECOND" "$THIRD" "$(git rev-parse HEAD)"
 uname -a
 lscpu
 free -b
 lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
 df -B1 "$PWD" "$PRIVATE"
 findmnt -T "$PRIVATE"
 env | grep -E '^(ImageOS|ImageVersion|RUNNER_NAME|RUNNER_ARCH|RUNNER_OS)='
 go version
} > "$OUT/environment.txt"
sudo mount -o remount,barrier=1 /
findmnt -T "$PRIVATE" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$OUT/mount.txt"
test "$(findmnt -T "$PRIVATE" -n -o FSTYPE)" = ext4
! findmnt -T "$PRIVATE" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
curl --fail --location --retry 3 -o "$PRIVATE/etcd.tar.gz" https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz
tar -xzf "$PRIVATE/etcd.tar.gz" -C "$PRIVATE"
cp "$PRIVATE/etcd-v3.7.2-linux-arm64/etcd" "$PRIVATE/etcd-v3.7.2-linux-arm64/etcdctl" "$PRIVATE/bin/"
echo '17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d  '"$PRIVATE/bin/etcd" | sha256sum -c -
git diff --exit-code
git status --short > "$OUT/build-status.txt"
git archive "$BASELINE" | tar -x -C "$PRIVATE/baseline"
git archive "$CANDIDATE" | tar -x -C "$PRIVATE/candidate"
if [[ -n "$SECOND" ]]; then
 mkdir -p "$PRIVATE/second"
 git archive "$SECOND" | tar -x -C "$PRIVATE/second"
fi
if [[ -n "$THIRD" ]]; then
 mkdir -p "$PRIVATE/third"
 git archive "$THIRD" | tar -x -C "$PRIVATE/third"
fi
for source in "${SOURCES[@]}"; do
 check_time
 python3 experiments/bench75/install-observe.py "$PRIVATE/$source"
 (cd "$PRIVATE/$source" && go build -o "$PRIVATE/bin/$source" ./cmd/tigerstore)
done
check_time
go build -o "$PRIVATE/bin/load" ./experiments/bench75/load
check_time
go test -short ./experiments/bench75/load
printf 'baseline %s\ncandidate %s\nsecond %s\nthird %s\nexperiment %s\n' "$BASELINE" "$CANDIDATE" "$SECOND" "$THIRD" "$(git rev-parse HEAD)" > "$OUT/sources.txt"
{
 "$PRIVATE/bin/etcd" --version
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 go version -m "$PRIVATE/bin/baseline"
 go version -m "$PRIVATE/bin/candidate"
 if [[ -n "$SECOND" ]]; then go version -m "$PRIVATE/bin/second"; fi
 if [[ -n "$THIRD" ]]; then go version -m "$PRIVATE/bin/third"; fi
 go version -m "$PRIVATE/bin/load"
} > "$OUT/versions.txt"
bash scripts/dev-certs.sh "$PRIVATE/certs"
timeout --kill-after=5s 650s taskset -c 3 python3 experiments/bench75/run.py
if [[ ${CAPTURE_PROFILES:-0} == 1 && $CLIENTS == 256 && $WORKLOAD == put ]]; then
 check_time
 gzip -c "$PRIVATE/bin/$PROFILE_STORE" > "$OUT/profile-$PROFILE_STORE/candidate-symbols.gz"
fi
python3 experiments/bench75/summarize.py "$BENCH_OUT"

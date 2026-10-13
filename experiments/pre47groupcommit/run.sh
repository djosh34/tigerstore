#!/usr/bin/env bash
set -euo pipefail
check_time() { [[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]; }
check_time
BASELINE=72bd5d7680dbf456e7811b02f300c26e2b3b805a
PROTOTYPE=acfe8fa43993c447b1a322e6dfd59a0492222297
OUT=$PWD/evidence/bench68/c${CLIENTS:?}
PRIVATE=$RUNNER_TEMP/bench68
mkdir -p "$OUT" "$PRIVATE/bin" "$PRIVATE/baseline" "$PRIVATE/prototype"
trap 'rm -rf "$PRIVATE"' EXIT
{
 date -u +%FT%TZ
 git rev-parse HEAD "$BASELINE" "$PROTOTYPE"
 echo 'main merge base: 72bd5d7680dbf456e7811b02f300c26e2b3b805a'
 echo 'PR base work/47-lease: 4f65ce5c14f72277b42bf0030de994656c03eefa'
 sha256sum experiments/pre47groupcommit/observability.patch
 uname -a
 lscpu
 free -b
 lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
 df -B1 "$PWD" "$PRIVATE"
 findmnt -T "$PRIVATE"
 env | grep -E '^(ImageOS|ImageVersion)='
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
git archive "$PROTOTYPE" | tar -x -C "$PRIVATE/prototype"
# Apply precisely the same pre-work counters to each archive. No behavioral patch.
OBSERVABILITY=$PWD/experiments/pre47groupcommit/observability.patch
for source in baseline prototype; do
 check_time
 (cd "$PRIVATE/$source" && patch --batch --fuzz=0 -p1 < "$OBSERVABILITY")
done
check_time
(cd "$PRIVATE/baseline" && go build -o "$PRIVATE/bin/baseline" ./cmd/tigerstore)
check_time
(cd "$PRIVATE/prototype" && go build -o "$PRIVATE/bin/tigerstore" ./cmd/tigerstore)
check_time
go build -o "$PRIVATE/bin/load" ./experiments/pre50profile/load
for source in baseline prototype; do
 check_time
 (cd "$PRIVATE/$source" && go test -short ./internal/node ./internal/core ./internal/wal)
done
check_time
go test -short ./experiments/pre50profile/load
printf 'baseline %s\nprototype %s\nexperiment %s\n' "$BASELINE" "$PROTOTYPE" "$(git rev-parse HEAD)" > "$OUT/sources.txt"
{
 "$PRIVATE/bin/etcd" --version
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 go version -m "$PRIVATE/bin/baseline"
 go version -m "$PRIVATE/bin/tigerstore"
 go version -m "$PRIVATE/bin/load"
} > "$OUT/versions.txt"
bash scripts/dev-certs.sh "$PRIVATE/certs"
timeout --kill-after=5s 650s taskset -c 3 python3 experiments/pre47groupcommit/run.py
python3 experiments/pre47groupcommit/summarize.py

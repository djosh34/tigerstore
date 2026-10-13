#!/usr/bin/env bash
set -euo pipefail
[[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]
BASELINE=79bd08f326ec8c02e06c9fbc1e8f8373ec5234d7
OUT=$PWD/evidence/pre47batch256/${MODE:-diagnose}/c${CLIENTS:?}
PRIVATE=$RUNNER_TEMP/pre47batch256
mkdir -p "$OUT" "$PRIVATE/bin" "$PRIVATE/baseline"
trap 'rm -rf "$PRIVATE"' EXIT
{
 date -u +%FT%TZ
 git rev-parse HEAD "$BASELINE"
 echo 'main base: b8bac8b371c94f6d17136ad14aaa04354bbff01e'
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
(cd "$PRIVATE/baseline" && go build -o "$PRIVATE/bin/prototype" ./cmd/tigerstore)
go build -o "$PRIVATE/bin/tigerstore" ./cmd/tigerstore
go build -o "$PRIVATE/bin/load" ./experiments/pre50profile/load
go test -short ./internal/node ./internal/core ./internal/wal ./experiments/pre50profile/load
{
 "$PRIVATE/bin/etcd" --version
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 go version -m "$PRIVATE/bin/prototype"
 go version -m "$PRIVATE/bin/tigerstore"
 go version -m "$PRIVATE/bin/load"
} > "$OUT/versions.txt"
bash scripts/dev-certs.sh "$PRIVATE/certs"
timeout --kill-after=5s 700s taskset -c 3 python3 experiments/pre47batch256/run.py
python3 experiments/pre47batch256/summarize.py

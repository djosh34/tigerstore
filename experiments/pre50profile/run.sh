#!/usr/bin/env bash
set -euo pipefail
[[ $(date -u +%s) -lt $(date -ud 2026-10-13T22:00:00Z +%s) ]]
OUT=$PWD/evidence/pre50profile/raw
PRIVATE=$RUNNER_TEMP/pre50profile
mkdir -p "$OUT" "$PRIVATE/bin"
{
 date -u +%FT%TZ
 git rev-parse HEAD
 echo 'main base: 08d620707e25182219d8bd23830b11fed14f6c6a'
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
go build -o "$PRIVATE/bin/tigerstore" ./cmd/tigerstore
go build -o "$PRIVATE/bin/load" ./experiments/pre50profile/load
go test ./experiments/pre50profile/load
{
 "$PRIVATE/bin/etcd" --version
 "$PRIVATE/bin/etcdctl" version
 sha256sum "$PRIVATE/bin/"*
 go version -m "$PRIVATE/bin/tigerstore"
 go version -m "$PRIVATE/bin/load"
} > "$OUT/versions.txt"
bash scripts/dev-certs.sh "$PRIVATE/certs"
timeout --kill-after=5s 650s taskset -c 3 python3 experiments/pre50profile/run.py
# Keep symbols and source IDs alongside profiles; never publish certificates.
for dir in "$OUT"/profile-*; do
 for file in "$dir"/*.pb.gz; do
  case "$file" in
   *allocs*) go tool pprof -sample_index=alloc_space -top -nodecount=60 "$PRIVATE/bin/tigerstore" "$file" > "$file.top.txt" ;;
   *) go tool pprof -top -nodecount=80 "$PRIVATE/bin/tigerstore" "$file" > "$file.top.txt" ;;
  esac
 done
done
cp "$PRIVATE/bin/tigerstore" "$OUT/tigerstore-symbols"
rm -rf "$PRIVATE"

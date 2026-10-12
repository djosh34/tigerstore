#!/usr/bin/env bash
set -euo pipefail
[ "$(date -u +%s)" -lt "$(date -u -d 2026-10-13T22:00:00Z +%s)" ]
cd "$(dirname "$0")"
base=$PWD
mkdir -p out work/{tmp,cache,mod,bin,cert}
export TMPDIR=$base/work/tmp GOCACHE=$base/work/cache GOMODCACHE=$base/work/mod
export GOTOOLCHAIN=local
{
  date -u +%FT%TZ
  uname -a
  lscpu
  free -b
  lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
  df -B1 .
  findmnt -T .
  getconf CLK_TCK
  go version
  printf 'ImageOS=%s ImageVersion=%s\n' "$ImageOS" "$ImageVersion"
} > out/environment-before.txt
# Match #28 on this disposable runner. No loop device or extra filesystem layer.
sudo mount -o remount,barrier=1 /
findmnt -T . -o TARGET,SOURCE,FSTYPE,OPTIONS > out/mount-after.txt
if grep -Eq 'nobarrier|barrier=0' out/mount-after.txt; then exit 1; fi
[ "$(findmnt -T . -n -o FSTYPE)" = ext4 ]
# Public release binaries, unmodified. Build upstream's own benchmark at the same tag.
curl -fsSL --retry 3 https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz -o work/etcd.tar.gz
tar -xzf work/etcd.tar.gz --strip-components=1 -C work/bin
printf '17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d  work/bin/etcd\n' | sha256sum -c -
curl -fsSL --retry 3 https://github.com/etcd-io/etcd/archive/68c065e562994b89e333e77b039ad066f933c586.tar.gz -o work/source.tar.gz
mkdir work/source
tar -xzf work/source.tar.gz --strip-components=1 -C work/source
(cd work/source; go build -o "$base/work/bin/benchmark" ./tools/benchmark)
{
 work/bin/etcd --version
 work/bin/etcdctl version
 go version -m work/bin/benchmark
 sha256sum work/bin/{etcd,etcdctl,benchmark}
} > out/versions.txt
cp work/source/{go.mod,go.sum} out/
# Disposable self-signed CA/leaf for client and peer mTLS. Never upload the key.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=pre50 \
 -addext 'subjectAltName=IP:127.0.0.1,DNS:localhost' \
 -addext 'extendedKeyUsage=serverAuth,clientAuth' \
 -keyout work/cert/key.pem -out work/cert/cert.pem 2>out/cert-generation.txt
python3 baseline.py "$LAYOUT" 2>&1 | tee out/driver.log

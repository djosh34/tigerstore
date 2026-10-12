#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
for x in cc pkg-config fusermount3 timeout python3; do
  command -v "$x" >/dev/null || { echo "UNAVAILABLE missing $x"; exit 77; }
done
pkg-config --exists fuse3 || { echo 'UNAVAILABLE libfuse3 development files'; exit 77; }
[[ -c /dev/fuse && -r /dev/fuse && -w /dev/fuse ]] || { echo 'UNAVAILABLE /dev/fuse'; exit 77; }
work=$(mktemp -d "$TMPDIR/fuse.XXXXXX")
pid=''
cleanup() {
  local rc=$?
  fusermount3 -u "$work/mnt" 2>/dev/null || true
  if [[ -n $pid ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  if mountpoint -q "$work/mnt"; then echo 'FAIL mount still attached' >&2; return 1; fi
  rm -rf -- "$work"; return "$rc"
}
trap cleanup EXIT
mkdir "$work/mnt"
cc -Wall -Wextra "$base/probes/fuse-smoke.c" $(pkg-config --cflags --libs fuse3) -o "$work/fuse-smoke"
"$work/fuse-smoke" -f -s "$work/mnt" 2>"$work/daemon.log" & pid=$!
for _ in {1..50}; do
  mountpoint -q "$work/mnt" && break
  kill -0 "$pid" 2>/dev/null || break
  sleep .1
done
if ! mountpoint -q "$work/mnt"; then
  cp "$work/daemon.log" "$base/results/fuse-daemon.log"
  echo 'UNAVAILABLE actual unprivileged FUSE mount failed'; exit 77
fi
echo "INSTALLED actual FUSE mount=$work/mnt libfuse=$(pkg-config --modversion fuse3)"
timeout -k 2 8 python3 - "$work/mnt/probe" <<'PY'
import errno, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    assert os.write(fd, b'runner-fuse') == 11
    assert os.pread(fd, 11, 0) == b'runner-fuse'
    try:
        os.fsync(fd)
    except OSError as e:
        assert e.errno == errno.EIO, e
        print('FIRED kernel-visible fsync EIO; real process write/read succeeded')
    else:
        raise RuntimeError('expected fsync EIO did not occur')
finally:
    os.close(fd)
PY
cp "$work/daemon.log" "$base/results/fuse-daemon.log"
grep -q 'FIRED fsync errno=EIO' "$work/daemon.log"
echo 'PASS FUSE mount and fsync callback exercised without sudo or fuse.conf changes'

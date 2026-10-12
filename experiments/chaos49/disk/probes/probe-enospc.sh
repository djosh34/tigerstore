#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
mode=${1:-tmpfs}
[[ $mode == tmpfs || $mode == loop ]] || { echo 'Usage: probe-enospc.sh [tmpfs|loop]' >&2; exit 1; }
if [[ ${2:-} != --inner ]]; then
  exec sudo -n --preserve-env=GITHUB_ACTIONS,RUNNER_OS,RUNNER_ARCH,RUNNER_NAME \
    unshare --mount --propagation private bash "$base/probes/probe-enospc.sh" "$mode" --inner
fi
[[ $(id -u) == 0 ]] || exit 1
work=$(mktemp -d "$TMPDIR/enospc.XXXXXX")
loop=''
cleanup() {
  local rc=$?
  if mountpoint -q "$work/mnt"; then umount "$work/mnt" || return 1; fi
  if [[ -n $loop ]]; then losetup -d "$loop" || return 1; fi
  rm -rf -- "$work"; return "$rc"
}
trap cleanup EXIT
mkdir "$work/mnt"
if [[ $mode == tmpfs ]]; then
  mount -t tmpfs -o size=1m,nr_inodes=128,mode=700 tmpfs "$work/mnt"
else
  command -v mkfs.ext4 >/dev/null || exit 77
  truncate -s 32M "$work/disk.img"
  loop=$(losetup --find --show "$work/disk.img")
  mkfs.ext4 -q -F -m 0 "$loop"
  mount -t ext4 -o rw "$loop" "$work/mnt"
fi
findmnt -T "$work/mnt" -o TARGET,SOURCE,FSTYPE,OPTIONS | tee "$base/results/enospc-$mode-mount.txt"
echo "INSTALLED bounded $mode mount=$work/mnt"
timeout -k 2 15 python3 - "$work/mnt" <<'PY'
import errno, os, pathlib, sys
path = pathlib.Path(sys.argv[1])/'fill'
fd = os.open(path, os.O_CREAT|os.O_RDWR, 0o600)
total = 0
try:
    for _ in range(1024):
        try:
            total += os.write(fd, b'x' * 65536)
            os.fsync(fd)
        except OSError as e:
            if e.errno != errno.ENOSPC:
                raise
            print('FIRED real kernel ENOSPC bytes_written=%d errno=%d' % (total, e.errno))
            break
    else:
        raise RuntimeError('bounded filesystem never returned ENOSPC')
finally:
    os.close(fd)
path.unlink()
with open(path, 'wb', buffering=0) as f:
    assert f.write(b'healed') == 6
    os.fsync(f.fileno())
print('PASS real ENOSPC and write recovery after freeing owned file')
PY

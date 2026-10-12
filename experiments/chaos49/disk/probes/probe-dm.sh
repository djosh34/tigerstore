#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
# Root operations happen only on an opted-in Actions runner, in a private mount namespace.
if [[ ${1:-} != --inner ]]; then
  exec sudo -n --preserve-env=GITHUB_ACTIONS,RUNNER_OS,RUNNER_ARCH,RUNNER_NAME \
    unshare --mount --propagation private bash "$base/probes/probe-dm.sh" --inner
fi
[[ $(id -u) == 0 ]] || exit 1
for x in dmsetup losetup blockdev dd timeout python3; do command -v "$x" >/dev/null || exit 77; done
work=$(mktemp -d "$TMPDIR/dm.XXXXXX")
loop='' names=()
cleanup() {
  local rc=$? failed=0
  for name in "${names[@]}"; do dmsetup remove "$name" || failed=1; done
  if [[ -n $loop ]]; then losetup -d "$loop" || failed=1; fi
  (( failed == 0 )) || { echo "FAIL cleanup; retain $work" >&2; return 1; }
  rm -rf -- "$work"; return "$rc"
}
trap cleanup EXIT
truncate -s 16M "$work/disk.img"
loop=$(losetup --find --show "$work/disk.img")
sectors=$(blockdev --getsz "$loop")
prefix="tigerstore-probe-$$"
dmsetup targets > "$base/results/dm-targets.txt"
# No modprobe. Creating a target tests whether the running kernel can provide it.
for target in flakey delay; do
  name="$prefix-$target"
  if [[ $target == flakey ]]; then
    table="0 $sectors flakey $loop 0 0 60 1 error_writes"
  else
    table="0 $sectors delay $loop 0 0 $loop 0 200 $loop 0 300"
  fi
  if ! dmsetup create "$name" --table "$table"; then
    echo "UNAVAILABLE $target target creation failed"; exit 77
  fi
  names+=("$name")
  dmsetup mknodes "$name"
  dmsetup table "$name" | tee "$base/results/dm-$target-table.txt"
  echo "INSTALLED $target on owned loop=$loop image=$work/disk.img"
  dev="/dev/mapper/$name"
  if [[ $target == flakey ]]; then
    if timeout -k 2 8 dd if=/dev/zero of="$dev" bs=4096 count=1 oflag=direct status=none 2>"$work/eio.log"; then
      echo 'FAIL error_writes did not fail write'; exit 1
    fi
    grep -q 'Input/output error' "$work/eio.log"
    timeout -k 2 8 dd if="$dev" of=/dev/null bs=4096 count=1 iflag=direct status=none
    echo 'FIRED dm-flakey direct write EIO; read remains healthy'
  else
    timeout -k 2 8 python3 - "$dev" <<'PY'
import subprocess, sys, time
start = time.monotonic()
subprocess.run(['dd', 'if=/dev/zero', 'of='+sys.argv[1], 'bs=4096', 'count=1',
                'oflag=direct', 'conv=fsync', 'status=none'], check=True, timeout=5)
elapsed = time.monotonic()-start
assert elapsed >= .18, elapsed
print('FIRED dm-delay direct write plus fsync elapsed_seconds=%.6f' % elapsed)
print('9-argument flush table installed; flush-only delay is not independently established by this timing')
PY
  fi
  dmsetup status "$name" | tee "$base/results/dm-$target-status.txt"
done
echo 'PASS dm-flakey and dm-delay real block I/O exercised; no existing device was modified'

#!/usr/bin/env bash
# Experiment only. The production bench workflow supplies the runner and binaries.
set -euo pipefail
export GOFLAGS=-p=2 GOMEMLIMIT=2GiB
[ "${GITHUB_ACTIONS:-}" = true ] || { echo 'measurements require Actions' >&2; exit 2; }
[ "$(date -u +%s)" -lt "$(date -ud 2026-10-13T22:00:00Z +%s)" ] || exit 2
mkdir -p results
sudo mount -o remount,barrier=1 /
test "$(findmnt -T "$RUNNER_TEMP" -n -o FSTYPE)" = ext4
! findmnt -T "$RUNNER_TEMP" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
{
  date -u; git rev-parse HEAD; git status --short; go version
  lscpu; free -b; lsblk -b; findmnt -T "$RUNNER_TEMP"; uname -a
  sha256sum bin/*; bin/etcd --version; go version -m bin/tigerstore
  echo 'leader CPU0; followers CPU1/2; generator and sampler CPU3; server GOMAXPROCS=2; generator=1'
} > results/runner.txt
scripts/dev-certs.sh "$RUNNER_TEMP/certs"
export CERTS="$RUNNER_TEMP/certs" ETCDCTL="$PWD/bin/etcdctl"
taskset -c 3 timeout --kill-after=5s 720s python3 experiments/shape/run.py

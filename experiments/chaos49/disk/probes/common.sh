#!/usr/bin/env bash
# Source only from these runner probes. No package installs or config changes.
set -euo pipefail
export LC_ALL=C
if [[ ${GITHUB_ACTIONS:-} != true || ${RUNNER_OS:-} != Linux ]]; then
  echo 'Runner only: require GITHUB_ACTIONS=true and RUNNER_OS=Linux.' >&2; exit 1
fi
if (( $(date -u +%s) >= $(date -u -d '2026-10-13T22:00:00Z' +%s) )); then
  echo 'Deadline reached; starting nothing.' >&2; exit 1
fi
base=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$base/.tmp" "$base/.cache" "$base/results"
export TMPDIR="$base/.tmp" XDG_CACHE_HOME="$base/.cache" PYTHONDONTWRITEBYTECODE=1
printf 'Runner=%s arch=%s kernel=%s uid=%s time=%s\n' "${RUNNER_NAME:-unknown}" "${RUNNER_ARCH:-unknown}" "$(uname -r)" "$(id -u)" "$(date -u +%FT%TZ)"

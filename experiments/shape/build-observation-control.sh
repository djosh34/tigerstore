#!/usr/bin/env bash
set -euo pipefail
[ "$(date -u +%s)" -lt "$(date -ud 2026-10-13T22:00:00Z +%s)" ] || exit 2
source=e5c3327e3800edf64f86ed8c1971ba7ebc1e8342
mkdir -p results/observation-control "$RUNNER_TEMP/pristine"
git cat-file -e "$source^{commit}" 2>/dev/null || git fetch --no-tags --depth=1 origin "$source"
git archive "$source" | tar -x -C "$RUNNER_TEMP/pristine"
(cd "$RUNNER_TEMP/pristine" && go build -o "$GITHUB_WORKSPACE/bin/pristine" ./cmd/tigerstore)
ln -s tigerstore bin/tigerstore-off
printf '%s\n' "$source" > results/observation-control/pristine-commit.txt
sha256sum bin/pristine bin/tigerstore bin/tigerstore-off > results/observation-control/binaries-sha256.txt
# No file-counter claims for these native-status-fenced diagnostics.

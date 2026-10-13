#!/usr/bin/env bash
# One-constant controls. Do not merge these variants.
set -euo pipefail
[ "$(date -u +%s)" -lt "$(date -ud 2026-10-13T22:00:00Z +%s)" ] || exit 2
mkdir -p results/variants
for target in cap1m cap256k; do
  dir="$RUNNER_TEMP/$target"
  mkdir -p "$dir"
  git archive HEAD | tar -x -C "$dir"
  cp experiments/shape/cap_experiment_test.go.txt "$dir/internal/core/cap_experiment_test.go"
  TARGET="$target" DIR="$dir" python3 - <<'PY'
import os
from pathlib import Path
p=Path(os.environ['DIR'])/'internal/core/node.go'
s=p.read_text()
old='MaxBatchBytes:  4 << 20,'
new='MaxBatchBytes:  1 << 20,' if os.environ['TARGET']=='cap1m' else 'MaxBatchBytes:  256 << 10,'
assert s.count(old)==1
p.write_text(s.replace(old,new))
PY
  diff -u internal/core/node.go "$dir/internal/core/node.go" > "results/variants/$target.diff" || [ "$?" = 1 ]
  (cd "$dir" && go test -short -parallel 2 -timeout 3m ./internal/core ./internal/node ./internal/server ./internal/lint && go build -o "$GITHUB_WORKSPACE/bin/$target" ./cmd/tigerstore) > "results/variants/$target-test.txt" 2>&1
  sha256sum "$dir/internal/core/node.go" "bin/$target" > "results/variants/$target-sha256.txt"
done

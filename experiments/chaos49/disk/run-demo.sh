#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
if (( $(date -u +%s) >= $(date -u -d '2026-10-13T22:00:00Z' +%s) )); then
  echo 'Deadline reached; starting nothing.' >&2; exit 1
fi
mkdir -p .tmp .cache/go-build .cache/go-mod results
export TMPDIR="$PWD/.tmp" GOCACHE="$PWD/.cache/go-build" GOMODCACHE="$PWD/.cache/go-mod"
export XDG_CACHE_HOME="$PWD/.cache" GOTOOLCHAIN=local PYTHONDONTWRITEBYTECODE=1
build=$(mktemp -d "$TMPDIR/demo.XXXXXX")
trap 'rm -rf -- "$build" "$GOCACHE" "$GOMODCACHE"' EXIT
cd demo
go vet ./...
go vet -tags=faultfs ./...
go build -o "$build/release" .
go build -tags=faultfs -o "$build/fault-worker" .
cd ..
python3 demo/controller.py "$build/fault-worker" "$build/release" "$build/data" 2>&1 | tee results/demo.log
cp "$build/data/events.json" results/demo-events.json

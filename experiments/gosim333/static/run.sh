#!/usr/bin/env bash
# Reproduces the experiment commands run individually by helper A.
# Source-code inspection used functions.read and rg, with citations in REPORT.md.
set -euo pipefail
repo=/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/research-333
work=/tmp/r333-internals
gosim=/tmp/r333-gosim-internals
out=/tmp/r333-internals-artifacts
mkdir -p "$out"
gh issue view 333 -R djosh34/tigerstore
if ! test -d "$work"; then
  git -C "$repo" worktree add "$work" origin/main --detach
fi
trap 'git -C "$repo" worktree remove --force "$work"' EXIT
if ! test -d "$gosim/.git"; then
  git clone -q https://github.com/jellevandenhooff/gosim "$gosim"
fi
git -C "$gosim" checkout -q ffd3a613542675755e4cbf8186b5edaf404ed95c
test "$(git -C "$work" rev-parse HEAD)" = b8e39bba6fe200efe7f1828f68146ca18337e591
go env GOROOT
go env GOEXPERIMENT GOAMD64 GOARM64
git -C "$repo" show origin/exp/291-gosim:experiments/gosim291/README.md
git -C "$repo" show origin/exp/291-gosim:experiments/gosim291/hook-audit.patch > /tmp/r333-internals-audit.patch
git -C "$repo" show origin/exp/291-gosim:experiments/gosim291/results/repro-audit.log > /tmp/r333-internals-audit.log
cp /tmp/r333-internals-audit.log "$out/repro-audit.log"
if ! test -d "$out/go123/go/src"; then
  curl -fsSL https://go.dev/dl/go1.23.12.src.tar.gz -o "$out/go1.23.12.src.tar.gz"
  mkdir -p "$out/go123"
  tar -xzf "$out/go1.23.12.src.tar.gz" -C "$out/go123"
fi
cp "$out/enumerate.go" "$work/enumerate.go"
cd "$work"
gofmt -w enumerate.go
for arch in arm64 amd64; do
  flock /tmp/r333-heavy.lock env GOFLAGS=-p=2 GOMAXPROCS=2 GOOS=linux GOARCH=$arch CGO_ENABLED=0 go list -tags=sim -deps -test -json ./internal/sim ./internal/node > "$out/deps-$arch.json"
done
flock /tmp/r333-heavy.lock env GOFLAGS=-p=2 GOMAXPROCS=2 go build -o "$out/enumerate" ./enumerate.go
for arch in arm64 amd64; do
  flock /tmp/r333-heavy.lock env GOFLAGS=-p=2 GOMAXPROCS=2 "$out/enumerate" -deps "$out/deps-$arch.json" -gosim "$gosim" -oldroot "$out/go123/go" -arch "$arch" > "$out/inventory-$arch.tsv"
done
python3 "$out/summarize.py"

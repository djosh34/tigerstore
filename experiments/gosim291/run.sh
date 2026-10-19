#!/usr/bin/env bash
# Question: does current gosim run tigerstore's real driver on simfs on Go 1.27.2?
set -euo pipefail
cd "$(dirname "$0")"
export GOMAXPROCS=2 GOFLAGS=-p=2
export GOTOOLCHAIN=go1.27.2
rev=ffd3a613542675755e4cbf8186b5edaf404ed95c
mode=${1:-all}

setup() {
  mkdir -p .cache results
  for dir in upstream gosim; do
    if [[ ! -d .cache/$dir ]]; then
      git clone -q https://github.com/jellevandenhooff/gosim.git ".cache/$dir"
      git -C ".cache/$dir" checkout -q "$rev"
    fi
    [[ $(git -C ".cache/$dir" rev-parse HEAD) == "$rev" ]]
  done
  (cd .cache/upstream && go build -o ../gosim-upstream ./cmd/gosim)
  if git -C .cache/gosim apply --check ../../tools-update.patch 2>/dev/null; then
    git -C .cache/gosim apply ../../tools-update.patch
  else
    git -C .cache/gosim apply --reverse --check ../../tools-update.patch
  fi
  (cd .cache/gosim && go build -o ../gosim-tools ./cmd/gosim)
  (
    cd .cache/gosim
    git apply ../../hook-audit.patch
    trap 'git apply --reverse ../../hook-audit.patch' EXIT
    go build -o ../gosim-audit ./cmd/gosim
  )
  go mod download
}

case "$mode" in
  setup) setup ;;
  native) go test -v -count=1 -run '^TestDriverGate$' . ;;
  upstream) .cache/gosim-upstream test -v -seeds=1,1,2 -run '^TestDriverGate$' . ;;
  tools) .cache/gosim-tools test -v -seeds=1,1,2 -run '^TestDriverGate$' . ;;
  # Diagnostic translation ONLY: suppressing the first exit does not implement hooks.
  audit) .cache/gosim-audit translate . ;;
  control)
    cd .cache/upstream
    GOTOOLCHAIN=go1.23.12 go run ./cmd/gosim test -v -logformat=raw -seeds=1,1,2 -run '^TestGosim$' ./examples
    ;;
  all)
    setup > results/repro-setup.log 2>&1
    bad=0
    for pair in native:0 upstream:1 tools:1 audit:1 control:0; do
      name=${pair%:*}; want=${pair#*:}
      if bash ./run.sh "$name" > "results/repro-$name.log" 2>&1; then got=0; else got=$?; fi
      echo "$name: exit=$got (expected $want); results/repro-$name.log"
      if [[ $got != "$want" ]]; then bad=1; fi
    done
    exit "$bad"
    ;;
  *) echo 'usage: bash run.sh [setup|native|upstream|tools|audit|control|all]' >&2; exit 2 ;;
esac

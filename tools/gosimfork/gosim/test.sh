#!/bin/bash
cd "${0%/*}"
set -e

# task is pinned in the cmd/gosim module.
go run -C cmd/gosim github.com/go-task/task/v3/cmd/task --dir "$PWD" test "$@"

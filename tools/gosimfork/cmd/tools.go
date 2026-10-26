//go:build never

package main

// Tools used by this repository's scripts, so that this module pins them.
import (
	_ "github.com/go-task/task/v3/cmd/task"
	_ "golang.org/x/tools/cmd/goimports"
	_ "mvdan.cc/gofumpt"
)

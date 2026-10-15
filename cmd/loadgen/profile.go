package main

import (
	"fmt"
	"os"
	"runtime/pprof"
)

// startProfile starts a CPU profile written to path, or does nothing if path
// is empty. Temporary, for issue #188.
func startProfile(path string) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cpu profile: %w", err)
	}
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
	}, nil
}

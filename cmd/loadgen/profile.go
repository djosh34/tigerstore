package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
)

// startProfile starts a CPU profile written to path, and a mutex profile
// written to path.mutex, or does nothing if path is empty. Temporary, for
// issue #188.
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
	runtime.SetMutexProfileFraction(10)
	return func() {
		pprof.StopCPUProfile()
		_ = f.Close()
		if m, err := os.Create(path + ".mutex"); err == nil {
			_ = pprof.Lookup("mutex").WriteTo(m, 0)
			_ = m.Close()
		}
		runtime.SetMutexProfileFraction(0)
	}, nil
}

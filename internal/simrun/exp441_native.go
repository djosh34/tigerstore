//go:build !sim

package simrun

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
)

var markStart = time.Now()
var memoryProfiles int

// Mark writes msg with the real time to stderr (#441).
func Mark(msg string) {
	if strings.HasPrefix(msg, "MEM448") && os.Getenv("GOSIM_MEMDIR") != "" {
		runtime.GC()
		if dir := os.Getenv("GOSIM_MEMDIR"); dir != "" {
			memoryProfiles++
			if f, err := os.Create(filepath.Join(dir, fmt.Sprintf("heap-%03d.pprof", memoryProfiles))); err == nil {
				_ = pprof.WriteHeapProfile(f)
				_ = f.Close()
			}
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(os.Stderr, "MEMSTAT heap=%d inuse=%d idle=%d released=%d sys=%d nextgc=%d stacks=%d totalalloc=%d tag=%s\n", ms.HeapAlloc, ms.HeapInuse, ms.HeapIdle, ms.HeapReleased, ms.Sys, ms.NextGC, ms.StackInuse, ms.TotalAlloc, msg)
	fmt.Fprintf(os.Stderr, "REALMARK t=%.3f unix=%d native %s\n", time.Since(markStart).Seconds(), time.Now().UnixNano(), msg)
}

// RealCreate creates a file, nil if it fails (#441).
func RealCreate(path string) io.WriteCloser {
	f, err := os.Create(path)
	if err != nil {
		return nil
	}
	return f
}

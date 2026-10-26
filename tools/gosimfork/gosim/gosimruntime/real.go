package gosimruntime

// Escape hatches for measuring a run from inside it (research #441, exp
// branch only): real time, the real Go heap, files on the real disk, and a
// CPU profile of each run. None of them changes what a run does.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

var realStart = time.Now()
var memoryProfiles int

// RealMark writes msg to the real stderr with the real time since the
// process started, the run's scheduler step and the real heap.
func RealMark(msg string) {
	if strings.HasPrefix(msg, "MEM448") {
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
	fmt.Fprintf(os.Stderr, "REALMARK t=%.3f unix=%d step=%d heap=%dMiB sys=%dMiB gcs=%d gcpause=%.3fs gccpu=%.4f %s\n",
		time.Since(realStart).Seconds(), time.Now().UnixNano(), GetStep(), ms.HeapAlloc>>20, ms.Sys>>20, ms.NumGC,
		float64(ms.PauseTotalNs)/1e9, ms.GCCPUFraction, msg)
}

// RealNanotime is the real clock.
func RealNanotime() int64 { return time.Now().UnixNano() }

var (
	realMu    sync.Mutex
	realFiles = map[int]*os.File{}
	realNext  = 1
)

// RealCreate creates a file on the real disk, 0 if it fails.
func RealCreate(path string) int {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK create %s: %v\n", path, err)
		return 0
	}
	realMu.Lock()
	defer realMu.Unlock()
	id := realNext
	realNext++
	realFiles[id] = f
	return id
}

// RealWrite writes b to the file, false if it fails.
func RealWrite(id int, b []byte) bool {
	realMu.Lock()
	f := realFiles[id]
	realMu.Unlock()
	if f == nil {
		return false
	}
	_, err := f.Write(b)
	return err == nil
}

// RealClose closes the file.
func RealClose(id int) bool {
	realMu.Lock()
	f := realFiles[id]
	delete(realFiles, id)
	realMu.Unlock()
	return f != nil && f.Close() == nil
}

var profileRuns int

// startProfile starts a CPU profile of a run in $GOSIM_CPUPROFILE.<n>,
// and returns what stops it.
func startProfile() func() {
	path := os.Getenv("GOSIM_CPUPROFILE")
	if path == "" {
		return func() {}
	}
	profileRuns++
	f, err := os.Create(fmt.Sprintf("%s.%d", path, profileRuns))
	if err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		return func() {}
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		f.Close()
		return func() {}
	}
	return func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

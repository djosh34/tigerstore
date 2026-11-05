package gosimruntime

// Escape hatches for measuring a run from inside it (research #441, exp
// branch only): real time, the real Go heap, files on the real disk, and a
// CPU profile of each run. None of them changes what a run does.

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var realStart = time.Now()

// RealMark writes msg to the real stderr with the real time since the
// process started, the run's scheduler step and the real heap.
func RealMark(msg string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	goroutines, timers := 0, 0
	if g := gs.get(); g != nil {
		goroutines, timers = len(g.goroutines), g.clock.heap.len()
	}
	fmt.Fprintf(os.Stderr, "REALMARK t=%.3f unix=%d step=%d heap=%dMiB sys=%dMiB gcs=%d gcpause=%.3fs gccpu=%.4f picks=%d fires=%d goroutines=%d timers=%d %s\n",
		time.Since(realStart).Seconds(), time.Now().UnixNano(), GetStep(), ms.HeapAlloc>>20, ms.Sys>>20, ms.NumGC,
		float64(ms.PauseTotalNs)/1e9, ms.GCCPUFraction, expPicks, expFires, goroutines, timers, msg)
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

var (
	profileRuns int
	// profileFile is the profile being written, profilePath its name
	// without the window's tag (#473).
	profileFile *os.File
	profilePath string
	// expPicks counts the goroutine steps the scheduler ran, expFires the
	// timers that fired (#473).
	expPicks, expFires int64
)

// RealProfileNext ends the CPU profile's current window and starts the
// next, in $GOSIM_CPUPROFILE.<run>.<tag> (#473).
func RealProfileNext(tag string) {
	if profileFile == nil {
		return
	}
	pprof.StopCPUProfile()
	profileFile.Close()
	profileFile = nil
	f, err := os.Create(profilePath + "." + tag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		return
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		f.Close()
		return
	}
	profileFile = f
}

// startProfile starts a CPU profile of a run in $GOSIM_CPUPROFILE.<n>,
// and returns what stops it.
func startProfile() func() {
	path := os.Getenv("GOSIM_CPUPROFILE")
	if path == "" {
		return func() {}
	}
	profileRuns++
	profilePath = fmt.Sprintf("%s.%d", path, profileRuns)
	f, err := os.Create(profilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		return func() {}
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "REALMARK cpuprofile: %v\n", err)
		f.Close()
		return func() {}
	}
	profileFile = f
	return func() {
		if profileFile != nil {
			pprof.StopCPUProfile()
			profileFile.Close()
			profileFile = nil
		}
	}
}

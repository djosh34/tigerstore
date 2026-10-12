package osfs

// Throwaway follow-up instrumentation. The paired baseline predates this file.
import (
	"os"
	"sync/atomic"
	"time"
)

var (
	timing                                           = os.Getenv("PRE50_DISK_TIMING") == "1"
	writes, writeBytes, writeNanos, syncs, syncNanos atomic.Uint64
)

// Pre50Stats returns cumulative file calls. Directory syncs are excluded.
func Pre50Stats() map[string]uint64 {
	return map[string]uint64{"writes": writes.Load(), "write_bytes": writeBytes.Load(), "write_ns": writeNanos.Load(), "syncs": syncs.Load(), "sync_ns": syncNanos.Load()}
}

func (f file) Write(b []byte) (int, error) {
	if !timing {
		return f.File.Write(b)
	}
	start := time.Now()
	n, err := f.File.Write(b)
	writeNanos.Add(uint64(time.Since(start)))
	writeBytes.Add(uint64(n))
	writes.Add(1)
	return n, err
}

func (f file) Sync() error {
	if !timing {
		return f.File.Sync()
	}
	start := time.Now()
	err := f.File.Sync()
	syncNanos.Add(uint64(time.Since(start)))
	syncs.Add(1)
	return err
}

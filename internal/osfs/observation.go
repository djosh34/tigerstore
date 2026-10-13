package osfs

import (
	"sync/atomic"
	"time"
)

// WallNSBounds are inclusive, disjoint histogram bucket upper bounds, in
// nanoseconds. A final bucket counts durations above the last bound.
func WallNSBounds() []uint64 {
	return []uint64{1_000, 4_000, 16_000, 64_000, 256_000, 1_024_000, 4_096_000, 16_384_000, 65_536_000, 262_144_000, 1_048_576_000}
}

const wallBuckets = 12

// FileObservation counts completed file Write and Sync calls. It excludes
// directory syncs. Attach it before opening files; it cannot be enabled or
// disabled on files already opened. The zero value is ready to use.
// Snapshots load counters independently, so concurrent completion can leave
// a snapshot partway through a counter update. Quiescent snapshots agree.
type FileObservation struct {
	write fileCounters
	fsync fileCounters
}

type fileCounters struct {
	calls, errors, bytes, wall atomic.Uint64
	buckets                    [wallBuckets]atomic.Uint64
}

// FileOperation is a process-lifetime total. Buckets are not cumulative.
type FileOperation struct {
	Calls         uint64   `json:"calls"`
	Errors        uint64   `json:"errors"`
	Bytes         uint64   `json:"bytes"`
	WallNSSum     uint64   `json:"wall_ns_sum"`
	WallNSBuckets []uint64 `json:"wall_ns_buckets"`
}

// FileSnapshot contains separate write and file-fsync totals.
type FileSnapshot struct {
	Write FileOperation `json:"write"`
	Fsync FileOperation `json:"fsync"`
}

func (c *fileCounters) record(elapsed time.Duration, n int, err error) {
	ns := uint64(max(elapsed, 0))
	bucket := 0
	// The bounds are powers of four, starting at 1 us. Keep this loop
	// allocation-free instead of constructing the exported bounds slice.
	for bound := uint64(1_000); bucket < wallBuckets-1 && ns > bound; bound *= 4 {
		bucket++
	}
	c.buckets[bucket].Add(1)
	c.wall.Add(ns)
	if n > 0 {
		c.bytes.Add(uint64(n))
	}
	if err != nil {
		c.errors.Add(1)
	}
	c.calls.Add(1)
}

func (c *fileCounters) snapshot() FileOperation {
	s := FileOperation{
		Calls: c.calls.Load(), Errors: c.errors.Load(), Bytes: c.bytes.Load(),
		WallNSSum: c.wall.Load(), WallNSBuckets: make([]uint64, wallBuckets),
	}
	for i := range c.buckets {
		s.WallNSBuckets[i] = c.buckets[i].Load()
	}
	return s
}

// Snapshot reads totals without resetting them.
func (o *FileObservation) Snapshot() FileSnapshot {
	return FileSnapshot{Write: o.write.snapshot(), Fsync: o.fsync.snapshot()}
}

// Observe returns a directory whose subsequently opened files count their
// writes and fsyncs. A nil observation keeps the original file adapter.
func (d Dir) Observe(o *FileObservation) Dir {
	d.observation = o
	return d
}

type observedFile struct {
	file
	observation *FileObservation
}

func (f observedFile) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := f.File.Write(p)
	f.observation.write.record(time.Since(start), n, err)
	return n, err
}

func (f observedFile) Sync() error {
	start := time.Now()
	err := f.File.Sync()
	f.observation.fsync.record(time.Since(start), 0, err)
	return err
}

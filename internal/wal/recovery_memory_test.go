package wal_test

import (
	"bytes"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestPackRecoveryMemory reproduces #292 at quarter scale: a 16 MiB
// pack, 16 KiB values, and a 32 MiB node budget. Use the real disk adapter:
// simfs's ReadAll does not have osfs's growing io.ReadAll buffers.
// With GC disabled, HeapAlloc's increase is the recovery high-water mark,
// including transient buffers. This makes the bound deterministic rather
// than hoping a sampling goroutine catches append's old and new buffers.
func TestPackRecoveryMemory(t *testing.T) {
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir = dir.WithoutSync()
	const valueBytes = 16 << 10
	const values = 1024
	const budget = 32 << 20
	func() {
		w, _, err := wal.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		pw, err := w.NewPack()
		if err != nil {
			t.Fatal(err)
		}
		value := bytes.Repeat([]byte{0x5a}, valueBytes)
		for i := range values {
			if err := pw.Add(uint64(i+1), 0, value); err != nil {
				t.Fatal(err)
			}
		}
		p, err := pw.Finish()
		if err != nil {
			t.Fatal(err)
		}
		w.InstallPacks(p, nil)
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	runtime.GC()
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	w, _, err := wal.Open(dir)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	peak := after.HeapAlloc - before.HeapAlloc
	t.Logf("pack payload=%d, recovery peak heap increase=%d, map accounting=%d, node budget=%d", values*valueBytes, peak, w.MapBytes(), budget)
	if peak > budget {
		t.Errorf("recovery allocated %d bytes for a %d-byte pack: exceeds proportional node budget %d", peak, values*valueBytes, budget)
	}
	// The fix must be bounded, not merely replace io.ReadAll with one
	// allocation of the whole pack. Include map construction in this cap.
	if peak > 2<<20 {
		t.Errorf("recovery allocated %d bytes; want bounded scratch plus the position map (<2 MiB)", peak)
	}
	for _, i := range []uint64{1, values / 2, values} {
		b, err := w.Value(i, 0, valueBytes)
		if err != nil || !bytes.Equal(b, bytes.Repeat([]byte{0x5a}, valueBytes)) {
			t.Fatalf("value %d did not survive recovery: %v", i, err)
		}
	}
}

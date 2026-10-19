package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// recoveryValues puts values directly into packs, without retaining a
// second in-memory copy in the fixture. Recovery itself uses core.values.
type recoveryValues struct {
	pw *wal.PackWriter
	n  uint64
}

func (v *recoveryValues) Place(b []byte) (mvcc.Ref, error) {
	v.n++
	return mvcc.Ref{Index: v.n, Len: uint32(len(b))}, v.pw.Add(v.n, 0, b)
}
func (*recoveryValues) Read(mvcc.Ref, *[]byte) ([]byte, bool, error) {
	panic("fixture must not read values")
}

// TestCheckpointRecoveryMemory checks the rest of #292's startup, not
// merely openPack. A quarter-scale 32 MiB node holds 48 MiB of live
// values in three ~16 MiB packs. The checkpoint has only value references.
func TestCheckpointRecoveryMemory(t *testing.T) {
	dir, err := osfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir = dir.WithoutSync()
	const count = 512
	const size = 96 << 10 // the replay's cold pressure values
	const budget = 32 << 20
	// internal/node.GoMemory and ProcessMemory, as cmd/tigerstore uses
	// them. core cannot import node (node imports core).
	const goLimit = budget - budget/8
	const processLimit = goLimit - goLimit/4
	cfg := DefaultConfig(1, []uint64{1})
	cfg.MemoryBytes = processLimit
	func() {
		w, _, err := wal.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		v := &recoveryValues{}
		st := NewState(MinLeaseTTL(cfg), v)
		st.members[1] = &pb.Member{ID: 1}
		value := bytes.Repeat([]byte{0x5a}, size)
		w.AppendRoot(wal.Root{Index: count, Term: 1})
		for i := range count {
			if i%171 == 0 {
				v.pw, err = w.NewPack()
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.kv.Put(&pb.PutRequest{Key: fmt.Appendf(nil, "/pressure/%06d", i), Value: value}); err != nil {
				t.Fatal(err)
			}
			if i%171 == 170 || i == count-1 {
				p, err := v.pw.Finish()
				if err != nil {
					t.Fatal(err)
				}
				w.InstallPacks(p, nil)
			}
		}
		f, err := w.CreateCheckpoint(count)
		if err != nil {
			t.Fatal(err)
		}
		header, err := st.header(count, 1).encode()
		if err != nil {
			t.Fatal(err)
		}
		crc := uint32(0)
		write := func(b []byte) {
			crc = crc32.Update(crc, castagnoli, b)
			if _, err := f.Write(b); err != nil {
				t.Fatal(err)
			}
		}
		write(header)
		st.kv.Freeze()
		for {
			b, _, done := st.kv.CopyFrozen(nil, 64<<10)
			write(b)
			if done {
				break
			}
		}
		if _, err := f.Write(binary.LittleEndian.AppendUint32(nil, crc)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
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
	w, rec, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	n, err := New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	debug.SetGCPercent(old)
	peak := after.HeapAlloc - before.HeapAlloc
	m := n.Metrics()
	if peak > processLimit || m.CacheBytes != 0 || n.storage.total() != 0 {
		t.Fatalf("recovery peak %d > %d, cache %d, window %d", peak, processLimit, m.CacheBytes, n.storage.total())
	}
	if n.kv.Counts().Keys != count {
		t.Fatalf("loaded %d keys, want %d", n.kv.Counts().Keys, count)
	}
	runtime.GC()
	// Exactly memoryGauge's accounting, with an empty cache: live heap
	// plus memory outside objects, excluding free and released spans.
	names := []string{"/gc/heap/live:bytes", "/memory/classes/total:bytes", "/memory/classes/heap/released:bytes", "/memory/classes/heap/free:bytes", "/memory/classes/heap/objects:bytes"}
	samples := make([]metrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	metrics.Read(samples)
	v := func(i int) int64 { return int64(samples[i].Value.Uint64()) }
	accounted := v(0) + v(1) - v(2) - v(3) - v(4)
	n.SetOtherMemory(accounted)
	t.Logf("live values=%d, recovery peak increase=%d, index=%d, WAL map=%d, cache=%d, process accounting=%d / %d", count*size, peak, n.kv.Bytes(), w.MapBytes(), n.cache.bytes, accounted, processLimit)
	if accounted > processLimit || n.cache.bytes > max(processLimit-accounted, 0) {
		t.Fatalf("restart exceeds the process/cache accounting: %d / %d", accounted, processLimit)
	}
	runtime.KeepAlive(n)
}

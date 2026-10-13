package batchmem

import (
	"bytes"
	"testing"
)

func TestPinnedChunksNeverOverwrite(t *testing.T) {
	for _, mode := range []Mode{Ring, Pool} {
		t.Run(string(mode), func(t *testing.T) {
			b, err := New(mode, []int{ChunkBytes, ChunkBytes})
			if err != nil {
				t.Fatal(err)
			}
			a := b.Acquire(1536 << 10)
			v := a.Alloc(1536 << 10)
			for i := range v {
				v[i] = 0x6a
			}
			a.Retain()
			a.Release() // owner finished, slow asynchronous user still holds bytes
			x := b.Acquire(1)
			if x == nil || b.Acquire(1) != nil {
				t.Fatal("pinned bank did not reject")
			}
			if !bytes.Equal(v, bytes.Repeat([]byte{0x6a}, len(v))) {
				t.Fatal("pinned bytes overwritten")
			}
			x.Release()
			a.Release()
			for range 8 {
				x = b.Acquire(ChunkBytes)
				if x == nil {
					t.Fatal("recycle failed")
				}
				x.Release()
			}
			s := b.Stats()
			if s.InUseBytes != 0 || s.RetainedBytes != 2*ChunkBytes || s.Rejected != 1 {
				t.Fatalf("stats %+v", s)
			}
		})
	}
}

func TestStartupAndBounds(t *testing.T) {
	for _, mode := range []Mode{Ring, Pool} {
		b, err := New(mode, []int{64})
		if err != nil {
			t.Fatal(err)
		}
		if got := b.Stats().RetainedBytes; (mode == Ring && got != 64) || (mode == Pool && got != 0) {
			t.Fatalf("%s retained %d", mode, got)
		}
		a := b.Acquire(64)
		if len(a.Alloc(64)) != 64 || a.Alloc(1) != nil || b.Acquire(65) != nil {
			t.Fatal("capacity exceeded")
		}
		a.Release()
	}
}

// Run on the parent's ARM Actions job, not as a local measurement.
func BenchmarkBatchArena(b *testing.B) {
	for _, mode := range []Mode{Ring, Pool} {
		b.Run(string(mode), func(b *testing.B) {
			bank, err := New(mode, []int{ChunkBytes, ChunkBytes})
			if err != nil {
				b.Fatal(err)
			}
			a := bank.Acquire(1)
			a.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				a = bank.Acquire(1024)
				v := a.Alloc(1024)
				v[0] = 1
				a.Release()
			}
		})
	}
}

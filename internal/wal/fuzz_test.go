package wal_test

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// FuzzOpen recovers a log of two segments with any bytes. Open must refuse
// it or open it without a crash, and every entry it then reports must read
// back or fail with an error.
func FuzzOpen(f *testing.F) {
	d := simfs.New()
	w, _, err := wal.Open(d)
	if err != nil {
		f.Fatal(err)
	}
	w.SetSegmentBytes(512)
	for i := uint64(1); i <= 20; i++ {
		w.Append(&wal.HardState{Term: 1 + i/10, Vote: 2, Commit: i - 1}, []*raftpb.Entry{entry(1+i/10, i, fmt.Sprintf("value %d", i))})
		if i == 12 {
			w.AppendRoot(wal.Root{Index: 10, Term: 1})
		}
		if err := w.Sync(); err != nil {
			f.Fatal(err)
		}
	}
	names := d.DurableNames()
	for i := 0; i+1 < len(names); i++ {
		f.Add(d.DurableBytes(names[i]), d.DurableBytes(names[i+1]))
	}
	f.Fuzz(func(t *testing.T, a, b []byte) {
		d := simfs.New()
		for i, data := range [][]byte{a, b} {
			if i == 1 && len(data) == 0 {
				break
			}
			file, err := d.Create(fmt.Sprintf("%016x.wal", i+1))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.SyncDir(); err != nil {
			t.Fatal(err)
		}
		w, rec, err := wal.Open(d)
		if err != nil {
			return
		}
		for i := rec.Root.Index + 1; i <= rec.Last; i++ {
			if e, err := w.Entry(i); err == nil && e.GetIndex() != i {
				t.Fatalf("entry %d reads back as entry %d", i, e.GetIndex())
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

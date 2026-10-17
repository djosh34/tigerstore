package wal_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
)

func TestCompressionGateAndFallback(t *testing.T) {
	random := make([]byte, 32768)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		format byte
		body []byte
		compressed bool
	}{
		{"old", 3, bytes.Repeat([]byte("metadata managedFields namespace spec status "), 1000), false},
		{"new", 4, bytes.Repeat([]byte("metadata managedFields namespace spec status "), 1000), true},
		{"random", 4, random, false},
		{"small", 4, []byte("small"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := append([]byte{0, tc.format, 1}, tc.body...)
			e := entry(1, 1, string(raw))
			out, err := wal.CompressEntries([]*raftpb.Entry{e})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(e.Data, raw) {
				t.Fatal("compression mutated Raft's entry")
			}
			if tc.compressed {
				if len(out[0].Data) >= len(raw)/4 {
					t.Fatalf("stored %d bytes of %d", len(out[0].Data), len(raw))
				}
			} else if out[0] != e || !bytes.Equal(out[0].Data, raw) {
				t.Fatal("raw fallback allocated or changed the entry")
			}
			d := simfs.New()
			w, _ := open(t, d)
			w.Append(nil, out)
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, _ = open(t, d.Durable())
			t.Cleanup(func() { if err := w.Close(); err != nil { t.Error(err) } })
			got, err := w.Entry(1)
			if err != nil || !bytes.Equal(got.GetData(), raw) {
				t.Fatalf("entry round trip: %v", err)
			}
			wantValue(t, w, 1, 3, uint32(len(tc.body)), string(tc.body))
			if _, _, direct := w.ValuePlace(1, 3, 1); direct == tc.compressed {
				t.Fatal("compressed value exposed a direct file offset")
			}
			if _, ok := w.Where(1, 3, 1); !ok {
				t.Fatal("cleaner cannot locate entry")
			}
		})
	}
}

func TestCompressedValueAfterCleaning(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	body := bytes.Repeat([]byte("managedFields status metadata "), 1000)
	raw := append([]byte{0, 4, 1}, body...)
	for i := uint64(1); i <= 10; i++ {
		entries, err := wal.CompressEntries([]*raftpb.Entry{entry(1, i, string(raw))})
		if err != nil { t.Fatal(err) }
		w.Append(nil, entries)
		if err := w.Sync(); err != nil { t.Fatal(err) }
	}
	w.AppendRoot(wal.Root{Index: 6, Term: 1})
	if err := w.Sync(); err != nil { t.Fatal(err) }
	segs, _ := cleanable(w, 6)
	if len(segs) == 0 { t.Fatal("no segments to clean") }
	p := writePack(t, w, [3]uint32{2, 3, uint32(len(body))})
	w.InstallPacks(p, nil)
	if err := w.Sync(); err != nil { t.Fatal(err) }
	if err := w.RemoveCleaned(segs[len(segs)-1], nil); err != nil { t.Fatal(err) }
	if _, err := w.Entry(2); !errors.Is(err, wal.ErrNotOnDisk) { t.Fatalf("entry still present: %v", err) }
	if err := w.Close(); err != nil { t.Fatal(err) }
	w, _ = open(t, d.Durable())
	defer func() { if err := w.Close(); err != nil { t.Error(err) } }()
	wantValue(t, w, 2, 3, uint32(len(body)), string(body))
	got, err := w.Entry(7)
	if err != nil || !bytes.Equal(got.GetData(), raw) { t.Fatalf("tail: %v", err) }
}

func TestCompressedEntryCorruption(t *testing.T) {
	for _, data := range [][]byte{
		{0, 4, 0},
		{0, 4, 0, 0xff, 0xff, 0xff, 0xff},
		{0, 4, 0, 100, 0, 0, 0, 1, 2, 3},
	} {
		w, _ := open(t, simfs.New())
		w.Append(nil, []*raftpb.Entry{entry(1, 1, string(data))})
		if err := w.Sync(); err != nil { t.Fatal(err) }
		if _, err := w.Entry(1); !errors.Is(err, wal.ErrCorrupt) { t.Fatalf("entry accepted %x: %v", data, err) }
		if _, err := w.Value(1, 0, 1); !errors.Is(err, wal.ErrCorrupt) { t.Fatalf("value accepted %x: %v", data, err) }
		if err := w.Close(); err != nil { t.Fatal(err) }
	}
}

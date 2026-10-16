package wal_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// files returns the segment and spare names on d.
func files(t *testing.T, d *simfs.Disk) (segs, spares []string) {
	t.Helper()
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		switch {
		case strings.HasSuffix(n, ".wal"):
			segs = append(segs, n)
		case strings.HasPrefix(n, "spare-"):
			spares = append(spares, n)
		}
	}
	return segs, spares
}

// TestSegmentsReuseSpares removes old segments, which become spares, and
// writes on: new segments take the spare files instead of new ones.
// TestSetSparesRemovesExtra turns spares off with spare files on disk:
// they go, and a removal after it removes the segment's file (#181).
func TestSetSparesRemovesExtra(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 5, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(5); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) == 0 {
		t.Fatal("no spares after the removal")
	}
	if err := w.SetSpares(1); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) != 1 {
		t.Fatalf("spares %v after SetSpares(1)", spares)
	}
	if err := w.SetSpares(0); err != nil {
		t.Fatal(err)
	}
	w.AppendRoot(wal.Root{Index: 7, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(7); err != nil {
		t.Fatal(err)
	}
	if _, spares := files(t, d); len(spares) != 0 {
		t.Fatalf("spares %v with spares off", spares)
	}
}

func TestSegmentsReuseSpares(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 8)
	w.AppendRoot(wal.Root{Index: 7, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.RemoveBelow(7); err != nil {
		t.Fatal(err)
	}
	segs, spares := files(t, d)
	if len(spares) != wal.MaxSpares {
		t.Fatalf("%d spares after removal, want %d: %v", len(spares), wal.MaxSpares, segs)
	}
	before := len(segs) + len(spares)
	fill(t, w, 9, 9+wal.MaxSpares-1)
	segs, spares = files(t, d)
	if len(spares) != 0 || len(segs) != before {
		t.Fatalf("after %d new segments: %v and %v, want %d segments", wal.MaxSpares, segs, spares, before)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Root.Index != 7 || rec.Last != 9+wal.MaxSpares-1 {
		t.Fatalf("recovered %+v", rec)
	}
	if got := describe(read(t, w, rec)); !strings.HasPrefix(got, "8/1:v8 9/1:v9 10/1:v10 ") {
		t.Fatalf("entries after the root: %s", got)
	}
}

// TestStaleRecordsAfterTheEnd puts the records of an older segment, which
// chain among themselves, right after the last record of the newest one,
// as a reused file holds them: recovery cuts them off as a torn tail and
// does not take them for damage before valid records. A sealed segment
// never holds them: rotation cuts a reused file to its records.
func TestStaleRecordsAfterTheEnd(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 4)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	older := d.DurableBytes(segs[1])
	rewrite(t, d, segs[len(segs)-1], func(b []byte) []byte { return append(b, older[24:]...) })
	w, rec := open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:v1 2/1:v2 3/1:v3 4/1:v4 " {
		t.Fatalf("entries: %s", got)
	}
}

// v1Segment returns segment 1 in format version 1: entries 1 and 2 and a
// hard state, chained from crc 0 without the sequence number.
func v1Segment() []byte {
	table := crc32.MakeTable(crc32.Castagnoli)
	b := []byte("tigerwal")
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = binary.LittleEndian.AppendUint32(b, 0)
	crc := uint32(0)
	rec := func(typ byte, payload []byte) {
		start := len(b)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
		b = append(b, 0, 0, 0, 0, typ)
		b = append(b, payload...)
		crc = crc32.Update(crc, table, b[start+8:])
		binary.LittleEndian.PutUint32(b[start+4:], crc)
	}
	for i := uint64(1); i <= 2; i++ {
		var p []byte
		p = binary.LittleEndian.AppendUint64(p, 1)
		p = binary.LittleEndian.AppendUint64(p, i)
		p = append(p, 0)
		rec(1, append(p, fmt.Sprintf("v%d", i)...))
	}
	var hs []byte
	hs = binary.LittleEndian.AppendUint64(hs, 1)
	hs = binary.LittleEndian.AppendUint64(hs, 2)
	hs = binary.LittleEndian.AppendUint64(hs, 2)
	rec(2, hs)
	return b
}

// TestReadsVersion1 opens a log in format version 1, appends to its
// newest segment in that format, and starts the next one in the new.
func TestReadsVersion1(t *testing.T) {
	d := simfs.New()
	f, err := d.Create("0000000000000001.wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(v1Segment()); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Sync(), f.Close(), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
	w, rec := open(t, d)
	if rec.Last != 2 || rec.HardState != (wal.HardState{Term: 1, Vote: 2, Commit: 2}) {
		t.Fatalf("recovered %+v", rec)
	}
	fill(t, w, 3, 3)
	w.SetSegmentBytes(1)
	fill(t, w, 4, 5)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, rec = open(t, d)
	if got := describe(read(t, w, rec)); got != "1/1:v1 2/1:v2 3/1:v3 4/1:v4 5/1:v5 " {
		t.Fatalf("entries: %s", got)
	}
	if b := d.DurableBytes("0000000000000002.wal"); len(b) < 12 || binary.LittleEndian.Uint32(b[8:]) != wal.Version {
		t.Fatalf("the next segment is not in version %d", wal.Version)
	}
}

// TestCrashWithSpares writes, names roots, removes the segments below
// them, so that new segments reuse their files, and crashes, also in the
// middle of a sync or a new segment, and in recovery. Recovery must
// return the synced entries after the root plus a prefix of the unsynced
// ones, and never call the log corrupt.
func TestCrashWithSpares(t *testing.T) {
	spared := 0
	for seed := range uint64(500) {
		rng := rand.New(rand.NewPCG(seed, 11))
		d := simfs.New()
		w, _ := open(t, d)
		size := int64(64 + rng.IntN(256))
		w.SetSegmentBytes(size)
		// log[i] is entry i+1; synced is the part that is surely durable.
		var log, synced []string
		var pending []*raftpb.Entry
		var root uint64
		term := uint64(1)
		madeSpares := false
		sync := func() error {
			err := w.Sync()
			if err == nil {
				synced, pending = slices.Clone(log), nil
			}
			return err
		}
		crash := func() {
			d.Crash(rng)
			for rng.IntN(2) == 0 {
				failFrom(d, 1+rng.IntN(6))
				if _, _, err := wal.Open(d); err != nil && !errors.Is(err, errCrash2) {
					t.Fatalf("seed %d: open during crash: %v", seed, err)
				}
				d.Fault = nil
				d.Crash(rng)
			}
			var rec *wal.Recovered
			w, rec = open(t, d)
			w.SetSegmentBytes(size)
			if rec.Root.Index < root {
				t.Fatalf("seed %d: root %d recovered, %d was synced", seed, rec.Root.Index, root)
			}
			got := describe(read(t, w, rec))
			want := func(l []string) string {
				var b strings.Builder
				for i := rec.Root.Index; i < uint64(len(l)); i++ {
					fmt.Fprintf(&b, "%d/%s ", i+1, l[i])
				}
				return b.String()
			}
			l := slices.Clone(synced)
			match := slices.Clone(l)
			ok := got == want(l)
			for _, e := range pending {
				l = append(l[:e.GetIndex()-1:e.GetIndex()-1], fmt.Sprintf("%d:%s", e.GetTerm(), e.GetData()))
				if !ok && got == want(l) {
					ok, match = true, slices.Clone(l)
				}
			}
			if !ok {
				t.Fatalf("seed %d: recovered %s, synced %s, pending %s", seed, got, want(synced), describe(pending))
			}
			log, synced, pending, root = match, slices.Clone(match), nil, rec.Root.Index
		}
		for range 40 + rng.IntN(40) {
			switch r := rng.IntN(10); {
			case r == 0:
				crash()
				continue
			case r == 1 && len(synced) > int(root)+1:
				// A root at a synced entry, then the removal.
				index := root + 1 + uint64(rng.IntN(len(synced)-int(root)))
				w.AppendRoot(wal.Root{Index: index, Term: 1})
				if err := sync(); err != nil {
					t.Fatal(err)
				}
				root = index
				if err := w.RemoveBelow(root); err != nil {
					t.Fatal(err)
				}
				if _, sp := files(t, d); len(sp) > 0 && !madeSpares {
					madeSpares = true
					spared++
				}
				continue
			case r == 2:
				// A fault at one of the next writing operations of a sync,
				// which may start a new segment; then the node stops.
				failFrom(d, 1+rng.IntN(5))
				_ = sync()
				d.Fault = nil
				crash()
				continue
			}
			// Append a batch, sometimes replacing a suffix after the root.
			last := uint64(len(log))
			start := last + 1
			if last > root+1 && rng.IntN(4) == 0 {
				start = last - uint64(rng.IntN(int(min(last-root-1, 3))))
				term++
			}
			var ents []*raftpb.Entry
			for i := start; i < start+uint64(1+rng.IntN(4)); i++ {
				e := entry(term, i, fmt.Sprintf("v%d", i))
				ents = append(ents, e)
				log = append(log[:i-1:i-1], fmt.Sprintf("%d:v%d", term, i))
			}
			w.Append(&wal.HardState{Term: term}, ents)
			pending = append(pending, ents...)
			if rng.IntN(3) > 0 {
				if err := sync(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if spared < 400 {
		t.Fatalf("only %d of 500 seeds made spares", spared)
	}
}

// TestSpareOfAnUnsyncedRemoval renames a segment to a spare without
// syncing the directory, as the cleaner does before its own sync, and
// lets the next segment take the spare and crash before its directory
// sync. The removal must be durable before the spare gets a new header:
// else the old name comes back with the new segment's header, and the log
// does not open.
func TestSpareOfAnUnsyncedRemoval(t *testing.T) {
	d := simfs.New()
	w, _ := open(t, d)
	w.SetSegmentBytes(1)
	fill(t, w, 1, 6)
	w.AppendRoot(wal.Root{Index: 5, Term: 1})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	segs, _ := files(t, d)
	if err := d.Rename(segs[0], "spare-0000000000000001"); err != nil {
		t.Fatal(err)
	}
	renamed := false
	d.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpRename {
			renamed = true
		}
		if op == simfs.OpSyncDir && renamed {
			return errCrash
		}
		return nil
	}
	w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: 7}, []*raftpb.Entry{entry(1, 7, "v7")})
	if err := w.Sync(); !errors.Is(err, errCrash) {
		t.Fatalf("sync into the spare = %v", err)
	}
	d.Fault = nil
	d.Crash(rand.New(rand.NewPCG(1, 2)))
	_, rec := open(t, d)
	if rec.Root.Index != 5 || rec.Last < 6 {
		t.Fatalf("recovered %+v", rec)
	}
}

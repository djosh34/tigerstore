package wal_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"strings"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// files returns the segment names on d.
func files(t *testing.T, d *simfs.Disk) (segs []string) {
	t.Helper()
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if strings.HasSuffix(n, ".wal") {
			segs = append(segs, n)
		}
	}
	return segs
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
	segs := files(t, d)
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
	segs := files(t, d)
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

// TestPrepareSegment readies segment files under names of their own: a
// new file, never the file of the segment before, which may wait for its
// name, a file readied before, and the oldest spare. A spare that a
// removal takes meanwhile is skipped, and a file readied for the same
// segment before is removed.
func TestPrepareSegment(t *testing.T) {
	d := simfs.New()
	names := func() string {
		t.Helper()
		ns, err := d.List()
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(ns, " ")
	}
	prepare := func(seq uint64, want string) {
		t.Helper()
		if err := wal.PrepareSegment(d, seq); err != nil {
			t.Fatal(err)
		}
		if got := names(); got != want {
			t.Fatalf("after readying %d: %s, want %s", seq, got, want)
		}
	}
	prepare(5, "next-0000000000000005")
	prepare(6, "next-0000000000000005 next-0000000000000006")
	for _, name := range []string{"spare-0000000000000001", "spare-0000000000000002"} {
		f, err := d.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	prepare(8, "next-0000000000000006 next-0000000000000008 spare-0000000000000001 spare-0000000000000002")
	prepare(9, "next-0000000000000008 next-0000000000000009 spare-0000000000000001 spare-0000000000000002")
	if err := errors.Join(d.Remove("next-0000000000000008"), d.Remove("next-0000000000000009")); err != nil {
		t.Fatal(err)
	}
	d.Fault = func(op simfs.Op, name string) error {
		if op == simfs.OpRename && name == "spare-0000000000000001" {
			d.Fault = nil
			return d.Remove(name)
		}
		return nil
	}
	prepare(10, "next-000000000000000a")
	// A file readied for 10 before, as a crash leaves it, is removed: it
	// may hold records of 10.
	rewrite(t, d, "next-000000000000000a", func([]byte) []byte { return []byte("records of 10") })
	prepare(10, "next-000000000000000a")
	if b := d.Files()["next-000000000000000a"]; len(b) > 0 {
		t.Fatalf("readied 10 again in the file that held %q", b)
	}
	prepare(11, "next-000000000000000a next-000000000000000b")
}

// TestDamagedHeaderBeforeRecords opens logs whose newest segment has a
// damaged header: with a record of its own after it, in format 1 too, or
// as the only segment left when it is not segment 1, the log is corrupt;
// with only another segment's old records after it, as a reused spare
// holds them, the log starts the segment again.
func TestDamagedHeaderBeforeRecords(t *testing.T) {
	t.Run("format 1", func(t *testing.T) {
		d := simfs.New()
		b := v1Segment()
		b[0] ^= 1
		write(t, d, "0000000000000001.wal", b)
		wantCorrupt(t, d)
	})
	t.Run("only segment", func(t *testing.T) {
		d := simfs.New()
		w, _ := open(t, d)
		w.SetSegmentBytes(1)
		fill(t, w, 1, 4)
		w.AppendRoot(wal.Root{Index: 4, Term: 1})
		w.Append(&wal.HardState{Term: 1, Commit: 5}, []*raftpb.Entry{entry(1, 5, "v5")})
		if err := errors.Join(w.Sync(), w.Close()); err != nil {
			t.Fatal(err)
		}
		// Keep only the segment with the root and entry 5, as removal
		// keeps the one before a segment that never started.
		segs := files(t, d)
		keep := segs[len(segs)-2]
		for _, name := range segs {
			if name != keep {
				if err := errors.Join(d.Remove(name), d.SyncDir()); err != nil {
					t.Fatal(err)
				}
			}
		}
		rewrite(t, d, keep, func(b []byte) []byte { b[0] ^= 1; return b })
		wantCorrupt(t, d)
	})
	t.Run("old spare bytes", func(t *testing.T) {
		d := simfs.New()
		names := segments(t, d, 2)
		old := d.DurableBytes(names[0])
		rewrite(t, d, names[len(names)-1], func([]byte) []byte {
			b := append([]byte("tigerwaX"), make([]byte, 12)...)
			return append(b, old[20:]...)
		})
		_, rec := open(t, d)
		if rec.Last != 2 {
			t.Fatalf("recovered entries to %d, want 2", rec.Last)
		}
	})
}

// write creates file name on d with b in it, durable.
func write(t *testing.T, d *simfs.Disk, name string, b []byte) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Sync(), f.Close(), d.SyncDir()); err != nil {
		t.Fatal(err)
	}
}

// TestHeaderWaitsForRecords flushes a new segment before anything is
// appended to it, as the node does before a snapshot install. The header
// must wait and reach the disk in one write with the first records: a
// crash can keep a later unsynced write and lose an earlier one (#295).
func TestHeaderWaitsForRecords(t *testing.T) {
	d := simfs.New()
	var writes []string
	d.Trace = func(format string, args ...any) {
		if s := fmt.Sprintf(format, args...); strings.HasPrefix(s, "write ") {
			writes = append(writes, s)
		}
	}
	w, _ := open(t, d)
	// Recovery wrote the copies of the term and vote.
	writes = nil
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(writes) > 0 {
		t.Fatalf("a flush with nothing appended wrote: %s", strings.Join(writes, "; "))
	}
	w.Append(&wal.HardState{Term: 1, Vote: 2, Commit: 1}, []*raftpb.Entry{entry(1, 1, "v1")})
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 || !strings.Contains(writes[0], " at 0: ") {
		t.Fatalf("writes: %s, want one at 0 with the header and the records", strings.Join(writes, "; "))
	}
}

package wl

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func testEntry(index, term uint64, data string) *pb.Entry {
	return &pb.Entry{Index: new(index), Term: new(term), Type: pb.EntryNormal.Enum(), Data: []byte(data)}
}

// syncWAL writes everything in w's stage to disk and waits for it.
func syncWAL(t *testing.T, w *wal) {
	t.Helper()
	w.lastKick = time.Time{}
	w.pending = append(w.pending, walJob{end: w.off})
	w.kick()
	deadline := time.Now().Add(10 * time.Second)
	for w.done.Load() < w.off {
		if err := w.failed.Load(); err != nil {
			t.Fatal(*err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the log is durable to %d of %d after 10 s", w.done.Load(), w.off)
		}
		time.Sleep(time.Millisecond)
	}
	w.inflight = false
	w.pending = w.pending[:0]
	w.stable = w.off
}

func TestWALScanReadsWhatWasWritten(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	ents := []*pb.Entry{testEntry(1, 1, "one"), testEntry(2, 1, ""), testEntry(3, 2, "three")}
	if err := w.write(ents, []byte("hard state")); err != nil {
		t.Fatal(err)
	}
	offs := append([]int64{0}, w.offs...) // by index
	// Read back from the stage, before the disk has it.
	got := make([]byte, 5)
	if err := w.read(got, offs[3]); err != nil || string(got) != "three" {
		t.Fatalf("read from the stage: %q, %v", got, err)
	}
	syncWAL(t, w)
	if err := w.write([]*pb.Entry{testEntry(4, 2, "four")}, nil); err != nil {
		t.Fatal(err)
	}
	offs = append(offs, w.offs...)
	syncWAL(t, w)
	// One from the disk: the stage's block is not where it reads.
	if err := w.read(got[:3], offs[1]); err != nil || string(got[:3]) != "one" {
		t.Fatalf("read from the segment: %q, %v", got[:3], err)
	}

	type rec struct {
		kind        byte
		index, term uint64
		data        string
		off         int64
	}
	var recs []rec
	end, err := scanWAL(sysEnv{}, dir, 0, func(r *walRecord) error {
		recs = append(recs, rec{r.kind, r.index, r.term, string(r.data), r.off})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if end != w.off {
		t.Fatalf("scan ends at %d, the log at %d", end, w.off)
	}
	want := []rec{
		{recEntry, 1, 1, "one", offs[1]},
		{recEntry, 2, 1, "", offs[2]},
		{recEntry, 3, 2, "three", offs[3]},
		{recHardState, 0, 0, "hard state", 0},
		{recEntry, 4, 2, "four", offs[4]},
	}
	if len(recs) != len(want) {
		t.Fatalf("scan found %d records, want %d: %+v", len(recs), len(want), recs)
	}
	for i := range want {
		g := recs[i]
		if g.kind == recHardState {
			g.off = 0
		}
		if g != want[i] {
			t.Errorf("record %d: %+v, want %+v", i, g, want[i])
		}
	}
}

func TestWALScanStopsAtATornRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, "torn"), testEntry(3, 1, "after")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A crash tore the write at sectors: the torn record's sector never
	// reached the disk, which still holds the zeros past the log's end.
	tear(b, w.offs[1]-walEntHdr)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	var got []string
	end, err := scanWAL(sysEnv{}, dir, 0, func(r *walRecord) error {
		got = append(got, string(r.data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "kept" {
		t.Fatalf("scan read %q, want only the record before the torn one", got)
	}
	if torn := w.offs[1] - walEntHdr; end != torn {
		t.Fatalf("scan ends at %d, want the torn record's start %d", end, torn)
	}
}

// tear zeros b from off to the end of its sector, as a crash leaves a
// sector of a write that never reached the disk.
func tear(b []byte, off int64) {
	clear(b[off:min(int64(len(b)), (off/sector+1)*sector)])
}

// A record damaged where no crash can have torn it, a flipped bit with no
// sector of zeros in it, is refused, not cut as a torn tail: the records
// after it were durable, and cutting them would lose what the node
// acknowledged.
func TestWALRefusesADamagedRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, "damaged"), testEntry(3, 1, "after")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	for _, at := range []string{"damaged", "length"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		orig := slices.Clone(b)
		switch at {
		case "damaged":
			b[bytes.Index(b, []byte("damaged"))] ^= 0x10
		case "length":
			// A flipped bit of the length: past the longest record.
			b[w.offs[1]-walEntHdr+7] ^= 0x80
		}
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := scanWAL(sysEnv{}, dir, 0, nil); !errors.Is(err, ErrCorruptLog) {
			t.Errorf("%s: scan returned %v, want ErrCorruptLog", at, err)
		}
		if err := os.WriteFile(path, orig, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWALStageTailIsZeros(t *testing.T) {
	w, err := openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	// A lap of the stage ago, the bytes after the end held a record.
	for i := range w.stage[:2*block] {
		w.stage[i] = 0xa5
	}
	if err := w.write([]*pb.Entry{testEntry(1, 1, "x")}, nil); err != nil {
		t.Fatal(err)
	}
	if tail := w.stage[w.off:block]; bytes.ContainsFunc(tail, func(r rune) bool { return r != 0 }) {
		t.Fatalf("the stage after the log's end holds %x, want zeros up to the block's end", tail[:16])
	}
}

func TestWALReadPastTheEndFails(t *testing.T) {
	w, err := openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.read(make([]byte, 1), 0); err == nil {
		t.Fatal("a read of an empty log succeeded")
	}
}

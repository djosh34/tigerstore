package wl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"slices"
	"strings"
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

// A record damaged where no crash can have torn it, a flipped bit of its
// value or checksum before a record that checks, is refused, not cut as a
// torn tail: the records after it were durable, and cutting them would
// lose what the node acknowledged.
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
	for _, at := range []string{"damaged", "checksum"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		orig := slices.Clone(b)
		switch at {
		case "damaged":
			b[bytes.Index(b, []byte("damaged"))] ^= 0x10
		case "checksum":
			b[w.offs[1]-walEntHdr] ^= 0x10
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

// A sector of a segment before the last that holds data, zeroed as a
// write the drive reported done but never made, is refused: a crash tears
// only the last segment, as the helper writes each segment's part of a
// write durably before the next's.
func TestWALRefusesALostWriteBeforeTheLastSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, strings.Repeat("x", 2*sector)), testEntry(3, 1, "after")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The long record's second sector reads as zeros.
	lost := ((w.offs[1]-walEntHdr)/sector + 1) * sector
	clear(b[lost : lost+sector])
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if end, err := scanWAL(sysEnv{}, dir, 0, nil); err != nil || end != w.offs[1]-walEntHdr {
		t.Fatalf("in the last segment the scan ended at %d with %v, want a torn tail at %d", end, err, w.offs[1]-walEntHdr)
	}
	// The segment is whole and the next holds data.
	if err := os.Truncate(path, segSize); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segPath(dir, 1), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := scanWAL(sysEnv{}, dir, 0, nil); !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("before a later segment the scan returned %v, want ErrCorruptLog", err)
	}
}

// What a crash leaves at the log's end is cut, whatever follows it there:
// garbage in place of the last record's tail, as a torn write may leave,
// and a record torn at a sector of zeros before a later sector of the same
// write that reached the disk.
func TestWALCutsWhatACrashLeft(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	long := strings.Repeat("x", 2*sector)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, long), testEntry(3, 1, "after")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	torn := w.offs[1] - walEntHdr
	for _, at := range []string{"garbage", "sector"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		orig := slices.Clone(b)
		switch at {
		case "garbage":
			end := w.offs[2] - walEntHdr
			for i := end - 100; i < end; i++ {
				b[i] = byte(i*7 + 3)
			}
			b = b[:end]
		case "sector":
			// The long record's second sector never reached the disk; the
			// record after it did.
			s := (torn/sector + 1) * sector
			clear(b[s : s+sector])
		}
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		end, err := scanWAL(sysEnv{}, dir, 0, nil)
		if err != nil || end != torn {
			t.Errorf("%s: scan ended at %d with %v, want the torn record's start %d", at, end, err, torn)
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

// A crash after a start removed the segments past the log's end may keep
// some of the removes and lose others. The next start removes every
// segment past the end, not only those up to the first gap, so that the
// segments it makes do not exist yet.
func TestWALRemovesSegmentsPastAGap(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.write([]*pb.Entry{testEntry(1, 1, "one")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	w.close()
	// Segment 1's remove was kept, segment 2's lost.
	if err := os.Remove(segPath(dir, 1)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(segPath(dir, 2), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	w, err = openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	// The helper, idle, would take segments 1 and 2 as the log reaches
	// them.
	for k := int64(1); k <= 2; k++ {
		if _, err := w.segFD(k); err != nil {
			t.Fatalf("segment %d: %v", k, err)
		}
	}
}

// A record whose length is damaged to claim more than the log holds is
// refused when records that check follow its start: a scan that took it
// for a crash's torn tail cut every entry after it, entries the node had
// reported durable (TestWLRefusesAMisreadLog, seed 1, where a misread
// sector at a segment's start gave a record's length the bytes of another
// sector).
func TestWALRefusesARecordThatClaimsMoreThanTheLogHolds(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, "long"), testEntry(3, 1, "after"), testEntry(4, 1, "last")}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	head := w.offs[1] - walEntHdr
	binary.LittleEndian.PutUint32(b[head+4:], 1<<20)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if end, err := scanWAL(sysEnv{}, dir, 0, nil); !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("scan returned end %d and %v, want ErrCorruptLog", end, err)
	}
}

// A record a crash tore at the log's end, its rest zeros, is still the
// log's end: nothing that checks follows its start.
func TestWALCutsARecordTornAtTheEnd(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(sysEnv{}, dir, sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	if err := w.write([]*pb.Entry{testEntry(1, 1, "kept"), testEntry(2, 1, strings.Repeat("torn", 300))}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	path := segPath(dir, 0)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	head := w.offs[1] - walEntHdr
	clear(b[head+walEntHdr+100:])
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	end, err := scanWAL(sysEnv{}, dir, 0, nil)
	if err != nil || end != head {
		t.Fatalf("scan returned end %d and %v, want %d and none", end, err, head)
	}
}

// A sync that wrote loadBytes or more holds the next back for loadGap
// after it started, and a smaller one for syncGap: a leader under load
// takes large batches on every run, and a lightly loaded node's syncs
// follow each other as before (#689, "0 ms: the bisect").
func TestASyncAfterALargeOneWaitsTheLoadGap(t *testing.T) {
	w, err := openWAL(sysEnv{}, t.TempDir(), sizeFor(0).stage, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if err := w.write([]*pb.Entry{testEntry(1, 1, string(make([]byte, loadBytes)))}, nil); err != nil {
		t.Fatal(err)
	}
	syncWAL(t, w)
	if w.lastBytes < loadBytes {
		t.Fatalf("the sync of %d bytes recorded %d", w.off, w.lastBytes)
	}
	if err := w.write([]*pb.Entry{testEntry(2, 1, "x")}, nil); err != nil {
		t.Fatal(err)
	}
	w.pending = append(w.pending, walJob{end: w.off})
	w.lastKick = time.Now().Add(-2 * syncGap)
	w.kick()
	if !w.deferred || w.inflight {
		t.Fatalf("a sync started %v after one of %d bytes", 2*syncGap, w.lastBytes)
	}
	w.lastBytes = loadBytes - 1
	w.kick()
	if w.deferred || !w.inflight {
		t.Fatalf("a sync %v after one of %d bytes waited", 2*syncGap, w.lastBytes)
	}
	if w.lastBytes >= loadBytes {
		t.Fatalf("a sync of a small append recorded %d bytes", w.lastBytes)
	}
}

// The helper waiting for a segment stops when the log closes: its maker
// stops then and may never hand the segment over. Waiting on, it held
// the log's close for good (#689's area-1 review).
func TestTheHelperStopsWaitingForASegmentWhenTheLogCloses(t *testing.T) {
	w := &wal{made: make(chan segment), stop: make(chan struct{})}
	close(w.stop)
	got := make(chan error, 1)
	go func() {
		_, err := w.segFD(0)
		got <- err
	}()
	select {
	case err := <-got:
		if !errors.Is(err, errWALClosed) {
			t.Fatalf("segFD after the close: %v, want errWALClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the helper still waits for a segment after the log closed")
	}
}

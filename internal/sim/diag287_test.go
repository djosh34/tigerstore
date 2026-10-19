//go:build planted

package sim

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestDiag287SnapshotCatch deterministically takes the path from #287:
// B04c admits a decodable damaged record at recovery, but the snapshot
// sender checks the record again. No scheduler or seed search is needed.
func TestDiag287SnapshotCatch(t *testing.T) {
	planted.Active = ""
	t.Cleanup(func() { planted.Active = "" })
	d := simfs.New()
	w, _, err := wal.Open(d)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(&wal.HardState{Term: 1, Commit: 2}, []*raftpb.Entry{
		{Term: new(uint64(1)), Index: new(uint64(1)), Data: []byte("payload")},
		{Term: new(uint64(1)), Index: new(uint64(2)), Data: []byte("later")},
	})
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := core.ServeSnapshot(w, nil, 1, 2, io.Discard); err != nil {
		t.Fatalf("healthy snapshot: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	flipped := false
	for _, name := range d.DurableNames() {
		if ends := wal.PayloadEnds(d.DurableBytes(name)); len(ends) > 0 {
			d.FlipDurable(name, ends[0])
			flipped = true
			break
		}
	}
	if !flipped {
		t.Fatal("no payload to damage")
	}
	// The normal binary refuses the same damaged log; only B04c admits it.
	if normal, _, err := wal.Open(d.Durable()); !errors.Is(err, wal.ErrCorrupt) {
		if normal != nil {
			_ = normal.Close()
		}
		t.Fatalf("normal recovery: %v, want corruption", err)
	}
	planted.Active = "B04c"
	w, _, err = wal.Open(d.Durable())
	if err != nil {
		t.Fatalf("B04c recovery should admit decodable damage: %v", err)
	}
	defer w.Close()
	err = core.ServeSnapshot(w, nil, 1, 2, io.Discard)
	re, ok := errors.AsType[*wal.RecordError](err)
	if !ok || re.Entry != 1 || re.Segment != 1 {
		t.Fatalf("snapshot: %v, want damaged entry 1 in segment 1", err)
	}
	failure := fmt.Errorf("seed 1 at 20806 ms: node 3 stopped: %w", err)
	t.Logf("snapshot found the planted corruption: %v", failure)
	want := regexp.MustCompile(plantedCases["B04c"].want)
	if !want.MatchString(failure.Error()) {
		t.Fatalf("B04c rejects its own snapshot detection: %v", failure)
	}
	for _, unrelated := range []string{
		"node 3 stopped: file already closed",
		"node 3 stopped: unexpected EOF",
		"node 3 stopped: wal: corrupt log: unrelated damage",
	} {
		if want.MatchString(unrelated) {
			t.Errorf("B04c accepts unrelated failure: %s", unrelated)
		}
	}
}

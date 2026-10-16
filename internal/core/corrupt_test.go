package core

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestCorruptValueBeforeCheckpoint is #160's deterministic case. A
// checkpoint has been copied but not yet written when a bit of a value
// flips in a sealed segment. The read of that value stops the node with
// mvcc.ErrBadValue before it answers; the checkpoint written after the
// stop asks for no root, so nothing removes the damaged segment; and a
// restart refuses the log as corrupt, which fences the node.
func TestCorruptValueBeforeCheckpoint(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, tinyCheckpoints)
	s.holdCkpt = true
	val := bytes.Repeat([]byte("v"), 256)
	key := func(i int) []byte { return fmt.Appendf(nil, "k%02d", i) }
	// Every key once, so that the first segments are sealed, then until
	// a checkpoint is copied.
	for i := 0; i < 40 || !slices.ContainsFunc(s.heldCkpt, func(c CheckpointChunk) bool { return c.Done }); i++ {
		if i == 200 {
			t.Fatal("no checkpoint was copied")
		}
		s.do(uint64(10+i), &pb.PutRequest{Key: key(i % 40), Value: val})
	}
	// A key whose newest value is on disk only, in a sealed segment.
	var damaged []byte
	for i := range 40 {
		refs := s.n.Store().Refs(key(i))
		if len(refs) == 0 {
			continue
		}
		r := refs[len(refs)-1]
		seq, at, ok := s.log.ValuePlace(r.Index, r.Off, r.Len)
		if r.Index >= s.n.FirstIndex() || !ok || seq >= s.log.Seq() {
			continue
		}
		disk.Rot(wal.SegmentName(seq), int(at)+int(r.Len)/2)
		damaged = key(i)
		break
	}
	if damaged == nil {
		t.Fatal("no value is in a sealed segment only")
	}
	s.n.Submit(Request{ID: 1000, Op: &pb.RangeRequest{Key: damaged}})
	var err error
	for range 10 {
		var eff Effects
		if eff, err = s.n.Process(); err != nil {
			break
		}
		for _, r := range eff.Replies {
			if r.ID == 1000 {
				t.Fatalf("the damaged value was answered: %v", r)
			}
		}
		for _, r := range eff.Reads {
			s.n.ValuesRead(r, r.Run())
		}
	}
	if !errors.Is(err, mvcc.ErrBadValue) {
		t.Fatalf("the node read the damaged value with %v, want mvcc.ErrBadValue", err)
	}
	s.holdCkpt = false
	s.writeCheckpoint(s.heldCkpt)
	if eff, _ := s.n.Process(); slices.ContainsFunc(eff.Persist, func(j PersistJob) bool { return j.Root != nil }) {
		t.Fatal("the stopped node asked for the checkpoint's root")
	}
	if _, _, err := wal.Open(disk); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("restart: %v, want the log refused as corrupt", err)
	}
}

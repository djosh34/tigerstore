package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// Scratch diagnosis: a cut held behind an unwritten checkpoint must run
// after that checkpoint is written, even when no further request arrives.
func TestDiag546CutWaitingWithoutWrites(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.CheckpointBytes, c.CheckpointRatio = 16<<10, 2 })
	s.holdCkpt = true
	for i := 0; i < 100; i++ {
		s.n.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%03d", i), Value: make([]byte, 1024)}})
		s.run()
	}
	if !s.n.cutDue || s.n.unwritten == 0 || s.n.copy != nil {
		t.Fatalf("want due cut held by write: due=%v unwritten=%d copying=%v", s.n.cutDue, s.n.unwritten, s.n.copy != nil)
	}
	first, applied := s.n.unwritten, s.n.applied
	t.Logf("before release: unwritten=%d applied=%d sinceCkpt=%d cutDue=%v", first, applied, s.n.sinceCkpt, s.n.cutDue)
	s.holdCkpt = false
	s.writeCheckpoint(s.heldCkpt)
	s.heldCkpt = nil
	s.run()
	t.Logf("after release without requests: last=%d applied=%d unwritten=%d cutDue=%v", s.n.last.index, s.n.applied, s.n.unwritten, s.n.cutDue)
	if s.n.last.index != applied || s.n.last.index <= first {
		t.Fatal("waiting checkpoint did not cut at the applied index")
	}
}

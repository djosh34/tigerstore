package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestCompactionCuts checks that every compaction cuts a checkpoint, in
// every cluster version, and that one applied while a copy is under way
// waits for it: the next entry after the copy cuts, with one copy at a
// time.
func TestCompactionCuts(t *testing.T) {
	for _, version := range []uint32{1, MaxVersion} {
		t.Run(fmt.Sprintf("version=%d", version), func(t *testing.T) {
			s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) {
				cfg.MaxVersion = version
				cfg.CheckpointBytes = 1 << 30
			})
			put := func(id uint64) int64 {
				r, ok := s.ok(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id), Value: []byte("v")}).(*pb.PutResponse)
				if !ok {
					t.Fatal("not a put response")
				}
				return r.Header.Revision
			}
			rev := put(1)
			s.ok(2, &pb.CompactionRequest{Revision: rev})
			if got, want := s.n.DurableCheckpoint().Index, s.n.Applied(); got != want {
				t.Fatalf("the compaction at %d left the checkpoint at %d", want, got)
			}
			rev = put(3)
			s.holdCopy = true
			s.ok(4, &pb.CompactionRequest{Revision: rev})
			copying := s.n.copy.index
			if copying != s.n.Applied() {
				t.Fatalf("copying the checkpoint at %d after the compaction at %d", copying, s.n.Applied())
			}
			s.ok(6, &pb.CompactionRequest{Revision: put(5)})
			if s.n.copy.index != copying {
				t.Fatalf("a compaction during the copy at %d cut at %d", copying, s.n.copy.index)
			}
			s.holdCopy = false
			s.run()
			if got := s.n.DurableCheckpoint().Index; got != copying {
				t.Fatalf("the checkpoint is at %d after the copy at %d", got, copying)
			}
			put(7)
			if got, want := s.n.DurableCheckpoint().Index, s.n.Applied(); got != want {
				t.Fatalf("the entry after the copy is %d, the checkpoint %d", want, got)
			}
		})
	}
}

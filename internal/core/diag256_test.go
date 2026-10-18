package core

import (
	"testing"

	"github.com/djosh34/tigerstore/internal/simfs"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Core is single-threaded. Queue a watch followed by a put before draining
// Process, which is the suspected within-one-driver-turn interleaving.
func TestDiag256CreatedRevisionFrozen(t *testing.T) {
	s := startSingle(t, simfs.New(), 1)
	for i := range 100 {
		before := s.n.kv.Rev()
		stream := uint64(i + 1)
		s.n.Watch(stream, Caller{}, create(&pb.WatchCreateRequest{Key: []byte("a")}))
		s.n.Submit(Request{ID: stream, Op: &pb.PutRequest{Key: []byte("a"), Value: []byte("v")}})
		s.run()
		rs := s.responses()
		if len(rs) != 2 || !rs[0].Created || len(rs[1].Events) != 1 {
			t.Fatalf("responses = %v", rs)
		}
		created, event := rs[0].Header.Revision, rs[1].Events[0].Kv.ModRevision
		if created != before || event <= created {
			t.Fatalf("before=%d created=%d first-event=%d", before, created, event)
		}
		s.n.CloseWatchStream(stream)
	}
}

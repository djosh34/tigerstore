package core

import (
	"bytes"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
)

func comparisonSingle(t *testing.T, memory int64, count, size int) (*single, []byte) {
	t.Helper()
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) {
		c.WindowBytes, c.WindowMaxBytes = 4<<10, 8<<10
	})
	value := bytes.Repeat([]byte("v"), size)
	for i := range count {
		if r := s.do(uint64(i+1), &pb.PutRequest{Key: fmt.Appendf(nil, "k%04d", i), Value: value}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	fill(t, s, 1000, 200)
	s.n.cfg.MemoryBytes = memory
	s.n.cache.setLimit(0)
	s.hold = true
	return s, value
}

func comparisonTxn(value []byte, write bool) *pb.TxnRequest {
	r := &pb.TxnRequest{Compare: []*pb.Compare{{
		Key: []byte("k"), RangeEnd: []byte("l"), Target: pb.Compare_VALUE,
		Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: value},
	}}}
	if write {
		r.Success = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("result"), Value: []byte("yes")}}}}
		r.Failure = []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("result"), Value: []byte("no")}}}}
	}
	return r
}

// completeComparison keeps every input read on the test's side of the
// driver boundary until its reservation and retained values are checked.
func completeComparison(t *testing.T, s *single, allowLargeValue bool) (Reply, int, int) {
	t.Helper()
	batches, values := 0, 0
	for batches < 300 {
		if len(s.held) != 1 {
			t.Fatalf("comparison read not bounded: readers=%d reserved=%d", len(s.held), s.n.replyBytes)
		}
		r := s.held[0]
		charge := mvcc.CompareBytes(r.fills)
		if !r.compare || r.held != charge || s.n.replyBytes != charge || (charge > s.n.cfg.MemoryBytes && (!allowLargeValue || len(r.fills) != 1)) {
			t.Fatalf("comparison read not bounded: values=%d charge=%d held=%d total=%d budget=%d", len(r.fills), charge, r.held, s.n.replyBytes, s.n.cfg.MemoryBytes)
		}
		if p := r.read; p != nil && len(p.fetched) != 0 {
			t.Fatalf("comparison retained %d values from an earlier batch", len(p.fetched))
		}
		if w := s.n.paused; w != nil && len(w.fetched) != 0 {
			t.Fatalf("apply retained %d values from an earlier batch", len(w.fetched))
		}
		batches++
		values += len(r.fills)
		s.held = nil
		s.n.ValuesRead(r, r.Run())
		got := s.run()
		if len(got) != 0 {
			if len(got) != 1 || got[0].ID != 3000 {
				t.Fatalf("unexpected comparison replies: %+v", got)
			}
			return got[0], batches, values
		}
	}
	t.Fatal("comparison made no bounded progress")
	return Reply{}, 0, 0
}

func TestCompareMemoryBounded(t *testing.T) {
	for _, write := range []bool{false, true} {
		var hash uint64
		for _, budget := range []int64{32 << 10, 64 << 10} {
			t.Run(fmt.Sprintf("write=%v/budget=%d", write, budget), func(t *testing.T) {
				s, value := comparisonSingle(t, budget, 100, 1024)
				s.n.Submit(Request{ID: 3000, Op: comparisonTxn(value, write)})
				if got := s.run(); len(got) != 0 {
					t.Fatalf("comparison rejected before its bounded read: %+v", got)
				}
				r, batches, values := completeComparison(t, s, false)
				if r.Err != nil || !r.Resp.(*pb.TxnResponse).Succeeded || r.Held == 0 || batches < 2 || values != 100 {
					t.Fatalf("comparison response=%+v batches=%d values=%d", r, batches, values)
				}
				s.n.ReleaseReplyBytes(r.Held)
				if s.n.replyBytes != 0 {
					t.Fatalf("comparison retained %d bytes", s.n.replyBytes)
				}
				if current := s.n.kv.Hash(); hash != 0 && current != hash {
					t.Fatalf("budget changed committed state: %x != %x", current, hash)
				} else {
					hash = current
				}
				t.Logf("100 cold KiB compared in %d batches under %d bytes", batches, budget)
			})
		}
	}
}

func TestCompareMemoryRevisionChange(t *testing.T) {
	s, value := comparisonSingle(t, 32<<10, 100, 1024)
	s.n.Submit(Request{ID: 3000, Op: comparisonTxn(value, false)})
	s.run()
	if len(s.held) != 1 {
		t.Fatalf("initial readers=%d", len(s.held))
	}
	first := s.held[0]
	s.held = nil
	s.n.ValuesRead(first, first.Run())
	if got := s.run(); len(got) != 0 || len(s.held) != 1 {
		t.Fatalf("comparison did not stop for its second batch: replies=%v reads=%d", got, len(s.held))
	}
	// This key already matched in the first batch. Reusing that result
	// at a new revision would take the wrong branch.
	if r := s.do(3001, &pb.PutRequest{Key: []byte("k0000"), Value: []byte("changed")}); r.Err != nil {
		t.Fatal(r.Err)
	}
	r, _, _ := completeComparison(t, s, false)
	if r.Err != nil || r.Resp.(*pb.TxnResponse).Succeeded || r.Resp.(*pb.TxnResponse).Header.Revision != s.n.kv.Rev() {
		t.Fatalf("comparison reused a stale prefix: %+v", r)
	}
	s.n.ReleaseReplyBytes(r.Held)
}

func TestCompareMemoryApplyWaits(t *testing.T) {
	s, value := comparisonSingle(t, 32<<10, 100, 1024)
	held := s.n.cfg.MemoryBytes
	s.n.SetReplyBytes(held)
	s.n.Submit(Request{ID: 3000, Op: comparisonTxn(value, true)})
	if got := s.run(); len(got) != 0 || len(s.held) != 0 || s.n.paused == nil || s.n.paused.read != nil {
		t.Fatalf("apply did not wait for input memory: replies=%v reads=%d pause=%+v", got, len(s.held), s.n.paused)
	}
	before, err := s.n.kv.Range(&pb.RangeRequest{Key: []byte("result"), KeysOnly: true})
	if err != nil || len(before.Kvs) != 0 || s.n.Err() != nil {
		t.Fatalf("memory pressure changed the branch: result=%v err=%v node=%v", before, err, s.n.Err())
	}
	s.n.ReleaseReplyBytes(held)
	if got := s.run(); len(got) != 0 || len(s.held) == 0 {
		t.Fatalf("apply did not resume: replies=%v reads=%d", got, len(s.held))
	}
	r, _, _ := completeComparison(t, s, false)
	if r.Err != nil || !r.Resp.(*pb.TxnResponse).Succeeded {
		t.Fatalf("resumed apply: %+v", r)
	}
	s.n.ReleaseReplyBytes(r.Held)
	if s.n.paused != nil || s.n.replyBytes != 0 {
		t.Fatalf("apply retained state: pause=%+v bytes=%d", s.n.paused, s.n.replyBytes)
	}
}

func TestCompareMemorySingleLargeValue(t *testing.T) {
	s, value := comparisonSingle(t, 16<<10, 1, 64<<10)
	s.n.Submit(Request{ID: 3000, Op: comparisonTxn(value, true)})
	if got := s.run(); len(got) != 0 {
		t.Fatalf("committed comparison refused a single value: %+v", got)
	}
	r, batches, values := completeComparison(t, s, true)
	if r.Err != nil || !r.Resp.(*pb.TxnResponse).Succeeded || batches != 1 || values != 1 {
		t.Fatalf("single-value progress: %+v batches=%d values=%d", r, batches, values)
	}
	s.n.ReleaseReplyBytes(r.Held)
}

package mvcc

import (
	"errors"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRangeReservationBeforeValues(t *testing.T) {
	for _, order := range []pb.RangeRequest_SortTarget{pb.RangeRequest_KEY, pb.RangeRequest_VALUE} {
		h := &coldHeap{}
		s := New(h)
		put(t, s, "a", "value-a")
		put(t, s, "b", "value-b")
		h.evict()
		refused := errors.New("no response memory")
		called := false
		_, err := s.RangeKVsReserved(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), SortTarget: order, SortOrder: pb.RangeRequest_ASCEND, Limit: 1}, func(bytes int64) error {
			called = true
			if bytes < 4*int64(len("avalue-a")) {
				t.Errorf("reservation omitted the page's values: %d", bytes)
			}
			if order == pb.RangeRequest_VALUE && bytes < 4*int64(len("avalue-abvalue-b")) {
				t.Errorf("reservation omitted candidate values: %d", bytes)
			}
			return refused
		})
		if !called || !errors.Is(err, refused) || len(s.TakeFills()) != 0 {
			t.Fatalf("order %v read values before reserving: called=%v err=%v", order, called, err)
		}
	}
}

// A page that is not sorted by value reads only its own values, so the
// values of keys the limit drops must not count against it: a one-key
// page next to large values would get ResourceExhausted, however small
// its limit.
func TestRangeReservationLeavesOutDroppedValues(t *testing.T) {
	large := strings.Repeat("v", 1<<20)
	for _, order := range []pb.RangeRequest_SortTarget{pb.RangeRequest_KEY, pb.RangeRequest_MOD} {
		s := New(&coldHeap{})
		put(t, s, "a", "small")
		put(t, s, "b", large)
		put(t, s, "c", large)
		var reserved int64
		res, err := s.RangeKVsReserved(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("d"), SortTarget: order, SortOrder: pb.RangeRequest_ASCEND, Limit: 1}, func(bytes int64) error {
			reserved = bytes
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.KVs) != 1 || string(res.KVs[0].Key) != "a" {
			t.Fatalf("order %v: got %d keys, want only a", order, len(res.KVs))
		}
		if reserved == 0 || reserved >= int64(len(large)) {
			t.Errorf("order %v reserved %d bytes for a page of %q", order, reserved, "small")
		}
	}
}

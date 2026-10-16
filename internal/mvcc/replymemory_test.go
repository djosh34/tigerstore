package mvcc

import (
	"errors"
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
			if bytes < 4*int64(len("avalue-abvalue-b")) {
				t.Errorf("reservation omitted candidate values: %d", bytes)
			}
			return refused
		})
		if !called || !errors.Is(err, refused) || len(s.TakeFills()) != 0 {
			t.Fatalf("order %v read values before reserving: called=%v err=%v", order, called, err)
		}
	}
}

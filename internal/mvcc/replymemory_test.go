package mvcc

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRangeReservationCandidates(t *testing.T) {
	if size := unsafe.Sizeof(KV{}); size != kvReplyBytes {
		t.Fatalf("KV size=%d, reservation size=%d", size, kvReplyBytes)
	}
	s := New(&coldHeap{})
	const keys = 10000
	for i := range keys {
		put(t, s, fmt.Sprintf("%05d", i), "v")
	}
	r := &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, Limit: 1, KeysOnly: true, SortOrder: pb.RangeRequest_DESCEND}
	var reserved int64
	res, err := s.RangeKVsReserved(r, func(bytes int64) error {
		reserved = bytes
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := int64(1024 + (keys-1)*int(unsafe.Sizeof(KV{})) + 512 + 4*5)
	if reserved != want || len(res.KVs) != 1 || string(res.KVs[0].Key) != "09999" {
		t.Fatalf("reserved=%d want=%d, result=%+v", reserved, want, res)
	}
}

func TestRangeReservationBeforeAllocation(t *testing.T) {
	s := New(&coldHeap{})
	for i := range 10000 {
		put(t, s, fmt.Sprintf("%05d", i), "v")
	}
	r := &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, KeysOnly: true}
	refused := errors.New("no candidate memory")
	b := testing.Benchmark(func(b *testing.B) {
		for range b.N {
			_, err := s.RangeKVsReserved(r, func(int64) error { return refused })
			if !errors.Is(err, refused) {
				b.Fatal(err)
			}
		}
	})
	if bytes := b.AllocedBytesPerOp(); bytes > 8192 {
		t.Fatalf("refused range allocated %d bytes before admission", bytes)
	}
}

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

package mvcc

import (
	"errors"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// coldHeap is a Heap whose values up to number disk are on disk only, as
// values whose entries left memory are.
type coldHeap struct {
	Heap
	disk uint64
}

func (h *coldHeap) Read(r Ref, arena *[]byte) ([]byte, bool, error) {
	if r.Index <= h.disk {
		return nil, false, nil
	}
	return h.Heap.Read(r, arena)
}

// evict puts every value so far on disk only.
func (h *coldHeap) evict() { h.disk = uint64(len(h.vals)) }

// fillAll reads every fill from the heap, as a helper reads them from
// disk.
func fillAll(t *testing.T, s *Store, h *coldHeap) int {
	t.Helper()
	fills := s.TakeFills()
	for _, f := range fills {
		b, _, err := h.Heap.Read(f.Ref, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Set(b); err != nil {
			t.Fatal(err)
		}
	}
	return len(fills)
}

// TestColdValuesComeAsFills builds every response that carries values
// while those values are on disk only: each holds its values' positions
// as fills, and once they are read it equals the response built with the
// values in memory.
func TestColdValuesComeAsFills(t *testing.T) {
	ops := []proto.Message{
		&pb.RangeRequest{Key: []byte("a")},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")},
		&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), Limit: 2},
		&pb.PutRequest{Key: []byte("a"), Value: []byte("new"), PrevKv: true},
		&pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte("d"), PrevKv: true},
		&pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 2}}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("b")}}}},
			Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("c")}}}},
		},
		&pb.TxnRequest{
			Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 99}}},
			Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("b")}}}},
			Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("c")}}}},
		},
	}
	for _, op := range ops {
		hot, cold := &coldHeap{}, &coldHeap{}
		stores := [2]*Store{New(hot), New(cold)}
		var resps [2]proto.Message
		for i, s := range stores {
			for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
				put(t, s, kv[0], kv[1])
			}
			if i == 1 {
				cold.evict()
			}
			var err error
			switch op := op.(type) {
			case *pb.RangeRequest:
				resps[i], err = s.Range(op)
			case *pb.PutRequest:
				resps[i], err = s.Put(op)
			case *pb.DeleteRangeRequest:
				resps[i], err = s.DeleteRange(op)
			case *pb.TxnRequest:
				resps[i], err = s.Txn(op)
			}
			if err != nil {
				t.Fatalf("%v: %v", op, err)
			}
		}
		if n := fillAll(t, stores[0], hot); n != 0 {
			t.Fatalf("%v: %d fills for values in memory", op, n)
		}
		if n := fillAll(t, stores[1], cold); n == 0 {
			t.Fatalf("%v: no fills for values on disk", op)
		}
		if !proto.Equal(resps[0], resps[1]) {
			t.Fatalf("%v: with values on disk %v, in memory %v", op, resps[1], resps[0])
		}
	}
}

// TestColdEventAndRangeKVs checks the watch event of a change and a
// range without messages with their values on disk.
func TestColdEventAndRangeKVs(t *testing.T) {
	h := &coldHeap{}
	s := New(h)
	put(t, s, "a", "1")
	put(t, s, "a", "2")
	ch := s.Changes()[0]
	h.evict()
	ev := event(t, ch, true)
	if ev.Kv.Value != nil || ev.PrevKv.Value != nil {
		t.Fatal("an event read values from disk")
	}
	// A compaction after the event is built changes nothing in it.
	if err := s.Compact(s.Rev()); err != nil {
		t.Fatal(err)
	}
	if n := fillAll(t, s, h); n != 2 {
		t.Fatalf("%d fills for an event with a value and a previous one", n)
	}
	if string(ev.Kv.Value) != "2" || string(ev.PrevKv.Value) != "1" || ev.PrevKv.ModRevision != 2 {
		t.Fatalf("event after the reads: %v", ev)
	}
	res, err := s.RangeKVs(&pb.RangeRequest{Key: []byte("a")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The key and the value on disk, by its length.
	if res.Bytes != 2 {
		t.Fatalf("a range of one key and value of 1 byte counts %d bytes", res.Bytes)
	}
	fillAll(t, s, h)
	if string(res.KVs[0].Value) != "2" {
		t.Fatalf("range without messages: %q", res.KVs[0].Value)
	}
}

// TestColdCompareAndSort runs compares and sorts on values that are on
// disk only: each returns a ColdError naming them and changes nothing,
// unless the answer does not depend on them.
func TestColdCompareAndSort(t *testing.T) {
	h := &coldHeap{}
	s := New(h)
	put(t, s, "a", "1")
	put(t, s, "b", "2")
	h.evict()
	rev := s.Rev()
	value := func(k, v string) *pb.Compare {
		return &pb.Compare{Key: []byte(k), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte(v)}}
	}
	write := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("c"), Value: []byte("3")}}}
	byValue := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: pb.RangeRequest_VALUE}
	for _, tc := range []struct {
		name string
		do   func() error
		need int
	}{
		{"value compare", func() error {
			_, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{value("a", "1")}, Success: []*pb.RequestOp{write}})
			return err
		}, 1},
		{"value compare over a range", func() error {
			c := value("a", "1")
			c.RangeEnd = []byte("z")
			_, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{c}, Success: []*pb.RequestOp{write}})
			return err
		}, 2},
		{"sort by value", func() error {
			_, err := s.Range(byValue)
			return err
		}, 2},
		{"sort by value after a write in a transaction", func() error {
			_, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{write, {Request: &pb.RequestOp_RequestRange{RequestRange: byValue}}}})
			return err
		}, 2},
	} {
		var cold *ColdError
		if err := tc.do(); !errors.As(err, &cold) || len(cold.Need) != tc.need {
			t.Fatalf("%s: %v, want a ColdError for %d values", tc.name, err, tc.need)
		}
		if s.Rev() != rev {
			t.Fatalf("%s: revision %d after a ColdError, want %d", tc.name, s.Rev(), rev)
		}
	}
	// A compare that a key in memory fails needs nothing from disk.
	put(t, s, "c", "x")
	c := value("a", "1")
	c.RangeEnd = []byte("z")
	r, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{c}})
	if err != nil || r.Succeeded {
		t.Fatalf("compare failed by a key in memory: %v, %v", r, err)
	}
	// With the values in memory, the same work runs.
	h.disk = 0
	r, err = s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{value("a", "1")}, Success: []*pb.RequestOp{write}})
	if err != nil || !r.Succeeded {
		t.Fatalf("value compare in memory: %v, %v", r, err)
	}
}

// TestFillChecksum hands a fill the bytes of another value of the same
// length: it fails instead of keeping them.
func TestFillChecksum(t *testing.T) {
	h := &coldHeap{}
	s := New(h)
	put(t, s, "a", "11")
	put(t, s, "b", "22")
	h.evict()
	if _, err := s.Range(&pb.RangeRequest{Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	f := s.TakeFills()[0]
	if err := f.Set([]byte("22")); !errors.Is(err, ErrBadValue) {
		t.Fatalf("fill with another value's bytes: %v", err)
	}
	if *f.Dst != nil {
		t.Fatal("a fill kept bytes that failed their checksum")
	}
}

// TestRangeKVsBytes counts the keys and values of an answer, also when a
// sort by value has already read the values, and no values for KeysOnly:
// the writer charges a reply by these bytes before it encodes it.
func TestRangeKVsBytes(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", strings.Repeat("v", 1<<20))
	put(t, s, "b", "12")
	for _, c := range []struct {
		r    *pb.RangeRequest
		want int64
	}{
		{&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c")}, 2 + 1<<20 + 2},
		{&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), SortTarget: pb.RangeRequest_VALUE}, 2 + 1<<20 + 2},
		{&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), KeysOnly: true}, 2},
	} {
		res, err := s.RangeKVs(c.r, 0)
		if err != nil {
			t.Fatal(err)
		}
		if res.Bytes != c.want {
			t.Errorf("%v: %d bytes, want %d", c.r, res.Bytes, c.want)
		}
	}
}

// TestRangeTooLarge checks the one size check: an answer whose keys and
// values pass the limit is refused before any value is read, and the
// values of keys that a page drops count only for a sort on values.
func TestRangeTooLarge(t *testing.T) {
	h := &coldHeap{}
	s := New(h)
	put(t, s, "a", strings.Repeat("v", 100))
	put(t, s, "b", strings.Repeat("v", 100))
	h.evict()
	page := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), Limit: 1}
	byValue := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), Limit: 1, SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND}
	whole := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c")}
	keys := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), KeysOnly: true}
	for _, c := range []struct {
		r     *pb.RangeRequest
		limit int64
		ok    bool
	}{
		{page, 101, true},
		{page, 100, false},
		{byValue, 201, false},
		{byValue, 202, true},
		{whole, 201, false},
		{keys, 2, true},
		{whole, 0, true},
	} {
		// A sort on values that fits still needs them from disk first.
		_, err := s.RangeKVs(c.r, c.limit)
		if _, cold := errors.AsType[*ColdError](err); c.ok && err != nil && !cold {
			t.Errorf("%v under %d: %v", c.r, c.limit, err)
		}
		if !c.ok && !errors.Is(err, ErrTooLarge) {
			t.Errorf("%v under %d: %v, want ErrTooLarge", c.r, c.limit, err)
		}
		if fills := s.TakeFills(); !c.ok && len(fills) > 0 {
			t.Errorf("%v under %d: a refused answer left %d value reads", c.r, c.limit, len(fills))
		}
	}
}

package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
	res, _, err := s.RangeFirst(&pb.RangeRequest{Key: []byte("a")}, true, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// The size of the KeyValue field counts the value on disk by its
	// length.
	if want := int64(proto.Size(&pb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("a"), Value: []byte("2"), CreateRevision: 2, ModRevision: 3, Version: 2}}})); res.Fields != want {
		t.Fatalf("a range of one key with a value of 1 byte counts %d bytes of fields, want %d", res.Fields, want)
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

// TestRangeFields sizes the KeyValue fields of an answer as protobuf
// encodes them, also when a sort by value has already read the values,
// and with no values for KeysOnly: the writer sizes the message it sends
// by them before it has every value.
func TestRangeFields(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", strings.Repeat("v", 1<<20))
	put(t, s, "b", "12")
	for _, r := range []*pb.RangeRequest{
		{Key: []byte("a"), RangeEnd: []byte("c")},
		{Key: []byte("a"), RangeEnd: []byte("c"), SortTarget: pb.RangeRequest_VALUE},
		{Key: []byte("a"), RangeEnd: []byte("c"), KeysOnly: true},
	} {
		res, _, err := s.RangeFirst(r, true, 64)
		if err != nil {
			t.Fatal(err)
		}
		want, err := s.Range(r)
		if err != nil {
			t.Fatal(err)
		}
		if n := int64(proto.Size(&pb.RangeResponse{Kvs: want.Kvs})); res.Fields != n || res.N != 2 {
			t.Errorf("%v: %d keys, %d bytes of fields, want 2 and %d", r, res.N, res.Fields, n)
		}
	}
}

// TestSortByValueTooLarge checks the one check left of the size of an
// answer: a sort by value needs every value at once, and is refused when
// they pass SortBytes, before any value is read. Every other answer goes
// out in pieces, whatever its size.
func TestSortByValueTooLarge(t *testing.T) {
	h := &coldHeap{}
	s := New(h)
	put(t, s, "a", strings.Repeat("v", 100))
	put(t, s, "b", strings.Repeat("v", 100))
	h.evict()
	byValue := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c"), Limit: 1, SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_ASCEND}
	whole := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("c")}
	for _, c := range []struct {
		r     *pb.RangeRequest
		limit int64
		ok    bool
	}{
		// Two KVs in the list and their 200 bytes of values.
		{byValue, 2*kvBytes + 199, false},
		{byValue, 2*kvBytes + 200, true},
		{whole, 1, true},
	} {
		s.SortBytes = c.limit
		// A sort on values that fits still needs them from disk first.
		_, _, err := s.RangeFirst(c.r, true, 1<<20)
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

// fetchHeap is a coldHeap that also returns the values a driver read for
// the last ColdError, and nothing it read before.
type fetchHeap struct {
	coldHeap
	fetched map[Ref][]byte
}

func (h *fetchHeap) Read(r Ref, arena *[]byte) ([]byte, bool, error) {
	if b, ok := h.fetched[r]; ok {
		return b, true, nil
	}
	return h.coldHeap.Read(r, arena)
}

// TestCompareInBatches runs a VALUE compare over 40 values of 100 KiB on
// disk only, as a driver does: after each ColdError it reads only the
// values named, drops those of the batch before, and runs the
// transaction again from where it got. Each batch must stay near
// compareBatch, the compare must stop at the batch with the first value
// that fails it, and a write in between must make it start over.
func TestCompareInBatches(t *testing.T) {
	const keys, size = 40, 100 << 10
	value := bytes.Repeat([]byte("v"), size)
	setup := func(bad int) (*Store, *fetchHeap) {
		h := &fetchHeap{}
		s := New(h)
		for i := range keys {
			v := value
			if i == bad {
				v = bytes.Repeat([]byte("x"), size)
			}
			put(t, s, fmt.Sprintf("k%02d", i), string(v))
		}
		h.evict()
		return s, h
	}
	txn := &pb.TxnRequest{Compare: []*pb.Compare{{Key: []byte("k"), RangeEnd: []byte("l"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: value}}}}
	// run runs txn to its end and returns its result and the batches it
	// read. between, if set, runs after the first batch.
	run := func(s *Store, h *fetchHeap, between func()) (bool, int) {
		var resume *Compares
		batches := 0
		for {
			s.Resume(resume)
			r, err := s.Txn(txn)
			cold, ok := errors.AsType[*ColdError](err)
			if !ok {
				if err != nil {
					t.Fatal(err)
				}
				return r.Succeeded, batches
			}
			read := 0
			h.fetched = map[Ref][]byte{}
			for _, f := range cold.Need {
				b, _, err := h.Heap.Read(f.Ref, nil)
				if err != nil {
					t.Fatal(err)
				}
				h.fetched[f.Ref] = b
				read += len(b)
			}
			if read > compareBatch+size {
				t.Fatalf("a batch of %d bytes", read)
			}
			batches++
			resume = cold.Compares
			if between != nil && batches == 1 {
				between()
			}
		}
	}
	s, h := setup(-1)
	if ok, n := run(s, h, nil); !ok || n != 4 {
		t.Errorf("all equal: succeeded %v after %d batches, want true after 4", ok, n)
	}
	s, h = setup(15)
	if ok, n := run(s, h, nil); ok || n != 2 {
		t.Errorf("k15 differs: succeeded %v after %d batches, want false after 2", ok, n)
	}
	// A write to a key the first batch matched makes the compare start
	// over and see it.
	s, h = setup(-1)
	if ok, n := run(s, h, func() { put(t, s, "k05", "x") }); ok || n != 1 {
		t.Errorf("k05 changed after the first batch: succeeded %v after %d batches, want false after 1", ok, n)
	}
}

// TestUnansweredWritesReadNothing applies each write that reads besides
// writing to two stores: one answered with its values in memory, and one
// Unanswered, as a node where no client waits is, with its values on
// disk (#561). The unanswered one asks for no value, not even those a
// range sorts by, its answer holds none, and both end with the same keys,
// revision and counts.
func TestUnansweredWritesReadNothing(t *testing.T) {
	rng := func(r *pb.RangeRequest) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: r}}
	}
	ops := []proto.Message{
		&pb.PutRequest{Key: []byte("a"), Value: []byte("new"), PrevKv: true},
		&pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte("d"), PrevKv: true},
		&pb.TxnRequest{Success: []*pb.RequestOp{
			rng(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}),
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("new"), PrevKv: true}}},
			rng(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: pb.RangeRequest_VALUE, SortOrder: pb.RangeRequest_DESCEND}),
		}},
	}
	for _, op := range ops {
		var stores [2]*Store
		var resps [2]proto.Message
		for i := range stores {
			h := &coldHeap{}
			s := New(h)
			stores[i] = s
			for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
				put(t, s, kv[0], kv[1])
			}
			if i == 1 {
				h.evict()
				s.Unanswered = true
			}
			var err error
			switch op := op.(type) {
			case *pb.PutRequest:
				resps[i], err = s.Put(op)
			case *pb.DeleteRangeRequest:
				resps[i], err = s.DeleteRange(op)
			case *pb.TxnRequest:
				resps[i], err = s.Txn(op)
			}
			if err != nil {
				t.Fatalf("%v, unanswered %v: %v", op, s.Unanswered, err)
			}
		}
		a, b := stores[0], stores[1]
		if n := len(b.TakeFills()); n != 0 {
			t.Fatalf("%v: %d fills on the unanswered store", op, n)
		}
		if len(b.TakeRests()) != 0 {
			t.Fatalf("%v: rests on the unanswered store", op)
		}
		if n := answerKVs(resps[1]); n != 0 {
			t.Fatalf("%v: the unanswered store answered with %d key-values: %v", op, n, resps[1])
		}
		if answerKVs(resps[0]) == 0 {
			t.Fatalf("%v: the answered store answered with no key-values: %v", op, resps[0])
		}
		if a.Rev() != b.Rev() || a.Hash() != b.Hash() || a.Counts() != b.Counts() {
			t.Fatalf("%v: answered rev %d hash %x counts %+v, unanswered rev %d hash %x counts %+v",
				op, a.Rev(), a.Hash(), a.Counts(), b.Rev(), b.Hash(), b.Counts())
		}
	}
}

// answerKVs counts the key-values an answer of a put, a delete or a Txn
// carries.
func answerKVs(m proto.Message) int {
	switch r := m.(type) {
	case *pb.PutResponse:
		if r.PrevKv != nil {
			return 1
		}
	case *pb.DeleteRangeResponse:
		return len(r.PrevKvs)
	case *pb.TxnResponse:
		n := 0
		for _, op := range r.Responses {
			if p := op.GetResponsePut(); p != nil {
				n += answerKVs(p)
			}
			n += len(op.GetResponseRange().GetKvs())
		}
		return n
	}
	return 0
}

// TestColdTxnCountsOnce runs a Txn that compares, checks a put's previous
// value and sorts a range by value on two stores with their values on
// disk: one answered, which reads the values and runs the Txn again, and
// one Unanswered, which reads none (#561). Both count the Txn once.
func TestColdTxnCountsOnce(t *testing.T) {
	version1 := &pb.Compare{Key: []byte("a"), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Version{Version: 1}}
	txn := &pb.TxnRequest{
		Compare: []*pb.Compare{version1, version1},
		Success: []*pb.RequestOp{
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("2"), PrevKv: true}}},
			{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a"), SortTarget: pb.RangeRequest_VALUE}}},
		},
	}
	var stores [2]*Store
	for i := range stores {
		h := &coldHeap{}
		s := New(h)
		stores[i] = s
		put(t, s, "a", "1")
		h.evict()
		s.Unanswered = i == 1
		_, err := s.Txn(txn)
		if cold, ok := errors.AsType[*ColdError](err); ok && i == 0 {
			h.disk = 0
			s.Resume(cold.Compares)
			_, err = s.Txn(txn)
		} else if ok {
			t.Fatal("the unanswered store waits for values")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if a, b := stores[0].Counts(), stores[1].Counts(); a != b || a.Txns != 1 {
		t.Fatalf("answered after a read counts %+v, unanswered %+v; want the same, with one Txn", a, b)
	}
}

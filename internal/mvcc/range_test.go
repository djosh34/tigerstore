package mvcc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"
)

// pieces reads r through a cursor with room bytes a piece, and returns
// the answer as one message, the number of pieces and the result of the
// first.
func pieces(t *testing.T, s *Store, r *pb.RangeRequest, total bool, room int) (*pb.RangeResponse, int, RangeResult) {
	t.Helper()
	res, c, err := s.RangeFirst(r, total, room)
	if err != nil {
		t.Fatalf("%v: %v", r, err)
	}
	kvs := res.KVs
	n := 1
	for c != nil {
		piece, done, err := c.Next(room)
		if err != nil {
			t.Fatalf("%v: piece %d: %v", r, n, err)
		}
		if len(piece) == 0 {
			t.Fatalf("%v: piece %d is empty", r, n)
		}
		kvs = append(kvs, piece...)
		n++
		if done {
			c = nil
		}
	}
	first := res
	res.KVs = kvs
	return res.Response(), n, first
}

// TestCursorPiecesMakeTheAnswer reads ranges of every shape in pieces of
// 64 bytes over a store with deletes and several versions per key: the
// pieces put together must be the whole answer, and the first piece must
// give its count, its number of keys and the size of its fields.
func TestCursorPiecesMakeTheAnswer(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 505))
	s := New(&Heap{})
	for i := range 400 {
		k := fmt.Appendf(nil, "k%03d", rng.IntN(120))
		if rng.IntN(5) == 0 {
			if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: k}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := s.Put(&pb.PutRequest{Key: k, Value: fmt.Appendf(nil, "value %d of %s", i, k)}); err != nil {
			t.Fatal(err)
		}
	}
	mid := s.Rev() / 2
	var reqs []*pb.RangeRequest
	for _, limit := range []int64{0, 1, 7, 50, 1000} {
		for _, f := range []func(*pb.RangeRequest){
			func(*pb.RangeRequest) {},
			func(r *pb.RangeRequest) { r.KeysOnly = true },
			func(r *pb.RangeRequest) { r.CountOnly = true },
			func(r *pb.RangeRequest) { r.Revision = mid },
			func(r *pb.RangeRequest) { r.MinModRevision = mid },
			func(r *pb.RangeRequest) { r.MaxCreateRevision = mid },
			func(r *pb.RangeRequest) { r.SortTarget, r.SortOrder = pb.RangeRequest_MOD, pb.RangeRequest_DESCEND },
			func(r *pb.RangeRequest) { r.SortTarget, r.SortOrder = pb.RangeRequest_KEY, pb.RangeRequest_DESCEND },
			func(r *pb.RangeRequest) { r.SortTarget = pb.RangeRequest_VALUE },
			func(r *pb.RangeRequest) { r.SortTarget, r.KeysOnly = pb.RangeRequest_VERSION, true },
			func(r *pb.RangeRequest) { r.RangeEnd = []byte{0} },
		} {
			r := &pb.RangeRequest{Key: []byte("k02"), RangeEnd: []byte("k09"), Limit: limit}
			f(r)
			reqs = append(reqs, r)
		}
	}
	reqs = append(reqs, &pb.RangeRequest{Key: []byte("k050")})
	for _, r := range reqs {
		for _, total := range []bool{true, false} {
			want, _, err := s.reader().rangeAt(r, total, 1<<30, true)
			if err != nil {
				t.Fatalf("%v: %v", r, err)
			}
			got, n, first := pieces(t, s, r, total, 64)
			if w := want.Response(); !proto.Equal(got, w) {
				t.Fatalf("%v, total %v: in %d pieces %v, whole %v", r, total, n, got, w)
			}
			if fields := int64(proto.Size(&pb.RangeResponse{Kvs: got.Kvs})); first.N != int64(len(got.Kvs)) || first.Fields != fields {
				t.Fatalf("%v, total %v: the first piece says %d keys and %d bytes, the answer has %d and %d", r, total, first.N, first.Fields, len(got.Kvs), fields)
			}
			if len(got.Kvs) > 3 && n < 2 {
				t.Fatalf("%v: %d keys came in one piece of 64 bytes", r, len(got.Kvs))
			}
		}
	}
}

// TestCursorReadsItsPosition reads the rest of an answer after writes and
// a compaction below its revision: the pieces are the answer at the
// cursor's position. A compaction past it fails the next piece.
func TestCursorReadsItsPosition(t *testing.T) {
	s := New(&Heap{})
	for i := range 50 {
		put(t, s, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i))
	}
	r := &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}
	want, err := s.Range(r)
	if err != nil {
		t.Fatal(err)
	}
	res, c, err := s.RangeFirst(r, true, 64)
	if err != nil || c == nil {
		t.Fatalf("the first piece: %v, cursor %v", err, c)
	}
	rev := s.Rev()
	for i := range 50 {
		put(t, s, fmt.Sprintf("k%02d", i), "later")
	}
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(rev); err != nil {
		t.Fatal(err)
	}
	kvs := res.KVs
	for {
		piece, done, err := c.Next(64)
		if err != nil {
			t.Fatal(err)
		}
		kvs = append(kvs, piece...)
		if done {
			break
		}
	}
	res.KVs = kvs
	if got := res.Response(); !proto.Equal(got, want) {
		t.Fatalf("after writes and a compaction at its revision: %v, want %v", got, want)
	}
	_, c, err = s.RangeFirst(&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: rev}, true, 64)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, "z", "z")
	if err := s.Compact(rev + 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Next(64); !errors.Is(err, rpctypes.ErrGRPCCompacted) {
		t.Fatalf("a piece after a compaction past the cursor: %v", err)
	}
}

// TestTxnRangeReadsItsPosition reads a range inside a Txn between two of
// its puts: it sees the first and not the second, as etcd's does.
func TestTxnRangeReadsItsPosition(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")
	op := func(o any) *pb.RequestOp {
		switch o := o.(type) {
		case *pb.PutRequest:
			return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: o}}
		case *pb.RangeRequest:
			return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: o}}
		}
		return nil
	}
	resp, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		op(&pb.PutRequest{Key: []byte("b"), Value: []byte("2")}),
		op(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}),
		op(&pb.PutRequest{Key: []byte("c"), Value: []byte("3")}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if kvs := resp.Responses[1].GetResponseRange().Kvs; len(kvs) != 2 || string(kvs[1].Key) != "b" {
		t.Fatalf("the range between two puts of a Txn read %v", kvs)
	}
}

// TestHistoryInRevisionOrder logs writes of one and many keys, compacts
// part of them away, and reads the history from a revision: each change
// comes once, in revision order, as History finds them, also after the
// store is loaded from a checkpoint.
func TestHistoryInRevisionOrder(t *testing.T) {
	s := New(&Heap{})
	for i := range 30 {
		put(t, s, fmt.Sprintf("k%02d", i%7), strconv.Itoa(i))
		if i%10 == 9 {
			if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Compact(12); err != nil {
		t.Fatal(err)
	}
	check := func(s *Store) {
		t.Helper()
		for from := int64(12); from <= s.Rev()+1; from++ {
			want := s.History([]byte{0}, []byte{0}, from)
			var got []Change
			for pos := s.HistoryFrom(from); pos < s.HistoryEnd(); pos++ {
				got = append(got, s.Changed(pos))
			}
			if len(got) != len(want) {
				t.Fatalf("from %d: %d changes, want %d", from, len(got), len(want))
			}
			for i := range got {
				if got[i].k != want[i].k || got[i].i != want[i].i {
					t.Fatalf("from %d: change %d is %s at %d, want %s at %d", from, i, got[i].Key(), got[i].ModRev(), want[i].Key(), want[i].ModRev())
				}
			}
		}
	}
	check(s)
	s.Freeze()
	var b []byte
	for done := false; !done; {
		b, _, done = s.CopyFrozen(b, 1<<20)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	check(loaded)
}

// TestTxnRestsMakeTheAnswer runs a Txn with two ranges and a delete with
// previous values under a room of 64 bytes for its KeyValues, and adds
// what each Rest's cursor reads to its message: the answer must be the
// one a store without the room gives, and every range or delete past the
// room must have a Rest.
func TestTxnRestsMakeTheAnswer(t *testing.T) {
	stores := [2]*Store{New(&Heap{}), New(&Heap{})}
	stores[1].PieceBytes = 64
	op := func(o any) *pb.RequestOp {
		switch o := o.(type) {
		case *pb.PutRequest:
			return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: o}}
		case *pb.RangeRequest:
			return &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: o}}
		case *pb.DeleteRangeRequest:
			return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: o}}
		}
		return nil
	}
	txn := &pb.TxnRequest{Success: []*pb.RequestOp{
		op(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("b")}),
		op(&pb.PutRequest{Key: []byte("x"), Value: []byte("new")}),
		op(&pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")}),
		op(&pb.DeleteRangeRequest{Key: []byte("c"), RangeEnd: []byte("d"), PrevKv: true}),
	}}
	var resps [2]*pb.TxnResponse
	for i, s := range stores {
		for j := range 20 {
			put(t, s, fmt.Sprintf("a%02d", j), fmt.Sprintf("value of a%02d", j))
			put(t, s, fmt.Sprintf("c%02d", j), fmt.Sprintf("value of c%02d", j))
		}
		resp, err := s.Txn(txn)
		if err != nil {
			t.Fatal(err)
		}
		rests := s.TakeRests()
		if i == 0 && len(rests) > 0 {
			t.Fatalf("%d rests without a room", len(rests))
		}
		if i == 1 && len(rests) != 3 {
			t.Fatalf("%d rests under a room of 64 bytes, want one per range and delete", len(rests))
		}
		for _, r := range rests {
			for done := false; !done; {
				kvs, d, err := r.Cursor.Next(64)
				if err != nil {
					t.Fatal(err)
				}
				done = d
				switch m := r.Msg.(type) {
				case *pb.RangeResponse:
					m.Kvs = append(m.Kvs, KeyValues(kvs)...)
				case *pb.DeleteRangeResponse:
					m.PrevKvs = append(m.PrevKvs, KeyValues(kvs)...)
				}
			}
		}
		resps[i] = resp
	}
	if !proto.Equal(resps[0], resps[1]) {
		t.Fatalf("in pieces %v, whole %v", resps[1], resps[0])
	}
}

// TestEventSize sizes the events of puts, overwrites, deletes and a Txn
// that writes a key it deleted, with and without previous values, as
// protobuf encodes them in a WatchResponse.
func TestEventSize(t *testing.T) {
	s := New(&Heap{})
	var all []Change
	keep := func() { all = append(all, s.Changes()...) }
	put(t, s, "a", "1")
	keep()
	put(t, s, "a", "22")
	keep()
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	keep()
	put(t, s, "b", "")
	keep()
	if _, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("c"), Value: []byte("333"), Lease: 0}}},
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("b")}}},
	}}); err != nil {
		t.Fatal(err)
	}
	keep()
	for _, c := range all {
		for _, withPrev := range []bool{false, true} {
			ev, err := c.Event(withPrev)
			if err != nil {
				t.Fatal(err)
			}
			want := proto.Size(&pb.WatchResponse{Events: []*mvccpb.Event{ev}})
			if got := c.EventSize(withPrev); got != want {
				t.Fatalf("%s at %d, prev %v: sized %d, encoded %d", c.Key(), c.ModRev(), withPrev, got, want)
			}
		}
	}
}

// TestTxnRangesShareTheRoom runs a Txn of sixteen ranges over values of
// 1 KiB under a room of 32 KiB: the first reply's KeyValues, values
// included, keep to the room between them, and the rest come as Rests.
func TestTxnRangesShareTheRoom(t *testing.T) {
	s := New(&Heap{})
	for i := range 100 {
		put(t, s, fmt.Sprintf("k%03d", i), strings.Repeat("v", 1024))
	}
	s.PieceBytes = 32 << 10
	txn := &pb.TxnRequest{}
	for range 16 {
		txn.Success = append(txn.Success, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}}})
	}
	resp, err := s.Txn(txn)
	if err != nil {
		t.Fatal(err)
	}
	size := 0
	for _, r := range resp.Responses {
		size += proto.Size(&pb.RangeResponse{Kvs: r.GetResponseRange().Kvs})
	}
	if size > s.PieceBytes+2<<10 {
		t.Fatalf("the first reply of sixteen ranges holds %d bytes of KeyValues, with a room of %d", size, s.PieceBytes)
	}
	if rests := s.TakeRests(); len(rests) != 16 {
		t.Fatalf("%d of sixteen ranges have a Rest", len(rests))
	}
}

// TestSortOrderNoneSorts reads with a target other than the key and the
// order NONE, which etcd sorts ascending: with a limit, it sorts only the
// first limit+1 keys, as etcd pushes that limit into the index. A list
// that passes SortBytes is refused, but not inside a Txn.
func TestSortOrderNoneSorts(t *testing.T) {
	s := New(&Heap{})
	for _, k := range []string{"c", "b", "a"} {
		put(t, s, k, "v")
	}
	keys := func(r *pb.RangeResponse) string {
		var out []string
		for _, kv := range r.Kvs {
			out = append(out, string(kv.Key))
		}
		return strings.Join(out, " ")
	}
	for _, target := range []pb.RangeRequest_SortTarget{pb.RangeRequest_MOD, pb.RangeRequest_CREATE, pb.RangeRequest_VALUE} {
		r := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: target}
		if target == pb.RangeRequest_VALUE {
			// Values in the order c, b, a.
			for i, k := range []string{"c", "b", "a"} {
				put(t, s, k, strconv.Itoa(i))
			}
		}
		if got := keys(get(t, s, r)); got != "c b a" {
			t.Errorf("%v with order NONE: %q, want %q", target, got, "c b a")
		}
		r.Limit = 1
		resp, _, _ := pieces(t, s, r, true, 1)
		if got := keys(resp); got != "b" || !resp.More || resp.Count != 3 {
			t.Errorf("%v with order NONE and limit 1: %q, more %v, count %d, want %q, more, count 3", target, got, resp.More, resp.Count, "b")
		}
	}
	s.SortBytes = 2 * kvBytes
	sorted := &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z"), SortTarget: pb.RangeRequest_MOD}
	if _, _, err := s.RangeFirst(sorted, true, 1<<20); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a sorted list of three keys over a budget of two: %v, want ErrTooLarge", err)
	}
	// Inside a write the limit, which is the node's own, does not apply:
	// every node must apply the write alike.
	txn, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("d"), Value: []byte("v")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: sorted}},
	}})
	if err != nil || len(txn.Responses[1].GetResponseRange().Kvs) != 4 {
		t.Errorf("a Txn with a sorted range over the budget: %v, %v", txn, err)
	}
}

// TestKVBytes checks that a sorted list counts at least what a KV takes.
func TestKVBytes(t *testing.T) {
	if size := unsafe.Sizeof(KV{}); size > kvBytes {
		t.Fatalf("a KV takes %d bytes, the sorted list counts %d", size, kvBytes)
	}
}

// TestHistoryOfAKeyChangedTwice deletes a range and puts a key of it
// again in one Txn, many times, compacts at one such revision, and walks
// the history from there: the changes that are not Gone are every version
// the index holds, in order, also after the store is loaded from a
// checkpoint, which keeps the slots of removed versions empty. A walk of
// a revision of 100,000 deletes and one put takes linear time.
func TestHistoryOfAKeyChangedTwice(t *testing.T) {
	s := New(&Heap{})
	again := func(n int) {
		t.Helper()
		for i := range n {
			put(t, s, fmt.Sprintf("k%06d", i), "v")
		}
		if _, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
			{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}}},
			{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("k000001"), Value: []byte("again")}}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	again(3)
	compact := s.Rev()
	again(3)
	if err := s.Compact(compact); err != nil {
		t.Fatal(err)
	}
	check := func(s *Store) {
		t.Helper()
		want := s.History([]byte{0}, []byte{0}, compact)
		var got []Change
		gone := 0
		for pos := s.HistoryFrom(compact); pos < s.HistoryEnd(); pos++ {
			if c := s.Changed(pos); c.Gone() {
				gone++
			} else {
				got = append(got, c)
			}
		}
		if len(got) != len(want) || gone == 0 {
			t.Fatalf("%d changes and %d gone, want %d and some gone", len(got), gone, len(want))
		}
		for i := range got {
			if got[i].k != want[i].k || got[i].i != want[i].i {
				t.Fatalf("change %d is %s at %d, want %s at %d", i, got[i].Key(), got[i].ModRev(), want[i].Key(), want[i].ModRev())
			}
		}
	}
	check(s)
	s.Freeze()
	var b []byte
	for done := false; !done; {
		b, _, done = s.CopyFrozen(b, 1<<20)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	check(loaded)

	again(100_000)
	start := time.Now()
	n := 0
	for pos := s.HistoryFrom(s.Rev()); pos < s.HistoryEnd(); pos++ {
		if !s.Changed(pos).Gone() {
			n++
		}
	}
	if took := time.Since(start); n != 100_001 || took > 2*time.Second {
		t.Fatalf("a walk of the revision found %d changes in %v", n, took)
	}
}

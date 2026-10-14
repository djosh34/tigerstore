package mvcc

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func put(t *testing.T, s *Store, k, v string) *pb.PutResponse {
	t.Helper()
	r, err := s.Put(&pb.PutRequest{Key: []byte(k), Value: []byte(v)})
	if err != nil {
		t.Fatalf("put %q: %v", k, err)
	}
	return r
}

func get(t *testing.T, s *Store, r *pb.RangeRequest) *pb.RangeResponse {
	t.Helper()
	resp, err := s.Range(r)
	if err != nil {
		t.Fatalf("range %q: %v", r.Key, err)
	}
	return resp
}

func event(t *testing.T, c Change, withPrev bool) *mvccpb.Event {
	t.Helper()
	ev, err := c.Event(withPrev)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func keys(kvs []*mvccpb.KeyValue) string {
	var b strings.Builder
	for _, kv := range kvs {
		b.WriteString(string(kv.Key) + "=" + string(kv.Value) + ";")
	}
	return b.String()
}

func TestPutVersionsAndRevisions(t *testing.T) {
	s := New(&Heap{})
	if got := put(t, s, "a", "1").Header.Revision; got != 2 {
		t.Fatalf("first put revision = %d, want 2", got)
	}
	put(t, s, "a", "2")
	resp := get(t, s, &pb.RangeRequest{Key: []byte("a")})
	kv := resp.Kvs[0]
	if kv.CreateRevision != 2 || kv.ModRevision != 3 || kv.Version != 2 || string(kv.Value) != "2" {
		t.Fatalf("kv = %v", kv)
	}
	if resp.Header.Revision != 3 {
		t.Fatalf("header revision = %d, want 3", resp.Header.Revision)
	}

	old := get(t, s, &pb.RangeRequest{Key: []byte("a"), Revision: 2})
	if string(old.Kvs[0].Value) != "1" || old.Header.Revision != 3 {
		t.Fatalf("read at 2 = %v, header %d", old.Kvs, old.Header.Revision)
	}
	if _, err := s.Range(&pb.RangeRequest{Key: []byte("a"), Revision: 4}); !errors.Is(err, rpctypes.ErrGRPCFutureRev) {
		t.Fatalf("future revision error = %v", err)
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")
	d, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true})
	if err != nil {
		t.Fatal(err)
	}
	if d.Deleted != 1 || d.Header.Revision != 3 || string(d.PrevKvs[0].Value) != "1" {
		t.Fatalf("delete = %v", d)
	}
	// Deleting nothing does not move the revision.
	d, err = s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if d.Deleted != 0 || d.Header.Revision != 3 || s.Rev() != 3 {
		t.Fatalf("empty delete = %v, store rev %d", d, s.Rev())
	}
	put(t, s, "a", "2")
	kv := get(t, s, &pb.RangeRequest{Key: []byte("a")}).Kvs[0]
	if kv.CreateRevision != 4 || kv.Version != 1 {
		t.Fatalf("recreated kv = %v", kv)
	}
	if n := len(get(t, s, &pb.RangeRequest{Key: []byte("a"), Revision: 3}).Kvs); n != 0 {
		t.Fatalf("read at tombstone found %d keys", n)
	}
}

func TestRangeOptions(t *testing.T) {
	s := New(&Heap{})
	for _, k := range []string{"b", "a", "d", "c", "e"} {
		put(t, s, k, "v"+k)
	}
	all := []byte{0}
	r := get(t, s, &pb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("e")})
	if keys(r.Kvs) != "b=vb;c=vc;d=vd;" || r.Count != 3 || r.More {
		t.Fatalf("range [b,e) = %s count %d", keys(r.Kvs), r.Count)
	}
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: all, Limit: 2})
	if keys(r.Kvs) != "a=va;b=vb;" || r.Count != 5 || !r.More {
		t.Fatalf("limit 2 = %s count %d more %v", keys(r.Kvs), r.Count, r.More)
	}
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: all, CountOnly: true})
	if len(r.Kvs) != 0 || r.Count != 5 {
		t.Fatalf("count only = %v", r)
	}
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: all, KeysOnly: true, Limit: 1})
	if keys(r.Kvs) != "a=;" {
		t.Fatalf("keys only = %s", keys(r.Kvs))
	}
	r = get(t, s, &pb.RangeRequest{
		Key: []byte("a"), RangeEnd: all,
		SortTarget: pb.RangeRequest_MOD, SortOrder: pb.RangeRequest_DESCEND, Limit: 2,
	})
	if keys(r.Kvs) != "e=ve;c=vc;" || !r.More {
		t.Fatalf("sort by mod descending = %s", keys(r.Kvs))
	}
	// Count ignores revision filters, as in etcd.
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: all, MinModRevision: 4})
	if keys(r.Kvs) != "c=vc;d=vd;e=ve;" || r.Count != 5 {
		t.Fatalf("min mod revision = %s count %d", keys(r.Kvs), r.Count)
	}
}

func cas(key string, mod int64, val string) *pb.TxnRequest {
	return &pb.TxnRequest{
		Compare: []*pb.Compare{{
			Key: []byte(key), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL,
			TargetUnion: &pb.Compare_ModRevision{ModRevision: mod},
		}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(key), Value: []byte(val)}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(key)}}}},
	}
}

func TestTxnCompareAndSwap(t *testing.T) {
	s := New(&Heap{})
	r, err := s.Txn(cas("k", 0, "1"))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Succeeded || r.Header.Revision != 2 {
		t.Fatalf("create txn = %v", r)
	}
	r, err = s.Txn(cas("k", 0, "2"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Succeeded || r.Header.Revision != 2 || s.Rev() != 2 {
		t.Fatalf("failed txn = %v, rev %d", r, s.Rev())
	}
	got := r.Responses[0].GetResponseRange()
	if keys(got.Kvs) != "k=1;" || got.Kvs[0].ModRevision != 2 {
		t.Fatalf("failure range = %v", got)
	}
	r, err = s.Txn(cas("k", 2, "3"))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Succeeded || r.Header.Revision != 3 || r.Responses[0].GetResponsePut().Header.Revision != 3 {
		t.Fatalf("update txn = %v", r)
	}
}

func TestTxnVersionCompareOnMissingKey(t *testing.T) {
	s := New(&Heap{})
	r, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{{
		Key: []byte("x"), Target: pb.Compare_VERSION, Result: pb.Compare_EQUAL,
		TargetUnion: &pb.Compare_Version{Version: 0},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Succeeded {
		t.Fatal("version 0 compare on a missing key failed")
	}
	r, err = s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{{
		Key: []byte("x"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL,
		TargetUnion: &pb.Compare_Value{Value: nil},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Succeeded {
		t.Fatal("value compare on a missing key succeeded")
	}
}

func TestTxnReadsOwnWrites(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")
	r, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("2")}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rr := r.Responses[1].GetResponseRange()
	if len(rr.Kvs) != 0 || rr.Header.Revision != 3 {
		t.Fatalf("range after delete in txn = %v", rr)
	}
	if r.Header.Revision != 3 || s.Rev() != 3 {
		t.Fatalf("txn revision = %d, store %d", r.Header.Revision, s.Rev())
	}
	ch := s.Changes()
	if len(ch) != 2 {
		t.Fatalf("changes = %v", ch)
	}
	if ev := event(t, ch[0], true); ev.Type != mvccpb.Event_DELETE || string(ev.PrevKv.Value) != "1" {
		t.Fatalf("delete event = %v", ev)
	}
	if ev := event(t, ch[1], true); ev.Type != mvccpb.Event_PUT || ev.PrevKv != nil || ev.Kv.ModRevision != 3 {
		t.Fatalf("create event = %v", ev)
	}
}

func TestReadOnlyTxnKeepsRevision(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")
	r, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Header.Revision != 2 || s.Rev() != 2 {
		t.Fatalf("read-only txn revision = %d", r.Header.Revision)
	}
}

func TestTxnChecksBeforeWriting(t *testing.T) {
	s := New(&Heap{})
	_, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), IgnoreValue: true}}},
	}})
	if !errors.Is(err, rpctypes.ErrGRPCKeyNotFound) {
		t.Fatalf("ignore value on missing key: %v", err)
	}
	if s.Rev() != 1 || len(get(t, s, &pb.RangeRequest{Key: []byte("a")}).Kvs) != 0 {
		t.Fatal("failed txn wrote a key")
	}
	if _, err := s.Put(&pb.PutRequest{Key: []byte("a"), Lease: 7}); !errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) {
		t.Fatalf("put with unknown lease: %v", err)
	}
}

func TestCheckTxnDuplicateKeys(t *testing.T) {
	putOp := func(k string) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(k)}}}
	}
	delOp := func(k, end string) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte(k), RangeEnd: []byte(end)}}}
	}
	txnOp := func(then, els []*pb.RequestOp) *pb.RequestOp {
		return &pb.RequestOp{Request: &pb.RequestOp_RequestTxn{RequestTxn: &pb.TxnRequest{Success: then, Failure: els}}}
	}
	cases := []struct {
		name string
		ops  []*pb.RequestOp
		dup  bool
	}{
		{"two puts", []*pb.RequestOp{putOp("a"), putOp("a")}, true},
		{"put in deleted range", []*pb.RequestOp{delOp("a", "c"), putOp("b")}, true},
		{"put outside deleted range", []*pb.RequestOp{delOp("a", "c"), putOp("c")}, false},
		{"put in both nested branches", []*pb.RequestOp{txnOp([]*pb.RequestOp{putOp("a")}, []*pb.RequestOp{putOp("a")})}, false},
		{"nested put and outer put", []*pb.RequestOp{txnOp([]*pb.RequestOp{putOp("a")}, nil), putOp("a")}, true},
		{"nested put and outer delete", []*pb.RequestOp{delOp("a", ""), txnOp(nil, []*pb.RequestOp{putOp("a")})}, true},
	}
	for _, c := range cases {
		err := CheckTxn(&pb.TxnRequest{Success: c.ops})
		if got := errors.Is(err, rpctypes.ErrGRPCDuplicateKey); got != c.dup {
			t.Errorf("%s: duplicate = %v (err %v), want %v", c.name, got, err, c.dup)
		}
	}
}

func TestCheckTxnTooManyOps(t *testing.T) {
	ops := make([]*pb.RequestOp, MaxTxnOps+1)
	for i := range ops {
		ops[i] = &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}
	}
	if err := CheckTxn(&pb.TxnRequest{Success: ops}); !errors.Is(err, rpctypes.ErrGRPCTooManyOps) {
		t.Fatalf("too many ops: %v", err)
	}
	if err := CheckTxn(&pb.TxnRequest{Success: ops[:MaxTxnOps]}); err != nil {
		t.Fatalf("max ops: %v", err)
	}
}

func TestHashFollowsState(t *testing.T) {
	a, b := New(&Heap{}), New(&Heap{})
	put(t, a, "k", "1")
	put(t, b, "k", "1")
	if a.Hash() != b.Hash() {
		t.Fatal("equal stores hash differently")
	}
	put(t, b, "k", "2")
	if a.Hash() == b.Hash() {
		t.Fatal("different stores hash the same")
	}
}

func TestEtcdRangeEdges(t *testing.T) {
	s := New(&Heap{})
	s.LeaseExists = func(id int64) bool { return id == 7 }
	if _, err := s.Put(&pb.PutRequest{Key: []byte("a"), Value: []byte("1"), Lease: 7}); err != nil {
		t.Fatal(err)
	}
	r := get(t, s, &pb.RangeRequest{Key: []byte("a"), KeysOnly: true})
	if r.Kvs[0].Lease != 0 || r.Kvs[0].Value != nil || r.Kvs[0].ModRevision != 2 {
		t.Fatalf("keys-only kv = %v, want no lease and no value", r.Kvs[0])
	}
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), KeysOnly: true, SortTarget: pb.RangeRequest_VALUE})
	if r.Kvs[0].Lease != 7 {
		t.Fatalf("keys-only kv sorted by value = %v, want the lease", r.Kvs[0])
	}
	r = get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, Limit: math.MaxInt64})
	if len(r.Kvs) != 1 || r.Count != 1 || r.More {
		t.Fatalf("limit MaxInt64 = %v", r)
	}
	txn, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a"), Revision: -1}}},
	}})
	if err != nil {
		t.Fatalf("txn range at revision -1: %v", err)
	}
	if len(txn.Responses[0].GetResponseRange().Kvs) != 1 {
		t.Fatalf("txn range at revision -1 = %v", txn)
	}
}

func TestHistory(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")                                                                                       // 2
	put(t, s, "b", "1")                                                                                       // 3
	put(t, s, "a", "2")                                                                                       // 4
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte("c")}); err != nil { // 5.0, 5.1
		t.Fatal(err)
	}
	put(t, s, "c", "1") // 6
	var got []string
	for _, c := range s.History([]byte("a"), []byte("c"), 3) {
		ev := event(t, c, true)
		prev := ""
		if ev.PrevKv != nil {
			prev = string(ev.PrevKv.Value)
		}
		got = append(got, ev.Type.String()+" "+string(ev.Kv.Key)+"@"+strconv.FormatInt(ev.Kv.ModRevision, 10)+" prev="+prev)
	}
	want := []string{"PUT b@3 prev=", "PUT a@4 prev=1", "DELETE a@5 prev=2", "DELETE b@5 prev=1"}
	if strings.Join(got, ";") != strings.Join(want, ";") {
		t.Fatalf("history = %v, want %v", got, want)
	}
	if !InRange([]byte("b"), []byte("a"), []byte{0}) || InRange([]byte("c"), []byte("a"), []byte("c")) || !InRange([]byte("a"), []byte("a"), nil) {
		t.Fatal("InRange is wrong")
	}
}

func TestCompact(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "1")                                                                // 2
	put(t, s, "a", "2")                                                                // 3
	put(t, s, "b", "1")                                                                // 4
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("b")}); err != nil { // 5
		t.Fatal(err)
	}
	put(t, s, "c", "1")                                                                // 6
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("c")}); err != nil { // 7
		t.Fatal(err)
	}
	for _, c := range []struct {
		rev  int64
		want error
	}{{8, rpctypes.ErrGRPCFutureRev}, {7, nil}, {7, rpctypes.ErrGRPCCompacted}, {0, rpctypes.ErrGRPCCompacted}} {
		if err := s.Compact(c.rev); !errors.Is(err, c.want) {
			t.Fatalf("compact at %d: %v, want %v", c.rev, err, c.want)
		}
	}
	if s.Rev() != 7 || s.CompactRev() != 7 {
		t.Fatalf("rev %d, compact rev %d", s.Rev(), s.CompactRev())
	}
	if _, err := s.Range(&pb.RangeRequest{Key: []byte("a"), Revision: 6}); !errors.Is(err, rpctypes.ErrGRPCCompacted) {
		t.Fatalf("range below the compaction: %v", err)
	}
	r := get(t, s, &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte{0}, Revision: 7})
	if keys(r.Kvs) != "a=2;" {
		t.Fatalf("range at the compaction = %s", keys(r.Kvs))
	}
	// b's old tombstone is gone with the key; c's tombstone at 7 stays for
	// a watch from 7.
	h := s.History([]byte("a"), []byte{0}, 0)
	if len(h) != 2 || string(h[0].Key()) != "a" || string(h[1].Key()) != "c" || event(t, h[1], false).Type != mvccpb.Event_DELETE {
		t.Fatalf("history after compaction has %d changes", len(h))
	}
	if _, ok := s.keys.Get(&keyIndex{key: []byte("b")}); ok {
		t.Fatal("a key deleted before the compaction is still indexed")
	}
	put(t, s, "a", "3")
	if kv := get(t, s, &pb.RangeRequest{Key: []byte("a")}).Kvs[0]; kv.Version != 3 || kv.CreateRevision != 2 {
		t.Fatalf("a after compaction and put = %v", kv)
	}
}

// swapped reads the value of another Ref of the same length: what a
// wrong position or a damaged disk gives.
type swapped struct {
	Heap
	from, to uint64
}

func (s *swapped) Read(r Ref) ([]byte, error) {
	if r.Index == s.from {
		r.Index = s.to
	}
	return s.Heap.Read(r)
}

// TestValueChecksum reads a value from the wrong place, with the same
// length: every read that would return it fails instead.
func TestValueChecksum(t *testing.T) {
	v := &swapped{}
	s := New(v)
	put(t, s, "a", "11")
	put(t, s, "b", "22")
	get(t, s, &pb.RangeRequest{Key: []byte("a")})
	v.from, v.to = 1, 2
	if _, err := s.Range(&pb.RangeRequest{Key: []byte("a")}); !errors.Is(err, ErrBadValue) {
		t.Fatalf("range of a misplaced value: %v", err)
	}
	if _, err := s.Range(&pb.RangeRequest{Key: []byte("a"), KeysOnly: true}); err != nil {
		t.Fatalf("keys-only range reads no value: %v", err)
	}
	if _, err := s.Put(&pb.PutRequest{Key: []byte("a"), Value: []byte("3"), PrevKv: true}); !errors.Is(err, ErrBadValue) {
		t.Fatalf("put with prev_kv of a misplaced value: %v", err)
	}
	if _, err := s.DeleteRange(&pb.DeleteRangeRequest{Key: []byte("a"), PrevKv: true}); !errors.Is(err, ErrBadValue) {
		t.Fatalf("delete with prev_kv of a misplaced value: %v", err)
	}
	cmp := &pb.Compare{Key: []byte("a"), Target: pb.Compare_VALUE, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_Value{Value: []byte("11")}}
	if _, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{cmp}}); !errors.Is(err, ErrBadValue) {
		t.Fatalf("value compare of a misplaced value: %v", err)
	}
	mod := &pb.Compare{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: 2}}
	if _, err := s.Txn(&pb.TxnRequest{Compare: []*pb.Compare{mod}}); err != nil {
		t.Fatalf("a compare on the revision reads no value: %v", err)
	}
	if s.Rev() != 3 {
		t.Fatalf("failed writes moved the revision to %d", s.Rev())
	}
}

// TestKeyIsCopied changes the bytes a put's key came from: the index
// keeps its own copy.
func TestKeyIsCopied(t *testing.T) {
	s := New(&Heap{})
	key := []byte("a")
	if _, err := s.Put(&pb.PutRequest{Key: key, Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	key[0] = 'z'
	if r := get(t, s, &pb.RangeRequest{Key: []byte("a")}); keys(r.Kvs) != "a=1;" {
		t.Fatalf("after the request's bytes changed: %s", keys(r.Kvs))
	}
}

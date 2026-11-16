package mvcc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

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
// checkpoint, which keeps the sub-revisions of that revision. A walk of
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
	// live says the store compacted itself: its history keeps a slot for
	// each removed change, which is Gone. A loaded store has none.
	check := func(s *Store, live bool) {
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
		if len(got) != len(want) || live != (gone > 0) {
			t.Fatalf("%d changes and %d gone, want %d, and gone ones only in a live store (%v)", len(got), gone, len(want), live)
		}
		for i := range got {
			if got[i].k != want[i].k || got[i].i != want[i].i {
				t.Fatalf("change %d is %s at %d, want %s at %d", i, got[i].Key(), got[i].ModRev(), want[i].Key(), want[i].ModRev())
			}
		}
	}
	check(s, true)
	s.Freeze()
	var b []byte
	for done := false; !done; {
		b, _, done = s.CopyFrozen(b, 1<<20)
	}
	loaded := New(&Heap{})
	if _, err := loaded.Load(bufio.NewReader(bytes.NewReader(b)), s.Rev(), s.CompactRev()); err != nil {
		t.Fatal(err)
	}
	check(loaded, false)

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

// TestRebuildRefusesAHugeSubRevision loads stores with one damaged
// version, as a damaged checkpoint could hold: a sub-revision of 2^40,
// a negative one before a large one, a revision past the store's, and a
// revision far past the others. The history must refuse each as
// malformed rather than fill or span that many slots.
func TestRebuildRefusesAHugeSubRevision(t *testing.T) {
	for _, c := range []struct {
		name   string
		damage func(s *Store, vers []*version)
	}{
		{"huge sub-revision", func(_ *Store, v []*version) { v[1].mod.sub = 1 << 40 }},
		{"negative then large sub-revision", func(_ *Store, v []*version) {
			v[0].mod, v[1].mod = rev{main: 2, sub: -(1 << 40)}, rev{main: 2, sub: 1 << 30}
		}},
		{"revision past the store's", func(s *Store, v []*version) { v[1].mod.main = s.rev + 1 }},
		{"revisions far apart", func(s *Store, v []*version) { s.rev, v[1].mod.main = 1<<40, 1<<40 }},
		{"store revision far past its versions", func(s *Store, _ []*version) { s.rev = 1 << 24 }},
		{"two sub-revisions at the largest int64", func(_ *Store, v []*version) {
			v[0].mod, v[1].mod = rev{main: 2, sub: math.MaxInt64}, rev{main: 2, sub: math.MaxInt64}
		}},
	} {
		s := New(&Heap{})
		put(t, s, "a", "v")
		put(t, s, "b", "v")
		var vers []*version
		s.each(func(k *keyIndex) bool {
			vers = append(vers, &k.vers()[0])
			return true
		})
		c.damage(s, vers)
		if err := s.hist.rebuild(s, 0); !errors.Is(err, ErrBadCheckpoint) {
			t.Errorf("%s: %v, want ErrBadCheckpoint", c.name, err)
		}
	}
}

// TestCheckpointOfAKeyChangedThrice deletes a key, puts it and deletes it
// again in one Txn, compacts at that revision, which keeps only the last
// delete, and loads the store's checkpoint: the load must take it, though
// the one version it keeps has two removed changes before it, and the
// history from that revision must hold the changes the index keeps.
func TestCheckpointOfAKeyChangedThrice(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "v")
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("a"), RangeEnd: []byte{0}}}}
	if _, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		del,
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("again")}}},
		del,
	}}); err != nil {
		t.Fatal(err)
	}
	compact := s.Rev()
	if err := s.Compact(compact); err != nil {
		t.Fatal(err)
	}
	check := func(s *Store) {
		t.Helper()
		want := s.History([]byte{0}, []byte{0}, compact)
		var got []Change
		for pos := s.HistoryFrom(compact); pos < s.HistoryEnd(); pos++ {
			if c := s.Changed(pos); !c.Gone() {
				got = append(got, c)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("%d changes, want %d", len(got), len(want))
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
		t.Fatalf("the store's own checkpoint does not load: %v", err)
	}
	check(loaded)
}

// TestLoadRefusesRevisionsWithoutRoom loads empty checkpoints at the
// largest int64 revision and at 2^62, too near it for the writes after the
// load, ones compacted below -1, a store's "never", which lets a later
// compaction wrap, and one at revision 5 never compacted, which would
// hold versions: all are malformed. An empty store just below 2^62 and
// compacted there, and a new one at revision 1, never compacted, load.
func TestLoadRefusesRevisionsWithoutRoom(t *testing.T) {
	empty := []byte{0}
	for _, c := range []struct {
		rev, compact int64
		ok           bool
	}{
		{math.MaxInt64, 0, false},
		{maxLoadRev, 0, false},
		{maxLoadRev - 1, maxLoadRev - 1, true},
		{5, -2, false},
		{5, math.MinInt64, false},
		{1, -1, true},
		{5, -1, false},
	} {
		_, err := New(&Heap{}).Load(bufio.NewReader(bytes.NewReader(empty)), c.rev, c.compact)
		if c.ok != (err == nil) || (err != nil && !errors.Is(err, ErrBadCheckpoint)) {
			t.Errorf("revision %d compacted at %d: %v, want ok %v", c.rev, c.compact, err, c.ok)
		}
	}
}

// TestReadBeforeAKeyChangedTwice puts a key and deletes it again in one
// Txn at revision R: a read at R-1 must not see it, since it did not
// exist then, and a read at R must not either.
func TestReadBeforeAKeyChangedTwice(t *testing.T) {
	s := New(&Heap{})
	put(t, s, "a", "v")
	if _, err := s.Txn(&pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("b"), Value: []byte("new")}}},
		{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("b"), RangeEnd: []byte{0}}}},
	}}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []int64{s.Rev() - 1, s.Rev()} {
		resp, err := s.Range(&pb.RangeRequest{Key: []byte("b"), Revision: r})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) != 0 {
			t.Errorf("a read at %d sees %q, which did not exist then", r, resp.Kvs[0].Value)
		}
	}
}

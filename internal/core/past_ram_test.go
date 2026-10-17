package core

import (
	"errors"
	"maps"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// Cache references must include the Raft index. The same offset and
// length in a later overwrite must not find the earlier value.
func TestCacheReferenceIdentity(t *testing.T) {
	c := newCache()
	c.setLimit(4096)
	old := mvcc.Ref{Index: 10, Off: 20, Len: 3}
	current := mvcc.Ref{Index: 11, Off: 20, Len: 3}
	c.add(old, []byte("old"))
	if value, ok := c.get(current); ok {
		t.Fatalf("stale cache entry after overwrite: %q", value)
	}
	c.add(current, []byte("new"))
	if value, ok := c.get(current); !ok || string(value) != "new" {
		t.Fatalf("current value: %q, found %v", value, ok)
	}
}

// A delayed read must not cache its old value as the current value after
// an overwrite. Complete helpers in the opposite order from submission.
func TestColdReadsCompleteInReverse(t *testing.T) {
	s, _ := coldSingle(t, func(c *Config) { c.MemoryBytes = 1 << 20 })
	s.n.cache.clear()
	s.hold = true
	for id, key := range []string{"a", "b"} {
		s.n.Submit(Request{ID: uint64(700 + id), Op: &pb.RangeRequest{Key: []byte(key)}})
	}
	if got := replies(s); len(got) != 0 || len(s.held) != 2 {
		t.Fatalf("before completion: %d replies, %d helpers", len(got), len(s.held))
	}
	s.ok(702, &pb.PutRequest{Key: []byte("a"), Value: []byte("new")})
	s.ok(703, &pb.CompactionRequest{Revision: s.n.kv.Rev()})
	got := map[uint64]Reply{}
	for _, r := range slices.Backward(s.held) {
		s.n.ValuesRead(r, r.Run())
		maps.Copy(got, replies(s))
	}
	s.held, s.hold = nil, false
	for id, want := range map[uint64]string{700: "va", 701: "vb"} {
		r, ok := got[id]
		if !ok || r.Err != nil {
			t.Fatalf("reply %d: %+v", id, r)
		}
		kvs := r.Resp.(*pb.RangeResponse).Kvs
		if len(kvs) != 1 || string(kvs[0].Value) != want {
			t.Fatalf("reply %d: %v, want %s", id, kvs, want)
		}
	}
	if value, _ := s.get(704, "a", 0); value != "new" {
		t.Fatalf("late cache fill returned %q, want new", value)
	}
}

// EIO must reach the core as an error. It may not turn into missing or
// empty bytes, nor may the read receive a successful response.
func TestColdReadEIO(t *testing.T) {
	s, disk := coldSingle(t, nil)
	s.hold = true
	s.n.Submit(Request{ID: 700, Op: &pb.RangeRequest{Key: []byte("a")}})
	if got := replies(s); len(got) != 0 || len(s.held) != 1 {
		t.Fatalf("before EIO: %d replies, %d helpers", len(got), len(s.held))
	}
	reads := 0
	disk.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpRead {
			reads++
			return simfs.ErrIO
		}
		return nil
	}
	r := s.held[0]
	err := r.Run()
	disk.Fault = nil
	if reads == 0 || !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("%d reads, error %v", reads, err)
	}
	s.n.ValuesRead(r, err)
	eff, err := s.n.Process()
	if !errors.Is(err, simfs.ErrIO) {
		t.Fatalf("core stopped with %v, want EIO", err)
	}
	for _, reply := range eff.Replies {
		if reply.ID == 700 && reply.Err == nil {
			t.Fatal("EIO returned a successful value response")
		}
	}
}

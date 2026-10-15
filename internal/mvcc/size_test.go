package mvcc

import (
	"bufio"
	"bytes"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

// etcdRows is what etcd v3.7.2 writes for the versions a store holds,
// those a compaction held back by a checkpoint copy left out: per version
// a 17-byte revision key, a tombstone mark, and the marshaled KeyValue.
func etcdRows(s *Store) int64 {
	var n int64
	s.keys.Ascend(func(k *keyIndex) bool {
		for i := k.dropped(s.pendingCompact()); i < len(k.vers); i++ {
			v := &k.vers[i]
			kv := &mvccpb.KeyValue{Key: k.key}
			n += 17
			if v.ver == 0 {
				n++
			} else {
				kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease = v.create, v.mod.main, v.ver, v.lease
				kv.Value = make([]byte, v.value.Len)
			}
			n += int64(proto.Size(kv))
		}
		return true
	})
	return n
}

func TestSizeCountsEtcdRows(t *testing.T) {
	s := New(&Heap{})
	s.LeaseExists = func(int64) bool { return true }
	check := func(when string) {
		t.Helper()
		if got, want := s.Size(), etcdRows(s); got != want {
			t.Fatalf("%s: Size %d, etcd's rows %d", when, got, want)
		}
	}
	check("empty")
	put(t, s, "a", "1")
	if _, err := s.Put(&pb.PutRequest{Key: []byte("b"), Value: bytes.Repeat([]byte("x"), 300), Lease: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	put(t, s, "a", "")
	del(t, s, "b")
	put(t, s, "c", "3")
	check("after writes")
	s.Freeze()
	if err := s.Compact(4); err != nil {
		t.Fatal(err)
	}
	check("after a compaction a checkpoint copy holds back")
	var buf bytes.Buffer
	for done := false; !done; {
		var b []byte
		b, _, done = s.CopyFrozen(nil, 1<<20)
		buf.Write(b)
	}
	check("after the copy")
	if err := s.Compact(6); err != nil {
		t.Fatal(err)
	}
	check("after a compaction at the newest revision")
	l := New(&Heap{})
	if _, err := l.Load(bufio.NewReader(&buf), 6, 4); err != nil {
		t.Fatal(err)
	}
	if l.Size() != etcdRows(l) {
		t.Fatalf("loaded: Size %d, etcd's rows %d", l.Size(), etcdRows(l))
	}
}

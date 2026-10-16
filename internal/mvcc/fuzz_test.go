package mvcc

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// FuzzLoad loads any bytes as the keys of a checkpoint at revision rev,
// compacted at compact. Load must refuse them or load them without a
// crash, and a store it loaded must answer a range over every key.
func FuzzLoad(f *testing.F) {
	s := New(&Heap{})
	for i := range 40 {
		key := fmt.Appendf(nil, "k%02d", i%13)
		var err error
		if i%7 == 6 {
			_, err = s.DeleteRange(&pb.DeleteRangeRequest{Key: key})
		} else {
			_, err = s.Put(&pb.PutRequest{Key: key, Value: bytes.Repeat([]byte{byte(i)}, i)})
		}
		if err != nil {
			f.Fatal(err)
		}
	}
	if err := s.Compact(s.Rev() - 10); err != nil {
		f.Fatal(err)
	}
	s.Freeze()
	b, _, done := s.CopyFrozen(nil, 1<<20)
	if !done {
		f.Fatal("the copy is not done")
	}
	f.Add(b, s.Rev(), s.CompactRev())
	f.Fuzz(func(t *testing.T, b []byte, rev, compact int64) {
		s := New(&Heap{})
		if _, err := s.Load(bufio.NewReader(bytes.NewReader(b)), rev, compact); err != nil {
			return
		}
		_, _ = s.Range(&pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}})
	})
}

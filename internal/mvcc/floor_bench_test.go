package mvcc

import (
	"github.com/djosh34/tigerstore/internal/floor"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"testing"
)

type floorValues struct{ n uint64 }

func (v *floorValues) Place(p []byte) (Ref, error) {
	v.n++
	return Ref{Index: v.n, Len: uint32(len(p))}, nil
}
func (v *floorValues) Read(Ref, *[]byte) ([]byte, bool, error) { panic("unexpected read") }
func BenchmarkFloorApply(b *testing.B) {
	for _, txn := range []bool{false, true} {
		name := "put"
		if txn {
			name = "txn"
		}
		b.Run(name, func(b *testing.B) {
			puts := make([]PutOp, 10000)
			ts := make([]*pb.TxnRequest, 10000)
			revs := make([]int64, 10000)
			for i := range puts {
				p := floor.Put(i)
				puts[i] = PutOp{Key: p.Key, Value: p.Value}
				ts[i] = floor.Txn(i, 1)
			}
			var s *Store
			setup := func() {
				s = New(&floorValues{})
				for i := range puts {
					_, err := s.PutKV(&puts[i])
					if err != nil {
						b.Fatal(err)
					}
					revs[i] = s.Rev()
				}
			}
			setup()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i > 0 && i%65536 == 0 {
					b.StopTimer()
					setup()
					b.StartTimer()
				}
				k := (i * 7919) % 10000
				if txn {
					ts[k].Compare[0].TargetUnion.(*pb.Compare_ModRevision).ModRevision = revs[k]
					r, err := s.Txn(ts[k])
					if err != nil || !r.Succeeded {
						b.Fatalf("txn: %v %v", r, err)
					}
				} else {
					if _, err := s.PutKV(&puts[k]); err != nil {
						b.Fatal(err)
					}
				}
				revs[k] = s.Rev()
			}
		})
	}
}
func BenchmarkFloorIndex(b *testing.B) {
	s := New(&floorValues{})
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = floor.Key(i)
		s.keys.ReplaceOrInsert(&keyIndex{key: keys[i], vers: []version{{mod: rev{main: 1}, ver: 1}}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k, ok := s.get(keys[(i*7919)%10000])
		if !ok {
			b.Fatal("missing")
		}
		k.vers[0].mod.main++
		s.rev++
	}
}

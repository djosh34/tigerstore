package mvcc

import (
	"errors"
	"math/rand/v2"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestCheckFlatMatchesIntervals checks that the check of a branch without
// nested transactions fails exactly where checkIntervals does.
func TestCheckFlatMatchesIntervals(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	key := func() []byte { return []byte{byte('a' + r.IntN(4))} }
	for range 20000 {
		var ops []*pb.RequestOp
		for range r.IntN(5) {
			switch r.IntN(3) {
			case 0:
				ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key()}}})
			case 1:
				d := &pb.DeleteRangeRequest{Key: key()}
				if r.IntN(2) == 0 {
					d.RangeEnd = key()
				}
				ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: d}})
			default:
				ops = append(ops, &pb.RequestOp{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key()}}})
			}
		}
		_, _, want := checkIntervals(ops)
		if got := checkFlat(ops); (got == nil) != (want == nil) || !errors.Is(got, want) {
			t.Fatalf("%v: checkFlat %v, checkIntervals %v", ops, got, want)
		}
	}
}

//go:build difftest

package difftest

import (
	"bytes"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestSortByValue runs the replies that #721's second ruling lets
// tigerstore refuse where etcd answers (deliberate), next to the same
// ranges that it answers as etcd does. Its 40 values of 1.4 MiB, 56 MiB
// in all, are past one piece of a reply at tigerstore's default budget
// (a connection's share of 48 MiB) and past one message (2 MiB).
func TestSortByValue(t *testing.T) {
	runCases(t, "etcd-v3.7.2-sortbyvalue.json", []testCase{{"sort-by-value", sortByValue}})
}

func sortByValue(r *run) {
	const keys, size = 40, 1400 << 10
	key := func(i int) []byte { return r.key(fmt.Sprintf("v/%02d", i)) }
	for i := range keys {
		// Values in the reverse order of the keys.
		v := append([]byte(fmt.Sprintf("%02d", keys-i)), bytes.Repeat([]byte("v"), size-2)...)
		r.put(fmt.Sprintf("put %02d", i), &pb.PutRequest{Key: key(i), Value: v})
	}
	all := func() *pb.RangeRequest { return &pb.RangeRequest{Key: r.key("v/"), RangeEnd: r.key("v0")} }
	few := all()
	few.RangeEnd, few.SortTarget = key(4), pb.RangeRequest_VALUE
	r.get("sorted by value, within one piece", few)
	byValue := all()
	byValue.SortTarget, byValue.SortOrder = pb.RangeRequest_VALUE, pb.RangeRequest_DESCEND
	r.get("sorted by value, past one piece", byValue)
	byMod := all()
	byMod.SortTarget, byMod.SortOrder = pb.RangeRequest_MOD, pb.RangeRequest_DESCEND
	r.get("sorted by mod revision, in pieces", byMod)
	r.txn("txn whose range passes one message", &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestRange{RequestRange: all()}},
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: r.key("w"), Value: []byte("applied")}}},
	}})
	r.get("the txn's put applied", &pb.RangeRequest{Key: r.key("w")})
	r.del("delete with prev_kv past one message", &pb.DeleteRangeRequest{Key: r.key("v/"), RangeEnd: r.key("v0"), PrevKv: true})
	r.get("the delete applied", &pb.RangeRequest{Key: r.key("v/"), RangeEnd: r.key("v0"), CountOnly: true})
}

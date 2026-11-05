package server

import (
	"fmt"
	"github.com/djosh34/tigerstore/internal/k8ssize"
	"github.com/djosh34/tigerstore/internal/mvcc"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
	"math/rand/v2"
	"testing"
)

// BenchmarkResearch484Encoding measures production encoders, allocating as
// they do in service. LIST pages model the first page in 1M keys / 1020
// groups, weighted by resource object counts. It is a CPU microbenchmark,
// not a cluster or storage benchmark; the group occupancy is approximate.
func BenchmarkResearch484Encoding(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	resources := k8ssize.Resources()
	var pages []mvcc.RangeResult
	var schedule []int
	for ri, r := range resources {
		count := 0
		for _, x := range k8ssize.Buckets {
			if x.Resource == r {
				count += x.Count
			}
		}
		n := min(500, 1+int(999999*float64(count)/6345/1020))
		page := mvcc.RangeResult{Rev: 100000, Count: int64(n)}
		for i := range n {
			kv := mvcc.KV{Key: []byte(fmt.Sprintf("/registry/%s/bench-000-0000/object-%012d", r, i)), Value: make([]byte, k8ssize.Buckets[k8ssize.Draw(rng, r)].Size), CreateRevision: 10000, ModRevision: 100000, Version: 3}
			page.KVs = append(page.KVs, kv)
			page.Bytes += int64(len(kv.Key) + len(kv.Value))
		}
		pages = append(pages, page)
		for range count {
			schedule = append(schedule, ri)
		}
	}
	rng.Shuffle(len(schedule), func(i, j int) { schedule[i], schedule[j] = schedule[j], schedule[i] })
	b.Run("LISTWeighted", func(b *testing.B) {
		var bytes int64
		for _, i := range schedule {
			p, _ := appendRangeReply(12345678, 12345678, 2, &pages[i])
			bytes += int64(len(p))
		}
		b.SetBytes(bytes / int64(len(schedule)))
		b.ReportAllocs()
		at := 0
		b.ResetTimer()
		for b.Loop() {
			if _, err := appendRangeReply(12345678, 12345678, 2, &pages[schedule[at%len(schedule)]]); err != nil {
				b.Fatal(err)
			}
			at++
		}
	})
	k := &mvccpb.KeyValue{Key: []byte("/registry/events/bench-000-0000/object-000000000000"), Value: make([]byte, 1458), CreateRevision: 10000, ModRevision: 100000, Version: 3}
	for _, op := range []string{"get", "update", "create", "delete"} {
		var reply proto.Message
		h := &pb.ResponseHeader{Revision: 100000, RaftTerm: 2, ClusterId: 12345678, MemberId: 12345678}
		switch op {
		case "get":
			reply = &pb.RangeResponse{Header: h, Count: 1, Kvs: []*mvccpb.KeyValue{k}}
		case "delete":
			reply = &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: &pb.DeleteRangeResponse{Header: h, Deleted: 1}}}}}
		default:
			reply = &pb.TxnResponse{Header: h, Succeeded: true, Responses: []*pb.ResponseOp{{Response: &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: h}}}}}
		}
		b.Run("Reply/"+op, func(b *testing.B) {
			b.SetBytes(int64(proto.Size(reply)))
			b.ReportAllocs()
			for b.Loop() {
				if t, ok := reply.(*pb.TxnResponse); ok {
					if _, err := appendTxnReply(t); err != nil {
						b.Fatal(err)
					}
				} else {
					if _, err := marshal(reply); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
		if op == "get" {
			continue
		}
		ev := &mvccpb.Event{Kv: k, PrevKv: k}
		if op == "create" {
			ev.PrevKv = nil
		}
		if op == "delete" {
			ev.Type = mvccpb.Event_DELETE
			ev.Kv = &mvccpb.KeyValue{Key: k.Key, ModRevision: 100000}
		}
		watch := &pb.WatchResponse{Header: h, Events: []*mvccpb.Event{ev}}
		b.Run("Watch/"+op, func(b *testing.B) {
			b.SetBytes(int64(proto.Size(watch)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := marshal(watch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

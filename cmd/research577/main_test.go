package main

import (
	"bytes"
	"math"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
)

func TestPrimitiveFixtures(t *testing.T) {
	fs, _ := fixtures()
	var weight float64
	for _, f := range fs {
		weight += f.Weight
		if f.Write != 0 {
			var req pb.TxnRequest
			if err := proto.Unmarshal(f.Request, &req); err != nil {
				t.Fatal(err)
			}
			if len(req.Compare) != 1 || len(req.Success) != 1 {
				t.Fatal("bad transaction")
			}
			_ = parse(f.Request, 2)
		} else {
			var req pb.RangeRequest
			if err := proto.Unmarshal(f.Request, &req); err != nil {
				t.Fatal(err)
			}
			_ = parse(f.Request, 0)
		}
		for _, k := range append(append([]*mvccpb.KeyValue{}, f.KVs...), f.WatchKVs...) {
			got := encodeKV(nil, k)
			want, err := proto.Marshal(k)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("KV encoder differs: %s", f.Resource)
			}
		}
	}
	if math.Abs(weight-1) > 1e-9 {
		t.Fatalf("weights %g", weight)
	}
}

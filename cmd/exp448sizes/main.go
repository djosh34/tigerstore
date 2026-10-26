package main

import (
	"fmt"
	"github.com/djosh34/tigerstore/history"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"unsafe"
)

func main() {
	fmt.Printf("pb.Event=%d pb.KV=%d pb.WatchResponse=%d pb.RangeRequest=%d pb.RangeResponse=%d pb.TxnRequest=%d pb.TxnResponse=%d pb.PutRequest=%d pb.Compare=%d history.Op=%d history.Event=%d history.KV=%d\n", unsafe.Sizeof(mvccpb.Event{}), unsafe.Sizeof(mvccpb.KeyValue{}), unsafe.Sizeof(pb.WatchResponse{}), unsafe.Sizeof(pb.RangeRequest{}), unsafe.Sizeof(pb.RangeResponse{}), unsafe.Sizeof(pb.TxnRequest{}), unsafe.Sizeof(pb.TxnResponse{}), unsafe.Sizeof(pb.PutRequest{}), unsafe.Sizeof(pb.Compare{}), unsafe.Sizeof(history.Op{}), unsafe.Sizeof(history.Event{}), unsafe.Sizeof(history.KeyValue{}))
}

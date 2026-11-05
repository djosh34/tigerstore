//go:build exp485stub

package stack

import (
	"context"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/node"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// One event from the production node's outbound watch queue to a clientv3
// watcher; no Put, no Raft work, and watch establishment is outside the timer.
func BenchmarkWatchEvent1KiB(b *testing.B) {
	cli, _ := newClient(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	wc := cli.Watch(ctx, "key", clientv3.WithCreatedNotify())
	var ws *node.WatchStream
	select {
	case ws = <-node.Exp485Watch:
	case <-ctx.Done():
		b.Fatal(ctx.Err())
	}
	select {
	case r := <-wc:
		if !r.Created {
			b.Fatal("not created")
		}
	case <-ctx.Done():
		b.Fatal(ctx.Err())
	}
	event := &pb.WatchResponse{Header: &pb.ResponseHeader{ClusterId: 1, MemberId: 1, Revision: 3, RaftTerm: 1}, WatchId: 0, Events: []*mvccpb.Event{{Type: mvccpb.Event_PUT, Kv: &mvccpb.KeyValue{Key: []byte("key"), Value: make([]byte, 1024), CreateRevision: 2, ModRevision: 3, Version: 2}}}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		ws.Exp485Deliver(event)
		select {
		case r := <-wc:
			if r.Err() != nil || len(r.Events) != 1 || len(r.Events[0].Kv.Value) != 1024 {
				b.Fatalf("bad event: %+v", r)
			}
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		}
	}
}

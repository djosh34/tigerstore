package batchfront

import (
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/node"
)

func TestSuccessfulChannelSendCounters(t *testing.T) {
	c, _ := testConn(t)
	c.control = make(chan control, 1)
	c.sendControl(control{id: 1, open: true})
	c.sendControl(control{id: 3, open: true})
	if got := c.s.Stats().ControlSends; got != 1 {
		t.Fatalf("successful control sends = %d", got)
	}

	c, _ = testConn(t)
	c.out = make(chan []encoded, 1)
	if !c.start(1) || !c.start(3) {
		t.Fatal("start")
	}
	c.Complete([]node.Completion{{Tag: 1, Resp: &pb.PutResponse{}}})
	c.Complete([]node.Completion{{Tag: 3, Resp: &pb.PutResponse{}}})
	if got := c.s.Stats().ResponseSends; got != 1 {
		t.Fatalf("successful response sends = %d", got)
	}
}

func TestHTTPHealthDoesNotWaitBeforeVersion(t *testing.T) {
	c, _ := testConn(t)
	c.s.n = startNode(t)
	if !c.start(1) || !c.start(3) {
		t.Fatal("start")
	}
	var a admission
	if err := c.dispatch(1, &incoming{method: "GET", path: "/health"}, &a); err != nil {
		t.Fatal(err)
	}
	if len(a.ops) != 1 || len(a.legacyTags) != 1 {
		t.Fatal("health did not use grouped admission")
	}
	if err := c.dispatch(3, &incoming{method: "GET", path: "/version"}, &a); err != nil {
		t.Fatal(err)
	}
	batch := <-c.out
	if len(batch) != 1 || batch[0].id != 3 {
		t.Fatal("version waited for health")
	}
	if err := c.flush(&a); err != nil {
		t.Fatal(err)
	}
	select {
	case batch = <-c.out:
		if len(batch) != 1 || batch[0].id != 1 {
			t.Fatal("health completion")
		}
	case <-time.After(time.Second):
		t.Fatal("health completion timed out")
	}
}

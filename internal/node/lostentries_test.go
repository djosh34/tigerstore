package node

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestLostEntriesStopNode sends a follower a heartbeat from its leader
// whose commit index is past the end of the follower's log, as a leader
// sends one to a follower whose log lost entries it acknowledged. Raft
// panics on it, which crashed the process; the node must stop with
// core.ErrLostEntries instead, and the other nodes go on (#493).
func TestLostEntriesStopNode(t *testing.T) {
	c, err := startThree(t, nil)
	defer c.close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := do(c.nodes[1], &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	leader := c.nodes[1].Leader()
	f := uint64(1)
	if f == leader {
		f = 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := c.nodes[f].Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[f].Deliver(&raftpb.Message{
		Type:   raftpb.MsgHeartbeat.Enum(),
		From:   new(leader),
		To:     new(f),
		Term:   new(st.RaftTerm),
		Commit: new(st.RaftIndex + 1000),
	})
	select {
	case err := <-c.ran[f]:
		if !errors.Is(err, core.ErrLostEntries) {
			t.Fatalf("node %d stopped with %v, want core.ErrLostEntries", f, err)
		}
		t.Logf("node %d stopped: %v", f, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("node %d still runs 10 s after a heartbeat past the end of its log", f)
	}
	c.net.leave(f)
	c.stops[f]()
	delete(c.stops, f)
	if _, err := do(c.nodes[leader], &pb.PutRequest{Key: []byte("k"), Value: []byte("w")}); err != nil {
		t.Fatalf("the other nodes no longer write: %v", err)
	}
}

package server_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestMoveLeaderAndDefragment checks MoveLeader as etcd v3.7.2 answers
// it: only on the leader, only to a voting member, and once the
// transferee leads; and that Defragment answers without a header.
func TestMoveLeaderAndDefragment(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 79)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	leader := c.nodes[1].Leader()
	target := leader%3 + 1
	other := target%3 + 1
	ctx := context.Background()
	m := func(id uint64) pb.MaintenanceClient { return pb.NewMaintenanceClient(c.dial(id, cert)) }
	if _, err := m(other).MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: target}); !errors.Is(err, rpctypes.ErrGRPCNotLeader) {
		t.Fatalf("MoveLeader on a follower: %v", err)
	}
	if _, err := m(leader).MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: 9}); !errors.Is(err, rpctypes.ErrGRPCBadLeaderTransferee) {
		t.Fatalf("MoveLeader to a stranger: %v", err)
	}
	resp, err := m(leader).MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: target})
	if err != nil || resp.Header != nil {
		t.Fatalf("MoveLeader: %v %v", resp, err)
	}
	// The node answers once Raft has the transferee lead; the leader it
	// reports follows when the driver takes that in.
	for deadline := time.Now().Add(time.Second); c.nodes[leader].Leader() != target; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("node %d sees %d lead after the move to %d", leader, c.nodes[leader].Leader(), target)
		}
	}
	// The leader leads itself already.
	if _, err := m(target).MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: target}); err != nil {
		t.Fatalf("MoveLeader to the leader: %v", err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		r, err := c.client(leader, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")})
		if err == nil && c.nodes[leader].Leader() != target {
			err = fmt.Errorf("leader %d", c.nodes[leader].Leader())
		}
		return r, err
	})
	d, err := m(other).Defragment(ctx, &pb.DefragmentRequest{})
	if err != nil || d.Header != nil {
		t.Fatalf("Defragment: %v %v", d, err)
	}
}

// TestMoveLeaderWaitsForTheCaller checks that MoveLeader to a member that
// cannot take the leadership waits as long as the caller does, as in etcd
// v3.7.2, not only for the node's request timeout, here 1 s.
func TestMoveLeaderWaitsForTheCaller(t *testing.T) {
	c := newClusterWith(t, func(c *cluster) {
		c.coreConfig = func(cfg *core.Config) { cfg.RequestTicks = int(time.Second / c.tick) }
	})
	cert, _, _, err := c.ca.Issue("client", 83)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	leader := c.nodes[1].Leader()
	target := leader%3 + 1
	c.running[target].stop()
	delete(c.running, target)
	const wait = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	start := time.Now()
	_, err = pb.NewMaintenanceClient(c.dial(leader, cert)).MoveLeader(ctx, &pb.MoveLeaderRequest{TargetID: target})
	if took := time.Since(start); status.Code(err) != codes.DeadlineExceeded || took < wait-100*time.Millisecond {
		t.Fatalf("MoveLeader to a stopped member answered %v after %v, want the caller's deadline after %v", err, took, wait)
	}
	if got := c.nodes[leader].Leader(); got != leader {
		t.Fatalf("node %d sees %d lead, want itself", leader, got)
	}
}

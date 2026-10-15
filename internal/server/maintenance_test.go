package server_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
	for id := uint64(1); id <= 3; id++ {
		if l := c.nodes[id].Leader(); id == leader && l != target {
			t.Fatalf("node %d sees %d lead after the move to %d", id, l, target)
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

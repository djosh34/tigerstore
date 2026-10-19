package server_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

// The tests in this file check the guards of etcd v3.7.2 around Raft
// that #232 lists and the server copies.

// TestLeaderlessNode stops two of three nodes, so that node 2 loses its
// leader. A watch that requires a leader ends after three election
// timeouts without one, as etcd's monitorLeader checks every election
// timeout (row 40). A write whose client gives up waiting is dropped at
// once, as etcd's raft.Node drops a proposal whose context ends while no
// leader is known (row 32).
func TestLeaderlessNode(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(2, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("1")})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ws, err := pb.NewWatchClient(c.dial(2, cert)).Watch(metadata.AppendToOutgoingContext(ctx, rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("k")}}}); err != nil {
		t.Fatal(err)
	}
	if r, err := ws.Recv(); err != nil || !r.Created {
		t.Fatalf("watch: %v, %v", r, err)
	}
	for _, id := range []uint64{1, 3} {
		c.running[id].stop()
		delete(c.running, id)
	}
	stopped := time.Now()
	for {
		_, err := ws.Recv()
		if err == nil {
			continue
		}
		if !errors.Is(rpctypes.Error(err), rpctypes.ErrNoLeader) {
			t.Fatalf("the watch ended with %v", err)
		}
		break
	}
	// Losing the leader takes up to two election timeouts, and the watch
	// ends after three more: 0.5 s at 10 ms ticks.
	if took := time.Since(stopped); took > 2*time.Second {
		t.Fatalf("the watch ended %v after the leader was lost", took)
	}

	short, cancelShort := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelShort()
	if _, err := kv.Put(short, &pb.PutRequest{Key: []byte("k"), Value: []byte("2")}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a write without a leader: %v", err)
	}
	// The write's own deadline at the node is 7 s.
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		m, err := c.nodes[2].Metrics(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if m.ProposalsPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d writes still wait 2 s after their client went away", m.ProposalsPending)
		}
	}
}

// TestCancelDuringBlockedAdmission holds the committer of a node without
// a leader and fills its inbox, so the server's batch waits to go in. The
// write's grpc-timeout passes meanwhile; once the node takes the batch,
// the server still cancels the write, and the node drops it (row 32).
func TestCancelDuringBlockedAdmission(t *testing.T) {
	var hold atomic.Bool
	held, release := make(chan struct{}, 1), make(chan struct{})
	c := newClusterWith(t, func(c *cluster) {
		c.nodeConfig = func(cfg *node.Config) {
			if cfg.Core.ID != 2 {
				return
			}
			cfg.Turn = func(*core.Node, core.Effects, error) {
				if hold.CompareAndSwap(true, false) {
					held <- struct{}{}
					<-release
				}
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(2, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("1")})
	})
	for _, id := range []uint64{1, 3} {
		c.running[id].stop()
		delete(c.running, id)
	}
	n := c.nodes[2]
	for deadline := time.Now().Add(5 * time.Second); n.Leader() != 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("node 2 keeps a leader")
		}
	}
	hold.Store(true)
	n.Unreachable(1)
	<-held
	// Unreachable drops what a full inbox has no room for, so more than
	// the inbox holds fills it.
	for range 20000 {
		n.Unreachable(1)
	}
	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := kv.Put(short, &pb.PutRequest{Key: []byte("k"), Value: []byte("2")}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a write to a held node: %v", err)
	}
	close(release)
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		m, err := n.Metrics(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if m.ProposalsPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d writes still wait after their call ended", m.ProposalsPending)
		}
	}
}

package server_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
)

// lyingPeer is the transport of node self that hears node liar's HashKV
// answers with another hash, as if liar's data were damaged: liar itself
// hears every other node's that way.
type lyingPeer struct {
	peer.Transport
	self, liar uint64
}

func (l lyingPeer) Call(ctx context.Context, id uint64, req []byte) ([]byte, error) {
	b, err := l.Transport.Call(ctx, id, req)
	if err != nil || (id != l.liar && l.self != l.liar) {
		return b, err
	}
	var resp pb.HashKVResponse
	if err := proto.Unmarshal(b, &resp); err != nil {
		return nil, err
	}
	resp.Hash++
	return proto.Marshal(&resp)
}

// corruptCluster runs a cluster whose nodes hear node 3's hashes wrong,
// with change applied to the nodes' configuration.
func corruptCluster(t *testing.T, change func(*node.Config)) *cluster {
	return newClusterWith(t, func(c *cluster) {
		c.nodeConfig = change
		c.wrap = func(id uint64, t peer.Transport) peer.Transport { return lyingPeer{Transport: t, self: id, liar: 3} }
	})
}

// waitCorrupt waits for a CORRUPT alarm and checks that it is the only
// one and is for member want.
func waitCorrupt(t *testing.T, m pb.MaintenanceClient, want ...uint64) {
	t.Helper()
	got := retry(t, func(ctx context.Context) ([]*pb.AlarmMember, error) {
		resp, err := m.Alarm(ctx, &pb.AlarmRequest{Alarm: pb.AlarmType_CORRUPT})
		if err == nil && len(resp.Alarms) == 0 {
			err = errors.New("no CORRUPT alarm yet")
		}
		return resp.GetAlarms(), err
	})
	if len(got) != 1 || !slices.Contains(want, got[0].MemberID) {
		t.Fatalf("CORRUPT alarms: %v, want one for one of %v", got, want)
	}
}

// TestPeriodicCorruptCheck runs etcd's periodic check every 100 ms: the
// leader finds node 3's hash wrong and raises CORRUPT for it, which then
// refuses writes. Like etcd's, the check trusts the leader: with node 3
// leading, the first follower it checks gets the alarm.
func TestPeriodicCorruptCheck(t *testing.T) {
	c := corruptCluster(t, func(cfg *node.Config) { cfg.CorruptCheckTime = 100 * time.Millisecond })
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	want := []uint64{3}
	if c.nodes[1].Leader() == 3 {
		want = []uint64{1, 2}
	}
	waitCorrupt(t, pb.NewMaintenanceClient(c.dial(1, cert)), want...)
	if _, err := c.client(2, cert).Put(context.Background(), &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}); err == nil {
		t.Fatal("a put went through under CORRUPT")
	}
}

// TestCompactHashCheck turns etcd's compaction hash check on: after a
// compaction, node 3's hash of it disagrees with the other two, a
// majority, so it gets the CORRUPT alarm, whichever node leads.
func TestCompactHashCheck(t *testing.T) {
	c := corruptCluster(t, func(cfg *node.Config) { cfg.CompactHashCheckTime = 100 * time.Millisecond })
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	var rev int64
	for _, v := range []string{"1", "2", "3"} {
		rev = retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte(v)})
		}).Header.Revision
	}
	if _, err := kv.Compact(context.Background(), &pb.CompactionRequest{Revision: rev - 1}); err != nil {
		t.Fatal(err)
	}
	waitCorrupt(t, pb.NewMaintenanceClient(c.dial(1, cert)), 3)
}

// TestInitialCorruptCheck runs etcd's initial check: it passes against
// honest peers and fails when a peer's hash of the same revision differs.
func TestInitialCorruptCheck(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	// Every member must have applied the put, or their revisions differ.
	time.Sleep(200 * time.Millisecond)
	n := c.nodes[1]
	if err := n.InitialCheck(context.Background(), c.transports[1]); err != nil {
		t.Fatalf("against honest peers: %v", err)
	}
	err = n.InitialCheck(context.Background(), lyingPeer{Transport: c.transports[1], self: 1, liar: 2})
	if err == nil || !slices.Contains([]string{"node: 1 found data inconsistency with peers"}, err.Error()) {
		t.Fatalf("against a peer with another hash: %v", err)
	}
}

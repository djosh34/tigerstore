// Question (#291): can gosim run the real tigerstore driver, retaining simfs,
// deterministically? This is the smallest driver gate before attempting a cluster.
package gosim291_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/simfs"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

// Single-member gate: no TCP yet. Do not mistake this for the requested cluster.
// If translation fails here, a three-member transport cannot fix it.
func TestDriverGate(t *testing.T) {
	d := simfs.New()
	n, err := node.NewOnFS(node.Config{
		Core:   core.DefaultConfig(1, []uint64{1}),
		Tick:   time.Millisecond,
		Memory: func(int64) (int64, bool) { return 0, false },
		Logf:   t.Logf,
	}, &disk{d: d})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	// Let the single member elect itself before issuing the request.
	time.Sleep(100 * time.Millisecond)
	call, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, err := n.Do(call, &pb.PutRequest{Key: []byte("probe"), Value: []byte("real-driver-simfs")}); err != nil {
		t.Fatal(err)
	}
	msg, err := n.Do(call, &pb.RangeRequest{Key: []byte("probe")})
	if err != nil {
		t.Fatal(err)
	}
	r := msg.(*pb.RangeResponse)
	if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "real-driver-simfs" {
		t.Fatalf("read: %v", r)
	}
	t.Log("real driver put/read OK (disk is tigerstore simfs)")
}

type noPeers struct{}

func (noPeers) Send(*raftpb.Message) {}
func (noPeers) Fetch(context.Context, uint64, uint64, uint64) (io.ReadCloser, error) {
	panic("no peers")
}
func (noPeers) Call(context.Context, uint64, []byte) ([]byte, error) { panic("no peers") }
func (noPeers) SetPeers(map[uint64]string, []uint64)                 {}
func (noPeers) ActiveSince(uint64) time.Time                         { return time.Time{} }
func (noPeers) Close() error                                         { return nil }

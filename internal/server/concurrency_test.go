package server

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
)

// noPeers is the transport of a node without peers.
type noPeers struct{}

func (noPeers) Send(*raftpb.Message) {}
func (noPeers) Close() error         { return nil }
func (noPeers) Fetch(context.Context, uint64, uint64, uint64) (io.ReadCloser, error) {
	return nil, errors.New("no peers")
}

func (noPeers) SetPeers(map[uint64]string, []uint64) {}

func (noPeers) Call(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("no peers")
}

// TestWatchEventsResumes holds the first event that watchEvents hands
// on while 20 more puts land. The node's watch queue holds one response,
// so it drops the stream as too slow. watchEvents must watch again after
// the last event it handed on: every put arrives once, in order.
func TestWatchEventsResumes(t *testing.T) {
	n, err := node.New(node.Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: 50 * time.Millisecond, WatchQueue: 1, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
	})
	put := func() int64 {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			resp, err := n.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
			if err == nil {
				return resp.(*pb.PutResponse).Header.Revision
			}
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	first := put()
	held, release := make(chan struct{}), make(chan struct{})
	var revs []int64
	done := make(chan error, 1)
	go func() {
		c := client{s: &Server{n: n}}
		done <- c.watchEvents(ctx, &pb.WatchCreateRequest{Key: []byte("k"), StartRevision: first}, func(w *pb.WatchResponse) bool {
			if len(revs) == 0 {
				close(held)
				<-release
			}
			for _, ev := range w.Events {
				revs = append(revs, ev.Kv.ModRevision)
			}
			return len(revs) >= 21
		})
	}()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("no first event")
	}
	var want []int64
	for range 20 {
		want = append(want, put())
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("not every event after 10 s")
	}
	if want = append([]int64{first}, want...); !slices.Equal(revs, want) {
		t.Fatalf("events at %v, want %v", revs, want)
	}
}

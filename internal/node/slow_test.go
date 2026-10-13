package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// errSlowWatcher is the failure TestPlanted counts as catching B13; any
// other error is a setup failure.
var errSlowWatcher = errors.New("the slow watcher held up the node")

type noPeers struct{}

func (noPeers) Send(*raftpb.Message) {}
func (noPeers) Close() error         { return nil }

// slowWatcherIsCut opens a watch stream with a queue of 8 responses on a
// single node, never reads it, and writes 50 keys it watches, one at a
// time. The stream must be closed as too slow, and a Put and a Range must
// then each finish within 10 election timeouts.
func slowWatcherIsCut(t *testing.T) error {
	cfg := core.DefaultConfig(1, []uint64{1})
	// The leader stops itself when an append takes two election
	// timeouts (core.StallTicks); with 10 ms ticks that is 200 ms, which
	// one slow fsync on a shared runner can exceed.
	tick := 50 * time.Millisecond
	n, err := New(Config{Core: cfg, DataDir: t.TempDir(), Tick: tick, WatchQueue: 8, Logf: t.Logf})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		// With the committer stuck on a slow watcher (planted B13), Run
		// does not return; the test still ends.
		select {
		case <-ran:
		case <-time.After(2 * time.Second):
		}
	}()
	bound := 10 * time.Duration(cfg.ElectionTicks) * tick
	put := func(i int) error {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		_, err := n.Do(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v")})
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for put(0) != nil {
		if time.Now().After(deadline) {
			return errors.New("the node never took a write")
		}
	}
	ws, err := n.OpenWatch(ctx)
	if err != nil {
		return err
	}
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: []byte("k"), RangeEnd: []byte("l"),
	}}}); err != nil {
		return err
	}
	for i := 1; i <= 50; i++ {
		if err := put(i); err != nil {
			// If the node stopped, say why.
			select {
			case runErr := <-ran:
				ran <- runErr
				return fmt.Errorf("put %d: the node stopped: %w", i, runErr)
			default:
			}
			return fmt.Errorf("%w: put %d did not finish within %v: %w", errSlowWatcher, i, bound, err)
		}
	}
	select {
	case <-ws.Done():
		if !errors.Is(ws.Err(), ErrWatchTooSlow) {
			return fmt.Errorf("the slow stream ended with %w", ws.Err())
		}
	case <-time.After(bound):
		return fmt.Errorf("%w: the slow stream was never closed", errSlowWatcher)
	}
	if err := put(51); err != nil {
		return fmt.Errorf("%w: a put after it did not finish within %v: %w", errSlowWatcher, bound, err)
	}
	rctx, rcancel := context.WithTimeout(context.Background(), bound)
	defer rcancel()
	if _, err := n.Do(rctx, &pb.RangeRequest{Key: []byte("k1")}); err != nil {
		return fmt.Errorf("%w: a read after it did not finish within %v: %w", errSlowWatcher, bound, err)
	}
	return nil
}

// TestSlowWatcherIsCut: see slowWatcherIsCut.
func TestSlowWatcherIsCut(t *testing.T) {
	if err := slowWatcherIsCut(t); err != nil {
		t.Fatal(err)
	}
}

package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// errSlowWatcher is the failure TestPlanted counts as catching B13; any
// other error is a setup failure.
var errSlowWatcher = errors.New("the slow watcher held up the node")

type noPeers struct{}

func (noPeers) Send(*raftpb.Message) {}
func (noPeers) Close() error         { return nil }
func (noPeers) Fetch(context.Context, uint64, uint64, uint64) (io.ReadCloser, error) {
	return nil, errors.New("no peers")
}

func (noPeers) SetPeers(map[uint64]string, []uint64) {}

func (noPeers) ActiveSince(uint64) time.Time { return time.Time{} }

func (noPeers) Call(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("no peers")
}

// slowWatcherIsCut opens a watch stream with a queue of 8 responses on a
// single node, never reads it, and writes 50 keys it watches, one at a
// time. The stream must be closed as too slow, and a Put and a Range must
// then each finish within 10 election timeouts.
func slowWatcherIsCut(t *testing.T) error {
	cfg := core.DefaultConfig(1, []uint64{1})
	// The bound below is 10 election timeouts; with 10 ms ticks that is
	// 1 s, which a few slow fsyncs on a shared runner can exceed.
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
	ws, err := n.OpenWatch(ctx, core.Caller{})
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
			// If the node stopped, say why. Run returns only after its
			// disk goroutine, so wait for it.
			if errors.Is(err, rpctypes.ErrGRPCStopped) {
				select {
				case runErr := <-ran:
					ran <- runErr
					return fmt.Errorf("put %d: the node stopped: %w", i, runErr)
				case <-time.After(bound):
				}
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

// TestWatchBytesBound closes a watch stream that is behind by more bytes
// than its limit, although its count of responses is far below the queue.
func TestWatchBytesBound(t *testing.T) {
	cfg := core.DefaultConfig(1, []uint64{1})
	n, err := New(Config{Core: cfg, DataDir: t.TempDir(), Tick: 50 * time.Millisecond, WatchBytes: 8 << 10, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
	}()
	put := func(i int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := n.Do(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: make([]byte, 1024)})
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for put(0) != nil {
		if time.Now().After(deadline) {
			t.Fatal("the node never took a write")
		}
	}
	ws, err := n.OpenWatch(ctx, core.Caller{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: []byte("k"), RangeEnd: []byte("l"),
	}}}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 20; i++ {
		if err := put(i); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-ws.Done():
		if !errors.Is(ws.Err(), ErrWatchTooSlow) {
			t.Fatalf("the stream ended with %v", ws.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a stream behind by 20 KiB was not closed; %d responses queued", len(ws.C))
	}
}

// TestMemoryPressureClosesSlowestWatch runs a node whose process uses
// more than its whole memory budget, so the value cache cannot give
// anything back. After a collection, the node must close the watch stream
// that is furthest behind as too slow, and keep one that keeps up.
func TestMemoryPressureClosesSlowestWatch(t *testing.T) {
	cfg := core.DefaultConfig(1, []uint64{1})
	cfg.MemoryBytes = 1
	n, err := New(Config{Core: cfg, DataDir: t.TempDir(), Tick: 20 * time.Millisecond, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
	}()
	put := func(key string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := n.Do(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	watch := func(key string) *WatchStream {
		t.Helper()
		ws, err := n.OpenWatch(ctx, core.Caller{})
		if err != nil {
			t.Fatal(err)
		}
		if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte(key)}}}); err != nil {
			t.Fatal(err)
		}
		return ws
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := n.Do(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("v")})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the node never took a write")
		}
	}
	slow, fast := watch("k"), watch("z")
	// The fast client takes its created response; the slow one never
	// reads.
	select {
	case r := <-fast.C:
		fast.Taken(r)
	case <-time.After(5 * time.Second):
		t.Fatal("no created response")
	}
	for range 3 {
		put("k")
	}
	for {
		runtime.GC()
		select {
		case <-slow.Done():
			if !errors.Is(slow.Err(), ErrWatchTooSlow) {
				t.Fatalf("the slow stream ended with %v", slow.Err())
			}
			select {
			case <-fast.Done():
				t.Fatalf("the stream that kept up was closed too: %v", fast.Err())
			default:
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("the stream furthest behind was not closed")
		}
	}
}

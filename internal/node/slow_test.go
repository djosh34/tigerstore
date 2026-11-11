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
func (noPeers) HeldEntries(uint64) bool      { return false }

func (noPeers) Call(context.Context, uint64, []byte) ([]byte, error) {
	return nil, errors.New("no peers")
}

// slowWatcherWaits opens a watch stream with a queue of 8 responses on a
// single node, does not read it, and writes 50 keys it watches, one at a
// time, each of which must finish within 10 election timeouts, and so
// must a Put and a Range after them. The stream stays open: its watcher
// falls behind, and once the client reads, it gets all 50 events in
// order (#505).
func slowWatcherWaits(t *testing.T) error {
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
	if err := put(51); err != nil {
		return fmt.Errorf("%w: a put after it did not finish within %v: %w", errSlowWatcher, bound, err)
	}
	rctx, rcancel := context.WithTimeout(context.Background(), bound)
	defer rcancel()
	if _, err := n.Do(rctx, &pb.RangeRequest{Key: []byte("k1")}); err != nil {
		return fmt.Errorf("%w: a read after it did not finish within %v: %w", errSlowWatcher, bound, err)
	}
	next := int64(0)
	for next < 51 {
		select {
		case r := <-ws.C:
			ws.Taken(r)
			for _, ev := range r.Events {
				if want := fmt.Sprintf("k%d", next+1); string(ev.Kv.Key) != want {
					return fmt.Errorf("the slow stream got %s, want %s", ev.Kv.Key, want)
				}
				next++
			}
		case <-ws.Done():
			return fmt.Errorf("the slow stream was closed: %w", ws.Err())
		case <-time.After(10 * bound):
			return fmt.Errorf("the slow stream got %d of 51 events", next)
		}
	}
	return nil
}

// TestSlowWatcherWaits: see slowWatcherWaits.
func TestSlowWatcherWaits(t *testing.T) {
	if err := slowWatcherWaits(t); err != nil {
		t.Fatal(err)
	}
}

// TestWatchRoomBound writes 20 KiB to a stream with room for 8 KiB that
// is not read: the stream holds at most about its room, stays open, and
// once read gets every event in order (#505).
func TestWatchRoomBound(t *testing.T) {
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
		_, err := n.Do(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 1024)})
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
	if q := ws.queued.Load(); q > 8<<10+2<<10 {
		t.Fatalf("a stream with room for 8 KiB holds %d bytes", q)
	}
	next := 1
	for next <= 20 {
		select {
		case r := <-ws.C:
			ws.Taken(r)
			for _, ev := range r.Events {
				if want := fmt.Sprintf("k%02d", next); string(ev.Kv.Key) != want {
					t.Fatalf("the stream got %s, want %s", ev.Kv.Key, want)
				}
				next++
			}
		case <-ws.Done():
			t.Fatalf("the stream was closed: %v", ws.Err())
		case <-time.After(5 * time.Second):
			t.Fatalf("the stream got %d of 20 events", next-1)
		}
	}
}

// TestMemoryPressureHoldsWatches runs a node whose process uses more
// than its whole memory budget, so the value cache cannot give anything
// back. Its watch streams then get no events, whether their clients read
// or not, and none is closed: their watchers wait, while writes go on
// (#505).
func TestMemoryPressureHoldsWatches(t *testing.T) {
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
	ws, err := n.OpenWatch(ctx, core.Caller{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("k")}}}); err != nil {
		t.Fatal(err)
	}
	// The created response goes out whatever the room.
	select {
	case r := <-ws.C:
		ws.Taken(r)
		if !r.Created {
			t.Fatalf("the first response is %v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no created response")
	}
	for range 3 {
		put("k")
		runtime.GC()
	}
	select {
	case r := <-ws.C:
		t.Fatalf("a node over its memory budget sent %v", r)
	case <-ws.Done():
		t.Fatalf("a node over its memory budget closed a stream: %v", ws.Err())
	case <-time.After(time.Second):
	}
}

// TestBigRevisionReachesPrevKVWatcher deletes 300 keys of 1 KiB at one
// revision, on a stream whose room is 256 KiB, for a watcher with
// prev_kv that reads at once. The revision is larger than the room, so
// it goes out in parts, as one response; the stream must not be closed
// for it (#428).
func TestBigRevisionReachesPrevKVWatcher(t *testing.T) {
	n, err := New(Config{Core: core.DefaultConfig(1, []uint64{1}), DataDir: t.TempDir(), Tick: 10 * time.Millisecond, WatchBytes: 256 << 10, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() { cancel(); <-ran }()
	do := func(op *pb.RequestOp) {
		t.Helper()
		for {
			cctx, c := context.WithTimeout(ctx, 5*time.Second)
			_, err := n.Do(cctx, &pb.TxnRequest{Success: []*pb.RequestOp{op}})
			c()
			if err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for i := range 300 {
		do(&pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: fmt.Appendf(nil, "k%03d", i), Value: make([]byte, 1024)}}})
	}
	do(&pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}}})
	rev := int64(302) // 300 puts from revision 2, then the delete
	ws, err := n.OpenWatch(ctx, core.Caller{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), StartRevision: rev, PrevKv: true}}}); err != nil {
		t.Fatal(err)
	}
	events := 0
	for events < 300 {
		select {
		case r := <-ws.C:
			ws.Taken(r)
			events += len(r.Events)
			if len(r.Events) > 0 && len(r.Events) != 300 {
				t.Fatalf("the revision of 300 deletes came in a response of %d", len(r.Events))
			}
		case <-ws.Done():
			t.Fatalf("the stream closed after %d events: %v", events, ws.Err())
		case <-time.After(5 * time.Second):
			t.Fatalf("a watcher with prev_kv that reads at once got %d of the 300 deletes of one revision", events)
		}
	}
}

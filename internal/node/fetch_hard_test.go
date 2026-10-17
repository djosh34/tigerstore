package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestFetchHardCases are #162's cases of a follower whose snapshot fetch
// fails. Node 3 of a behind cluster needs a snapshot; each case breaks
// its fetches another way, and it must start again from its log and
// catch up without stopping, unless its own disk failed.
func TestFetchHardCases(t *testing.T) {
	t.Run("source gone, then back", func(t *testing.T) {
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		// A fetch that never reaches the leader costs a SnapshotTicks
		// (10 s here) before the leader gives the snapshot up and sends
		// another.
		c.net.mu.Lock()
		c.net.refuse = 2
		c.net.mu.Unlock()
		n3, err := c.start(3)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.caughtUp(n3, 45*time.Second); err != nil {
			t.Fatal(err)
		}
		c.net.mu.Lock()
		defer c.net.mu.Unlock()
		if c.net.refuse != 0 {
			t.Fatalf("%d fetches were not refused", c.net.refuse)
		}
	})
	t.Run("cut at five points, then healed", func(t *testing.T) {
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		// With a sync every 2 KiB of values and entries, the checkpoint
		// fills the first 11 to 16 KiB of the stream, the root follows,
		// then the entries. Four cuts land in the header, before the
		// first sync and after a few; the fifth fetch breaks off right
		// after its root, wherever that is. stages notes what each fetch
		// got to.
		stages := map[int][]string{}
		c.tune = func(id uint64, cfg *Config) {
			cfg.snapshotSync = 2 << 10
			cfg.stage = func(s string) error {
				c.net.mu.Lock()
				defer c.net.mu.Unlock()
				stages[c.net.fetches] = append(stages[c.net.fetches], s)
				if s == "root" && c.net.fetches == 5 {
					return errCut
				}
				return nil
			}
		}
		c.net.mu.Lock()
		c.net.cuts = []int{20, 3 << 10, 5 << 10, 9 << 10}
		c.net.mu.Unlock()
		n3, err := c.start(3)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.caughtUp(n3, 30*time.Second); err != nil {
			t.Fatal(err)
		}
		c.net.mu.Lock()
		defer c.net.mu.Unlock()
		// The fifth fetch breaks off after its root, which the install
		// syncs, so the node may catch up from there without a sixth
		// (#199).
		if c.net.cut != 4 || c.net.fetches < 5 {
			t.Fatalf("%d streams were cut and %d fetched, want 4 cut and the fifth broken off", c.net.cut, c.net.fetches)
		}
		// Before any sync, after a sync and before the root, after the
		// root.
		var none, synced, rooted bool
		for i := 1; i <= 5; i++ {
			st := stages[i]
			none = none || len(st) == 0
			synced = synced || slices.Contains(st, "sync") && !slices.Contains(st, "root")
			rooted = rooted || slices.Contains(st, "root")
		}
		if !none || !synced || !rooted {
			t.Fatalf("stages of the cut streams: %v", stages)
		}
	})
	t.Run("crash during the reload", func(t *testing.T) {
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		// The first stream breaks off right after its root. When node 3
		// opened its log again, before it replays it, the hook copies its
		// directory: what a crash at that point leaves. Node 3 then
		// starts from that copy.
		crash := filepath.Join(t.TempDir(), "crash")
		copied := make(chan error, 1)
		var rooted atomic.Bool
		var once sync.Once
		c.tune = func(id uint64, cfg *Config) {
			cfg.snapshotSync = 2 << 10
			cfg.stage = func(s string) error {
				if s == "root" && !rooted.Swap(true) {
					return errCut
				}
				return nil
			}
			cfg.reloading = func(*Node) {
				if rooted.Load() {
					once.Do(func() { copied <- os.CopyFS(crash, os.DirFS(c.dirs[3])) })
				}
			}
		}
		if _, err := c.start(3); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-copied:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("node 3 did not reload after its root")
		}
		if err := c.stop(3); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stop node 3: %v", err)
		}
		c.dirs[3], c.tune = crash, nil
		n3, err := c.start(3)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.caughtUp(n3, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("requests during the reloads", func(t *testing.T) {
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		c.net.mu.Lock()
		c.net.cuts = []int{2 << 10, 8 << 10, 16 << 10}
		c.net.mu.Unlock()
		n3, err := c.start(3)
		if err != nil {
			t.Fatal(err)
		}
		watch := func() (*WatchStream, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ws, err := n3.OpenWatch(ctx, core.Caller{})
			if err != nil {
				return nil, err
			}
			return ws, ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
				Key: []byte("k"), RangeEnd: []byte("l"),
			}}})
		}
		// A watch open from before the first reload stays open through
		// the fetches.
		held, err := watch()
		if err != nil {
			t.Fatal(err)
		}
		// Reads, writes and watches that open and wait for an event while
		// node 3 fetches and reloads: each call must come back, with an
		// answer or an error, within its deadline and a little more,
		// never hang.
		stop := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 3)
		for _, op := range []func(context.Context) error{
			func(ctx context.Context) error {
				_, err := n3.Do(ctx, &pb.RangeRequest{Key: []byte("k00")})
				return err
			},
			func(ctx context.Context) error {
				_, err := n3.Do(ctx, &pb.PutRequest{Key: []byte("w"), Value: []byte("v")})
				return err
			},
			func(ctx context.Context) error {
				ws, err := n3.OpenWatch(ctx, core.Caller{})
				if err != nil {
					return err
				}
				defer ws.Close()
				if err := ws.Send(ctx, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
					Key: []byte("w"),
				}}}); err != nil {
					return err
				}
				select {
				case <-ws.C:
				case <-ws.Done():
				case <-ctx.Done():
				}
				return nil
			},
		} {
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					start := time.Now()
					_ = op(ctx)
					cancel()
					if d := time.Since(start); d > 5*time.Second {
						errs <- fmt.Errorf("a call took %v", d)
						return
					}
				}
			})
		}
		err = c.caughtUp(n3, 30*time.Second)
		close(stop)
		wg.Wait()
		close(errs)
		if err != nil {
			t.Fatal(err)
		}
		for err := range errs {
			t.Fatal(err)
		}
		// The reloads and the install ended the held watch with an error
		// that tells its client to watch again; a new one gets the next
		// event.
		select {
		case <-held.Done():
			if err := held.Err(); !errors.Is(err, rpctypes.ErrGRPCStopped) && !errors.Is(err, ErrSnapshotInstalled) {
				t.Fatalf("the held watch ended with %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the held watch stayed open through the reloads")
		}
		ws, err := watch()
		if err != nil {
			t.Fatal(err)
		}
		defer ws.Close()
		if _, err := do(c.nodes[1], &pb.PutRequest{Key: []byte("k00"), Value: []byte("next")}); err != nil {
			t.Fatal(err)
		}
		for deadline := time.After(10 * time.Second); ; {
			select {
			case r := <-ws.C:
				if len(r.Events) > 0 && string(r.Events[0].Kv.Value) == "next" {
					return
				}
			case <-ws.Done():
				t.Fatalf("the new watch ended: %v", ws.Err())
			case <-deadline:
				t.Fatal("the new watch got no event of the put")
			}
		}
	})
	t.Run("own disk fails during the install", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes to a directory without write permission")
		}
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		// The fetch must not fail first: a leader that cuts a newer
		// checkpoint during it removes the one it serves (#239).
		for id := uint64(1); id <= 2; id++ {
			if err := checkpointSettled(c.nodes[id]); err != nil {
				t.Fatalf("node %d: %v", id, err)
			}
		}
		// Node 3 cannot create files in its log's directory once it runs,
		// so the install cannot write the checkpoint: that stops it.
		n, err := New(c.config(3))
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(c.dirs[3], "wal")
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c.net.join(ctx, n)
		ran := make(chan error, 1)
		go func() { ran <- n.Run(ctx, memPeer{net: c.net, id: 3}) }()
		select {
		case err := <-ran:
			if err == nil || errors.Is(err, core.ErrFetch) || !errors.Is(err, os.ErrPermission) {
				t.Fatalf("node 3 stopped with %v, want the failed write", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("node 3 kept running after its disk failed")
		}
	})
}

// checkpointSettled waits up to 10 s until n's newest durable checkpoint
// stayed the same for a second.
func checkpointSettled(n *Node) error {
	newest := func() uint64 {
		n.log.mu.Lock()
		defer n.log.mu.Unlock()
		if len(n.log.ckpts) == 0 {
			return 0
		}
		return n.log.ckpts[len(n.log.ckpts)-1].Index
	}
	last, since := newest(), time.Now()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if c := newest(); c != last {
			last, since = c, time.Now()
		} else if c > 0 && time.Since(since) >= time.Second {
			return nil
		}
	}
	return fmt.Errorf("its newest checkpoint did not settle, at %d", last)
}

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

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
		// Within the header, the checkpoint and the entries after it,
		// before and after the 16 KiB at which the log syncs what came.
		c.net.mu.Lock()
		c.net.cuts = []int{20, 2 << 10, 8 << 10, 15 << 10, 17 << 10}
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
		if c.net.cut != 5 {
			t.Fatalf("%d streams were cut, want 5", c.net.cut)
		}
	})
	t.Run("crash during the reload", func(t *testing.T) {
		c, err := newBehind(t)
		defer c.close()
		if err != nil {
			t.Fatal(err)
		}
		c.net.mu.Lock()
		c.net.cuts = []int{16 << 10, 16 << 10}
		c.net.mu.Unlock()
		if _, err := c.start(3); err != nil {
			t.Fatal(err)
		}
		// Stop node 3 as soon as its first stream broke off, while it
		// reloads, as a crash would, and start it again from its disk.
		for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(time.Millisecond) {
			c.net.mu.Lock()
			cut := c.net.cut
			c.net.mu.Unlock()
			if cut > 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no stream broke off")
			}
		}
		if err := c.stop(3); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stop node 3: %v", err)
		}
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
		// Reads, writes and a watch while node 3 fetches and reloads:
		// each call must come back, with an answer or an error, within
		// its deadline and a little more, never hang.
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
				ws.Close()
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
			if err == nil || errors.Is(err, errReload) || !errors.Is(err, os.ErrPermission) {
				t.Fatalf("node 3 stopped with %v, want the failed write", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("node 3 kept running after its disk failed")
		}
	})
}

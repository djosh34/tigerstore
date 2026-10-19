package node

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The tests in this file check the guards of etcd v3.7.2 around Raft
// that #232 lists and the node driver copies.

// leaderOf waits up to 10 s for a node of c to lead and returns it.
func leaderOf(c *behind) (*Node, error) {
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for _, n := range c.nodes {
			if n.Leader() == n.ID() {
				return n, nil
			}
		}
	}
	return nil, errors.New("no leader within 10 s")
}

// TestTransferLeadership runs etcd's TransferLeadership on the leader, as
// a stop does: it hands its leadership to the voter it has been
// connected to the longest (row 45).
func TestTransferLeadership(t *testing.T) {
	c, err := startThree(t, nil)
	defer c.close()
	if err != nil {
		t.Fatal(err)
	}
	lead, err := leaderOf(c)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	want := uint64(0)
	c.net.mu.Lock()
	c.net.since = map[uint64]time.Time{}
	for id := range c.nodes {
		c.net.since[id] = now.Add(-time.Duration(id) * time.Second)
		if id != lead.ID() {
			want = max(want, id)
		}
	}
	c.net.mu.Unlock()
	if err := lead.TransferLeadership(context.Background()); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Second); lead.Leader() != want; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("node %d leads after the transfer, want %d, the longest connected", lead.Leader(), want)
		}
	}
	// A follower has nothing to hand over.
	if err := c.nodes[want].TransferLeadership(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestLateHeartbeatCounts holds the leader's committer for three
// heartbeat intervals once: like etcd's leader, it then warns that it
// sent a heartbeat late and counts it (row 44).
func TestLateHeartbeatCounts(t *testing.T) {
	var stall atomic.Bool
	c, err := startThree(t, func(id uint64, cfg *Config) {
		cfg.Turn = func(n *core.Node, _ core.Effects, _ error) {
			if n.Leader() == id && stall.CompareAndSwap(true, false) {
				time.Sleep(3 * cfg.Tick)
			}
		}
	})
	defer c.close()
	if err != nil {
		t.Fatal(err)
	}
	lead, err := leaderOf(c)
	if err != nil {
		t.Fatal(err)
	}
	// A few heartbeats go out first.
	time.Sleep(time.Second)
	m, err := lead.Metrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before := m.HeartbeatSendFailures
	stall.Store(true)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		m, err := lead.Metrics(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if m.HeartbeatSendFailures > before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no late heartbeat counted after the stall: %d", m.HeartbeatSendFailures)
		}
	}
}

// leaderless starts node 1 of three alone on disk, so it never finds a
// leader, with change applied to its configuration.
func leaderless(t *testing.T, disk FS, change func(*Config)) (*Node, func()) {
	t.Helper()
	cfg := Config{Core: core.DefaultConfig(1, []uint64{1, 2, 3}), Tick: 10 * time.Millisecond}
	change(&cfg)
	n, err := newOnDisk(cfg, "", disk)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	return n, func() {
		cancel()
		if err := <-ran; err != nil {
			t.Error(err)
		}
	}
}

// TestRequestClockFollowsWallClock holds the committer of a node without
// a leader for a request timeout once, right after it took a write: the
// ticks it missed still count, so the write times out by the wall clock,
// as in etcd, not a whole request timeout later (row 35).
func TestRequestClockFollowsWallClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stall atomic.Bool
		var timeout time.Duration
		n, stop := leaderless(t, &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}, func(cfg *Config) {
			timeout = cfg.Tick * time.Duration(cfg.Core.RequestTicks)
			cfg.Turn = func(c *core.Node, _ core.Effects, _ error) {
				// Once the write waits in the core.
				if stall.Load() && c.Metrics().ProposalsPending > 0 && stall.CompareAndSwap(true, false) {
					time.Sleep(timeout)
				}
			}
		})
		defer stop()
		stall.Store(true)
		start := time.Now()
		_, err := n.Do(context.Background(), &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("a write without a leader: %v", err)
		}
		if took := time.Since(start); took > timeout+timeout/4 {
			t.Fatalf("the write timed out after %v, the request timeout is %v", took, timeout)
		}
	})
}

// TestCancelReachesCore ends the context of a write to a node without a
// leader: the node drops the write at once, as etcd's raft.Node drops a
// proposal whose context ends, instead of keeping it to its deadline
// (row 32).
func TestCancelReachesCore(t *testing.T) {
	// The write's own deadline is far off.
	n, stop := leaderless(t, &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}, func(cfg *Config) { cfg.Core.RequestTicks = 1000 })
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := n.Do(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a write without a leader: %v", err)
	}
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		m, err := n.Metrics(context.Background())
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

// TestCleanStopSyncsTail writes on a single node and stops it cleanly,
// then drops what its disk did not sync. As etcd's WAL.Close syncs its
// tail, the log still holds the commit index the node applied (row 47).
func TestCleanStopSyncsTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}
		cfg := Config{Core: core.DefaultConfig(1, []uint64{1}), Tick: 10 * time.Millisecond}
		n, err := newOnDisk(cfg, "", d)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		ran := make(chan error, 1)
		go func() { ran <- n.Run(ctx, noPeers{}) }()
		for i := range 5 {
			if _, err := do(n, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: []byte("v")}); err != nil {
				t.Fatal(err)
			}
		}
		// The commit index of the last put goes to disk in a write of its
		// own, which needs no sync.
		time.Sleep(time.Second)
		m, err := n.Metrics(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if err := <-ran; err != nil {
			t.Fatal(err)
		}
		d.mu.Lock()
		durable := d.d.Durable()
		d.mu.Unlock()
		w, rec, err := wal.Open(durable)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if rec.HardState.Commit < m.Applied {
			t.Fatalf("the synced log commits %d, the node applied %d", rec.HardState.Commit, m.Applied)
		}
	})
}

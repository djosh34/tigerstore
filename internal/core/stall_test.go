package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestStalledLeaderGoesSilent holds the leader's disk for good, as #233
// decided for a leader whose own disk is stuck: past StallTicks election
// timeouts it sends nothing to its peers, so the other two elect a new
// leader while the disk stays held. The old leader never stops, steps
// down on the new term, and once its disk is back it follows the new
// leader and applies a write through it.
func TestStalledLeaderGoesSilent(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.onReply = func(i int, r Reply) {
		switch {
		case r.Err == nil:
			c.replies++
		case r.ID != 1 || !errors.Is(r.Err, rpctypes.ErrGRPCTimeout):
			t.Fatalf("node %d answered %d with %v; %s", i+1, r.ID, r.Err, c.describe())
		}
	}
	c.holdDisk[0] = true
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	election := c.nodes[0].cfg.ElectionTicks
	tick := func() {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	lead := -1
	for range 12 * election {
		before := c.sent[0]
		tick()
		if c.silences[0] > 0 && c.nodes[0].isLeader && c.sent[0] != before {
			t.Fatalf("the silent leader sent %d messages; %s", c.sent[0]-before, c.describe())
		}
		if lead = c.leader(); lead > 0 {
			break
		}
	}
	if lead <= 0 {
		t.Fatalf("no new leader within 12 election timeouts while node 1's disk is held; %s", c.describe())
	}
	if m := c.nodes[0].Metrics(); c.silences[0] != 1 || m.HeartbeatSendFailures == 0 {
		t.Fatalf("the stalled leader went silent %d times and dropped %d heartbeats, want once and some", c.silences[0], m.HeartbeatSendFailures)
	}
	c.holdDisk[0] = false
	before := c.replies
	c.nodes[lead].Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("w")}})
	for range election {
		tick()
	}
	if c.replies <= before {
		t.Fatalf("node %d leads but took no write; %s", lead+1, c.describe())
	}
	if a, b := c.nodes[0].Applied(), c.nodes[lead].Applied(); a != b {
		t.Fatalf("node 1 applied %d after its disk came back, the leader %d; %s", a, b, c.describe())
	}
}

// TestSlowCommitWarns holds every follower's acknowledgement, so nothing
// the leader proposes commits: the leader warns once and counts it, and
// stays leader.
func TestSlowCommitWarns(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.holdAcks = true
	c.onReply = func(i int, r Reply) {
		if r.Err != nil && !errors.Is(r.Err, rpctypes.ErrGRPCTimeout) {
			t.Fatalf("node %d answered %d with %v", i+1, r.ID, r.Err)
		}
	}
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for range 2 * SlowCommitTicks * c.nodes[0].cfg.ElectionTicks {
		c.nodes[0].Tick()
		c.step()
	}
	m := c.nodes[0].Metrics()
	if c.slowCommits[0] != 1 || m.SlowCommits != 1 || !m.IsLeader {
		t.Fatalf("%d warnings, %d counted, leader %v; want one of each and still leader; %s", c.slowCommits[0], m.SlowCommits, m.IsLeader, c.describe())
	}
	if want := int64(2 * SlowCommitTicks * c.nodes[0].cfg.ElectionTicks); m.MaxCommitWait < want-1 {
		t.Fatalf("the longest commit wait is %d ticks, want about %d", m.MaxCommitWait, want)
	}
}

// TestStallAllowsForSize is #553: a leader whose 64 MiB of entries on
// their way to the disk take more than StallTicks election timeouts is
// not stuck if its disk writes at least stallBytesPerTick a tick, so it
// stays heard, whether the bytes are one append or forty written as one
// group; one that writes slower than that, or a small append that takes
// as long, goes silent.
func TestStallAllowsForSize(t *testing.T) {
	const election = 10
	base := int64(StallTicks * election)
	one := []int64{0}
	forty := make([]int64, 40)
	for _, c := range []struct {
		appends []int64
		bytes   int64
		at      int64
		silent  bool
	}{
		{appends: one, bytes: 100, at: base - 1, silent: false},
		{appends: one, bytes: 100, at: base, silent: true},
		{appends: one, bytes: 64 << 20, at: base, silent: false},
		{appends: one, bytes: 64 << 20, at: base + 64 - 1, silent: false},
		{appends: one, bytes: 64 << 20, at: base + 64, silent: true},
		{appends: forty, bytes: 40 << 20, at: base + 27, silent: false},
		{appends: forty, bytes: 40 << 20, at: base + 40, silent: true},
	} {
		n := &Node{cfg: Config{ElectionTicks: election}, isLeader: true, now: c.at, appending: c.appends, unpersisted: c.bytes}
		if got := n.stalled(); got != c.silent {
			t.Errorf("%d appends of %d bytes in all at tick %d: stalled %v, want %v", len(c.appends), c.bytes, c.at, got, c.silent)
		}
	}
}

// TestStallCountsQueuedBytes checks that a leader's appends add their
// entries' bytes to those on their way to the disk and take them off once
// durable, which the stall guard's allowance reads.
func TestStallCountsQueuedBytes(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	c.holdDisk[0] = true
	value := make([]byte, 64<<10)
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: value}})
	c.step()
	if n := c.nodes[0]; n.unpersisted < int64(len(value)) || len(n.appending) == 0 {
		t.Fatalf("a held %d-byte put: %d bytes on their way to the disk, %d appends", len(value), n.unpersisted, len(n.appending))
	}
	c.holdDisk[0] = false
	for range c.nodes[0].cfg.ElectionTicks {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	if n := c.nodes[0]; n.unpersisted != 0 || len(n.appending) != 0 {
		t.Fatalf("after the disk came back: %d bytes on their way to the disk, %d appends", n.unpersisted, len(n.appending))
	}
}

// TestStallCountsFromProgress is #564: a 40 MiB append and a 16 MiB one
// behind it, on a disk that writes the first at 10 MiB/s. Once the first
// is durable, the second has waited 40 ticks, more than its own allowance,
// but the disk has just made progress, so the leader is not stalled. A
// disk that then makes none for the allowance of the bytes left is.
func TestStallCountsFromProgress(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.MaxRequestBytes = MaxRequestBytesCeiling })
	// The second write times out while the disk makes no progress.
	c.onReply = func(i int, r Reply) {
		if r.Err != nil && !errors.Is(r.Err, rpctypes.ErrGRPCTimeout) {
			t.Fatalf("node %d answered %d with %v", i+1, r.ID, r.Err)
		}
	}
	for range 20 {
		c.step()
	}
	c.holdDisk[0] = true
	n := c.nodes[0]
	n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("big"), Value: make([]byte, 40<<20)}})
	c.step()
	n.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("next"), Value: make([]byte, 16<<20)}})
	c.step()
	if len(n.appending) != 2 {
		t.Fatalf("%d appends on their way to the disk, want 2", len(n.appending))
	}
	tick := func() {
		for _, node := range c.nodes {
			node.Tick()
		}
		c.step()
	}
	for range 40 {
		tick()
	}
	if n.stalled() {
		t.Fatal("stalled while 56 MiB are on their way and 40 ticks went by")
	}
	// The disk finishes the 40 MiB append and starts on the next.
	for len(c.jobs[0]) > 0 {
		j := c.jobs[0][0]
		c.jobs[0] = c.jobs[0][1:]
		n.Persisted(j)
		if len(j.Entries) > 0 {
			break
		}
	}
	if n.stalled() {
		t.Fatalf("stalled just after the disk finished 40 MiB in 40 ticks; %d appends left", len(n.appending))
	}
	limit := int64(StallTicks*n.cfg.ElectionTicks) + n.unpersisted/stallBytesPerTick
	for range limit {
		tick()
	}
	if !n.stalled() {
		t.Fatalf("not stalled after %d ticks without progress", limit)
	}
}

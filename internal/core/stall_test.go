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

// TestStallAllowsForSize is #553: a leader whose own 64 MiB append takes
// more than StallTicks election timeouts is not stuck if it writes at
// least stallBytesPerTick a tick, so it stays heard; one that writes
// slower than that, or a small append that takes as long, goes silent.
func TestStallAllowsForSize(t *testing.T) {
	const election = 10
	base := int64(StallTicks * election)
	for _, c := range []struct {
		bytes, at int64
		silent    bool
	}{
		{bytes: 100, at: base - 1, silent: false},
		{bytes: 100, at: base, silent: true},
		{bytes: 64 << 20, at: base, silent: false},
		{bytes: 64 << 20, at: base + 64 - 1, silent: false},
		{bytes: 64 << 20, at: base + 64, silent: true},
	} {
		n := &Node{cfg: Config{ElectionTicks: election}, isLeader: true, now: c.at, appending: []appendAt{{at: 0, bytes: c.bytes}}}
		if got := n.stalled(); got != c.silent {
			t.Errorf("an append of %d bytes at tick %d: stalled %v, want %v", c.bytes, c.at, got, c.silent)
		}
	}
}

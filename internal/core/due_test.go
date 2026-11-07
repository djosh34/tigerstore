package core

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

// dueDriver drives three cores as the node's committer does (Due): a
// core wakes when its next tick is due or a message comes, and then gets
// every tick it held back, each a Tick, before the message. It checks
// Due's promise: the held-back ticks put nothing out.
type dueDriver struct {
	t *testing.T
	c *memCluster
	// now is the tick of the wall clock, and last and wake the tick each
	// core was given last and the tick its next is due.
	now        int64
	last, wake [3]int64
	// woken counts the cores that woke because a tick was due, and
	// campaigned is the first tick any core asked for a vote at, or 0.
	woken      [3]int
	campaigned int64
}

// newDueDriver starts the cores. Each wakes at the first tick, as a core
// takes a turn before its driver asks it what is due.
func newDueDriver(t *testing.T) *dueDriver {
	return &dueDriver{t: t, c: newMemCluster(t, func(*Config) {})}
}

// catchUp gives core i the ticks up to now. Each before its due tick must
// put nothing out, the last before an input too.
func (d *dueDriver) catchUp(i int) {
	n := d.c.nodes[i]
	for d.last[i] < d.now {
		d.last[i]++
		n.Tick()
		if d.last[i] >= d.wake[i] {
			continue
		}
		eff, err := n.Process()
		if err != nil {
			d.t.Fatal(err)
		}
		if len(eff.Send) > 0 || len(eff.Persist) > 0 || len(eff.Replies) > 0 {
			d.t.Fatalf("node %d at tick %d, before its due tick %d: %d messages, %d disk writes, %d replies",
				i+1, d.last[i], d.wake[i], len(eff.Send), len(eff.Persist), len(eff.Replies))
		}
	}
}

// tick moves the clock one tick on: the cores whose tick is due wake,
// then every message goes out, each to a core that caught up first.
func (d *dueDriver) tick() {
	d.now++
	var msgs []*raftpb.Message
	// turn runs core i and its disk, which takes no time: each finished
	// write is a turn of its own, as in the committer.
	turn := func(i int) {
		n := d.c.nodes[i]
		for {
			eff, err := n.Process()
			if err != nil {
				d.t.Fatal(err)
			}
			msgs = append(msgs, eff.Send...)
			for _, m := range eff.Send {
				if t := m.GetType(); d.campaigned == 0 && (t == raftpb.MsgPreVote || t == raftpb.MsgVote) {
					d.campaigned = d.now
				}
			}
			if len(eff.Persist) == 0 {
				break
			}
			for _, j := range eff.Persist {
				n.Persisted(j)
			}
		}
		d.wake[i] = d.last[i] + n.Due()
	}
	for i := range d.c.nodes {
		if !d.c.down[i] && d.now >= d.wake[i] {
			d.woken[i]++
			d.catchUp(i)
			turn(i)
		}
	}
	for len(msgs) > 0 {
		m := msgs[0]
		msgs = msgs[1:]
		i := int(m.GetTo() - 1)
		if d.c.down[i] || d.c.down[m.GetFrom()-1] {
			continue
		}
		d.catchUp(i)
		if err := d.c.nodes[i].Step(m); err != nil {
			d.t.Fatalf("step %s: %v", m.GetType(), err)
		}
		turn(i)
	}
}

// TestDueHoldsTicks runs a cluster by Due: its followers wake for no tick
// while they hear from their leader, and once it is gone they still elect
// another, and the ticks they held back put nothing out.
func TestDueHoldsTicks(t *testing.T) {
	d := newDueDriver(t)
	for range 100 {
		d.tick()
	}
	lead := d.c.leader()
	if lead < 0 {
		t.Fatalf("no leader: %s", d.c.describe())
	}
	before := d.woken
	for range 100 {
		d.tick()
	}
	for i := range d.c.nodes {
		if woken := d.woken[i] - before[i]; i != lead && woken > 0 {
			t.Errorf("follower %d woke %d times for a tick while it heard from its leader", i+1, woken)
		}
	}
	// A follower campaigns once its timeout passes, which Raft draws below
	// twice ElectionTicks. Two candidates can split the vote and draw
	// again, so a new leader gets 20 timeouts.
	d.c.down[lead] = true
	downAt, election := d.now, int64(DefaultConfig(1, nil).ElectionTicks)
	d.campaigned = 0
	limit := 20 * election
	for range limit {
		if l := d.c.leader(); l >= 0 && l != lead {
			break
		}
		d.tick()
	}
	if l := d.c.leader(); l < 0 || l == lead {
		t.Fatalf("no new leader %d ticks after leader %d went down: %s", limit, lead+1, d.c.describe())
	}
	if d.campaigned == 0 || d.campaigned-downAt > 2*election {
		t.Fatalf("the first campaign came at tick %d, %d ticks after leader %d went down; Raft's largest timeout is %d", d.campaigned, d.campaigned-downAt, lead+1, 2*election-1)
	}
}

package core

import (
	"fmt"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

// heldCluster starts three nodes in memory whose links all carry the
// messages of held entries, except those to the nodes in old.
func heldCluster(t *testing.T, old ...uint64) *memCluster {
	return newMemCluster(t, func(cfg *Config) {
		cfg.HeldEntries = func(peer uint64) bool { return !slices.Contains(old, peer) }
	})
}

// writeThroughFollowers puts keys through both followers, ticking every
// node, until all of them are answered, and checks that every node holds
// the same state.
func writeThroughFollowers(t *testing.T, c *memCluster, keys int) {
	t.Helper()
	id := uint64(0)
	for k := range keys {
		for f := 1; f <= 2; f++ {
			id++
			c.nodes[f].Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d-%d", f, k), Value: fmt.Appendf(nil, "value %d through node %d", k, f+1)}})
		}
	}
	for step := 0; c.replies < int(id); step++ {
		if step > 2000 {
			t.Fatalf("%d of %d writes answered after %d steps: %s", c.replies, id, step, c.describe())
		}
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	for range 20 {
		c.step()
	}
	for i, n := range c.nodes[1:] {
		if n.Applied() != c.nodes[0].Applied() || n.Hash() != c.nodes[0].Hash() {
			t.Fatalf("node %d applied %d with hash %x, the leader %d with hash %x", i+2, n.Applied(), n.Hash(), c.nodes[0].Applied(), c.nodes[0].Hash())
		}
	}
}

// sideCopy reports whether m is a side copy.
func sideCopy(m *raftpb.Message) bool {
	return HeldForm(m) && m.GetContext()[len(heldTag)] == heldSideCopy
}

// TestHeldEntriesNameForwardedWrites checks that the leader's appends
// name the entries the followers forwarded instead of carrying them, and
// that every node still applies the same writes.
func TestHeldEntriesNameForwardedWrites(t *testing.T) {
	c := heldCluster(t)
	var full, named int
	c.drop = func(m *raftpb.Message) bool {
		if m.GetType() == raftpb.MsgApp && m.GetFrom() == 1 {
			byName := map[uint64]bool{}
			if HeldForm(m) {
				named++
				for ctx := m.GetContext()[len(heldTag)+1:]; len(ctx) > 0; {
					i, rest, _ := uvarint(ctx)
					byName[i], ctx = true, rest
				}
			}
			for i, e := range m.Entries {
				if k, ok := entryKey(e.GetData()); ok && !byName[uint64(i)] && k.origin != 1 && e.GetTerm() == c.nodes[0].term {
					full++
				}
			}
		}
		return false
	}
	writeThroughFollowers(t, c, 50)
	m := c.nodes[0].Metrics()
	if m.HeldNamed == 0 || named == 0 {
		t.Fatalf("the leader named %d entries in %d appends", m.HeldNamed, named)
	}
	if full != 0 {
		t.Fatalf("the leader sent %d forwarded entries in full", full)
	}
	for _, n := range c.nodes[1:] {
		if m := n.Metrics(); m.HeldParked != 0 || m.HeldAsked != 0 {
			t.Fatalf("node %d parked %d appends and asked %d times with every side copy delivered", n.cfg.ID, m.HeldParked, m.HeldAsked)
		}
	}
}

// TestLostSideCopy drops every side copy. A follower then holds none of
// the entries the other one forwarded: it parks the appends that name
// them, never handing Raft an entry it lacks, asks the leader for them in
// full after heldWaitTicks, and gets them when Raft sends again.
func TestLostSideCopy(t *testing.T) {
	c := heldCluster(t)
	c.drop = sideCopy
	writeThroughFollowers(t, c, 50)
	asked := c.nodes[1].Metrics().HeldAsked + c.nodes[2].Metrics().HeldAsked
	if asked == 0 {
		t.Fatal("no follower asked for entries in full, though no side copy arrived")
	}
}

// TestLateSideCopy holds side copies back for a few steps: the appends
// that name their entries wait for them and then go on, with no request
// to send them in full.
func TestLateSideCopy(t *testing.T) {
	c := heldCluster(t)
	var late []*raftpb.Message
	c.drop = func(m *raftpb.Message) bool {
		if sideCopy(m) {
			late = append(late, m)
			return true
		}
		return false
	}
	id := uint64(0)
	for k := range 20 {
		id++
		c.nodes[1].Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: []byte("v")}})
	}
	// No ticks: the parked appends do not time out.
	for range 10 {
		c.step()
	}
	if c.nodes[2].Metrics().HeldParked == 0 {
		t.Fatal("node 3 parked no append while its side copies were held back")
	}
	for _, m := range late {
		if err := c.nodes[m.GetTo()-1].Step(m); err != nil {
			t.Fatal(err)
		}
	}
	late = nil
	c.drop = nil
	for step := 0; c.replies < int(id); step++ {
		if step > 200 {
			t.Fatalf("%d of %d writes answered: %s", c.replies, id, c.describe())
		}
		c.step()
	}
	// Heartbeats bring node 3 the commit index.
	for range 10 {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	if m := c.nodes[2].Metrics(); m.HeldAsked != 0 {
		t.Fatalf("node 3 asked for entries in full %d times, though its side copies came", m.HeldAsked)
	}
	if c.nodes[2].Hash() != c.nodes[0].Hash() || c.nodes[2].Applied() != c.nodes[0].Applied() {
		t.Fatalf("node 3 applied %d, the leader %d", c.nodes[2].Applied(), c.nodes[0].Applied())
	}
}

// TestOldPeerGetsNoHeldForms gives node 3 a link of an older protocol:
// nothing sent to it is a message of held entries, and it applies every
// write.
func TestOldPeerGetsNoHeldForms(t *testing.T) {
	c := heldCluster(t, 3)
	c.drop = func(m *raftpb.Message) bool {
		if m.GetTo() == 3 && HeldForm(m) {
			t.Fatalf("node %d sent node 3 a %s of held entries", m.GetFrom(), m.GetType())
		}
		return false
	}
	writeThroughFollowers(t, c, 20)
	if c.nodes[0].Metrics().HeldNamed == 0 {
		t.Fatal("the leader named no entry to node 2")
	}
}

// TestEntryKey checks that an entry's key reads back from its stub, and
// that an entry whose first write has no request ID has none: two such
// entries of one node and run could share it.
func TestEntryKey(t *testing.T) {
	for _, format := range []uint32{1, 2, MaxVersion} {
		data, err := encodeBatch(format, 7, 99, []*pending{{req: Request{ID: 42, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}}}}, 64)
		if err != nil {
			t.Fatal(err)
		}
		k, ok := entryKey(data)
		if !ok || k.origin != 7 || k.boot != 99 || k.id != 42 || int(k.size) != len(data) {
			t.Fatalf("format %d: key %+v, %v", format, k, ok)
		}
		if back, ok := readKey(k.append(nil)); !ok || back != k {
			t.Fatalf("format %d: key %+v reads back as %+v, %v", format, k, back, ok)
		}
		data, err = encodeBatch(format, 7, 99, []*pending{{req: Request{Op: &pb.PutRequest{Key: []byte("k")}}}}, 64)
		if err != nil {
			t.Fatal(err)
		}
		if k, ok := entryKey(data); ok {
			t.Fatalf("format %d: an entry whose first write has no request ID has key %+v", format, k)
		}
	}
}

// TestKeptBytesBounded checks that a node holds at most heldMaxBytes, and
// nothing past heldKeepTicks.
func TestKeptBytesBounded(t *testing.T) {
	n := &Node{}
	entry := make([]byte, 1<<20)
	for i := range 2 * heldMaxBytes >> 20 {
		n.keep(heldKey{id: uint64(i + 1), size: 1 << 20}, entry)
		if n.kept.bytes > heldMaxBytes || len(n.kept.data) != len(n.kept.order) {
			t.Fatalf("after %d entries: %d bytes held in %d entries, %d in order", i+1, n.kept.bytes, len(n.kept.data), len(n.kept.order))
		}
	}
	n.now += heldKeepTicks + 1
	n.heldTick()
	if n.kept.bytes != 0 || len(n.kept.data) != 0 {
		t.Fatalf("%d bytes still held %d ticks later", n.kept.bytes, heldKeepTicks+1)
	}
}

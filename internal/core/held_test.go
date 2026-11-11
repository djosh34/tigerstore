package core

import (
	"fmt"
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
)

// heldCluster starts three nodes in memory whose links all carry the
// messages of held entries, except those to or from the nodes in old,
// which speak an older protocol.
func heldCluster(t *testing.T, old ...uint64) *memCluster {
	return newMemCluster(t, func(cfg *Config) {
		self := cfg.ID
		cfg.HeldEntries = func(peer uint64) bool { return !slices.Contains(old, peer) && !slices.Contains(old, self) }
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
	if c.nodes[1].Metrics().HeldFilled+c.nodes[2].Metrics().HeldFilled == 0 {
		t.Fatal("no follower filled in an append from the entries it held")
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

// TestOldPeerGetsNoHeldForms makes node 3 speak an older protocol:
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

// TestAlarmHasNoKey checks that a NOSPACE alarm, which takes the request ID
// of the write it answers, has no key: a put of the same node, run and ID
// can match its size and checksum, as with the key "\x85f\x8fZ" against
// a 14-byte alarm, and a follower would apply the put in its place.
func TestAlarmHasNoKey(t *testing.T) {
	alarm := &pending{req: Request{ID: 42, Op: &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: 7}}}
	data, err := encodeBatch(MaxVersion, 7, 99, []*pending{alarm}, 64)
	if err != nil {
		t.Fatal(err)
	}
	if k, ok := entryKey(data); ok {
		t.Fatalf("a NOSPACE alarm has key %+v, which a write with its request ID shares", k)
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

// TestOldForwarderIsNotNamed makes node 2 speak an older protocol: it sends
// no side copies, so the leader names its entries to no one, and node 3
// never waits for one.
func TestOldForwarderIsNotNamed(t *testing.T) {
	c := heldCluster(t, 2)
	writeThroughFollowers(t, c, 20)
	if m := c.nodes[2].Metrics(); m.HeldParked != 0 || m.HeldAsked != 0 {
		t.Fatalf("node 3 parked %d appends and asked %d times for entries of a forwarder that sends no side copies", m.HeldParked, m.HeldAsked)
	}
	if c.nodes[0].Metrics().HeldNamed == 0 {
		t.Fatal("the leader named none of node 3's entries")
	}
}

// TestLearnerIsNotNamed checks that the leader names an entry another node
// proposed to no learner, which gets no side copies.
func TestLearnerIsNotNamed(t *testing.T) {
	c := heldCluster(t)
	leader := c.nodes[0]
	leader.state.members[4] = &pb.Member{ID: 4, IsLearner: true}
	data, err := encodeBatch(MaxVersion, 2, 1, []*pending{{req: Request{ID: 9, Op: &pb.PutRequest{Key: []byte("k")}}}}, 64)
	if err != nil {
		t.Fatal(err)
	}
	app := func(to uint64) *raftpb.Message {
		return &raftpb.Message{
			Type: raftpb.MsgApp.Enum(), From: new(uint64(1)), To: new(to),
			Entries: []*raftpb.Entry{{Term: new(leader.term), Index: new(uint64(100)), Type: raftpb.EntryNormal.Enum(), Data: data}},
		}
	}
	if m := leader.nameHeld(app(4)); HeldForm(m) {
		t.Fatal("the leader named node 2's entry to a learner")
	}
	if m := leader.nameHeld(app(3)); !HeldForm(m) {
		t.Fatal("the leader did not name node 2's entry to voter 3")
	}
}

// TestHeartbeatContextIsNotHeld checks that a heartbeat whose own context
// starts like a held form, as Raft's read positions such as 0x02485354
// encode, is no message of held entries and steps Raft.
func TestHeartbeatContextIsNotHeld(t *testing.T) {
	beat := &raftpb.Message{
		Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Context: []byte{'T', 'S', 'H', heldNamed, 0, 0, 0, 0},
	}
	if HeldForm(beat) {
		t.Fatal("a heartbeat counts as a message of held entries")
	}
	n := &Node{}
	if took, _ := n.stepHeld(beat); took {
		t.Fatal("stepHeld took a heartbeat")
	}
}

// TestForwarderForgets drops what node 2 held, as a restart does, while
// the leader's appends that name its entries are on their way: node 2
// waits, asks for them in full, and applies what the others do.
func TestForwarderForgets(t *testing.T) {
	c := heldCluster(t)
	var held []*raftpb.Message
	c.drop = func(m *raftpb.Message) bool {
		if m.GetTo() == 2 && HeldForm(m) && m.GetType() == raftpb.MsgApp {
			held = append(held, m)
			return true
		}
		return false
	}
	for k := range 10 {
		c.nodes[1].Submit(Request{ID: uint64(k + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: []byte("v")}})
	}
	for range 10 {
		c.step()
	}
	if len(held) == 0 {
		t.Fatal("the leader named no entry to node 2")
	}
	c.nodes[1].kept = keptEntries{}
	c.drop = nil
	for _, m := range held {
		if err := c.nodes[1].Step(m); err != nil {
			t.Fatal(err)
		}
	}
	for step := 0; c.replies < 10; step++ {
		if step > 500 {
			t.Fatalf("%d of 10 writes answered: %s", c.replies, c.describe())
		}
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	if c.nodes[1].Metrics().HeldAsked == 0 {
		t.Fatal("node 2 never asked for the entries it forgot")
	}
	for range 20 {
		c.step()
	}
	if c.nodes[1].Hash() != c.nodes[0].Hash() || c.nodes[1].Applied() != c.nodes[0].Applied() {
		t.Fatalf("node 2 applied %d, the leader %d", c.nodes[1].Applied(), c.nodes[0].Applied())
	}
}

// TestLeaderChangeWithNamedAppends holds node 3's side copies and the
// leader's named appends to it, then stops the leader so node 2 takes
// over. The old appends come to node 3 late and wait for entries it never
// gets; it drops them, and the new leader sends what it misses in full.
// Every write a client got an answer for is applied on both survivors.
func TestLeaderChangeWithNamedAppends(t *testing.T) {
	c := heldCluster(t)
	var stale []*raftpb.Message
	c.drop = func(m *raftpb.Message) bool {
		if m.GetTo() != 3 {
			return false
		}
		if sideCopy(m) {
			return true
		}
		if HeldForm(m) && m.GetType() == raftpb.MsgApp {
			stale = append(stale, m)
			return true
		}
		return false
	}
	for k := range 10 {
		c.nodes[1].Submit(Request{ID: uint64(k + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", k), Value: []byte("v")}})
	}
	for range 10 {
		c.step()
	}
	if len(stale) == 0 {
		t.Fatal("the leader named no entry to node 3")
	}
	// Node 2 answers a write once it applied it, so a node 3 that ends
	// with node 2's state holds every answered write.
	if c.replies == 0 {
		t.Fatal("no write was answered before the leader stopped")
	}
	c.down[0] = true
	c.drop = nil
	for step := 0; !c.nodes[1].isLeader; step++ {
		if step > 1000 {
			t.Fatalf("node 2 did not take over: %s", c.describe())
		}
		c.nodes[1].Tick()
		c.nodes[2].Tick()
		c.step()
	}
	for _, m := range stale {
		if err := c.nodes[2].Step(m); err != nil {
			t.Fatal(err)
		}
	}
	for range 50 {
		for i, n := range c.nodes {
			if !c.down[i] {
				n.Tick()
			}
		}
		c.step()
	}
	if c.nodes[2].Hash() != c.nodes[1].Hash() || c.nodes[2].Applied() != c.nodes[1].Applied() {
		t.Fatalf("node 3 applied %d, the new leader %d: %s", c.nodes[2].Applied(), c.nodes[1].Applied(), c.describe())
	}
}

// TestCutOffFollowerKeepsUp drops every side copy to node 3 while node 2
// writes without pause, as for a follower cut off from the forwarder but
// not from the leader. Node 3 asks for everything in full and gets it, so
// it keeps up while the writes go on, not only once they stop.
func TestCutOffFollowerKeepsUp(t *testing.T) {
	c := heldCluster(t)
	c.drop = func(m *raftpb.Message) bool { return m.GetTo() == 3 && sideCopy(m) }
	id := uint64(0)
	for step := range 400 {
		id++
		c.nodes[1].Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id%50), Value: []byte("v")}})
		if step%5 == 0 {
			for _, n := range c.nodes {
				n.Tick()
			}
		}
		c.step()
	}
	lead, behind := c.nodes[0].Applied(), c.nodes[2].Applied()
	t.Logf("after 400 writes, the leader applied %d, node 3 %d, and node 3 asked %d times", lead, behind, c.nodes[2].Metrics().HeldAsked)
	if behind+lead/10 < lead {
		t.Fatalf("node 3 applied %d while the leader applied %d: it falls behind while writes go on", behind, lead)
	}
}

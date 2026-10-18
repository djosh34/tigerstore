package peer

import (
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// nodeCerts issues a peer certificate for each ID.
func nodeCerts(t *testing.T, ca *testcert.CA, ids ...uint64) map[uint64]tls.Certificate {
	t.Helper()
	certs := map[uint64]tls.Certificate{}
	for _, id := range ids {
		c, _, _, err := ca.Issue("node", int64(id+1))
		if err != nil {
			t.Fatal(err)
		}
		certs[id] = c
	}
	return certs
}

// within waits up to 5 s for ok.
func within(ok func() bool) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ok() {
			return true
		}
	}
	return ok()
}

func closeAll(t *testing.T, ends ...*end) {
	for _, e := range ends {
		if err := e.t.Close(); err != nil {
			t.Error(err)
		}
	}
}

// TestSilentRemovedMemberHearsRemoval is #263's case: member 4 has a
// connection to node 1 and nothing to send, as a learner with no leader.
// Node 1 removes it and closes the connection; member 4 must see that,
// dial again and hear that it was removed, though it sends nothing.
func TestSilentRemovedMemberHearsRemoval(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := nodeCerts(t, ca, 1, 4)
	addrs, lns := listeners(t, 1, 4)
	a := start(t, 1, lns[1], addrs, certs[1], ca.Pool)
	var removed atomic.Int64
	d := startWith(t, 4, lns[4], addrs, certs[4], ca.Pool, func(c *Config) { c.Removed = func() { removed.Add(1) } })
	defer closeAll(t, a, d)
	if !within(func() bool { return a.t.Versions().In[4] != 0 }) {
		t.Fatal("member 4 never connected to node 1")
	}
	a.t.SetPeers(map[uint64]string{}, []uint64{4})
	if !within(func() bool { return removed.Load() > 0 }) {
		t.Fatal("the silent member never heard of its removal")
	}
}

// TestUnknownMemberGetsReplies is #250's case: node 2 is behind on
// membership and does not know member 4, which asks it for a vote. As in
// etcd, node 2 takes the message from a member it has not removed and
// replies to the address in member 4's preface. The route lasts through
// an unrelated change of node 2's peers, and once node 2 removes member 4,
// member 4 hears it.
func TestUnknownMemberGetsReplies(t *testing.T) {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	certs := nodeCerts(t, ca, 2, 3, 4)
	addrs, lns := listeners(t, 2, 3, 4)
	b := startWith(t, 2, lns[2], addrs, certs[2], ca.Pool, func(c *Config) {
		c.Peers, c.Addr = map[uint64]string{3: addrs[3]}, addrs[2]
	})
	var removed atomic.Int64
	d := startWith(t, 4, lns[4], addrs, certs[4], ca.Pool, func(c *Config) {
		c.Peers, c.Addr = map[uint64]string{2: addrs[2]}, addrs[4]
		c.Removed = func() { removed.Add(1) }
	})
	defer closeAll(t, b, d)
	vote := func(term uint64) {
		t.Helper()
		d.t.Send(&raftpb.Message{Type: raftpb.MsgVote.Enum(), From: new(uint64(4)), To: new(uint64(2)), Term: new(term)})
		if m := receive(t, b, 1)[0]; m.GetType() != raftpb.MsgVote || m.GetFrom() != 4 {
			t.Fatalf("node 2 got %v, want member 4's vote request", m)
		}
		b.t.Send(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(4)), Term: new(term)})
		if m := receive(t, d, 1)[0]; m.GetType() != raftpb.MsgVoteResp || m.GetFrom() != 2 {
			t.Fatalf("member 4 got %v, want node 2's vote", m)
		}
	}
	vote(2)
	b.t.SetPeers(map[uint64]string{2: addrs[2], 3: addrs[3]}, nil)
	vote(3)
	b.t.SetPeers(map[uint64]string{2: addrs[2], 3: addrs[3]}, []uint64{4})
	if !within(func() bool { return removed.Load() > 0 }) {
		t.Fatal("member 4 never heard that node 2 removed it")
	}
}

package core

import (
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestReadsStopAtTheTurnsEnd gives a node many reads at once with a turn
// that is over after a few of them: each Process serves only those, keeps
// the rest, and reports Working, and the next ones serve them all (#660).
func TestReadsStopAtTheTurnsEnd(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, nil)
	for i := range 20 {
		if r := s.do(uint64(i+1), &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: []byte("v")}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	const room, reads = 3, 40
	asked := 0
	s.n.SetTurnOver(func() bool { asked++; return asked > room })
	most, turns := 0, 0
	s.afterTurn = func(eff Effects) {
		most, asked = max(most, len(eff.Replies)), 0
		turns++
	}
	for i := range reads {
		s.n.Submit(Request{ID: uint64(100 + i), Op: &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}})
	}
	got := 0
	for _, r := range s.run() {
		if r.Err != nil {
			t.Fatalf("read %d: %v", r.ID, r.Err)
		}
		if len(r.Range.KVs) != 20 {
			t.Fatalf("read %d got %d keys, want 20", r.ID, len(r.Range.KVs))
		}
		got++
	}
	if got != reads || most > room {
		t.Fatalf("%d of %d reads answered in %d turns, at most %d in one; want all, at most %d a turn", got, reads, turns, most, room)
	}
}

// TestAppliesStopAtTheTurnsEnd lets a follower's disk hold back many
// entries, then lets them go with a turn that is over after a few
// applies: each Process applies only those, and the follower catches up
// over the next turns (#660).
func TestAppliesStopAtTheTurnsEnd(t *testing.T) {
	c := newMemCluster(t, func(cfg *Config) { cfg.MaxBatchBytes = 256 })
	leader, follower := c.nodes[0], c.nodes[1]
	c.holdDisk[1] = true
	for i := range 200 {
		leader.Submit(Request{ID: uint64(i + 1), Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", i), Value: make([]byte, 200)}})
		c.step()
	}
	for range 20 {
		c.step()
	}
	want := leader.Applied()
	if follower.Applied() >= want {
		t.Fatalf("the follower applied %d with its disk held, the leader %d", follower.Applied(), want)
	}
	const room = 4
	asked := 0
	follower.SetTurnOver(func() bool { asked++; return asked > room })
	c.holdDisk[1] = false
	most := uint64(0)
	for step := 0; follower.Applied() < want; step++ {
		before := follower.Applied()
		asked = 0
		c.step()
		most = max(most, follower.Applied()-before)
		if step > 1000 {
			t.Fatalf("the follower applied %d of %d after %d steps: %s", follower.Applied(), want, step, c.describe())
		}
	}
	if most > room {
		t.Fatalf("the follower applied %d entries in one turn that was over after %d", most, room)
	}
	if follower.Hash() != leader.Hash() {
		t.Fatal("the follower's state differs from the leader's once it caught up")
	}
}

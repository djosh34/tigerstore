package sim

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestLivenessExcusesOnlyAnEndedInstall checks the liveness rule's excuse
// for a snapshot install (#775's ruling): a call that failed while its
// node installed a snapshot is excused, one after the install ended is
// not, and neither is one during an install that never ended.
func TestLivenessExcusesOnlyAnEndedInstall(t *testing.T) {
	s, err := newSim(DefaultConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.LiveFrom = 1
	// Enough calls that the rule's count holds, then one that timed out
	// on node 1.
	calls := func(failAt int64) *Result {
		res := &Result{}
		for i := range 60 {
			at := int64(10+i) * Second
			res.History = append(res.History, &Record{Client: 0, Node: 1, Call: at, Return: at + Millisecond})
		}
		res.History = append(res.History, &Record{Client: 1, Node: 0, Call: failAt, Return: failAt + 3*Second, Err: errClientTimeout})
		return res
	}
	n := s.nodes[0]
	for _, c := range []struct {
		name     string
		installs [][2]int64
		failAt   int64
		excused  bool
	}{
		{"no install", nil, 2 * Second, false},
		{"during an install that ended", [][2]int64{{Second, 5 * Second}}, 2 * Second, true},
		{"from before an install that ended", [][2]int64{{3 * Second, 9 * Second}}, 2 * Second, true},
		{"after the install ended", [][2]int64{{Second, 5 * Second}}, 6 * Second, false},
		{"during an install that never ended", [][2]int64{{Second, 0}}, 2 * Second, false},
	} {
		n.installs = c.installs
		err := s.checkLive(calls(c.failAt))
		if got := err == nil; got != c.excused {
			t.Errorf("%s: the rule gave %v, want excused %v", c.name, err, c.excused)
		}
		if err != nil && !strings.Contains(err.Error(), "client 1 call") {
			t.Errorf("%s: the rule failed on another call: %v", c.name, err)
		}
	}
}

// TestLivenessExcusesASlowInstall is #775's liveness failure in the
// small: a follower that restarts behind the leader's compaction installs
// a snapshot, which takes 5 s (Config.installWait), and a client pinned to
// it calls on. Its loop answers no client until the install ends, so
// calls time out then; the liveness rule excuses them while the node
// installs, and holds it to every call after.
func TestLivenessExcusesASlowInstall(t *testing.T) {
	if simrun.Drive(t, 811) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(811)
		cfg.Faults = Faults{DiskBudget: 1 << 20}
		cfg.Catchup = 4
		cfg.installWait = 5 * Second
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if t.Failed() {
				explainSim(t, cfg, s)
			}
			s.shutdown()
		}()
		for _, n := range s.nodes {
			if err := s.start(n); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(3 * time.Second)
		s.mu.Lock()
		lead := s.leaderIdx()
		if lead < 0 {
			s.mu.Unlock()
			t.Fatal("no leader after 3 s")
		}
		behind := s.nodes[(lead+1)%3]
		s.kill(behind)
		s.mu.Unlock()
		leader := s.nodes[lead]
		for i := range 64 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := s.callWL(leader.proc, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 128<<10)}, ctx)
			cancel()
			if err != nil {
				t.Fatalf("put %d through node %d: %v", i, leader.id, err)
			}
		}
		// Time for the leader to checkpoint and let its log go.
		time.Sleep(5 * time.Second)
		if err := s.start(behind); err != nil {
			t.Fatal(err)
		}
		// The rule holds the node from when it is back: it knows its
		// leader, or it began the install, which the leader's snapshot
		// starts.
		for {
			s.mu.Lock()
			back := behind.proc != nil && (behind.proc.leader() != 0 || len(behind.installs) > 0)
			if back {
				s.cfg.LiveFrom = s.now()
			}
			s.mu.Unlock()
			if back {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		// A client pinned to the node behind, for 15 s: through the
		// install, and on after it.
		c := &client{id: 0, sim: s}
		failed := 0
		for i := 0; ; i++ {
			s.mu.Lock()
			now := s.now()
			s.mu.Unlock()
			if now >= s.cfg.LiveFrom+15*Second {
				break
			}
			r := &Record{Client: c.id, Node: behind.idx, Call: now, req: &pb.PutRequest{Key: fmt.Appendf(nil, "c%d", i), Value: []byte("v")}}
			resp, err := s.call(behind, r.req, 3*Second)
			s.mu.Lock()
			c.n = i
			c.done(r, resp, err)
			s.mu.Unlock()
			if err != nil {
				failed++
			}
			time.Sleep(50 * time.Millisecond)
		}
		s.mu.Lock()
		installs := append([][2]int64(nil), behind.installs...)
		s.mu.Unlock()
		if len(installs) == 0 || installs[0][1] == 0 || installs[0][1]-installs[0][0] < 5*Second {
			t.Fatalf("node %d installs %v: want one that took the 5 s it was held", behind.id, installs)
		}
		if failed == 0 {
			t.Fatal("no call failed while the node installed its snapshot: the test tests nothing")
		}
		var history []*Record
		simrun.Aside(func() { history, err = s.records.records() })
		if err != nil {
			t.Fatal(err)
		}
		if err := s.checkLive(&Result{History: history}); err != nil {
			t.Fatalf("%d calls failed, during the install that took %s: %v", failed, seconds(installs[0][1]-installs[0][0]), err)
		}
		// Without the install's excuse, the rule fails on those calls.
		behind.installs = nil
		if err := s.checkLive(&Result{History: history}); err == nil {
			t.Fatal("the rule passed the calls that failed during the install without its excuse")
		}
	})
}

package sim

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// TestLivenessExcusesOnlyAnEndedInstall checks the liveness rule's excuse
// for a snapshot (#775's rulings): a call that failed and overlapped the
// span from when a snapshot reached its node to when the node caught up
// to the leader's commit as of its install is excused, if it caught up; a
// call that ended before the span began or started after the catch-up is
// not, nor one during a span that never caught up or never installed, nor
// one with no span.
func TestLivenessExcusesOnlyAnEndedInstall(t *testing.T) {
	s, err := newSim(DefaultConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	s.cfg.LiveFrom = 1
	// Enough calls that the rule's count holds, then one that timed out
	// on node 1.
	calls := func(failAt int64, failed error) *Result {
		res := &Result{}
		for i := range 60 {
			at := int64(10+i) * Second
			res.History = append(res.History, &Record{Client: 0, Node: 1, Call: at, Return: at + Millisecond})
		}
		res.History = append(res.History, &Record{Client: 1, Node: 0, Call: failAt, Return: failAt + 3*Second, Err: failed})
		return res
	}
	n := s.nodes[0]
	span := func(from, installed, caughtUp int64) []installSpan {
		return []installSpan{{from: from * Second, installed: installed * Second, caughtUp: caughtUp * Second}}
	}
	for _, c := range []struct {
		name     string
		installs []installSpan
		failAt   int64
		failed   error
		excused  bool
	}{
		{"no span", nil, 2 * Second, errClientTimeout, false},
		{"while the snapshot arrived and installed", span(1, 5, 7), 2 * Second, errClientTimeout, true},
		{"after the install, before the catch-up", span(1, 3, 7), 4 * Second, errClientTimeout, true},
		{"from before the span, into it", span(3, 6, 9), 2 * Second, errClientTimeout, true},
		{"another failure in the span", span(1, 5, 7), 2 * Second, errConnLost, true},
		{"ended before the span began", span(6, 8, 9), 2 * Second, errClientTimeout, false},
		{"after the catch-up", span(1, 3, 5), 6 * Second, errClientTimeout, false},
		{"installed, never caught up", span(1, 3, 0), 4 * Second, errClientTimeout, false},
		{"never installed", span(1, 0, 0), 2 * Second, errClientTimeout, false},
	} {
		n.installs = c.installs
		err := s.checkLive(calls(c.failAt, c.failed))
		if got := err == nil; got != c.excused {
			t.Errorf("%s: the rule gave %v, want excused %v", c.name, err, c.excused)
		}
		if err != nil && !strings.Contains(err.Error(), "client 1 call") {
			t.Errorf("%s: the rule failed on another call: %v", c.name, err)
		}
	}
}

// pinnedToAnInstall runs a cluster in which a follower stopped while the
// leader compacted past it restarts and installs a snapshot, held wait
// on its loop (Config.installWait), while a client pinned to it calls for
// 15 s from when it is back, and on until 60 more completed once the
// install caught up. It returns the run, the node, the calls'
// history and how many failed; the caller shuts the run down.
func pinnedToAnInstall(t *testing.T, seed uint64, wait int64) (s *Sim, behind *node, history []*Record, failed int) {
	t.Helper()
	cfg := DefaultConfig(seed)
	// A ring of 2 MiB: the 4 entries of 128 KiB the leader keeps for the
	// stopped follower fit it, which a ring of 512 KiB did not (#814).
	cfg.Faults = Faults{DiskBudget: 4 << 20}
	cfg.Catchup = 4
	cfg.installWait = wait
	s, err := newSim(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			explainSim(t, cfg, s)
		}
		s.shutdown()
	})
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
	behind = s.nodes[(lead+1)%3]
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
	// The rule holds the node from when it is back: it knows its leader,
	// or it began the install, which the leader's snapshot starts.
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
	// The calls go on for 15 s, and, once the install caught up, until 60
	// more completed: the rule counts 50 after healing, which a slow
	// install, as under the race detector, left too few of in 15 s.
	c := &client{id: 0, sim: s}
	okAfter := 0
	for i := 0; ; i++ {
		s.mu.Lock()
		now := s.now()
		caughtUp := slices.ContainsFunc(behind.installs, func(sp installSpan) bool { return sp.caughtUp != 0 })
		s.mu.Unlock()
		if now >= s.cfg.LiveFrom+15*Second && (okAfter >= 60 || !caughtUp) || now >= s.cfg.LiveFrom+120*Second {
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
		} else if caughtUp {
			okAfter++
		}
		time.Sleep(50 * time.Millisecond)
	}
	simrun.Aside(func() { history, err = s.records.records() })
	if err != nil {
		t.Fatal(err)
	}
	return s, behind, history, failed
}

// TestLivenessExcusesASlowInstall is #775's liveness failure in the
// small: a follower that restarts behind the leader's compaction installs
// a snapshot, which takes 5 s, and a client pinned to it calls on. Its
// loop answers no client until the install ends, so calls time out then;
// the liveness rule excuses them while the node installs, and holds it to
// every call after.
func TestLivenessExcusesASlowInstall(t *testing.T) {
	if simrun.Drive(t, 811) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		s, behind, history, failed := pinnedToAnInstall(t, 811, 5*Second)
		// The nodes hold still for the checks: no hook moves the installs.
		if err := s.freeze(); err != nil {
			t.Fatal(err)
		}
		installs := append([]installSpan(nil), behind.installs...)
		if len(installs) == 0 || installs[0].installed-installs[0].from < 5*Second || installs[0].caughtUp == 0 {
			t.Fatalf("node %d snapshots %+v: want one held its 5 s, and caught up", behind.id, installs)
		}
		if failed == 0 {
			t.Fatal("no call failed while the node installed its snapshot: the test tests nothing")
		}
		if err := s.checkLive(&Result{History: history}); err != nil {
			t.Fatalf("%d calls failed, during the snapshot that took %s to catch up: %v", failed, seconds(installs[0].caughtUp-installs[0].from), err)
		}
		// Without the install's excuse, the rule fails on those calls'
		// timeouts, as the old rule did.
		behind.installs = nil
		if err := s.checkLive(&Result{History: history}); !errors.Is(err, errClientTimeout) {
			t.Fatalf("without the install's excuse, the rule gave %v, want a timed-out call to node %d", err, behind.id)
		}
	})
}

// TestLivenessHoldsAnInstallThatNeverEnds runs the same, with the install
// held past the calls' end: it never ends, so the rule excuses none of the
// calls that timed out while it went on, and the report says so.
func TestLivenessHoldsAnInstallThatNeverEnds(t *testing.T) {
	if simrun.Drive(t, 812) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		s, behind, history, failed := pinnedToAnInstall(t, 812, 3600*Second)
		// Its loop waits in the install, so it can't be frozen.
		s.mu.Lock()
		installs := append([]installSpan(nil), behind.installs...)
		s.mu.Unlock()
		report := s.report()
		if len(installs) == 0 || installs[len(installs)-1].installed != 0 {
			t.Fatalf("node %d snapshots %+v: want one whose install never ended", behind.id, installs)
		}
		if failed == 0 {
			t.Fatal("no call failed while the node installed its snapshot: the test tests nothing")
		}
		if err := s.checkLive(&Result{History: history}); !errors.Is(err, errClientTimeout) {
			t.Fatalf("calls that timed out during an install that never ended: the rule gave %v, want a timed-out call to node %d", err, behind.id)
		}
		if !strings.Contains(report, "its install never ended") {
			t.Fatalf("the report does not say the install never ended:\n%s", report)
		}
	})
}

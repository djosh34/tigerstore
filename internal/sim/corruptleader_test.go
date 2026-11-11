package sim

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestFenceCorruptLeader deliberately makes a follower fetch an old record
// from a damaged leader. Every seed must fence that leader after its serve
// fails, including single-seed replays and race runs. The ordinary workload
// and all of its checkers run after the scenario.
func TestFenceCorruptLeader(t *testing.T) {
	simrun.Seeds(t, 1, 8, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.Duration, cfg.Heal = 20*Second, 8*Second
		cfg.SegmentBytes = 512
		cfg.keepLog, cfg.beforeClients = true, corruptLeaderScenario
		res := run(t, cfg)
		checkers(t, res)
		if res.Stats.FencedAfterServe != 1 {
			t.Fatalf("seed %d: fenced after serve = %d, want 1", seed, res.Stats.FencedAfterServe)
		}
		t.Logf("seed %d: fenced after serve %d, failed serves %d, cold restarts %d", seed, res.Stats.FencedAfterServe, res.Stats.ServeFailures, res.Stats.ColdRestarts)
		return res.Stats
	}, nil)
}

// corruptLeaderScenario keeps checkpoints off, as TestServeCorruptRecordStops
// does: no background checkpoint or cleaner may read the damage first. Raft
// still uses tinyLimits, so a follower held down past the window must fetch
// a snapshot. The snapshot streams the retained WAL records directly.
func corruptLeaderScenario(s *Sim) error {
	// Every wait checks a state predicate, with a deadline only for failure.
	wait := func(what string, ready func() bool) error {
		until := s.now() + 10*Second
		for {
			s.mu.Lock()
			err, ok := s.err, ready()
			s.mu.Unlock()
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			if s.now() >= until {
				return fmt.Errorf("waiting for %s at %d ms", what, s.now()/Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := wait("a leader", func() bool { return s.leaderIdx() >= 0 }); err != nil {
		return err
	}
	s.mu.Lock()
	leader := s.nodes[s.leaderIdx()]
	follower := s.nodes[(leader.idx+1)%3]
	s.down(follower)
	s.tracef("scenario: held node %d down; leader is node %d", follower.id, leader.id)
	s.mu.Unlock()

	// Use the normal client history path so the writes remain subject to
	// the log, real-time, witness and linearizability checkers.
	c := &client{id: 0, sim: s, node: -1}
	var old uint64
	for i := range 40 {
		time.Sleep(time.Nanosecond)
		req := &pb.PutRequest{Key: fmt.Appendf(nil, "scenario/%d", i), Value: append(fmt.Appendf(nil, "scenario-value-%d/", i), bytes.Repeat([]byte("v"), 256)...)}
		s.mu.Lock()
		r := &Record{Client: c.id, Node: leader.idx, Call: s.now(), req: req}
		s.mu.Unlock()
		resp, err := s.call(leader, req, s.cfg.ClientTimeout)
		s.mu.Lock()
		c.done(r, resp, err)
		if i == 0 && err == nil {
			old = leader.proc.core.Applied()
		}
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("scenario put %d: %w", i, err)
		}
	}
	if err := wait("the old entry to leave the leader's window and the healthy follower to catch up", func() bool {
		healthy := s.nodes[(leader.idx+2)%3]
		return leader.proc != nil && leader.proc.core.FirstIndex() > old+2 &&
			healthy.proc != nil && healthy.proc.core != nil && healthy.proc.core.Applied() >= leader.proc.core.Applied()
	}); err != nil {
		return err
	}

	s.mu.Lock()
	w, _, err := wal.Open(leader.disk.Durable())
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("opening the leader's durable log: %w", err)
	}
	seq, at, ok := w.ValuePlace(old, 0, 0)
	if err := w.Close(); err != nil {
		s.mu.Unlock()
		return err
	}
	name := wal.SegmentName(seq)
	if !ok || name == newestSegment(leader.disk) {
		s.mu.Unlock()
		return fmt.Errorf("old entry %d is not in a sealed segment", old)
	}
	// Rot changes the running and durable image. The record CRC fails on
	// serve and again on restart. Mark the exact record so exited accepts
	// only the expected corruption stop, not an unrelated process failure.
	leader.disk.Rot(name, int(at))
	leader.corrupted, leader.rotted = true, true
	leader.rotFile, leader.rotEntry, leader.rotSeg = name, old, seq
	s.stats.Rots++
	s.tracef("scenario: damaged entry %d in %s on leader %d; follower %d resumes", old, name, leader.id, follower.id)
	s.mu.Unlock()
	if err := s.start(follower); err != nil {
		return err
	}
	return wait("the leader to be fenced after serving damage", func() bool {
		return leader.fenced && leader.servedDamage && s.stats.FencedAfterServe == 1
	})
}

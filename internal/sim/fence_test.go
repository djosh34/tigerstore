package sim

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestSnapshotFromCorruptLeader makes a damaged leader serve a snapshot:
// a follower held down falls behind the leader's window, a record the
// snapshot streams rots on the leader's disk, and the follower comes back
// and fetches. The serve must find the damage and stop the leader, whose
// next start refuses its disk, so the follower catches up from the
// healthy node instead of reloading on every fetch (#201, campaign seed
// 5038431). Every seed must fence the leader after the failed serve, a
// replay of one seed and a run under the race detector too, and pass the
// checkers. It used to search seeds for one in which elections did this,
// and every change to the schedule moved that seed (#569).
func TestSnapshotFromCorruptLeader(t *testing.T) {
	simrun.Seeds(t, 1, 8, func(t *testing.T, seed uint64) Stats {
		cfg := DefaultConfig(seed)
		cfg.Faults = Faults{}
		cfg.Duration, cfg.Heal = 20*Second, 8*Second
		cfg.SegmentBytes = 512
		cfg.noCheckpoints, cfg.scenario = true, serveDamage
		res := run(t, cfg)
		checkers(t, res)
		if res.Stats.FencedAfterServe != 1 {
			t.Fatalf("seed %d: %d nodes fenced after a serve failed on their damage, want 1", seed, res.Stats.FencedAfterServe)
		}
		return res.Stats
	}, nil)
}

// serveDamage holds a follower down while the leader takes 40 puts, waits
// until the first of them left the leader's window of a few entries
// (tinyLimits) and the other follower has them all, rots that first entry
// in its sealed segment on the leader's disk, and starts the follower
// again. Its catch-up is a snapshot from the leader, which streams that
// record. The run has no checkpoints, so that nothing else reads the
// damage first.
func serveDamage(s *Sim) error {
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
				return fmt.Errorf("scenario: no %s by %d ms", what, s.now()/Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err := wait("leader", func() bool { return s.leaderIdx() >= 0 }); err != nil {
		return err
	}
	s.mu.Lock()
	leader := s.nodes[s.leaderIdx()]
	behind, healthy := s.nodes[(leader.idx+1)%3], s.nodes[(leader.idx+2)%3]
	s.down(behind)
	s.tracef("scenario: node %d held down; node %d leads", behind.id, leader.id)
	s.mu.Unlock()

	// The puts go through the history, so the checkers check them too.
	c := &client{id: 0, sim: s, node: -1}
	for i := range 40 {
		// Consecutive calls get distinct times.
		time.Sleep(time.Nanosecond)
		req := &pb.PutRequest{Key: fmt.Appendf(nil, "scenario/%d", i), Value: append(fmt.Appendf(nil, "value %d/", i), bytes.Repeat([]byte("v"), 256)...)}
		s.mu.Lock()
		r := &Record{Client: c.id, Node: leader.idx, Call: s.now(), req: req}
		s.mu.Unlock()
		resp, err := s.call(leader, req, s.cfg.ClientTimeout)
		s.mu.Lock()
		c.done(r, resp, err)
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("scenario: put %d: %w", i, err)
		}
	}
	// The entry of the first put, from the leader's log on disk: what the
	// scenario damages must be that put, whatever else the log holds.
	s.mu.Lock()
	first, err := entryOf(leader, []byte("scenario/0"))
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if err := wait("first put out of the leader's window with the healthy follower caught up", func() bool {
		return leader.proc != nil && leader.last.first > first+2 &&
			healthy.proc != nil && healthy.last.applied >= leader.last.applied
	}); err != nil {
		return err
	}

	s.mu.Lock()
	w, _, err := wal.Open(leader.disk.Durable())
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("scenario: opening the leader's log: %w", err)
	}
	seq, at, ok := w.ValuePlace(first, 0, 0)
	if err := w.Close(); err != nil {
		s.mu.Unlock()
		return err
	}
	name := wal.SegmentName(seq)
	if !ok || name == newestSegment(leader.disk) {
		s.mu.Unlock()
		return fmt.Errorf("scenario: entry %d is in no sealed segment", first)
	}
	// The rot shows in what the running node reads and on its disk, so
	// the serve and the next start both find it. The record says which
	// entry it is, so that only the stop it causes counts as expected.
	leader.disk.Rot(name, int(at))
	leader.corrupted, leader.rotted = true, true
	leader.rotFile, leader.rotEntry, leader.rotSeg = name, first, seq
	s.stats.Rots++
	s.tracef("scenario: entry %d rotted in %s on node %d; node %d comes back", first, name, leader.id, behind.id)
	s.mu.Unlock()
	if err := s.start(behind); err != nil {
		return err
	}
	return wait("fence of the leader after a failed serve", func() bool {
		return leader.fenced && leader.servedDamage && s.stats.FencedAfterServe == 1
	})
}

// entryOf returns the index of the first entry in n's log on disk that
// holds key. The lock is held.
func entryOf(n *node, key []byte) (index uint64, err error) {
	w, rec, err := wal.Open(n.disk.Durable())
	if err != nil {
		return 0, fmt.Errorf("scenario: opening node %d's log: %w", n.id, err)
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	for i := rec.Root.Index + 1; i <= rec.Last; i++ {
		e, err := w.Entry(i)
		if err != nil {
			return 0, fmt.Errorf("scenario: entry %d of node %d: %w", i, n.id, err)
		}
		if bytes.Contains(e.GetData(), key) {
			return i, nil
		}
	}
	return 0, fmt.Errorf("scenario: no entry of node %d holds %q", n.id, key)
}

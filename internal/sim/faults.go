package sim

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

func (s *Sim) scheduleDiskFaults() {
	f := s.cfg.Faults
	if f.DiskErrorEvery > 0 {
		s.push(&event{at: s.exp(f.DiskErrorEvery), kind: evDiskFault})
		// One more fault, due early in the faults and armed on the next
		// append with entries of any node, so that no run goes without: a
		// run that lost a node to corruption, or holds a partition, may
		// append nothing for many seconds. It stays due after healing.
		// TestSeeds checks that one fired.
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration/4), kind: evDiskFaultOnce})
	}
	if f.StallEvery > 0 {
		s.push(&event{at: s.exp(f.StallEvery), kind: evStall})
	}
	if f.Checkpoint {
		s.push(&event{at: s.cfg.Duration/2 + s.rng.Int64N(s.cfg.Duration/2) + 1, kind: evCheckpoint})
	}
}

// upNode picks a random node that is up, or nil.
func (s *Sim) upNode() *node {
	var up []*node
	for _, n := range s.nodes {
		if n.up {
			up = append(up, n)
		}
	}
	if len(up) == 0 {
		return nil
	}
	return up[s.rng.IntN(len(up))]
}

// injectDiskFault makes the next write or sync of a node fail: EIO on a
// sync, EIO on a write, or a short write with ENOSPC. It reports whether
// a node was up to take it.
func (s *Sim) injectDiskFault() bool {
	if s.healed {
		return false
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	s.armDiskFault(n, s.rng.IntN(3))
	return true
}

// armDiskFault arms a disk fault on n: EIO on the next sync (kind 0) or
// write (1), or a short write with ENOSPC (2).
func (s *Sim) armDiskFault(n *node, kind int) {
	s.mix(0xdf, uint64(n.idx), uint64(kind))
	switch kind {
	case 0, 1:
		target := simfs.OpSync
		if kind == 1 {
			target = simfs.OpWrite
		}
		d := n.disk
		d.Fault = func(op simfs.Op, _ string) error {
			if op != target {
				return nil
			}
			d.Fault = nil
			return simfs.ErrIO
		}
	case 2:
		n.disk.ShortWrite(s.rng)
	}
}

// injectStall holds back a random node's disk writes for 1.5 to 4 s.
func (s *Sim) injectStall() {
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		s.stall(n, s.now+1500*Millisecond+s.rng.Int64N(2500*Millisecond))
	}
	s.push(&event{at: s.exp(s.cfg.Faults.StallEvery), kind: evStall})
}

// stallLeader holds back the leader's disk writes until healing.
func (s *Sim) stallLeader() {
	for _, n := range s.nodes {
		if n.up && n.core.Leader() == n.id {
			s.stall(n, s.healAt)
		}
	}
}

func (s *Sim) stall(n *node, until int64) {
	n.stallUntil = until
	n.stalls = append(n.stalls, [2]int64{s.now, until})
	s.mix(0x57, uint64(n.idx), uint64(until))
}

// diskFailed handles a failed write or sync. Only injected faults may
// fail; the node stops at once and never retries, as a failed fsync
// leaves the file in an unknown state.
func (s *Sim) diskFailed(n *node, err error) {
	if !errors.Is(err, simfs.ErrIO) && !errors.Is(err, simfs.ErrNoSpace) {
		s.fail("node %d disk: %w", n.id, err)
		return
	}
	s.stats.DiskFaults++
	if s.healed {
		// The guaranteed fault fired after healing. The liveness rule
		// starts once the node is back (check.go); the clients keep going
		// for 3 s more than that, so the rule has calls to check.
		s.lateFaultAt = s.now
		if live := s.liveFrom() + 3*Second; live > s.stopAt {
			s.endAt += live - s.stopAt
			s.stopAt = live
		}
	}
	n.disk.Fault = nil
	s.stopNode(n)
}

// stopNode stops a node like a crash and restarts it later from its disk.
func (s *Sim) stopNode(n *node) {
	s.crash(n)
	s.push(&event{at: s.now + 1 + s.rng.Int64N(max(s.cfg.Faults.MaxDown, Second)), kind: evRestart, node: n.idx, gen: n.gen})
}

// maybeCorrupt damages the synced bytes of a node that just crashed, at
// most once per run and only if no node is damaged yet, so a quorum stays.
func (s *Sim) maybeCorrupt(n *node) {
	if !s.cfg.Faults.Corrupt || s.healed || s.rng.IntN(5) != 0 {
		return
	}
	for _, m := range s.nodes {
		if m.corrupted {
			return
		}
	}
	if n.disk.Corrupt(s.rng) {
		n.corrupted = true
		s.mix(0xc0, uint64(n.idx))
	}
}

// checkpoint is a copy of every disk as a power cut would leave it, the
// time it was taken, and the highest index any node had applied by then.
type checkpoint struct {
	at      int64
	applied uint64
	disks   []*simfs.Disk
	bad     []bool
}

func (s *Sim) newCheckpoint() *checkpoint {
	c := &checkpoint{at: s.now, applied: s.applied}
	for _, n := range s.nodes {
		c.disks = append(c.disks, n.disk.Durable())
		c.bad = append(c.bad, n.corrupted)
	}
	return c
}

func (s *Sim) takeCheckpoint() {
	s.checkpoints = append(s.checkpoints, s.newCheckpoint())
}

// checkColdRestarts erases each node's disk in turn at every checkpoint
// and at the end, and cold-starts the other two alone. Every entry applied
// anywhere by the checkpoint is committed, so the two must apply at least
// as far, and every such entry must be the one in the final log. An
// acknowledged write was applied before it was answered, so this covers
// every acknowledged write, delete, lease change and its revision. That
// is the guarantee that an acknowledged write survives the loss of one
// node.
func (s *Sim) checkColdRestarts(res *Result) error {
	final, err := s.live()[0].core.AppliedEntries()
	if err != nil {
		return err
	}
	for _, c := range append(slices.Clone(s.checkpoints), s.newCheckpoint()) {
		for erased := range c.disks {
			skip := false
			for i, bad := range c.bad {
				// A damaged disk cannot stand in for the erased one.
				skip = skip || (bad && i != erased)
			}
			if skip {
				continue
			}
			ents, err := s.coldRestart(c, erased)
			if err == nil {
				err = sameCommitted(ents, final, c.applied)
			}
			if err != nil {
				return fmt.Errorf("erasing node %d at %d ms: %w", erased+1, c.at/Millisecond, err)
			}
			res.Stats.ColdRestarts++
		}
	}
	return nil
}

// sameCommitted checks that ents reach index applied and match the final
// log up to it.
func sameCommitted(ents, final []*raftpb.Entry, applied uint64) error {
	if uint64(len(ents)) < applied {
		return fmt.Errorf("the two left apply %d entries, but %d were applied before", len(ents), applied)
	}
	for i := range applied {
		a, b := ents[i], final[i]
		if a.GetIndex() != i+1 || a.GetTerm() != b.GetTerm() || !bytes.Equal(a.GetData(), b.GetData()) {
			return fmt.Errorf("entry %d differs from the final log: term %d, want %d", i+1, a.GetTerm(), b.GetTerm())
		}
	}
	return nil
}

// coldRestart starts the two nodes whose disks were not erased, with no
// clients and no faults, until they have applied everything they hold,
// and returns their applied log.
func (s *Sim) coldRestart(c *checkpoint, erased int) ([]*raftpb.Entry, error) {
	r := newSim(Config{Seed: s.rng.Uint64(), Heal: 0})
	r.endAt = 6 * Second
	r.healed = true
	for i, d := range c.disks {
		if i == erased {
			d = simfs.New()
		}
		r.addNode(i, d.Durable())
		if i == erased {
			r.nodes[i].fenced = true
			continue
		}
		if err := r.start(r.nodes[i]); err != nil {
			return nil, err
		}
	}
	if err := r.loop(); err != nil {
		return nil, err
	}
	var first *node
	for _, n := range r.nodes {
		if n.fenced {
			continue
		}
		if first == nil {
			first = n
			continue
		}
		if n.core.Applied() != first.core.Applied() || n.core.Store().Hash() != first.core.Store().Hash() {
			return nil, fmt.Errorf("the two restarted nodes did not converge: applied %d and %d", first.core.Applied(), n.core.Applied())
		}
	}
	return first.core.AppliedEntries()
}

// stalledAt reports whether a node's disk was stalled at a time. A node
// with a stalled disk cannot apply, so it answers nothing; the liveness
// rule is about the healthy nodes.
func (n *node) stalledAt(t int64) bool {
	for _, w := range n.stalls {
		if t >= w[0] && t < w[1] {
			return true
		}
	}
	return false
}

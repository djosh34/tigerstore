package sim

import (
	"errors"
	"fmt"
	"slices"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
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
	if f.StepCrash {
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration/2), kind: evStepCrashDue})
	}
	if f.Rot {
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration*3/4), kind: evRot})
	}
	if f.HoldDown {
		s.downFor = min(3*Second+s.rng.Int64N(2*Second), s.cfg.Duration/2)
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration-s.downFor), kind: evDownFollower})
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

// downFollower crashes a follower and restarts it downFor later.
func (s *Sim) downFollower() {
	if s.healed {
		return
	}
	for _, n := range s.nodes {
		if n.up && n.core.Leader() != 0 && n.core.Leader() != n.id {
			s.crash(n)
			s.push(&event{at: s.now + s.downFor, kind: evRestart, node: n.idx, gen: n.gen})
			return
		}
	}
	// No follower yet; try again soon.
	s.push(&event{at: s.now + 200*Millisecond, kind: evDownFollower})
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
	n.disk.Fault = nil
	if planted.On("B11") && errors.Is(err, simfs.ErrIO) && n.diskBusy {
		// Planted: carry on as if the sync worked. diskBusy is set only
		// once the write is done, so this is a failed sync.
		n.diskBusy = false
		for _, j := range n.inflight {
			s.persisted(n, j)
		}
		s.process(n)
		return
	}
	s.stopNode(n)
}

// stopNode stops a node like a crash and restarts it later from its disk.
func (s *Sim) stopNode(n *node) {
	if s.healed && s.now <= s.healAt+s.cfg.LiveAfter {
		// The guaranteed disk fault fired after healing, or a watchdog
		// whose wait began before it. The liveness rule starts once the
		// node is back (check.go); the clients keep going for 3 s more
		// than that, so the rule has calls to check. Only a stop in the
		// first LiveAfter of healing counts, so the bound stays fixed: a
		// node that keeps stopping later fails the rule.
		s.lateStopAt = s.now
		s.extendLiveness()
	}
	s.crash(n)
	s.push(&event{at: s.now + 1 + s.rng.Int64N(max(s.cfg.Faults.MaxDown, Second)), kind: evRestart, node: n.idx, gen: n.gen})
}

// flipPayload is set only in planted builds (planted_flip.go).
var flipPayload func(*Sim, *node) bool

// maybeCorrupt damages the synced bytes of a node that just crashed, at
// most once per run and only if no node is damaged yet, so a quorum stays.
func (s *Sim) maybeCorrupt(n *node) {
	if !s.cfg.Faults.Corrupt || s.healed || s.rng.IntN(2) != 0 {
		return
	}
	if planted.On("B04c") && flipPayload != nil {
		// Planted: flip one payload bit that still decodes, and do not
		// expect the node to notice; the run must catch what it applies.
		if flipPayload(s, n) {
			s.mix(0xc4, uint64(n.idx))
		}
		return
	}
	for _, m := range s.nodes {
		if m.corrupted {
			return
		}
	}
	// Half the time the damage is in the checkpoint that recovery starts
	// from: the newest root on the disk, which the crash may have kept
	// though its sync never finished, so n.root does not know it yet
	// (seed 439 damaged the older one, which recovery no longer reads).
	if root, err := recoveredRoot(n); err == nil && root > 0 && s.rng.IntN(2) == 0 && n.disk.CorruptFile(s.rng, wal.CheckpointName(root)) {
		n.corrupted = true
		s.mix(0xc1, uint64(n.idx))
		return
	}
	if n.disk.Corrupt(s.rng) {
		n.corrupted = true
		s.mix(0xc0, uint64(n.idx))
	}
}

// recoveredRoot is the index of the root that recovery of n's disk would
// start from now.
func recoveredRoot(n *node) (uint64, error) {
	w, rec, err := wal.Open(n.disk.Durable())
	if err != nil {
		return 0, err
	}
	return rec.Root.Index, w.Close()
}

// checkpoint is a copy of every disk as a power cut would leave it, the
// time it was taken, and the highest index any node had applied by then.
type checkpoint struct {
	at      int64
	applied uint64
	disks   []*simfs.Disk
	bad     []bool
	// runs are the nodes' run numbers, which a real node keeps on its
	// disk with the log.
	runs     []uint64
	ids      []uint64
	inactive []bool
}

func (s *Sim) newCheckpoint() *checkpoint {
	c := &checkpoint{at: s.now, applied: s.applied}
	for _, n := range s.nodes {
		c.disks = append(c.disks, n.disk.Durable())
		c.bad = append(c.bad, n.corrupted)
		c.runs = append(c.runs, n.runs)
		c.ids = append(c.ids, n.id)
		c.inactive = append(c.inactive, n.inactive)
	}
	return c
}

func (s *Sim) takeCheckpoint() {
	s.checkpoints = append(s.checkpoints, s.newCheckpoint())
}

// checkColdRestarts erases each node's disk in turn at every checkpoint
// and at the end, and cold-starts the other two alone. Every entry applied
// anywhere by the checkpoint is committed, so the two must apply at least
// as far, and every entry and state they reach up to there must be the
// one in the run's record. An acknowledged write was applied before it was
// answered, so this covers every acknowledged write, delete, lease change
// and its revision. That is the guarantee that an acknowledged write
// survives the loss of one node.
func (s *Sim) checkColdRestarts(res *Result) error {
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
			if err := s.coldRestart(c, erased); err != nil {
				return fmt.Errorf("erasing node %d at %d ms: %w", erased+1, c.at/Millisecond, err)
			}
			res.Stats.ColdRestarts++
		}
	}
	return nil
}

// disagree returns two nodes that are not fenced and differ in their
// applied index or state, or nil.
func disagree(nodes []*node) (*node, *node) {
	var first *node
	for _, n := range nodes {
		if n.out() {
			continue
		}
		if first == nil {
			first = n
		} else if n.core.Applied() != first.core.Applied() || n.core.Hash() != first.core.Hash() {
			return first, n
		}
	}
	return nil, nil
}

// quorum reports whether the running nodes are a quorum of the voters
// that each of them knows.
func (s *Sim) quorum() bool {
	running := 0
	for _, n := range s.nodes {
		if n.up && !n.out() {
			running++
		}
	}
	for _, n := range s.nodes {
		if !n.up || n.out() {
			continue
		}
		voters := 0
		for _, m := range n.core.Members() {
			if !m.IsLearner {
				voters++
			}
		}
		if running < voters/2+1 {
			return false
		}
	}
	return true
}

// coldRestart starts the two nodes whose disks were not erased, with no
// clients and no faults, until they have applied everything they hold.
// Every entry and state they reach up to the index applied at the
// checkpoint is checked against the run's record as they go.
func (s *Sim) coldRestart(c *checkpoint, erased int) error {
	r := newSim(Config{Seed: s.rng.Uint64(), Heal: 0, Membership: s.cfg.Membership, MemoryBytes: s.cfg.MemoryBytes})
	r.endAt = 6 * Second
	r.healed = true
	r.base, r.baseUpTo = s.log, c.applied
	for i, d := range c.disks {
		if i == erased {
			d = simfs.New()
		}
		r.addNode(i, d.Durable())
		r.nodes[i].runs = c.runs[i]
		r.nodes[i].id = c.ids[i]
		// A process stopped by a peer's removed-member reply may not
		// have the removal on disk. Cold boot must recover that disk,
		// not copy the old process's volatile stopped flag. Only a node
		// that never joined stays absent without trying recovery.
		r.nodes[i].inactive = c.inactive[i] && c.runs[i] == 0
		if i == erased || r.nodes[i].inactive {
			r.nodes[i].fenced = true
			continue
		}
		if err := r.start(r.nodes[i]); err != nil {
			return err
		}
	}
	if err := r.loop(); err != nil {
		return err
	}
	refused := slices.ContainsFunc(r.nodes, func(n *node) bool { return n.inactive && !n.fenced })
	if refused && !r.quorum() {
		// A member whose disk commits its own removal refuses it, as
		// etcd's does, and with the erased disk the others can be left
		// without a quorum of the voters they know: nothing commits.
		return nil
	}
	// The leader may have proposed a lease expiry just before the end, so
	// the follower has yet to learn that it committed (seed 736998125).
	// One more second settles that.
	if a, _ := disagree(r.nodes); a != nil {
		r.endAt += Second
		if err := r.loop(); err != nil {
			return err
		}
	}
	if a, b := disagree(r.nodes); a != nil {
		return fmt.Errorf("cold restart did not converge: node %d applied %d, members %v, removed %v; node %d applied %d, members %v, removed %v; checkpoint applied %d, inactive %v",
			a.id, a.core.Applied(), a.core.Members(), a.core.RemovedMembers(), b.id, b.core.Applied(), b.core.Members(), b.core.RemovedMembers(), c.applied, c.inactive)
	}
	for _, n := range r.nodes {
		if !n.out() && n.core.Applied() < c.applied {
			var active []uint64
			for _, live := range r.live() {
				active = append(active, live.id)
			}
			return fmt.Errorf("cold restart: node %d applied %d entries, but %d were applied before; leader %d, members %v, active %v, inactive at copy %v",
				n.id, n.core.Applied(), c.applied, n.core.Leader(), n.core.Members(), active, c.inactive)
		}
	}
	return nil
}

// installedDuring reports whether a node started again from its log
// after a snapshot install between two times. The calls it had under way
// fail, as on a restart, and a client tries again.
func (n *node) installedDuring(from, to int64) bool {
	for _, t := range n.installs {
		if t >= from && t <= to {
			return true
		}
	}
	return false
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

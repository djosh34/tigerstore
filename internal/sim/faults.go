package sim

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// scheduleDiskFaults starts the disk faults of the run. The lock is held.
func (s *Sim) scheduleDiskFaults() {
	f := s.cfg.Faults
	if f.DiskErrorEvery > 0 {
		s.every(f.DiskErrorEvery, s.injectDiskFault)
		// One more fault, due early in the faults and armed on the next
		// write of any node's log, so that no run goes without: a run
		// that lost a node to corruption, or holds a partition, may
		// append nothing for many seconds. It stays due after healing.
		// TestSeeds checks that one fired.
		s.once(s.cfg.Duration/4, 500*Millisecond, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.stats.DiskFaults > 0 {
				return true
			}
			s.diskFaultDue = true
			return false
		})
	}
	if f.StallEvery > 0 {
		s.every(f.StallEvery, s.injectStall)
	}
	if f.FSDownEvery > 0 {
		s.every(f.FSDownEvery, func() { s.fsDown() })
	}
	if f.DiskFullEvery > 0 {
		s.every(f.DiskFullEvery, func() { s.diskFull() })
	}
	if f.Checkpoint {
		s.at(s.cfg.Duration/2+s.rng.Int64N(s.cfg.Duration/2)+1, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.takeCheckpoint()
		})
	}
	if f.StepCrash {
		s.once(s.cfg.Duration/2, 500*Millisecond, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.stepCrashDue = s.stats.StepCrashes == 0
			return !s.stepCrashDue
		})
	}
	if f.Rot {
		s.once(s.cfg.Duration*3/4, 500*Millisecond, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.healed || slices.ContainsFunc(s.nodes, func(n *node) bool { return n.corrupted }) {
				return true
			}
			// The node's committer rots a value at its next turn.
			s.rotDue = s.upNode()
			return false
		})
	}
	if f.HoldDown {
		s.downFor = min(3*Second+s.rng.Int64N(2*Second), s.cfg.Duration/2)
		// The follower is back 2 s before healing, so that its first
		// fetch, which the stream faults damage, comes during the faults.
		s.at(1+s.rng.Int64N(max(s.cfg.Duration-s.downFor-2*Second, 1)), func() {
			for !s.downFollower() && s.sleep(200*Millisecond) {
			}
		})
	}
}

// upNode picks a random node that is up, or nil. The lock is held.
func (s *Sim) upNode() *node {
	var up []*node
	for _, n := range s.nodes {
		if n.proc != nil {
			up = append(up, n)
		}
	}
	if len(up) == 0 {
		return nil
	}
	return up[s.rng.IntN(len(up))]
}

// injectDiskFault makes the next disk operation of a kind fail on a node:
// a sync that fails after part of its writes reached the disk, EIO on a
// write, a short write with ENOSPC, or EIO on any other file or directory
// operation.
func (s *Sim) injectDiskFault() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		s.armDiskFault(n, s.rng.IntN(4))
	}
}

// otherOps are the disk operations besides writes and syncs that a disk
// fault can fail.
var otherOps = []simfs.Op{simfs.OpCreate, simfs.OpOpen, simfs.OpRead, simfs.OpTruncate, simfs.OpRemove, simfs.OpSyncDir, simfs.OpRename}

// armDiskFault arms a disk fault on n: a failed sync (kind 0), EIO on the
// next write (1), a short write with ENOSPC (2), or EIO on the next
// operation of a random other kind (3). The lock is held.
func (s *Sim) armDiskFault(n *node, kind int) {
	d := n.disk
	target := simfs.OpWrite
	switch kind {
	case 0:
		d.FailSync(s.rng)
		s.tracef("node %d: disk fault armed: the next sync fails", n.id)
		return
	case 2:
		d.ShortWrite(s.rng)
		s.tracef("node %d: disk fault armed: the next write is short", n.id)
		return
	case 3:
		target = otherOps[s.rng.IntN(len(otherOps))]
	}
	s.tracef("node %d: disk fault armed: EIO on the next %v", n.id, target)
	d.Fault = func(op simfs.Op, _ string) error {
		if op != target {
			return nil
		}
		d.Fault = nil
		s.stats.IOErrors++
		return simfs.ErrIO
	}
}

// fsDown shuts a node's file system down, or makes it read-only, for 0.5
// to 3 s, as ext4 and XFS do after an error. The node stops at its next
// operation that fails; at the end the disk is mounted again, and a node
// still running is taken down first, as the unmount needs.
func (s *Sim) fsDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.upNode()
	if s.healed || n == nil {
		return false
	}
	s.stats.FSDowns++
	if s.rng.IntN(2) == 0 {
		n.disk.Shutdown()
	} else {
		n.disk.ReadOnly()
	}
	s.after(500*Millisecond+s.rng.Int64N(2500*Millisecond), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.healed {
			// Healing mounted it again.
			return
		}
		if n.proc != nil {
			n.why = "the run took it down to mount its disk again"
			s.stopNode(n)
		}
		n.disk.Remount()
	})
	return true
}

// diskFull fills a node's disk for 1.5 to 3.5 s, as another file taking
// every free byte does: its writes fail with ENOSPC, and it stops.
func (s *Sim) diskFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.upNode()
	if s.healed || n == nil {
		return false
	}
	s.stats.DiskFulls++
	d := n.disk
	d.Capacity = d.Bytes()
	s.tracef("node %d: disk full at %d bytes", n.id, d.Capacity)
	s.after(1500*Millisecond+s.rng.Int64N(2000*Millisecond), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.healed {
			d.Capacity = s.cfg.Faults.DiskCapacity
			s.tracef("node %d: disk no longer full", n.id)
		}
	})
	return true
}

// injectStall holds back a random node's log syncs for 1.5 to 4 s.
func (s *Sim) injectStall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		s.stall(n, s.now()+1500*Millisecond+s.rng.Int64N(2500*Millisecond))
	}
}

// stallLeader holds back the leader's log syncs until healing.
func (s *Sim) stallLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.leaderIdx(); i >= 0 {
		n := s.nodes[i]
		s.stall(n, s.healAt)
		s.stalledLeader = n
		for _, j := range n.pending {
			if j.first != math.MaxUint64 && !j.install {
				s.heldAppendAt = j.at
				break
			}
		}
	}
}

// downFollower crashes a follower and restarts it downFor later. It
// reports false if there was no follower yet.
func (s *Sim) downFollower() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return true
	}
	for _, n := range s.nodes {
		if p := n.proc; p != nil && p.node.Leader() != 0 && p.node.Leader() != n.id {
			n.why = "the run holds it down"
			s.crash(n)
			s.restartLater(n, s.downFor)
			return true
		}
	}
	return false
}

// stall holds back n's log syncs until a time. The lock is held.
func (s *Sim) stall(n *node, until int64) {
	n.stallUntil = until
	n.stalls = append(n.stalls, [2]int64{s.now(), until})
	s.tracef("node %d: disk stalled until %d ms", n.id, until/Millisecond)
}

// flipPayload is set only in planted builds (planted_flip.go).
var flipPayload func(*Sim, *node) bool

// maybeCorrupt damages the synced bytes of a node that just crashed, at
// most once per run and only if no node is damaged yet, so a quorum stays.
// The lock is held.
func (s *Sim) maybeCorrupt(n *node) {
	if !s.cfg.Faults.Corrupt || s.healed || s.rng.IntN(2) != 0 {
		return
	}
	if planted.On("B04c") && flipPayload != nil {
		// Planted: flip one payload bit that still decodes, and do not
		// expect the node to notice; the run must catch what it applies.
		if flipPayload(s, n) {
			s.tracef("node %d: flipped a payload bit", n.id)
		}
		return
	}
	for _, m := range s.nodes {
		if m.corrupted || m.fenced {
			return
		}
	}
	kind := damageKind(s.rng.IntN(int(numDamage)))
	if s.damage(n, kind) {
		s.stats.Damage[kind]++
		s.tracef("node %d: damage after a sync: %v", n.id, kind)
		return
	}
	if n.disk.Corrupt(s.rng) {
		n.corrupted = true
		s.stats.Damage[misdirected]++
		s.tracef("node %d: damage after a sync: %v", n.id, misdirected)
	}
}

// damageKind is a kind of damage to what a disk synced.
type damageKind int

const (
	// flippedRoot flips a bit of the checkpoint that recovery starts
	// from: the newest root on the disk, which the crash may have kept
	// though its sync never finished (seed 439 damaged the older one,
	// which recovery no longer reads).
	flippedRoot damageKind = iota
	// misdirected copies synced bytes over a sealed segment, like a write
	// that landed at the wrong place (simfs.Disk.Corrupt).
	misdirected
	// lostWrite zeros a sector of a sealed segment or of the root's
	// checkpoint, like a write the drive reported done but never made.
	lostWrite
	// badSector makes a sector of a sealed segment fail every read.
	badSector
	// misread makes recovery's read of a sealed segment return wrong data
	// once.
	misread
	// lastRecord flips a bit of the last record of the newest segment
	// when that record holds only a new term or vote: the log takes it
	// for a torn write and cuts it off, which loses a durable term and
	// vote (#357). Damage to a last record that holds entries is a
	// documented limit (docs/design.md), and the run does not do it.
	lastRecord
	numDamage
)

func (k damageKind) String() string {
	return [...]string{"flipped root checkpoint", "misdirected write", "lost write", "bad sector", "misread", "damaged last record"}[k]
}

// damage does damage of a kind to the synced bytes of n, which is down.
// All but a misread and a damaged last record leave the disk damaged for
// good, so n must refuse to start; a misread one refuses its next start,
// and a damaged last record must keep its term and vote. It reports
// whether n had what the kind needs. The lock is held.
func (s *Sim) damage(n *node, kind damageKind) bool {
	d := n.disk
	sealed := sealedSegments(d)
	var seg string
	if len(sealed) > 0 {
		seg = sealed[s.rng.IntN(len(sealed))]
	}
	switch kind {
	case flippedRoot:
		root, err := recoveredRoot(n)
		n.corrupted = err == nil && root > 0 && d.CorruptFile(s.rng, wal.CheckpointName(root))
		return n.corrupted
	case misdirected:
		n.corrupted = d.Corrupt(s.rng)
		return n.corrupted
	case lostWrite:
		if root, err := recoveredRoot(n); err == nil && root > 0 && s.rng.IntN(2) == 0 {
			seg = wal.CheckpointName(root)
		}
		n.corrupted = seg != "" && d.LoseWrite(s.rng, seg)
		return n.corrupted
	case badSector:
		n.corrupted = seg != "" && d.BadSector(s.rng, seg)
		return n.corrupted
	case misread:
		n.misread = seg != "" && d.Misread(s.rng, seg)
		return n.misread
	default:
		return s.damageLastRecord(n)
	}
}

// sealedSegments are the segments of a disk but the newest, with at least
// a sector of synced bytes. Recovery reads all of them, and damage to any
// is damage to synced records.
func sealedSegments(d *simfs.Disk) []string {
	var segs []string
	for _, name := range d.DurableNames() {
		if isSegment(name) {
			segs = append(segs, name)
		}
	}
	if len(segs) > 0 {
		segs = segs[:len(segs)-1]
	}
	return slices.DeleteFunc(segs, func(name string) bool { return len(d.DurableBytes(name)) < simfs.SectorBytes })
}

// damageLastRecord flips a bit at the end of n's newest segment if that
// loses only the term and vote of its last record, not an entry, and
// checks what recovery then gives (checkRecovery, turn). The lock is held.
func (s *Sim) damageLastRecord(n *node) bool {
	name := newestSegment(n.disk)
	size := len(n.disk.DurableBytes(name))
	before, err := recoveredState(n)
	if err != nil || size == 0 {
		return false
	}
	n.disk.FlipDurable(name, size-1)
	after, err := recoveredState(n)
	if err == nil && after.Last == before.Last && (after.HardState.Term != before.HardState.Term || after.HardState.Vote != before.HardState.Vote) {
		s.checkRecovery(n)
		return true
	}
	n.disk.FlipDurable(name, size-1)
	return false
}

// recoveredState is what recovery of n's synced disk gives now.
func recoveredState(n *node) (*wal.Recovered, error) {
	w, rec, err := wal.Open(n.disk.Durable())
	if err != nil {
		return nil, err
	}
	return rec, w.Close()
}

// recoveredRoot is the index of the root that recovery of n's disk would
// start from now.
func recoveredRoot(n *node) (uint64, error) {
	rec, err := recoveredState(n)
	if err != nil {
		return 0, err
	}
	return rec.Root.Index, nil
}

// checkpoint is a copy of every disk as a power cut would leave it, the
// time it was taken, and the highest index any node had applied by then.
type checkpoint struct {
	at       int64
	applied  uint64
	disks    []*simfs.Disk
	bad      []bool
	ids      []uint64
	inactive []bool
	// started marks the nodes that ever started, whose disks hold a run.
	started []bool
}

// coldDisks are the disks a failed cold restart started from, the time
// of their copy and the erased one, and the nodes of the cold restart.
type coldDisks struct {
	at     int64
	erased int
	disks  []*simfs.Disk
	nodes  []*node
}

// newCheckpoint copies every disk. The lock is held, so every disk is
// copied at the same instant.
func (s *Sim) newCheckpoint() *checkpoint {
	c := &checkpoint{at: s.now(), applied: s.applied}
	for _, n := range s.nodes {
		c.disks = append(c.disks, n.disk.Durable())
		c.bad = append(c.bad, n.corrupted)
		c.ids = append(c.ids, n.id)
		c.inactive = append(c.inactive, n.inactive)
		c.started = append(c.started, len(n.disk.DurableNames()) > 0)
	}
	return c
}

// takeCheckpoint keeps a copy of every disk for the cold restarts. The
// lock is held.
func (s *Sim) takeCheckpoint() {
	s.tracef("erase-one-disk copy")
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
	s.mu.Lock()
	all := append(slices.Clone(s.checkpoints), s.newCheckpoint())
	s.mu.Unlock()
	for _, c := range all {
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
// applied index or state, or nil. The nodes are frozen.
func disagree(nodes []*node) (*node, *node) {
	var first *node
	for _, n := range nodes {
		if n.out() || n.proc == nil {
			continue
		}
		if first == nil {
			first = n
		} else if c, f := n.proc.core, first.proc.core; c.Applied() != f.Applied() || c.Hash() != f.Hash() {
			return first, n
		}
	}
	return nil, nil
}

// quorum reports whether the running nodes are a quorum of the voters
// that each of them knows. The nodes are frozen.
func (s *Sim) quorum() bool {
	running := 0
	for _, n := range s.nodes {
		if n.proc != nil && !n.out() {
			running++
		}
	}
	for _, n := range s.nodes {
		if n.proc == nil || n.out() {
			continue
		}
		voters := 0
		for _, m := range n.proc.core.Members() {
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
func (s *Sim) coldRestart(c *checkpoint, erased int) (err error) {
	s.mu.Lock()
	seed := s.rng.Uint64()
	s.mu.Unlock()
	r, err := newSim(Config{Seed: seed, Membership: s.cfg.Membership, MemoryBytes: s.cfg.MemoryBytes})
	if err != nil {
		return err
	}
	defer r.shutdown()
	defer func() {
		if err != nil {
			// The failure's report shows what the cold restart did, and
			// how it left its nodes.
			err = fmt.Errorf("%w\nthe nodes of the cold restart:\n%s", err, r.report())
			r.mu.Lock()
			lines := slices.Clone(r.trace)
			r.mu.Unlock()
			s.mu.Lock()
			for _, l := range lines {
				s.tracef("cold restart: %s", l)
			}
			if s.cold == nil {
				s.cold = &coldDisks{at: c.at, erased: erased, disks: c.disks, nodes: r.nodes}
			}
			s.mu.Unlock()
		}
	}()
	r.healed, r.votes = true, true
	r.base, r.baseUpTo = s.log, c.applied
	for i, d := range c.disks {
		if i == erased {
			d = simfs.New()
		}
		n := r.nodes[i]
		n.disk, n.id = d.Durable(), c.ids[i]
		r.traceDisk(n)
		// A process stopped by a peer's removed-member reply may not
		// have the removal on disk. Cold boot must recover that disk,
		// not copy the old process's volatile stopped flag. Only a node
		// that never joined stays absent without trying recovery.
		n.inactive = c.inactive[i] && !c.started[i]
		if i == erased || n.inactive {
			n.fenced = true
		}
	}
	for _, n := range r.nodes {
		if err := r.start(n); err != nil {
			return err
		}
	}
	if err := r.runFor(6 * Second); err != nil {
		return err
	}
	refused := slices.ContainsFunc(r.nodes, func(n *node) bool { return n.inactive && !n.fenced })
	if refused && !r.quorum() {
		// A member whose disk commits its own removal refuses it, as
		// etcd's does, and with the erased disk the others can be left
		// without a quorum of the voters they know: nothing commits.
		return nil
	}
	// Two nodes that start within a message's delay of each other and draw
	// the same election timeouts split their votes, one round in ten, so
	// with no leader yet they get up to three more such periods
	// (TestSlowFetch seed 5042 on #268).
	for range 3 {
		if slices.ContainsFunc(r.nodes, func(n *node) bool { return n.proc != nil && !n.out() && n.proc.core.Leader() != 0 }) {
			break
		}
		r.thaw()
		if err := r.runFor(6 * Second); err != nil {
			return err
		}
	}
	// The leader may have proposed a lease expiry just before the end, so
	// the follower has yet to learn that it committed (seed 736998125).
	// One more second settles that.
	if a, _ := disagree(r.nodes); a != nil {
		r.thaw()
		if err := r.runFor(Second); err != nil {
			return err
		}
	}
	if a, b := disagree(r.nodes); a != nil {
		ca, cb := a.proc.core, b.proc.core
		return fmt.Errorf("cold restart did not converge: node %d applied %d, members %v, removed %v; node %d applied %d, members %v, removed %v; checkpoint applied %d, inactive %v",
			a.id, ca.Applied(), ca.Members(), ca.RemovedMembers(), b.id, cb.Applied(), cb.Members(), cb.RemovedMembers(), c.applied, c.inactive)
	}
	for _, n := range r.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		if cn := n.proc.core; cn.Applied() < c.applied {
			var active []uint64
			for _, live := range r.live() {
				active = append(active, live.id)
			}
			return fmt.Errorf("cold restart: node %d applied %d entries, but %d were applied before; leader %d, members %v, active %v, inactive at copy %v",
				n.id, cn.Applied(), c.applied, cn.Leader(), cn.Members(), active, c.inactive)
		}
	}
	return nil
}

// runFor lets a cold restart run for d, then freezes its nodes.
func (s *Sim) runFor(d int64) error {
	select {
	case <-time.After(time.Duration(d)):
	case <-s.failed():
	}
	if err := s.failure(); err != nil {
		return err
	}
	return s.freeze()
}

// reloadedDuring reports whether a node started again from its log
// between two times.
func (n *node) reloadedDuring(from, to int64) bool {
	for _, t := range n.reloads {
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

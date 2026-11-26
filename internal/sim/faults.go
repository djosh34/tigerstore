package sim

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/simrun"
	"github.com/djosh34/tigerstore/internal/wl"
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
		s.every(f.FSDownEvery, s.fsDown)
	}
	if f.DiskFullEvery > 0 {
		s.every(f.DiskFullEvery, s.diskFull)
	}
	if f.Checkpoint {
		s.at(s.cfg.Duration/2+s.rng.Int64N(s.cfg.Duration/2)+1, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.takeCheckpoint()
		})
	}
	s.installCrashDue, s.voteCrashDue = f.InstallCrash, f.VoteCrash
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
			if s.healed || s.count((*node).mayBeDamaged) >= s.spare() {
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
		kind := s.rng.IntN(4)
		switch {
		case s.cfg.ckptCreateFaults:
			kind = 3
		case s.cfg.syncFaultExits:
			kind = 0
		}
		s.armDiskFault(n, kind)
	}
}

// otherOps are the disk operations besides writes and syncs that a disk
// fault can fail.
var otherOps = []simfs.Op{simfs.OpCreate, simfs.OpOpen, simfs.OpRead, simfs.OpTruncate, simfs.OpRemove, simfs.OpSyncDir, simfs.OpRename, simfs.OpMkdir}

// armDiskFault arms a disk fault on n: a failed sync (kind 0), with EIO
// on the next truncate if cutFails is set, EIO on the next write (1), a
// short write with ENOSPC (2), or EIO on the next operation of a random
// other kind (3). The lock is held.
func (s *Sim) armDiskFault(n *node, kind int) {
	d := n.disk
	target := simfs.OpWrite
	switch kind {
	case 0:
		d.FailSync(s.rng)
		s.tracef("node %d: disk fault armed: the next sync fails", n.id)
		if !s.cfg.cutFails {
			return
		}
		target = simfs.OpTruncate
	case 2:
		d.ShortWrite(s.rng)
		s.tracef("node %d: disk fault armed: the next write is short", n.id)
		return
	case 3:
		target = otherOps[s.rng.IntN(len(otherOps))]
	}
	if s.cfg.ckptCreateFaults {
		target = simfs.OpCreate
	}
	s.tracef("node %d: disk fault armed: EIO on the next %v", n.id, target)
	if p := n.proc; p != nil && p.wl != nil {
		// This fault replaces a start's armed crash in recovery, if any:
		// the start no longer owns the disk's fault, and neither its
		// recovery nor its end clears this one (#860).
		p.wl.armed = false
	}
	d.Fault = func(op simfs.Op, name string) error {
		ckpt := op == simfs.OpCreate && isCheckpoint(name)
		if op != target || s.cfg.ckptCreateFaults && !ckpt {
			return nil
		}
		d.Fault = nil
		s.stats.IOErrors++
		if ckpt {
			s.stats.CheckpointCreateErrors++
		}
		return simfs.ErrIO
	}
}

// fsDown shuts a node's file system down, or makes it read-only, for 0.5
// to 3 s, as ext4 and XFS do after an error. The node stops at its next
// operation that fails; at the end the disk is mounted again, and a node
// still running is taken down first, as the unmount needs.
func (s *Sim) fsDown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.upNode()
	if s.healed || n == nil {
		return
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
		running := n.proc != nil
		s.remount(n)
		if running {
			s.restartLater(n, s.restartDelay())
		}
	})
}

// remount mounts n's disk again after it shut down or went read-only. A
// remount follows an unmount, which loses what the page cache could not
// write back (simfs.Disk.Remount). A node still running goes down first,
// as at a power loss. A node that is down loses its page cache now, as at
// a power loss: one that exited on the disk's error kept it. Else the
// pages a failed sync left readable, which no later sync writes, would
// outlive the remount (#371). The caller restarts a node it took down.
// The lock is held.
func (s *Sim) remount(n *node) {
	switch {
	case !n.disk.Down():
	case n.proc != nil:
		n.why = "the run took it down to mount its disk again"
		s.crash(n)
	default:
		s.loseUnsynced(n)
		n.crashImage = n.disk.Files()
		s.tracef("node %d: unmounted its disk, which kept what it synced", n.id)
	}
	n.disk.Remount()
}

// diskFull fills a node's disk for 1.5 to 3.5 s, as another file taking
// every free byte does: its writes fail with ENOSPC, and it stops.
func (s *Sim) diskFull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.upNode()
	if s.healed || n == nil {
		return
	}
	s.stats.DiskFulls++
	d := n.disk
	d.Capacity = d.Bytes()
	s.tracef("node %d: disk full at %d bytes", n.id, d.Capacity)
	s.after(1500*Millisecond+s.rng.Int64N(2000*Millisecond), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.healed {
			d.Capacity = s.diskCapacity()
			s.tracef("node %d: disk no longer full", n.id)
		}
	})
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
		if p := n.proc; p != nil && p.leader() != 0 && p.leader() != n.id {
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

// maybeCorrupt damages the synced bytes of a node that just crashed,
// only while fewer nodes than spare are damaged, so a quorum stays: at
// most once per run in a cluster of three.
// The lock is held.
func (s *Sim) maybeCorrupt(n *node) {
	// A test that picks the kinds of damage tries at every crash.
	if !s.cfg.Faults.Corrupt || s.healed || len(s.cfg.damageKinds) == 0 && !s.cfg.LoseQuorum && s.rng.IntN(2) != 0 {
		return
	}
	if s.cfg.LoseQuorum && s.now() < s.firstWritten+s.cfg.Duration/4 {
		// The run's first write committed something before the faults
		// (writeFirst), and the clients get a quarter of the faults'
		// window before the damage that loses the quorum starts.
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
	// A node damaged already is not damaged again. n's own torn image,
	// if it has one, is what this crash just did: only the other nodes'
	// leave the quorum short.
	if n.corrupted || s.count(func(m *node) bool { return m.corrupted || m.fenced || m != n && m.mayBeDamaged() }) >= s.spare() {
		return
	}
	kind := damageKind(s.rng.IntN(numDrawn))
	if k := s.cfg.damageKinds; len(k) > 0 {
		kind = k[s.rng.IntN(len(k))]
	}
	if s.damage(n, kind) {
		s.stats.Damage[kind]++
		s.tracef("node %d: damage after a sync: %v", n.id, kind)
		return
	}
	// The kind drawn found no place. A test that picks the kinds waits
	// for one that does.
	switch {
	case len(s.cfg.damageKinds) > 0:
	case (s.cfg.flipRecords || s.cfg.LoseQuorum) && s.formed > 0 && s.flipWLRecord(n):
		n.corrupted = true
		s.stats.Damage[flippedRecord]++
		s.tracef("node %d: damage after a sync: %v", n.id, flippedRecord)
	}
}

// flipWLRecord flips a bit of a synced record of wl node n's log that a
// record that checks follows, which a start must refuse (internal/wl/
// wal.go's head): the damage of a wl node where the kind drawn found no
// place, as during the faults, when most logs are a few hundred bytes in
// one segment and hold no checkpoint (seeds 22000 to 22003). The run
// decides from the log's format, not from wl's reader, which a start then
// checks (Recovered fails a start that takes it): any byte but the
// length's, whose CRC-32C no longer matches, unless the flip leaves a
// part of a sector all zeros, which wl takes by design for a write a
// crash tore in its last segment. Kept only when wl's reader refused it,
// a gap in wl's checks never failed a run (#689's area-4 review, finding
// 24). Only TestSeeds' runs do it (Config.flipRecords), once the cluster
// formed first: a node fenced in the first seconds of a cluster that
// forms under the faults left seed 2 of TestKubernetesLoad two nodes that
// the crashes kept from committing enough to test. The lock is held.
func (s *Sim) flipWLRecord(n *node) bool {
	d := n.disk
	ptr, err := wlPointer(d)
	if err != nil {
		return false
	}
	seg := func(k int64) []byte { return d.DurableBytes(wlSegment(k)) }
	recs := wl.Records(seg, ptr.LogFrom)
	if len(recs) < 2 {
		return false
	}
	// A ring's log finds damage to a record only once a later sync's mark
	// claims it durable: before that, it reads as a crash's torn last sync
	// (wl's logring.go). An idle log syncs a mark alone at its next tick,
	// so the records no mark claims are those of the last sync until then.
	pick := len(recs) - 1
	if slices.ContainsFunc(recs, func(r wl.Record) bool { return r.Kind == wl.KindSynced }) {
		claimed := slices.MaxFunc(recs, func(a, b wl.Record) int { return cmp.Compare(a.Claims, b.Claims) }).Claims
		pick = 0
		for pick < len(recs) && recs[pick].Off+recs[pick].Len <= claimed {
			pick++
		}
		if pick == 0 {
			return false
		}
	}
	r := recs[s.rng.IntN(pick)]
	// Any byte but the length's, bytes 4 to 7, which says where the next
	// record starts.
	at := s.rng.Int64N(r.Len - 4)
	if at >= 4 {
		at += 4
	}
	rec := logBytes(seg, r.Off, r.Len)
	rec[at] ^= 1
	if zeroSector(rec, r.Off) {
		return false
	}
	k, off := wl.SegmentPlace(r.Off + at)
	d.FlipDurable(wlSegment(k), int(off))
	return true
}

// logBytes returns a copy of the n bytes of a wl log at offset off.
func logBytes(seg func(k int64) []byte, off, n int64) []byte {
	out := make([]byte, 0, n)
	for int64(len(out)) < n {
		k, in := wl.SegmentPlace(off + int64(len(out)))
		b := seg(k)
		if int64(len(b)) <= in {
			break
		}
		out = append(out, b[in:min(int64(len(b)), in+n-int64(len(out)))]...)
	}
	return out
}

// wlSector is the sector a crash tears a wl log's writes at.
const wlSector = 512

// zeroSector reports whether rec, at log offset off, has a part within one
// sector that is all zeros: what a write torn at sectors leaves.
func zeroSector(rec []byte, off int64) bool {
	for len(rec) > 0 {
		k := min(int64(len(rec)), wlSector-off%wlSector)
		if !slices.ContainsFunc(rec[:k], func(c byte) bool { return c != 0 }) {
			return true
		}
		rec, off = rec[k:], off+k
	}
	return false
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
	// for a torn write and cuts it off, and the node must take the term
	// and vote from their copies outside the log (#357). Damage to a last
	// record that holds entries is a documented limit (docs/design.md),
	// and the run does not do it.
	lastRecord
	// flippedRecord flips a bit of a synced record of a wl node's log
	// that a record that checks follows (flipWLRecord). The run never
	// draws it: it is a wl node's damage where the kind drawn found no
	// place.
	flippedRecord
)

// numDamage is the number of damage kinds, and numDrawn of those the run
// draws.
const (
	numDamage = int(flippedRecord) + 1
	numDrawn  = int(lastRecord) + 1
)

func (k damageKind) String() string {
	return [...]string{"flipped root checkpoint", "misdirected write", "lost write", "bad sector", "misread", "damaged last record", "flipped record"}[k]
}

// damage does damage of a kind to the synced bytes of n, which is down.
// All but a misread and a damaged last record leave the disk damaged for
// good, so n must refuse to start; a misread one refuses its next start,
// and a damaged last record must keep its term and vote. It reports
// whether n had what the kind needs. The lock is held.
func (s *Sim) damage(n *node, kind damageKind) bool { return s.damageWL(n, kind) }

// damageWL is damage for a wl node (Config.wl), whose log is wal/*.seg
// and whose checkpoint is the one ckpt/CURRENT names. A start reads the
// log from the pointer's first segment on, and refuses a record that does
// not check where a later segment holds data (internal/wl/wal.go's head),
// so damage to the log goes only in a segment the start reads, after the
// first, with data two segments on: no record it damages reaches the last
// segment with data, where damage can look like a torn write. A damaged
// last record is main's alone, whose term and vote have copies outside
// the log. The lock is held.
func (s *Sim) damageWL(n *node, kind damageKind) bool {
	d := n.disk
	ptr, err := wlPointer(d)
	if err != nil {
		return false
	}
	ckpt := ""
	if ptr.Checkpoint != "" {
		ckpt = "ckpt/" + ptr.Checkpoint
	}
	sealed := wlSealed(d, ptr)
	seg := ""
	if len(sealed) > 0 {
		seg = sealed[s.rng.IntN(len(sealed))]
	}
	switch kind {
	case flippedRoot:
		n.corrupted = ckpt != "" && d.CorruptFile(s.rng, ckpt)
	case misdirected:
		n.corrupted = seg != "" && d.Misdirect(s.rng, seg)
	case lostWrite:
		if ckpt != "" && s.rng.IntN(2) == 0 {
			seg = ckpt
		}
		n.corrupted = seg != "" && d.LoseWrite(s.rng, seg)
	case badSector:
		n.corrupted = seg != "" && d.BadSector(s.rng, seg)
	case misread:
		n.misread = seg != "" && d.Misread(s.rng, seg)
		return n.misread
	case lastRecord, flippedRecord:
		// A damaged last record is main's alone; a flipped record is
		// never drawn (flipWLRecord).
		return false
	}
	return n.corrupted
}

// wlPointerFile is a wl node's checkpoint pointer on its disk.
const wlPointerFile = "ckpt/CURRENT"

// wlSegment is the name of segment k of a wl node's log on its disk.
func wlSegment(k int64) string { return fmt.Sprintf("wal/%016x.seg", k) }

// wlPointer is the checkpoint pointer a wl node's disk holds durably, the
// zero Pointer if none.
func wlPointer(d *simfs.Disk) (wl.Pointer, error) {
	b := d.DurableBytes(wlPointerFile)
	if b == nil {
		return wl.Pointer{}, nil
	}
	return wl.ReadPointer(b)
}

// wlSealed are the segments of a wl node's log after the first a start
// reads (ptr's) whose every byte is a record a start must find damaged if
// it is: in a ring's log, below the furthest place a sync's mark claims
// durable (records past it read as a torn last sync when damaged, wl's
// logring.go); in a log without marks, two segments before the end of its
// records. Where files have bytes says nothing in a ring: its segments
// are formatted whole and made two ahead, and damage past the log's end,
// which no start reads, failed seed 3231 of TestEachFaultTakesEffect and
// planted's control of B04a (#751's CI).
func wlSealed(d *simfs.Disk, ptr wl.Pointer) []string {
	seg := func(k int64) []byte { return d.DurableBytes(wlSegment(k)) }
	recs := wl.Records(seg, ptr.LogFrom)
	if len(recs) == 0 {
		return nil
	}
	last := recs[len(recs)-1]
	end, _ := wl.SegmentPlace(last.Off + last.Len)
	end -= 2
	if slices.ContainsFunc(recs, func(r wl.Record) bool { return r.Kind == wl.KindSynced }) {
		claimed := slices.MaxFunc(recs, func(a, b wl.Record) int { return cmp.Compare(a.Claims, b.Claims) }).Claims
		end, _ = wl.SegmentPlace(claimed)
		end--
	}
	var segs []string
	for k := ptr.FirstSegment + 1; k <= end; k++ {
		if len(seg(k)) > 0 {
			segs = append(segs, wlSegment(k))
		}
	}
	return segs
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
	// started marks the nodes that ever started, whose disks hold a run,
	// and joins those an operator replaced, which join the cluster.
	started, joins []bool
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
		d := n.disk.Durable()
		c.disks = append(c.disks, d)
		// A torn crash image that no start recovered yet may make
		// recovery refuse the log; without opening the copy to find out,
		// a cold restart counts it as damaged (mayBeDamaged).
		c.bad = append(c.bad, n.mayBeDamaged())
		c.ids = append(c.ids, n.id)
		c.inactive = append(c.inactive, n.inactive)
		c.started = append(c.started, len(n.disk.DurableNames()) > 0)
		c.joins = append(c.joins, n.joins)
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
// and at the end, and cold-starts the others alone; a cluster of one
// voter cold-starts with no disk erased. Every entry applied
// anywhere by the checkpoint is committed, so the others must apply at least
// as far, and every entry and state they reach up to there must be the
// one in the run's record. An acknowledged write was applied before it was
// answered, so this covers every acknowledged write, delete, lease change
// and its revision. That is the guarantee that an acknowledged write
// survives the loss of one node, in a cluster of three or five.
func (s *Sim) checkColdRestarts(res *Result) error {
	s.mu.Lock()
	all := append(slices.Clone(s.checkpoints), s.newCheckpoint())
	s.mu.Unlock()
	// A cluster of one voter cannot lose its disk: its cold restart
	// erases none (-1).
	erasures := []int{-1}
	if s.tolerance() > 0 {
		erasures = erasures[:0]
		for i := range all[0].disks {
			erasures = append(erasures, i)
		}
	}
	for _, c := range all {
		for _, erased := range erasures {
			// A damaged disk cannot stand in for the erased one: the
			// healthy disks left must hold a quorum of the voters.
			healthy := 0
			for i, bad := range c.bad {
				if !bad && i != erased {
					healthy++
				}
			}
			if healthy < s.cfg.voters()/2+1 {
				continue
			}
			if err := s.coldRestart(c, erased); err != nil {
				return fmt.Errorf("%s at %d ms: %w", erasing(erased), c.at/Millisecond, err)
			}
			res.Stats.ColdRestarts++
		}
	}
	return nil
}

// coldWaitMax is the most a run's cold restarts wait in all for a node
// to catch up past their first seconds (coldRestart): the run's clock
// gives its checks an hour past its length (runWith).
const coldWaitMax = 20 * 60 * Second

// caughtUp reports whether a cold restart's running nodes agree and each
// applied at least applied. The nodes are frozen.
func (s *Sim) caughtUp(applied uint64) bool {
	if a, _ := disagree(s.nodes); a != nil {
		return false
	}
	for _, n := range s.nodes {
		if !n.out() && n.proc != nil && n.proc.applied() < applied {
			return false
		}
	}
	return true
}

// progress is what a cold restart's nodes have done so far: how far the
// snapshot streams they served got, and the entries the running nodes
// applied. The nodes are frozen.
func (s *Sim) progress() int64 {
	n := s.snaps.served()
	for _, m := range s.nodes {
		if !m.out() && m.proc != nil {
			n += int64(m.proc.applied())
		}
	}
	return n
}

// snapProgress is how far the snapshot streams to each node got: a stream
// sent again from its start adds nothing until it passes where the last
// one to that node reached, so a snapshot refused and sent again and
// again is no progress (the review of #810). Streams go off the nodes'
// loops, so it takes its own lock.
type snapProgress struct {
	mu      sync.Mutex
	at, top map[uint64]int64
	total   int64
}

// chunk notes a chunk of n bytes of a stream to node to, the stream's
// first if first. It reports whether the stream passed another 16 MiB,
// and how far it is.
func (p *snapProgress) chunk(to uint64, first bool, n int) (bool, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var passed bool
	var at int64
	// Aside: a map that grows draws from gosim's random stream, which
	// would move the run's draws.
	simrun.Aside(func() {
		if p.at == nil {
			p.at, p.top = map[uint64]int64{}, map[uint64]int64{}
		}
		if first {
			p.at[to] = 0
		}
		before := p.at[to]
		p.at[to] += int64(n)
		if p.at[to] > p.top[to] {
			p.total += p.at[to] - p.top[to]
			p.top[to] = p.at[to]
		}
		passed, at = before>>24 != p.at[to]>>24, p.at[to]
	})
	return passed, at
}

// served is how far the streams got, summed over the nodes they went to.
func (p *snapProgress) served() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total
}

// erasing names the disk a cold restart erased, -1 for none.
func erasing(erased int) string {
	if erased < 0 {
		return "erasing no disk"
	}
	return fmt.Sprintf("erasing node %d", erased+1)
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
			continue
		}
		// A node's entries are checked one by one as they apply
		// (wlHooks.Applied): the applied index and revision tell.
		if a, b := n.proc.wl.turn, first.proc.wl.turn; a.Applied != b.Applied || a.Rev != b.Rev {
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
		ms, _ := n.members()
		for _, m := range ms {
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

// hasLeader reports whether a running node knows a leader. The nodes are
// frozen.
func hasLeader(nodes []*node) bool {
	return slices.ContainsFunc(nodes, func(n *node) bool { return n.proc != nil && !n.out() && n.proc.leader() != 0 })
}

// canElect reports whether a running node can win an election: it is a
// voter of the members it knows, and its last entry is at least as new as
// that of a majority of those voters, itself included, which run and so
// grant it their votes. A learner votes but does not campaign. The nodes
// are frozen, and nothing is appended without a leader, so each one's
// last entry is on its disk.
func (s *Sim) canElect() (bool, error) {
	type last struct{ term, index uint64 }
	logs := map[uint64]last{}
	for _, n := range s.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		ents, err := s.logOn(n)
		if err != nil {
			return false, err
		}
		if err := ents.close(); err != nil {
			return false, err
		}
		logs[n.id] = last{ents.lastTerm, ents.last}
	}
	for _, n := range s.nodes {
		own, ok := logs[n.id]
		if !ok {
			continue
		}
		voter, voters, votes := false, 0, 0
		ms, _ := n.members()
		for _, m := range ms {
			if m.IsLearner {
				continue
			}
			voters++
			voter = voter || m.ID == n.id
			if l, ok := logs[m.ID]; ok && (own.term > l.term || own.term == l.term && own.index >= l.index) {
				votes++
			}
		}
		if voter && votes > voters/2 {
			return true, nil
		}
	}
	return false, nil
}

// holdsApplied reports whether a running node's disk holds the entries up
// to applied, each as the run's record has it, with its term, and its type
// and data where the record knows them: in its checkpoint, whose state the
// start checked, or in its log. The nodes are frozen.
func (s *Sim) holdsApplied(applied uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		ents, err := s.logOn(n)
		if err != nil {
			return false, err
		}
		holds := ents.last >= applied
		for i := ents.root + 1; holds && i <= applied; i++ {
			e, err := ents.entry(i)
			if err != nil {
				return false, errors.Join(err, ents.close())
			}
			if b, ok := s.base.get(i); ok && (b.term != e.GetTerm() || b.known && (b.typ != e.GetType() || !bytes.Equal(s.base.bytes(b), e.GetData()))) {
				holds = false
			}
		}
		if err := ents.close(); err != nil {
			return false, err
		}
		if holds {
			return true, nil
		}
	}
	return false, nil
}

// coldRestart starts the nodes whose disks were not erased, with no
// clients and no faults, until they have applied everything they hold.
// Every entry and state they reach up to the index applied at the
// checkpoint is checked against the run's record as they go.
func (s *Sim) coldRestart(c *checkpoint, erased int) (err error) {
	s.mu.Lock()
	seed := s.rng.Uint64()
	s.mu.Unlock()
	r, err := newSimFrom(Config{Seed: seed, Nodes: s.cfg.Nodes, Membership: s.cfg.Membership, MemoryBytes: s.cfg.MemoryBytes, snapshotWait: s.cfg.snapshotWait}, s)
	if err != nil {
		return err
	}
	r.inColdRestart = true
	// A synthetic image's members keep their IDs (startWL), and its
	// record starts after its snapshot (entryLog.from).
	r.synth, r.log.from = s.synth, s.log.from
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
			d.KeepIn(r.pages, keepPages)
		}
		n := r.nodes[i]
		n.disk, n.id, n.joins = d.Durable(), c.ids[i], c.joins[i]
		r.traceDisk(n)
		// A process stopped by a peer's removed-member reply may not
		// have the removal on disk. Cold boot must recover that disk,
		// not copy the old process's volatile stopped flag. Only a node
		// that never joined stays absent without trying recovery.
		n.inactive = c.inactive[i] && !c.started[i]
		// A damaged disk, or one that may be, stays out, as its node
		// would refuse it.
		if i == erased || n.inactive || c.bad[i] {
			n.fenced = true
		}
	}
	for _, n := range r.nodes {
		if err := r.start(n); err != nil {
			return err
		}
	}
	if err := r.runSettle(6*Second, c.applied); err != nil {
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
		if hasLeader(r.nodes) {
			break
		}
		r.thaw()
		if err := r.runFor(6 * Second); err != nil {
			return err
		}
	}
	// The erased disk can leave no node able to win an election: one that
	// applied a member's promotion and needs it, a learner whose log is
	// newer but cannot campaign, and the rest behind them (#389). Nothing
	// commits then, so there is nothing to converge on, but what was
	// applied before must still be on a surviving disk.
	if !hasLeader(r.nodes) {
		ok, err := r.canElect()
		if err != nil {
			return err
		}
		if !ok {
			holds, err := r.holdsApplied(c.applied)
			if err != nil {
				return err
			}
			if !holds {
				return fmt.Errorf("cold restart: no node can win an election, and no surviving disk holds the %d entries applied before as the run's record has them", c.applied)
			}
			s.tracef("cold restart at %d ms %s: no node can win an election", c.at/Millisecond, erasing(erased))
			return nil
		}
	}
	// The leader may have proposed a lease expiry just before the end, so
	// the follower has yet to learn that it committed (seed 736998125).
	// Such proposals come about once a second, so one more second can end
	// just after the next one (seed 135 on #553): the nodes run on a tenth
	// of a second at a time, for up to three seconds, until they agree.
	for range 30 {
		if a, _ := disagree(r.nodes); a == nil {
			break
		}
		r.thaw()
		if err := r.runFor(Second / 10); err != nil {
			return err
		}
	}
	// A node behind may be taking a snapshot of the whole store, which
	// with a large one takes far longer than that: 160 MB under a 137 MiB
	// budget takes minutes (#809). The restart waits on, a second at a
	// time, until the nodes agree and each applied what was applied before,
	// while a snapshot stream is served or a node applies more: it gives up
	// after 10 s with neither, past the run's limit of real time, or once
	// the run's cold restarts waited coldWaitMax in all.
	waited, idle, why := int64(0), 0, ""
	for !r.caughtUp(c.applied) {
		if idle >= 10 {
			why = "nothing moved for 10 s"
			break
		}
		s.mu.Lock()
		over := s.coldWaited >= coldWaitMax
		if !over {
			s.coldWaited += Second
		}
		s.mu.Unlock()
		if over {
			why = fmt.Sprintf("the run's cold restarts waited %s in all", seconds(coldWaitMax))
			break
		}
		if simrun.WallLimitPassed() {
			return errors.New("cold restart: the run passed its limit of real time ($SIM_WALL) while the nodes caught up")
		}
		before := r.progress()
		r.thaw()
		if err := r.runFor(Second); err != nil {
			return err
		}
		waited += Second
		if r.progress() == before {
			idle++
		} else {
			idle = 0
		}
	}
	s.mu.Lock()
	s.coldSnapsServed += r.snaps.served()
	s.mu.Unlock()
	if why != "" {
		s.mu.Lock()
		s.tracef("cold restart at %d ms %s: waited %s for the nodes to catch up, %d MiB of snapshots served, until %s", c.at/Millisecond, erasing(erased), seconds(waited), r.snaps.served()>>20, why)
		s.mu.Unlock()
	}
	if a, b := disagree(r.nodes); a != nil {
		ma, ra := a.members()
		mb, rb := b.members()
		return fmt.Errorf("cold restart did not converge: node %d applied %d, members %v, removed %v; node %d applied %d, members %v, removed %v; checkpoint applied %d, inactive %v",
			a.id, a.proc.applied(), ma, ra, b.id, b.proc.applied(), mb, rb, c.applied, c.inactive)
	}
	for _, n := range r.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		if p := n.proc; p.applied() < c.applied {
			var active []uint64
			for _, live := range r.live() {
				active = append(active, live.id)
			}
			ms, _ := n.members()
			return fmt.Errorf("cold restart: node %d applied %d entries, but %d were applied before; leader %d, members %v, active %v, inactive at copy %v",
				n.id, p.applied(), c.applied, p.leader(), ms, active, c.inactive)
		}
	}
	return nil
}

// runSettle is runFor, which stops early, at a whole 100 ms of its time,
// once every running node follows one leader and has applied at least
// applied, all the same entries. A cold restart then has nothing left to
// show.
func (s *Sim) runSettle(d int64, applied uint64) error {
	for end := s.now() + d; s.now() < end; {
		if !s.sleep(min(100*Millisecond, end-s.now())) {
			break
		}
		s.mu.Lock()
		settled, failed := s.settled(applied), s.err != nil
		s.mu.Unlock()
		if settled || failed {
			break
		}
	}
	if err := s.failure(); err != nil {
		return err
	}
	return s.freeze()
}

// settled reports whether every running node follows one leader and has
// applied at least applied, the same index, as each last showed. The lock
// is held.
func (s *Sim) settled(applied uint64) bool {
	var lead, at uint64
	for _, n := range s.nodes {
		if n.out() || n.proc == nil {
			continue
		}
		l := n.last
		switch {
		case l.lead == 0, l.applied < applied, lead != 0 && (l.lead != lead || l.applied != at):
			return false
		}
		lead, at = l.lead, l.applied
	}
	return lead != 0
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
	return during(n.reloads, from, to)
}

// hungOnReload reports whether r is a clientv3 call that timed out while
// its node started again from its log. The server fails the attempt at
// once, but clientv3 hides that failure of a read and tries it again on
// the same node, which can wait out the call's deadline (#365). The
// liveness rule excuses a direct call that a start from the log failed,
// as a client tries another node; this is the same call through clientv3.
func (n *node) hungOnReload(r *Record) bool {
	return r.v3 && errors.Is(r.Err, errClientTimeout) && during(n.fromLog, r.Call, r.Return)
}

// during reports whether one of the times is between from and to.
func during(times []int64, from, to int64) bool {
	for _, t := range times {
		if t >= from && t <= to {
			return true
		}
	}
	return false
}

// installExcused reports whether r is a call that failed, other than as
// a rejection the rule counts as served, and overlapped a span in which
// its node took a snapshot: from when the snapshot reached the node until
// the node applied what the leader had committed when the install was
// done. Until then the node can't answer a write sent to it, and its loop
// answers no client while it loads the snapshot (#775's rulings). The
// client's timeout bounds how far back of the span a call can start. A
// span that never caught up excuses nothing, nor does a call that ended
// before the span began or started after it ended.
func (n *node) installExcused(r *Record) bool {
	if r.Err == nil || isRejection(r.Err) {
		return false
	}
	for _, w := range n.installs {
		if w.caughtUp != 0 && r.Call < w.caughtUp && r.Return >= w.from {
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

// diskLog is the log a node's disk holds: the entries from root+1 to last,
// the term of the last, and each entry until close.
type diskLog struct {
	root, last, lastTerm uint64
	entry                func(i uint64) (*raftpb.Entry, error)
	close                func() error
}

// logOn reads the log n's disk holds durably, as its next start would
// find it. The nodes are frozen.
func (s *Sim) logOn(n *node) (diskLog, error) {
	l, err := wl.ReadLog(s.os.NewNode(wlDataDir, n.disk.Durable(), nil), wlDataDir)
	if err != nil {
		return diskLog{}, err
	}
	d := diskLog{root: l.Base, last: l.Base + uint64(len(l.Entries)), lastTerm: l.BaseTerm}
	if len(l.Entries) > 0 {
		d.lastTerm = l.Entries[len(l.Entries)-1].GetTerm()
	}
	d.entry = func(i uint64) (*raftpb.Entry, error) { return l.Entries[i-l.Base-1], nil }
	d.close = func() error { return nil }
	return d, nil
}

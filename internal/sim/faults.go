package sim

import (
	"errors"
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// corruptTail is how far from the end of a file damage stays. Damage in
// the last record of the newest segment looks the same as a torn write,
// which the log cuts off; that is outside the guarantee (see the WAL's
// known limits in docs/design.md).
const corruptTail = 64 << 10

func (s *Sim) scheduleDiskFaults() {
	f := s.cfg.Faults
	if f.DiskErrorEvery > 0 {
		s.push(&event{at: s.exp(f.DiskErrorEvery), kind: evDiskFault})
		// One fault at a uniform time, so that no run goes without.
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration), kind: evDiskFault, gen: 1})
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
// sync, EIO on a write, or a short write with ENOSPC.
func (s *Sim) injectDiskFault(once bool) {
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		kind := s.rng.IntN(3)
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
	if !once {
		s.push(&event{at: s.exp(s.cfg.Faults.DiskErrorEvery), kind: evDiskFault})
	}
}

// injectStall holds back a node's disk writes for 1.5 to 4 s, or, for
// the scripted stall, the leader's until healing.
func (s *Sim) injectStall(leader bool) {
	if s.healed {
		return
	}
	if leader {
		for _, n := range s.nodes {
			if n.up && n.core.Leader() == n.id {
				n.stallUntil = s.healAt
				n.stalls = append(n.stalls, [2]int64{s.now, n.stallUntil})
				s.mix(0x57, uint64(n.idx), uint64(n.stallUntil))
			}
		}
		return
	}
	if n := s.upNode(); n != nil {
		n.stallUntil = s.now + 1500*Millisecond + s.rng.Int64N(2500*Millisecond)
		n.stalls = append(n.stalls, [2]int64{s.now, n.stallUntil})
		s.mix(0x57, uint64(n.idx), uint64(n.stallUntil))
	}
	s.push(&event{at: s.exp(s.cfg.Faults.StallEvery), kind: evStall})
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
	if n.disk.Corrupt(s.rng, corruptTail) {
		n.corrupted = true
		s.mix(0xc0, uint64(n.idx))
	}
}

// checkpoint is a copy of every disk as a power cut would leave it, and
// the time it was taken.
type checkpoint struct {
	at    int64
	disks []*simfs.Disk
	bad   []bool
}

func (s *Sim) takeCheckpoint() {
	c := &checkpoint{at: s.now}
	for _, n := range s.nodes {
		c.disks = append(c.disks, n.disk.Durable())
		c.bad = append(c.bad, n.corrupted)
	}
	s.checkpoints = append(s.checkpoints, c)
}

// checkColdRestarts erases each node's disk in turn at every checkpoint
// and at the end, cold-starts the other two alone, and requires every
// write acknowledged before the checkpoint in their log. That is the
// guarantee that an acknowledged write survives the loss of one node.
func (s *Sim) checkColdRestarts(res *Result) error {
	end := &checkpoint{at: s.endAt}
	for _, n := range s.nodes {
		end.disks = append(end.disks, n.disk.Durable())
		end.bad = append(end.bad, n.corrupted)
	}
	for _, c := range append(slices.Clone(s.checkpoints), end) {
		var acked []string
		for _, r := range res.History {
			if r.Err == nil && r.Return < c.at {
				acked = append(acked, putValues(r.req)...)
			}
		}
		for erased := range c.disks {
			skip := false
			for i, bad := range c.bad {
				// A damaged disk cannot stand in for the erased one.
				skip = skip || (bad && i != erased)
			}
			if skip {
				continue
			}
			log, err := s.coldRestart(c, erased)
			if err != nil {
				return fmt.Errorf("erasing node %d at %d ms: %w", erased+1, c.at/Millisecond, err)
			}
			have := map[string]bool{}
			for _, op := range log {
				for _, v := range putValues(op) {
					have[v] = true
				}
			}
			for _, v := range acked {
				if !have[v] {
					return fmt.Errorf("erasing node %d at %d ms loses the acknowledged write of %q", erased+1, c.at/Millisecond, v)
				}
			}
			res.Stats.ColdRestarts++
		}
	}
	return nil
}

// coldRestart starts the two nodes whose disks were not erased, with no
// clients and no faults, until they have applied everything they hold,
// and returns their committed log.
func (s *Sim) coldRestart(c *checkpoint, erased int) ([]proto.Message, error) {
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
	ents, err := first.core.AppliedEntries()
	if err != nil {
		return nil, err
	}
	var log []proto.Message
	for _, e := range ents {
		if len(e.GetData()) == 0 {
			continue
		}
		if err := core.DecodeBatch(e.GetData(), func(_, _, _ uint64, op proto.Message) error {
			if _, ok := op.(*pb.PutRequest); ok {
				log = append(log, op)
			}
			if _, ok := op.(*pb.TxnRequest); ok {
				log = append(log, op)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return log, nil
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

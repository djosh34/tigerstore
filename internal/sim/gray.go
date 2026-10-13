package sim

import (
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/simfs"
)

func (s *Sim) scheduleGrayFaults() {
	f := s.cfg.Faults
	if f.GrayEvery > 0 {
		s.push(&event{at: s.exp(f.GrayEvery), kind: evGray})
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration*3/4), kind: evGrayOnce})
	}
	if f.RecoveryCrash {
		s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration/4), kind: evRecoveryDue})
	}
	if f.GrayClient {
		s.grayCl = &grayClient{}
		s.push(&event{at: s.rng.Int64N(Second), kind: evGrayConnect})
	}
	if s.cfg.GrayLeaderAt > 0 {
		s.push(&event{at: s.cfg.GrayLeaderAt, kind: evGrayLeader})
	}
}

// startGray makes a random node gray for 2 to 6 s. It reports whether a
// node was up to take it and no gray period was on.
func (s *Sim) startGray() bool {
	if s.healed || s.gray != nil {
		return false
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	s.grayEnd = s.now + 2*Second + s.rng.Int64N(4*Second)
	// Half the periods make the leader gray and stay on it, so that the
	// commit watchdog gets to fire.
	s.grayStays = false
	if i := s.leaderIdx(); i >= 0 && s.rng.IntN(2) == 0 {
		n, s.grayStays = s.nodes[i], true
	}
	s.makeGray(n)
	s.push(&event{at: s.now + Second + s.rng.Int64N(2*Second), kind: evGraySwitch})
	return true
}

// makeGray makes n the gray node, with slow fsyncs half the time.
func (s *Sim) makeGray(n *node) {
	s.gray, s.lastGray, s.grayDropped = n, n.idx, false
	n.graySync = s.rng.IntN(2) == 0
	s.mix(0x6a, uint64(n.idx), uint64(s.grayEnd))
}

// switchGray ends the gray period or, half the time, moves it to another
// node, as when the gray node changes during repair.
func (s *Sim) switchGray() {
	if s.gray == nil || s.cfg.GrayLeaderAt > 0 {
		return
	}
	if s.grayOnce && !s.grayDropped {
		// The guaranteed gray period lasts until its node lost an append,
		// and moves on from a node that is down.
		if n := s.upNode(); !s.gray.up && n != nil {
			s.makeGray(n)
		}
		s.push(&event{at: s.now + Second, kind: evGraySwitch})
		return
	}
	if s.healed || s.now >= s.grayEnd {
		s.endGray()
		return
	}
	if !s.grayStays && s.rng.IntN(2) == 0 {
		if n := s.upNode(); n != nil && n != s.gray {
			s.makeGray(n)
		}
	}
	s.push(&event{at: min(s.grayEnd, s.now+Second+s.rng.Int64N(2*Second)), kind: evGraySwitch})
}

func (s *Sim) endGray() {
	if s.gray != nil {
		s.grayFaultEnded()
	}
	s.gray, s.grayOnce = nil, false
}

// grayFaultEnded records only fault effects that actually survived healing.
// A canceled disk completion from a crashed node never reaches this path.
func (s *Sim) grayFaultEnded() {
	if s.healed && s.now > s.healAt {
		s.grayHealedAt = max(s.grayHealedAt, s.now)
		s.extendLiveness()
	}
}

// grayLeader makes the leader gray until healing, without slow fsyncs, so
// only the commit watchdog can make way.
func (s *Sim) grayLeader() {
	if i := s.leaderIdx(); i >= 0 && !s.healed {
		s.grayEnd = s.healAt
		s.makeGray(s.nodes[i])
		s.gray.graySync = false
	}
}

// grayClient is a barely-alive client: it opens a stream with 10 watches,
// never reads it, and reconnects every 50 to 300 ms until healing.
type grayClient struct {
	open   bool
	node   int
	gen    uint64
	stream uint64
}

func (s *Sim) grayConnect() {
	c := s.grayCl
	if c.open {
		if n := s.nodes[c.node]; n.up && n.gen == c.gen && n.grayStreams[c.stream] {
			delete(n.grayStreams, c.stream)
			n.closing[c.stream] = true
			s.push(&event{at: s.now, kind: evStreamClose, node: n.idx, gen: n.gen, stream: c.stream})
		}
		c.open = false
	}
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		n.nextStream++
		c.open, c.node, c.gen, c.stream = true, n.idx, n.gen, n.nextStream
		n.grayStreams[c.stream] = true
		s.push(&event{at: s.now, kind: evGrayCreate, node: n.idx, gen: n.gen, stream: c.stream})
	}
	s.push(&event{at: s.now + 50*Millisecond + s.rng.Int64N(250*Millisecond), kind: evGrayConnect})
}

// grayCreate opens the gray client's watches: on the whole key space,
// from now or, one time in four, from revision 1, which a compaction may
// cancel.
func (s *Sim) grayCreate(e *event) {
	n := s.nodes[e.node]
	if !n.up || n.gen != e.gen || !n.grayStreams[e.stream] {
		return
	}
	for range 10 {
		var start int64
		if s.rng.IntN(4) == 0 {
			start = 1
		}
		n.core.Watch(e.stream, &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
			Key: []byte("k"), RangeEnd: []byte("l"), StartRevision: start, PrevKv: true,
		}}})
	}
	s.stats.GrayStreams++
	s.process(n)
}

// errRecoveryCrash is the simulator's crash inside recovery.
var errRecoveryCrash = errors.New("sim: crash during recovery")

// armRecoveryCrash makes every restart of a node with a log on its disk,
// until one recovery crashed and then one in three, crash during recovery, from its first to sixth writing disk
// operation on: the truncate and sync of a torn tail, or the six steps of
// replacing a damaged newest header. Every write from that one on fails,
// as nothing runs after a crash. A crashed recovery can crash again at the
// next restart. It reports whether it armed the crash.
func (s *Sim) armRecoveryCrash(n *node) bool {
	if !s.cfg.Faults.RecoveryCrash || s.healed || n.disk.Fault != nil || len(n.disk.DurableNames()) == 0 || (s.stats.RecoveryCrashes > 0 && s.rng.IntN(3) != 0) {
		return false
	}
	// The repair of a torn tail writes twice (truncate, sync), that of a
	// damaged newest header six times; until one recovery crashed, the
	// crash comes at one of the first two.
	k, writes := 1+s.rng.IntN(6), 0
	if s.stats.RecoveryCrashes == 0 {
		k = 1 + s.rng.IntN(2)
	}
	n.disk.Fault = func(op simfs.Op, _ string) error {
		if op == simfs.OpOpen || op == simfs.OpRead {
			return nil
		}
		writes++
		if writes >= k {
			return errRecoveryCrash
		}
		return nil
	}
	return true
}

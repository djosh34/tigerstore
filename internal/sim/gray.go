package sim

import (
	"errors"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	tnode "github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// scheduleGrayFaults starts the gray faults of the run. The lock is held.
func (s *Sim) scheduleGrayFaults() {
	f := s.cfg.Faults
	if f.GrayEvery > 0 {
		s.every(f.GrayEvery, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.startGray()
		})
		// One gray period that lasts, past healing if need be, until its
		// gray node lost an append: clients may write nothing for a while.
		s.once(s.cfg.Duration*3/4, 500*Millisecond, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.stats.GrayNodes > 0 || s.healed {
				return true
			}
			// A gray period already on becomes the guaranteed one.
			s.grayOnce = s.gray != nil || s.startGray()
			return false
		})
	}
	if f.RecoveryCrash {
		// Until a recovery crashed: most crashes leave no torn tail, and
		// a clean log needs no writes to recover.
		s.once(s.cfg.Duration/4, 500*Millisecond, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.recoveryDue = s.stats.RecoveryCrashes == 0
			return !s.recoveryDue
		})
	}
	if f.GrayClient {
		s.grayCl = &grayClient{}
		s.wg.Go(s.grayClientRun)
	}
	if s.cfg.GrayLeaderAt > 0 {
		s.at(s.cfg.GrayLeaderAt, s.grayLeader)
	}
}

// startGray makes a random node gray for 2 to 6 s. It reports whether a
// node was up to take it and no gray period was on. The lock is held.
func (s *Sim) startGray() bool {
	if s.healed || s.gray != nil {
		return false
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	s.grayEnd = s.now() + 2*Second + s.rng.Int64N(4*Second)
	// Half the periods make the leader gray and stay on it, so that its
	// writes wait for the gray period to end.
	s.grayStays = false
	if i := s.leaderIdx(); i >= 0 && s.rng.IntN(2) == 0 {
		n, s.grayStays = s.nodes[i], true
	}
	s.makeGray(n)
	s.after(Second+s.rng.Int64N(2*Second), s.switchGray)
	return true
}

// makeGray makes n the gray node, with slow fsyncs half the time. The
// lock is held.
func (s *Sim) makeGray(n *node) {
	s.gray, s.lastGray, s.grayDropped = n, n.idx, false
	n.graySync = s.rng.IntN(2) == 0
	s.tracef("node %d: gray until %d ms, slow fsyncs %v", n.id, s.grayEnd/Millisecond, n.graySync)
}

// switchGray ends the gray period or, half the time, moves it to another
// node, as when the gray node changes during repair.
func (s *Sim) switchGray() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gray == nil || s.cfg.GrayLeaderAt > 0 {
		return
	}
	if s.grayOnce && !s.grayDropped {
		// The guaranteed gray period lasts until its node lost an append,
		// and moves on from a node that is down.
		if n := s.upNode(); s.gray.proc == nil && n != nil {
			s.makeGray(n)
		}
		s.after(Second, s.switchGray)
		return
	}
	if s.healed || s.now() >= s.grayEnd {
		s.gray, s.grayOnce = nil, false
		return
	}
	if !s.grayStays && s.rng.IntN(2) == 0 {
		if n := s.upNode(); n != nil && n != s.gray {
			s.makeGray(n)
		}
	}
	s.after(min(s.grayEnd, s.now()+Second+s.rng.Int64N(2*Second))-s.now(), s.switchGray)
}

// graySyncEnded records the end of a gray fsync. One that outlasted
// healing moves the start of the liveness rule: until it ends, its node's
// term and vote changes and the peer messages behind them are held. A
// crashed node's canceled fsync never gets here. The gray network fault
// does not move the rule, though it can outlast healing too: votes and
// heartbeats pass a gray node, and its end is the first append after
// healing, which can come only after an election that the rule must
// still bound. The lock is held.
func (s *Sim) graySyncEnded() {
	if s.healed && s.now() > s.healAt {
		s.grayHealedAt = max(s.grayHealedAt, s.now())
		s.extendLiveness()
	}
}

// grayLeader makes the leader gray until healing, without slow fsyncs
// unless Config.GrayLeaderSync asks for them.
func (s *Sim) grayLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.leaderIdx(); i >= 0 && !s.healed {
		s.grayEnd = s.healAt
		s.makeGray(s.nodes[i])
		s.gray.graySync = s.cfg.GrayLeaderSync
	}
}

// grayClient is a barely-alive client: it opens a stream with 10 watches,
// never reads it, and reconnects every 50 to 300 ms until healing.
type grayClient struct {
	ws *tnode.WatchStream
}

func (s *Sim) grayClientRun() {
	c := s.grayCl
	s.mu.Lock()
	wait := s.rng.Int64N(Second)
	s.mu.Unlock()
	for s.sleep(wait) {
		if c.ws != nil {
			// Closes its stream, and once healed, stops.
			c.ws.Close()
			c.ws = nil
		}
		s.mu.Lock()
		if s.healed {
			s.mu.Unlock()
			return
		}
		n := s.upNode()
		var starts []int64
		for range 10 {
			// On the whole key space, from now or, one time in four, from
			// revision 1, which a compaction may cancel.
			var start int64
			if s.rng.IntN(4) == 0 {
				start = 1
			}
			starts = append(starts, start)
		}
		wait = 50*Millisecond + s.rng.Int64N(250*Millisecond)
		s.mu.Unlock()
		if n == nil {
			continue
		}
		ws, _, err := s.openStream(n, starts, func(start int64) *pb.WatchCreateRequest {
			return &pb.WatchCreateRequest{Key: []byte("k"), RangeEnd: []byte("l"), StartRevision: start, PrevKv: true}
		})
		if err == nil {
			c.ws = ws
			s.mu.Lock()
			s.stats.GrayStreams++
			s.mu.Unlock()
		}
	}
}

// openStream opens a watch stream on n's process with a watch for each
// start, as a client on the other end of a connection. It returns the
// stream and the process.
func (s *Sim) openStream(n *node, starts []int64, req func(int64) *pb.WatchCreateRequest) (*tnode.WatchStream, *proc, error) {
	s.mu.Lock()
	p, lat := n.proc, s.clientLatency()
	s.mu.Unlock()
	if p == nil || !s.sleep(lat) {
		return nil, nil, errConnRefused
	}
	ws, err := p.node.OpenWatch(p.ctx, core.Caller{})
	if err != nil {
		return nil, nil, err
	}
	for _, start := range starts {
		r := &pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: req(start)}}
		if err := ws.Send(p.ctx, r); err != nil {
			ws.Close()
			return nil, nil, err
		}
	}
	return ws, p, nil
}

// errRecoveryCrash is the simulator's crash inside recovery.
var errRecoveryCrash = errors.New("sim: crash during recovery")

// armRecoveryCrash makes every restart of a node with a log on its disk,
// until one recovery crashed and then one in three, crash during
// recovery, from its first to sixth writing disk operation on: the
// truncate and sync of a torn tail, or the six steps of replacing a
// damaged newest header. A sync of a file with nothing to write does not
// count, such as recovery's sync of a segment before it reads it (#372).
// Every write from that one on fails, as nothing runs after a crash. A
// crashed recovery can crash again at the next restart. It reports whether
// it armed the crash. The lock is held.
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
	n.disk.Fault = func(op simfs.Op, name string) error {
		if op == simfs.OpOpen || op == simfs.OpRead || op == simfs.OpSync && !n.disk.Unsynced(name) {
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

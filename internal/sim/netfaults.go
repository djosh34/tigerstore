package sim

// scheduleNetFaults starts the network and process faults of the run.
// The lock is held.
func (s *Sim) scheduleNetFaults() {
	f := s.cfg.Faults
	// Each fault also comes once at a random time early enough to take
	// effect, retried until it does, so that no run goes without;
	// TestSeeds checks the counts.
	once := func(every int64, fault func() bool) {
		if every > 0 {
			s.every(every, func() { fault() })
			s.once(s.cfg.Duration*3/4, 100*Millisecond, fault)
		}
	}
	once(f.LinkStallEvery, s.stallLink)
	once(f.PauseEvery, s.pause)
	once(f.HalfOpenEvery, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.halfOpen(-1, 0)
	})
	if f.SwitchEvery > 0 {
		s.every(f.SwitchEvery, s.switchLinks)
	}
	if f.HoldMaskAt > 0 {
		s.at(f.HoldMaskAt, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.healed {
				s.stats.LinkChanges++
				s.setLinks(f.HoldMask)
			}
		})
	}
}

// randomLink picks a random directed link. The lock is held.
func (s *Sim) randomLink() (from, to int) {
	from = s.rng.IntN(len(s.nodes))
	to = (from + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	return from, to
}

// stallLink holds back one directed link for 0.5 to 2 s: everything on it,
// including what is already on its way, waits and then arrives in order,
// like a stalled TCP stream.
func (s *Sim) stallLink() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return false
	}
	from, to := s.randomLink()
	l := &s.links[from][to]
	l.stallUntil = max(l.stallUntil, min(s.now()+500*Millisecond+s.rng.Int64N(1500*Millisecond), s.healAt))
	s.stats.LinkStalls++
	s.tracef("link %d->%d stalled until %d ms", from+1, to+1, l.stallUntil/Millisecond)
	return true
}

// pause stops one process for 0.2 to 2 s, like a stop signal or a long
// garbage collection: its committer, its log syncs and value reads, and
// its reads from the network wait, and then it handles everything at
// once, in order.
func (s *Sim) pause() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return false
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	// A second pause can only make the first one longer.
	n.pausedUntil = max(n.pausedUntil, min(s.now()+200*Millisecond+s.rng.Int64N(1800*Millisecond), s.healAt))
	s.stats.Pauses++
	s.tracef("node %d: paused until %d ms", n.id, n.pausedUntil/Millisecond)
	return true
}

// halfOpen makes a directed link half-open, from a random node or from
// node from if it is not -1: what is sent on it later vanishes. After d,
// or 0.5 to 5 s (the peer write timeout) if d is 0, the connections over
// it fail and the transports report the peer unreachable; then the link
// carries messages again. The lock is held.
func (s *Sim) halfOpen(from int, d int64) bool {
	if s.healed {
		return false
	}
	var to int
	if from < 0 {
		from, to = s.randomLink()
	} else {
		to = (from + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	}
	l := &s.links[from][to]
	if l.halfOpenUntil > s.now() {
		return false
	}
	if d == 0 {
		d = 500*Millisecond + s.rng.Int64N(halfOpenTimeout-500*Millisecond)
	}
	l.halfOpenUntil = s.now() + d
	s.halfOpens = append(s.halfOpens, halfOpenLink{from: from, to: to, start: s.now(), end: l.halfOpenUntil})
	s.stats.HalfOpens++
	s.tracef("link %d->%d half-open until %d ms", from+1, to+1, l.halfOpenUntil/Millisecond)
	s.after(d, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if l.halfOpenUntil > 0 && l.halfOpenUntil <= s.now() {
			l.halfOpenUntil = 0
			s.net.resetBetween(from, to)
		}
	})
	return true
}

// leaderIdx returns the index of a node that is up and leads, or -1. The
// lock is held.
func (s *Sim) leaderIdx() int {
	for _, n := range s.nodes {
		if p := n.proc; p != nil && p.node.Leader() == n.id {
			return n.idx
		}
	}
	return -1
}

// switchLinks moves to a random link mask.
func (s *Sim) switchLinks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return
	}
	s.stats.LinkChanges++
	s.setLinks(s.rng.Uint64N(s.allLinks() + 1))
}

// halfOpenTimeout is the peer write timeout: a half-open connection fails
// after it at the latest.
const halfOpenTimeout = 5 * Second

// halfOpenLink is a half-open link and when it was.
type halfOpenLink struct {
	from, to   int
	start, end int64
}

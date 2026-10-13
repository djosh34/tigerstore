package sim

func (s *Sim) scheduleNetFaults() {
	f := s.cfg.Faults
	if f.LinkStallEvery > 0 {
		s.push(&event{at: s.exp(f.LinkStallEvery), kind: evLinkStall})
	}
	if f.PauseEvery > 0 {
		s.push(&event{at: s.exp(f.PauseEvery), kind: evPause})
	}
	if f.ClockJumpEvery > 0 {
		s.push(&event{at: s.exp(f.ClockJumpEvery), kind: evClockJump})
	}
	if f.SwitchEvery > 0 {
		s.push(&event{at: s.exp(f.SwitchEvery), kind: evSwitch})
	}
	if f.HoldMaskAt > 0 {
		s.push(&event{at: f.HoldMaskAt, kind: evSwitch, gen: 1})
	}
}

// target is the node an event happens on, or nil.
func (s *Sim) target(e *event) *node {
	switch e.kind {
	case evTick, evDeliver, evDiskDone, evSubmit, evWatchCreate:
		return s.nodes[e.node]
	}
	return nil
}

// stallLink holds back one directed link for 0.5 to 2 s: everything on it
// waits and then arrives in order, like a stalled TCP stream.
func (s *Sim) stallLink() {
	if s.healed {
		return
	}
	from := s.rng.IntN(len(s.nodes))
	to := (from + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	l := &s.links[from][to]
	l.stallUntil = s.now + 500*Millisecond + s.rng.Int64N(1500*Millisecond)
	s.mix(0x5a, uint64(from), uint64(to), uint64(l.stallUntil))
	s.push(&event{at: s.exp(s.cfg.Faults.LinkStallEvery), kind: evLinkStall})
}

// pause stops one process for 0.2 to 2 s, like a stop signal or a long
// garbage collection: it handles nothing, then everything at once.
func (s *Sim) pause() {
	if s.healed {
		return
	}
	if n := s.upNode(); n != nil {
		n.pausedUntil = s.now + 200*Millisecond + s.rng.Int64N(1800*Millisecond)
		n.pauses = append(n.pauses, [2]int64{s.now, n.pausedUntil})
		s.stats.Pauses++
		s.mix(0x9a, uint64(n.idx), uint64(n.pausedUntil))
	}
	s.push(&event{at: s.exp(s.cfg.Faults.PauseEvery), kind: evPause})
}

// jumpClock moves one node's clock forward by 1 to 3 ticks at once.
func (s *Sim) jumpClock() {
	if s.healed {
		return
	}
	// At most one jump per node in 10 s, which keeps a lease's life within
	// the declared envelope (see checkLeasesExpire).
	if n := s.upNode(); n != nil && n.pausedUntil <= s.now && (n.lastJump == 0 || s.now-n.lastJump >= 10*Second) {
		n.lastJump = s.now
		k := 1 + s.rng.IntN(3)
		s.mix(0xc1, uint64(n.idx), uint64(k))
		for range k {
			n.core.Tick()
		}
		s.process(n)
	}
	s.push(&event{at: s.exp(s.cfg.Faults.ClockJumpEvery), kind: evClockJump})
}

// switchLinks moves to a random link mask, or for gen 1 to the held mask.
func (s *Sim) switchLinks(gen uint64) {
	if s.healed {
		return
	}
	s.stats.LinkChanges++
	if gen == 1 {
		s.setLinks(s.cfg.Faults.HoldMask)
		return
	}
	s.setLinks(s.rng.Uint64N(1 << 6))
	s.push(&event{at: s.exp(s.cfg.Faults.SwitchEvery), kind: evSwitch})
}

// pausedAt reports whether a node's process was paused at a time.
func (n *node) pausedAt(t int64) bool {
	for _, w := range n.pauses {
		if t >= w[0] && t < w[1] {
			return true
		}
	}
	return false
}

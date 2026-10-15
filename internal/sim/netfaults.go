package sim

import (
	"cmp"
	"slices"
)

func (s *Sim) scheduleNetFaults() {
	f := s.cfg.Faults
	// Each fault also comes once at a random time early enough to take
	// effect, retried until it does, so that no run goes without;
	// TestSeeds checks the counts.
	once := func(every int64, regular, single kind) {
		if every > 0 {
			s.push(&event{at: s.exp(every), kind: regular})
			s.push(&event{at: 1 + s.rng.Int64N(s.cfg.Duration*3/4), kind: single})
		}
	}
	once(f.LinkStallEvery, evLinkStall, evLinkStallOnce)
	once(f.PauseEvery, evPause, evPauseOnce)
	once(f.ClockJumpEvery, evClockJump, evClockJumpOnce)
	once(f.HalfOpenEvery, evHalfOpen, evHalfOpenOnce)
	if f.SwitchEvery > 0 {
		s.push(&event{at: s.exp(f.SwitchEvery), kind: evSwitch})
	}
	if f.HoldMaskAt > 0 {
		s.push(&event{at: f.HoldMaskAt, kind: evHoldMask})
	}
}

// again schedules the next regular fault of e's kind.
func (s *Sim) again(e *event, every int64) {
	if !s.healed {
		s.push(&event{at: s.exp(every), kind: e.kind})
	}
}

// retry tries a once-per-run fault again 100 ms later if it did not take
// effect.
func (s *Sim) retry(e *event, done bool) {
	if !done && !s.healed {
		s.push(&event{at: s.now + 100*Millisecond, kind: e.kind})
	}
}

// target is the node an event happens on, or nil.
func (s *Sim) target(e *event) *node {
	if slices.Contains([]kind{evTick, evDeliver, evDiskDone, evSubmit, evWatchCreate, evStreamClose, evHalfOpenEnd, evGrayCreate, evCheckpointSync, evSnapshotSent, evFetch}, e.kind) {
		return s.nodes[e.node]
	}
	return nil
}

// randomLink picks a random directed link.
func (s *Sim) randomLink() (from, to int) {
	from = s.rng.IntN(len(s.nodes))
	to = (from + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	return from, to
}

// stallLink holds back one directed link for 0.5 to 2 s: everything on it,
// including what is already on its way, waits and then arrives in order,
// like a stalled TCP stream.
func (s *Sim) stallLink() bool {
	if s.healed {
		return false
	}
	from, to := s.randomLink()
	l := &s.links[from][to]
	l.stallUntil = max(l.stallUntil, min(s.now+500*Millisecond+s.rng.Int64N(1500*Millisecond), s.healAt))
	s.holdLink(from, to)
	s.stats.LinkStalls++
	s.mix(0x5a, uint64(from), uint64(to), uint64(l.stallUntil))
	return true
}

// pause stops one process for 0.2 to 2 s, like a stop signal or a long
// garbage collection: it handles nothing, then everything at once, in
// order.
func (s *Sim) pause() bool {
	if s.healed {
		return false
	}
	n := s.upNode()
	if n == nil {
		return false
	}
	// A second pause can only make the first one longer, so that what is
	// held keeps its place ahead of what comes later.
	n.pausedUntil = max(n.pausedUntil, min(s.now+200*Millisecond+s.rng.Int64N(1800*Millisecond), s.healAt))
	s.stats.Pauses++
	s.mix(0x9a, uint64(n.idx), uint64(n.pausedUntil))
	s.push(&event{at: n.pausedUntil, kind: evResume, node: n.idx})
	return true
}

// resume hands a node what it missed while paused, in order, unless a
// longer pause is still on.
func (s *Sim) resume(n *node) {
	if n.pausedUntil > s.now {
		return
	}
	held := n.held
	n.held = nil
	for _, e := range held {
		s.handle(e)
	}
}

// jumpClock moves one node's clock forward by 1 to 3 ticks at once.
func (s *Sim) jumpClock() bool {
	if s.healed {
		return false
	}
	// At most one jump per node in 10 s, which keeps a lease's life within
	// the declared envelope (see checkLeasesExpire).
	n := s.upNode()
	if n == nil || n.pausedUntil > s.now || len(n.held) > 0 || (n.lastJump != 0 && s.now-n.lastJump < 10*Second) {
		return false
	}
	n.lastJump = s.now
	k := 1 + s.rng.IntN(3)
	s.stats.ClockJumps++
	s.mix(0xc1, uint64(n.idx), uint64(k))
	for range k {
		n.core.Tick()
	}
	s.process(n)
	return true
}

// halfOpen makes a directed link half-open, from a random node or from
// node from if it is not -1: the sender still sees its connection, but
// what is on the link and what it sends later vanish. After d, or 0.5 to
// 5 s (the peer write timeout) if d is 0, the sender's connection fails
// and Raft hears the peer is unreachable; then the link carries messages
// again.
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
	if l.halfOpenUntil > s.now {
		return false
	}
	if d == 0 {
		d = 500*Millisecond + s.rng.Int64N(halfOpenTimeout-500*Millisecond)
	}
	l.epoch++
	l.halfOpenUntil = s.now + d
	s.halfOpens = append(s.halfOpens, halfOpenLink{from: from, to: to, start: s.now, end: l.halfOpenUntil})
	s.stats.HalfOpens++
	s.mix(0x40, uint64(from), uint64(to), uint64(l.halfOpenUntil))
	s.push(&event{at: l.halfOpenUntil, kind: evHalfOpenEnd, node: from, from: to})
	return true
}

// halfOpenEnd fails the sender's connection: Raft hears that the peer is
// unreachable and probes it again.
func (s *Sim) halfOpenEnd(e *event) {
	n := s.nodes[e.node]
	if !n.up {
		return
	}
	n.core.Unreachable(s.nodes[e.from].id)
	s.process(n)
}

// leaderIdx returns the index of a node that is up and leads, or -1.
func (s *Sim) leaderIdx() int {
	for _, n := range s.nodes {
		if n.up && n.core.Leader() == n.id {
			return n.idx
		}
	}
	return -1
}

// switchLinks moves to a random link mask.
func (s *Sim) switchLinks() {
	if s.healed {
		return
	}
	s.stats.LinkChanges++
	s.setLinks(s.rng.Uint64N(1 << 6))
	s.push(&event{at: s.exp(s.cfg.Faults.SwitchEvery), kind: evSwitch})
}

// halfOpenTimeout is the peer write timeout: a half-open connection fails
// after it at the latest.
const halfOpenTimeout = 5 * Second

// halfOpenLink is a half-open link and when it was.
type halfOpenLink struct {
	from, to   int
	start, end int64
}

// holdLink moves what is on a stalled link to the end of the stall, in
// the order it was sent, one nanosecond apart, so that no message can
// overtake another; new messages on the link come after them (send).
func (s *Sim) holdLink(from, to int) {
	l := &s.links[from][to]
	var held []*event
	for _, e := range s.q {
		if e.kind == evDeliver && e.from == from && e.node == to {
			held = append(held, e)
		}
	}
	slices.SortFunc(held, func(a, b *event) int { return cmp.Compare(a.number, b.number) })
	at := l.stallUntil
	for _, e := range held {
		at = max(at, e.at)
		e.at = at
		at++
	}
	l.last = max(l.last, at)
	slices.SortFunc(s.q, func(a, b *event) int {
		if a.at != b.at {
			return cmp.Compare(a.at, b.at)
		}
		return cmp.Compare(a.seq, b.seq)
	})
}

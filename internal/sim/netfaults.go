package sim

import (
	"slices"
	"strconv"
)

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
	if f.NetemEvery > 0 {
		s.every(f.NetemEvery, s.netemFault)
		// A fault on a link that carries nothing hits no write, so the
		// run's own one comes again until one did.
		s.once(s.cfg.Duration*3/4, 100*Millisecond, func() bool {
			s.mu.Lock()
			hit := slices.ContainsFunc(s.stats.Netem[:], func(n int) bool { return n > 0 })
			s.mu.Unlock()
			if !hit {
				s.netemFault()
			}
			return hit
		})
	}
	once(f.ResetEvery, s.resetLink)
	once(f.SlowWatcherEvery, s.slowWatcher)
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

// clientOr picks a random link between the clients' host and a node one
// time in three, as a fault on a client's link, and a link of pick
// otherwise. The lock is held.
func (s *Sim) clientOr(pick func() (int, int)) (from, to int) {
	if s.rng.IntN(3) != 0 {
		return pick()
	}
	from, to = len(s.links), s.rng.IntN(len(s.clientLinks))
	if s.rng.IntN(2) == 0 {
		from, to = to, from
	}
	return from, to
}

// host names host i in the trace: a node's ID, or "client" for the
// clients' host.
func (s *Sim) host(i int) string {
	if i == len(s.links) {
		return "client"
	}
	return strconv.Itoa(i + 1)
}

// setClientLinks sets each direction of the links between the clients'
// host and the nodes up with probability up, and resets the connections
// over a link that went down. The lock is held.
func (s *Sim) setClientLinks(up float64) {
	for i := range s.clientLinks {
		for j := range 2 {
			l := &s.clientLinks[i][j]
			l.up = s.rng.Float64() < up
			if !l.up {
				s.net.resetBetween(i, len(s.links))
				s.tracef("client link %d (%d) down", i+1, j)
			}
		}
	}
}

// randomLink picks a random directed link. The lock is held.
func (s *Sim) randomLink() (from, to int) {
	from = s.rng.IntN(len(s.nodes))
	to = (from + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	return from, to
}

// stallLink holds back one directed link for 0.5 to 2 s, and one time in
// four for 5.5 to 7 s, past the peer write timeout: everything on it,
// including what is already on its way, waits and then arrives in order,
// like a stalled TCP stream, and a writer whose buffer fills waits too.
func (s *Sim) stallLink() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return false
	}
	from, to := s.clientOr(s.randomLink)
	l := s.link(from, to)
	d := 500*Millisecond + s.rng.Int64N(1500*Millisecond)
	if s.rng.IntN(4) == 0 {
		d = 5500*Millisecond + s.rng.Int64N(1500*Millisecond)
	}
	l.stallUntil = max(l.stallUntil, min(s.now()+d, s.healAt))
	s.stats.LinkStalls++
	s.tracef("link %s->%s stalled until %d ms", s.host(from), s.host(to), l.stallUntil/Millisecond)
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

// netemKind is a fault of a link for 1.5 to 3.5 s, as netem and the chaos
// proxies inject on real links. Loss and corruption of packets are what
// TCP repairs: the stream arrives late, as TCP retransmits, never wrong.
// A flipped, dropped or duplicated byte is what reaches the stream past
// TCP, as through a proxy that damages it, which TLS then refuses.
type netemKind int

const (
	// lossy loses 20% of the packets: a write arrives 200 to 400 ms late,
	// TCP's retransmission timeout, and what follows waits for it.
	lossy netemKind = iota
	// corrupting corrupts 5% of the packets, which TCP drops and sends
	// again, as lossy.
	corrupting
	// reordering sends half the packets up to 10 ms late; TCP puts the
	// stream back in order, so what follows waits for them.
	reordering
	// slow limits the link to 8 to 32 KiB/s.
	slow
	// delayed delays every write by 300 to 800 ms more, less than an
	// election timeout.
	delayed
	// switching switches the link's delay between 0 to 2 ms and 20 to
	// 80 ms more every 100 to 400 ms.
	switching
	// flipped flips a bit of the first write, then of one in four.
	flipped
	// dropped deletes a byte of the first write, then of one in four.
	dropped
	// duplicated repeats a byte of the first write, then of one in four.
	duplicated
)

// numNetem is the number of kinds of link faults.
const numNetem = int(duplicated) + 1

func (k netemKind) String() string {
	return [...]string{"lossy link", "corrupting link", "reordering link", "slow link", "delayed link", "switching delay", "flipped byte", "dropped byte", "duplicated byte"}[k]
}

// netemFault puts a fault of a random kind on a busy link: one to or from
// the leader, where the traffic is, unless the link has one already.
func (s *Sim) netemFault() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return
	}
	from, to := s.clientOr(s.busyLink)
	l := s.link(from, to)
	now := s.now()
	if l.netemUntil > now {
		return
	}
	l.netem = netemKind(s.rng.IntN(numNetem))
	if k := s.cfg.netemKinds; len(k) > 0 {
		l.netem = k[s.rng.IntN(len(k))]
	}
	l.netemUntil = min(now+1500*Millisecond+s.rng.Int64N(2*Second), s.healAt)
	l.bw = 8<<10 + s.rng.Int64N(24<<10)
	l.extra = 300*Millisecond + s.rng.Int64N(500*Millisecond)
	l.switchAt, l.hits = now, 0
	if l.netem == switching {
		l.extra = 0
	}
	s.tracef("link %s->%s: %v until %d ms", s.host(from), s.host(to), l.netem, l.netemUntil/Millisecond)
	if l.netem >= flipped {
		// Its end resets the connections whose bytes it damaged.
		s.at(l.netemUntil, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.net.resetDamaged(from, to)
		})
	}
}

// busyLink picks a random directed link to or from the leader, or any
// link while there is none. The lock is held.
func (s *Sim) busyLink() (from, to int) {
	lead := s.leaderIdx()
	if lead < 0 {
		return s.randomLink()
	}
	other := (lead + 1 + s.rng.IntN(len(s.nodes)-1)) % len(s.nodes)
	if s.rng.IntN(2) == 0 {
		return lead, other
	}
	return other, lead
}

// netem applies l's fault to a write of b, due at time at, on a pipe
// whose last write arrives at last. It returns the bytes that go on their
// way, when they arrive, and whether it damaged them. The lock is held.
func (s *Sim) netem(l *link, b []byte, at, last int64) ([]byte, int64, bool) {
	hit := true
	switch l.netem {
	case lossy, corrupting:
		pct := 20
		if l.netem == corrupting {
			pct = 5
		}
		hit = s.rng.IntN(100) < pct
		if hit {
			at += 200*Millisecond + s.rng.Int64N(200*Millisecond)
		}
	case reordering:
		hit = s.rng.IntN(2) == 0
		if hit {
			at += s.rng.Int64N(10*Millisecond) + 1
		}
	case slow:
		at = max(at, last) + int64(len(b))*Second/l.bw
	case switching:
		s.switchDelay(l, s.now())
		fallthrough
	case delayed:
		at += l.extra
	case flipped:
		b, hit = s.damageByte(l, b, func(b []byte, i int) []byte { b[i] ^= 1 << s.rng.IntN(8); return b })
	case dropped:
		b, hit = s.damageByte(l, b, func(b []byte, i int) []byte { return slices.Delete(b, i, i+1) })
	case duplicated:
		b, hit = s.damageByte(l, b, func(b []byte, i int) []byte { return slices.Insert(b, i, b[i]) })
	}
	if hit {
		l.hits++
		s.stats.Netem[l.netem]++
	}
	return b, at, hit && l.netem >= flipped
}

// damageByte damages a random byte of b with damage, in the first write
// of l's fault and one in four after it, and reports whether it did. The
// lock is held.
func (s *Sim) damageByte(l *link, b []byte, damage func([]byte, int) []byte) ([]byte, bool) {
	if len(b) == 0 || l.hits > 0 && s.rng.IntN(4) != 0 {
		return b, false
	}
	return damage(b, s.rng.IntN(len(b))), true
}

// switchDelay switches l's extra delay between 0 to 2 ms and 20 to 80 ms
// once its switch is due at time now. The lock is held.
func (s *Sim) switchDelay(l *link, now int64) {
	if now < l.switchAt {
		return
	}
	l.switchAt = now + 100*Millisecond + s.rng.Int64N(300*Millisecond)
	if l.extra < 20*Millisecond {
		l.extra = 20*Millisecond + s.rng.Int64N(60*Millisecond)
	} else {
		l.extra = s.rng.Int64N(2*Millisecond) + 1
	}
}

// resetLink resets every connection between two random nodes, as a
// middlebox's RST does, while the link stays up. It reports whether there
// was one.
func (s *Sim) resetLink() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return false
	}
	from, to := s.clientOr(s.randomLink)
	n := len(s.net.conns)
	s.net.resetBetween(from, to)
	if len(s.net.conns) == n {
		return false
	}
	s.stats.Resets++
	s.tracef("link %s-%s: connections reset", s.host(from), s.host(to))
	return true
}

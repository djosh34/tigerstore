package sim

import "time"

// clockTicks is a node-local ticker whose normal ticks drop while its
// clock is frozen. A forward jump sends extra ticks without waiting for
// time to pass. It does not pause the node, its disk, peers or clients.
// The process owns this goroutine: it continues through the simulator's
// final freeze, when a tick lets the committer reach its inspection hook.
// The lock is held by the caller.
func (s *Sim) clockTicks(p *proc, period time.Duration) <-chan time.Time {
	out := make(chan time.Time, 1)
	s.procs.Go(func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			s.mu.Lock()
			wake := p.n.tickWake
			s.mu.Unlock()
			normal := false
			select {
			case <-ticker.C:
				normal = true
			case <-wake:
			case <-p.ctx.Done():
				return
			}
			s.mu.Lock()
			burst := p.n.tickBurst
			p.n.tickBurst = 0
			if p.n.tickFrozenUntil > s.now() {
				normal = false
			}
			s.mu.Unlock()
			for range burst {
				select {
				case out <- time.Now():
					s.mu.Lock()
					s.jumpTicks = append(s.jumpTicks, s.now())
					s.stats.JumpTicks++
					s.mu.Unlock()
				case <-p.ctx.Done():
					return
				}
			}
			if normal {
				select {
				case out <- time.Now():
				default:
					// Like time.Ticker, normal ticks drop when the
					// committer has not consumed its buffered tick.
				}
			}
		}
	})
	return out
}

// scheduleClock moves the chosen node's clock forward by six seconds at
// 4 s and 16 s, and holds it still from 6 to 9 s and 12 to 15 s. A burst
// spans six election timeouts and more than the workload's largest TTL.
// Each phase picks the current leader or a current follower again: a
// previous forward jump can have replaced the original leader. If an
// election is in flight, wait for its result before targeting the role.
// The lock is held.
func (s *Sim) scheduleClock() {
	for _, at := range []int64{4 * Second, 6 * Second, 12 * Second, 16 * Second} {
		freeze := at == 6*Second || at == 12*Second
		s.at(at, func() {
			for !s.clockFault(freeze) && s.sleep(100*Millisecond) {
			}
		})
	}
}

func (s *Sim) clockFault(freeze bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healed {
		return true
	}
	lead := s.leaderIdx()
	if lead < 0 {
		return false
	}
	target := lead
	if s.cfg.NetworkTime == "clock-follower" {
		target = (lead + 1) % s.cfg.voters()
	}
	n := s.nodes[target]
	if freeze {
		n.tickFrozenUntil = min(s.now()+3*Second, s.healAt)
		s.stats.ClockFreezes++
		s.tracef("clock node %d: frozen until %d ms, leader now %d", n.id, n.tickFrozenUntil/Millisecond, lead+1)
	} else {
		n.tickBurst += 60
		s.tracef("clock node %d: forward 60 ticks, leader now %d", n.id, lead+1)
		close(n.tickWake)
		n.tickWake = make(chan struct{})
	}
	return true
}

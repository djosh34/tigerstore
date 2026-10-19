package sim

// membershipRollingFaults starts at a fixed time before healing, rather
// than after a promotion whose retries can consume the fault phase. A
// late membership change can overlap these restarts. A voter already down
// in a boundary fault must recover before this function power-cycles it.
func (s *Sim) membershipRollingFaults() {
	s.mu.Lock()
	s.stats.MembershipMixes[3]++
	s.mu.Unlock()
	for i := range 3 {
		for {
			s.mu.Lock()
			if s.healed {
				s.mu.Unlock()
				return
			}
			n := s.nodes[i]
			if n.proc == nil {
				s.mu.Unlock()
				if !s.sleep(100 * Millisecond) {
					return
				}
				continue
			}
			n.why = "round 3 rolling power loss"
			s.tracef("round 3 rolling power loss: node %d", n.id)
			s.crash(n)
			s.restartLater(n, Second)
			s.mu.Unlock()
			break
		}
		if !s.sleep(5 * Second) {
			return
		}
	}
}

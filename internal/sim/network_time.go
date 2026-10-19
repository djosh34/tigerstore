package sim

func (c Config) voters() int {
	if c.Voters == 0 {
		return 3
	}
	return c.Voters
}

// scheduleNetworkTime targets the elected leader rather than choosing an
// arbitrary mask. Clients remain connected to every node throughout.
// The lock is held. Each schedule leaves time after its last transition
// for the normal healing, lease and watch checks.
func (s *Sim) scheduleNetworkTime() {
	if s.cfg.NetworkTime == "" {
		return
	}
	s.at(4*Second, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		lead := s.leaderIdx()
		if lead < 0 {
			s.fail("network-time: no leader before the fault")
			return
		}
		target := lead
		if s.cfg.NetworkTime == "follower-island" {
			target = (lead + 1) % 3
		}
		s.tracef("network-time %s: original leader %d, target %d", s.cfg.NetworkTime, lead+1, target+1)
		switch s.cfg.NetworkTime {
		case "leader-incoming":
			// Swallow only incoming bytes. Unlike setLinks, this does not
			// reset both directions of an established TCP connection.
			for from := range s.nodes {
				if from != target {
					s.links[from][target].halfOpenUntil = s.healAt
					s.stats.HalfOpens++
				}
			}
		case "follower-island":
			s.island(target)
		case "minority-bridge":
			// Five voters: the old leader reaches just one follower, and
			// that follower still reaches the other three voters. The old
			// leader has two of five, not a quorum. Rotate the bridge while
			// the majority keeps writing and the old leader sees clients.
			for at := int64(0); at < s.cfg.Duration-4*Second; at += 2 * Second {
				s.after(at, func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					if s.healed {
						return
					}
					bridge := (lead + 1 + int(at/(2*Second))%4) % 5
					mask, bit := uint64(0), uint(0)
					for from := range s.nodes {
						for to := range s.nodes {
							if from == to {
								continue
							}
							if from != lead && to != lead || from == bridge || to == bridge {
								mask |= 1 << bit
							}
							bit++
						}
					}
					s.stats.LinkChanges++
					s.tracef("minority bridge: leader %d reaches only %d", lead+1, bridge+1)
					s.setLinks(mask)
				})
			}
		case "leader-flap":
			// The old leader alternates between isolation and reachability
			// every 250 ms, with a longer cut every two seconds to force
			// elections rather than only delaying appends.
			for at := int64(0); at < s.cfg.Duration-4*Second; at += 250 * Millisecond {
				s.after(at, func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					if s.healed {
						return
					}
					if at%(2*Second) < 1500*Millisecond || at%(500*Millisecond) == 0 {
						s.island(target)
					} else {
						s.stats.LinkChanges++
						s.setLinks(s.allLinks())
					}
				})
			}
		case "peer-slow-leader":
			// Only peer output waits. The node and its clients are not
			// paused, so reads must still obtain fresh quorum evidence.
			for to := range s.nodes {
				if to != target {
					s.links[target][to].stallUntil = s.healAt
					s.stats.LinkStalls++
				}
			}
		default:
			s.fail("unknown network-time schedule %q", s.cfg.NetworkTime)
		}
	})
}

// island disconnects only peers of target, not its clients. The lock is held.
func (s *Sim) island(target int) {
	mask, bit := uint64(0), uint(0)
	for from := range s.nodes {
		for to := range s.nodes {
			if from == to {
				continue
			}
			if from != target && to != target {
				mask |= 1 << bit
			}
			bit++
		}
	}
	s.stats.LinkChanges++
	s.setLinks(mask)
}

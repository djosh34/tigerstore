package sim

// membershipBoundaryFaults puts faults at the transitions rather than at
// independent random times. The ordinary clients keep writing, compacting
// and watching throughout. Crashes use simfs's power-loss semantics.
func (s *Sim) membershipBoundaryFaults() {
	s.mu.Lock()
	stage := s.membership.stage
	if stage == 0 || s.membership.mixed[stage] || s.healed {
		s.mu.Unlock()
		return
	}
	s.membership.mixed[stage] = true
	s.stats.MembershipMixes[stage-1]++
	if stage == 4 {
		s.mu.Unlock()
		// No membership changes remain. Restart every surviving voter,
		// including the leader, while compaction and cleaning continue.
		for i := range 3 {
			s.mu.Lock()
			if s.healed {
				s.mu.Unlock()
				return
			}
			n := s.nodes[i]
			n.why = "round 3 rolling power loss"
			s.crash(n)
			s.restartLater(n, Second)
			s.mu.Unlock()
			if !s.sleep(5 * Second) {
				return
			}
		}
		return
	}
	lead := s.leaderIdx()
	if lead < 0 {
		// Retry at the next membership attempt, not against a made-up leader.
		s.membership.mixed[stage] = false
		s.stats.MembershipMixes[stage-1]--
		s.mu.Unlock()
		return
	}
	learner := s.nodes[3]
	crashed := learner
	down := 6 * Second
	if stage == 2 {
		crashed, down = s.nodes[lead], 2*Second
	}
	crashed.why = "round 3 membership boundary power loss"
	s.crash(crashed)
	s.restartLater(crashed, down)
	// A second node has a stalled disk. A third has a directed partition.
	// This deliberately loses quorum briefly; liveness is required after
	// healing, not during an unavailable quorum.
	s.stall(s.nodes[(lead+1)%3], s.now()+2*Second)
	from, to := lead, (lead+2)%3
	if stage == 2 {
		// The leader is down. Cut the surviving voters' link, not a
		// link to the learner which the previous stage still holds down.
		from = (lead + 1) % 3
	}
	l := &s.links[from][to]
	l.up = false
	s.net.resetBetween(from, to)
	s.stats.LinkChanges++
	s.tracef("round 3 stage %d: crash %d, stall %d, cut %d->%d", stage, crashed.id, s.nodes[(lead+1)%3].id, s.nodes[from].id, s.nodes[to].id)
	s.after(4*Second, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		l.up = true
		s.tracef("round 3 stage %d: link healed", stage)
	})
	s.mu.Unlock()
}

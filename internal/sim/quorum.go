package sim

// hasQuorum reports whether a voter that can still run has an applied
// configuration in which a majority of the voters can still run. A node
// down for now comes back; a fenced or removed member does not. A joining
// node with no applied configuration cannot campaign, and a learner
// cannot campaign or vote. The lock is held.
func hasQuorum(nodes []*node) bool {
	alive := make(map[uint64]bool, len(nodes))
	for _, n := range nodes {
		alive[n.id] = !n.out()
	}
	for _, n := range nodes {
		if !alive[n.id] {
			continue
		}
		voters, votes, self := 0, 0, false
		for _, m := range n.lastMembers {
			if m.Learner {
				continue
			}
			voters++
			if alive[m.ID] {
				votes++
			}
			self = self || m.ID == n.id
		}
		if self && votes > voters/2 {
			return true
		}
	}
	return false
}

// recheckQuorum takes back, at the end, a loss of the quorum that a fence
// seemed to make, when a survivor has applied a configuration whose
// surviving voters are a majority by then: at the fence it held the
// change committed and not yet applied, or it applied it at a restart
// (#850's review). The run then takes the checks of a cluster that kept
// its quorum. A LoseQuorum run keeps its loss. The lock is held.
func (s *Sim) recheckQuorum() {
	if s.quorumLostAt == 0 || s.cfg.LoseQuorum || !hasQuorum(s.nodes) {
		return
	}
	s.tracef("the quorum lost at %d ms was not lost: a survivor applied a configuration with a surviving majority since", s.quorumLostAt/Millisecond)
	s.quorumLostAt, s.stats.QuorumLostAt = 0, 0
}

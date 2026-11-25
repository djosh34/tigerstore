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

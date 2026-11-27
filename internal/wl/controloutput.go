package wl

// #912: attribute only socket capacity actually added by inherited control
// serialization. Everything previously present, and every non-control growth,
// stays charged. This is not credit available to a new fetch destination.
func (c *client) controlSocketGrew(before int) {
	if delta := cap(c.out) - before; delta > 0 {
		c.controlCapacity += int64(delta)
		c.node.controlCapacity += int64(delta)
		c.controlOwner = c
	}
}

// Socket shrink/drop retires inherited capacity first. No exemption survives
// the backing it describes, including across pooled/empty socket reuse.
func (s *sock) shrinkControlCapacity(capacity int) {
	delta := min(s.controlCapacity, int64(max(0, cap(s.out)-capacity)))
	if delta > 0 {
		s.controlCapacity -= delta
		s.controlOwner.node.controlCapacity -= delta
	}
}

func (c *client) fundedSocketCapacity() int64 {
	return int64(cap(c.out)) - c.controlCapacity
}

// An empty control-grown buffer cannot be retained just because #897 has
// protected output: main would shrink it. Drop without a second live backing
// and convert already-funded independent capacity back into a finish promise.
func (s *sock) retireProtectedControlSocket() bool {
	if s.controlCapacity == 0 || s.outHold == nil || *s.outHold == 0 {
		return false
	}
	c := s.controlOwner
	if c.socketPromise == 0 {
		c.socketPromise = 64 << 10
		c.stagePromise += c.socketPromise
		c.node.stagePromises += c.socketPromise
	}
	s.shrinkControlCapacity(0)
	s.growOut(-cap(s.out))
	s.out = nil
	return true
}

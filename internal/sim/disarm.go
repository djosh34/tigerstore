package sim

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// disarmNoSpace disarms the NOSPACE alarm n raised when a disk fault left
// it no room for a checkpoint (wlHooks.NoRoom), the way an operator
// would, as etcd's runbook has one do once there is room again: once the
// fault is over, and 1 to 20 s later, it disarms n's NOSPACE through the
// leader, going again until a call is answered or the run stops. A disk
// that stays full keeps the alarm up, its refusals counted as such.
// Without an operator, a full disk that passed in seconds refused every
// write for the rest of the run (#769).
func (s *Sim) disarmNoSpace(n *node) {
	defer func() {
		s.mu.Lock()
		n.disarming = false
		s.mu.Unlock()
	}()
	for {
		s.mu.Lock()
		full := n.diskFull
		s.mu.Unlock()
		if !full {
			break
		}
		if !s.sleep(100 * Millisecond) {
			return
		}
	}
	s.mu.Lock()
	wait, id := Second+s.rng.Int64N(19*Second), n.id
	s.mu.Unlock()
	if !s.sleep(wait) {
		return
	}
	req := &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, MemberID: id, Alarm: pb.AlarmType_NOSPACE}
	for {
		s.mu.Lock()
		i, stopped := s.leaderIdx(), s.now() >= s.stopAt
		s.mu.Unlock()
		if stopped {
			return
		}
		if i >= 0 {
			_, err := s.call(s.nodes[i], req, s.cfg.ClientTimeout)
			s.mu.Lock()
			s.tracef("node %d: an operator disarms its NOSPACE: %v", id, err)
			if err == nil {
				s.stats.Disarms++
			}
			s.mu.Unlock()
			if err == nil {
				return
			}
		}
		if !s.sleep(500 * Millisecond) {
			return
		}
	}
}

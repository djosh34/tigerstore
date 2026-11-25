package sim

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/wl"
)

// disarmNoSpace disarms the NOSPACE alarm n raised when a disk fault left
// it no room for a checkpoint (wlHooks.NoRoom), the way an operator
// would, as etcd's runbook has one do once there is room again: once the
// fault is over, it watches the node while it tries its checkpoint again,
// twice wl's wait between tries and 1 to 20 s more, then disarms n's
// NOSPACE through the leader, going again until a call is answered or the
// run stops. A node that found no room again since the fault ended has a
// disk too small for its store, not a fault: the operator leaves its
// alarm up, and the calls NOSPACE refuses stay counted as refused. Without
// an operator, a full disk that passed in seconds refused every write for
// the rest of the run (#769).
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
	over, wait, id := s.now(), 2*int64(wl.CheckpointRoomRetry)+Second+s.rng.Int64N(19*Second), n.id
	s.mu.Unlock()
	if !s.sleep(wait) {
		return
	}
	req := &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, MemberID: id, Alarm: pb.AlarmType_NOSPACE}
	for {
		s.mu.Lock()
		i, stopped, short := s.leaderIdx(), s.now() >= s.stopAt, n.noRoomAt > over
		if short {
			s.tracef("node %d: no room again after the disk fault: an operator leaves its NOSPACE up", id)
		}
		s.mu.Unlock()
		if stopped || short {
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

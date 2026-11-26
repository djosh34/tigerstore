package sim

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/wl"
)

// growDisk is the operator of Faults.GrowDisk for a node whose disk is too
// small for its store, with no fault on it (wlHooks.NoRoom): 2 to 5 s
// later it grows every disk eight times, as an operator adds room, and
// then disarms every NOSPACE alarm as disarmNoSpace does, once the nodes
// had time to try their checkpoints again. It does so once a run: a test of a
// tight disk that recovers (#769, #846's ruling).
func (s *Sim) growDisk(n *node) {
	s.mu.Lock()
	wait := 2*Second + s.rng.Int64N(3*Second)
	s.mu.Unlock()
	if !s.sleep(wait) {
		s.mu.Lock()
		n.disarming = false
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	// The run's capacity grows, so that a start, a heal or a replacement,
	// which each set a disk's capacity from it, keep the room.
	s.cfg.Faults.DiskCapacity *= 8
	for _, m := range s.nodes {
		m.disk.Capacity = s.diskCapacity()
	}
	s.stats.DiskGrowths++
	s.tracef("node %d: no room for a checkpoint on its disk: an operator grows every disk to %d bytes", n.id, s.diskCapacity())
	s.mu.Unlock()
	s.disarmNoSpaceAfter(n, true)
}

// disarmNoSpace disarms the NOSPACE alarm n raised when a disk fault left
// it no room for a checkpoint (wlHooks.NoRoom), the way an operator
// would, as etcd's runbook has one do once there is room again: once the
// fault is over and no other came for twice wl's wait between tries and 1
// to 20 s more, while the node tries its checkpoint again, it disarms n's
// NOSPACE through the leader, going again until a call is answered or the
// run stops. A node that found no room with no fault on its disk since
// then has a disk too small for its store: the operator leaves its alarm
// up, and the calls NOSPACE refuses stay counted as refused. Without
// an operator, a full disk that passed in seconds refused every write for
// the rest of the run (#769).
func (s *Sim) disarmNoSpace(n *node) { s.disarmNoSpaceAfter(n, false) }

// disarmNoSpaceAfter waits for room, then disarms n's alarm, or with all,
// every NOSPACE alarm after a growth. Each alarm disarmed counts once;
// DisarmedAt is the last answer. The growth checks the list again until
// none is left, as another node may have raised its alarm on the old room.
func (s *Sim) disarmNoSpaceAfter(n *node, all bool) {
	defer func() {
		s.mu.Lock()
		n.disarming = false
		s.mu.Unlock()
	}()
	s.mu.Lock()
	wait, id := 2*int64(wl.CheckpointRoomRetry)+Second+s.rng.Int64N(19*Second), n.id
	s.mu.Unlock()
	// The fault over, and over for wait, as another may come meanwhile.
	var over int64
	for {
		s.mu.Lock()
		full, now := n.diskFull, s.now()
		if all {
			for _, m := range s.nodes {
				full = full || m.diskFull
			}
		}
		s.mu.Unlock()
		if full {
			over = 0
		} else if over == 0 {
			over = now
		}
		if over > 0 && now-over >= wait {
			break
		}
		if !s.sleep(100 * Millisecond) {
			return
		}
	}
	for {
		s.mu.Lock()
		i, stopped, tight := s.leaderIdx(), s.now() >= s.stopAt, n
		if all {
			for _, m := range s.nodes {
				if m.noRoomAt > tight.noRoomAt {
					tight = m
				}
			}
		}
		short := tight.noRoomAt > over
		if short {
			s.tracef("node %d: no room after the disk fault ended: an operator leaves its NOSPACE up", tight.id)
		}
		s.mu.Unlock()
		if stopped || short {
			return
		}
		if i >= 0 {
			alarms := []*pb.AlarmMember{{MemberID: id, Alarm: pb.AlarmType_NOSPACE}}
			var listErr error
			if all {
				resp, err := s.call(s.nodes[i], &pb.AlarmRequest{Action: pb.AlarmRequest_GET}, s.cfg.ClientTimeout)
				listErr = err
				if err == nil {
					alarms = resp.(*pb.AlarmResponse).Alarms
				}
			}
			if listErr == nil {
				found := false
				for _, a := range alarms {
					if a.Alarm != pb.AlarmType_NOSPACE {
						continue
					}
					found = true
					req := &pb.AlarmRequest{Action: pb.AlarmRequest_DEACTIVATE, MemberID: a.MemberID, Alarm: pb.AlarmType_NOSPACE}
					resp, err := s.call(s.nodes[i], req, s.cfg.ClientTimeout)
					s.mu.Lock()
					s.tracef("node %d: an operator disarms its NOSPACE: %v", a.MemberID, err)
					// wl answers a deactivate with the alarm only if it
					// was raised: one cleared already, by the other
					// operator or before a lagging leader applied it, is
					// not counted again.
					if r, ok := resp.(*pb.AlarmResponse); err == nil && ok && len(r.Alarms) > 0 {
						s.stats.Disarms++
						s.stats.DisarmedAt = s.now()
					}
					s.mu.Unlock()
					if err == nil && !all {
						return
					}
				}
				if !found {
					return
				}
			}
		}
		if !s.sleep(500 * Millisecond) {
			return
		}
	}
}

package sim

import (
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// membershipWork serializes changes, but not KV traffic or faults. The
// oracle uses the requested URLs and learner state, not the core's state
// hash: a mistake shared by every replica must still fail.
type membershipWork struct {
	stage int
	id    uint64
	list  bool
	// removedAt is when the removal of the fourth member completed, and
	// last the last answer that did not complete a stage.
	removedAt int64
	last      string
}

const (
	memberURL        = "https://member:2380"
	updatedMemberURL = "https://updated-member:2380"
)

// changeMembership adds the fourth member as a learner, moves it to
// another URL, promotes it and removes it, one change after the other,
// each through the leader as a client would, until all four completed or
// the clients stop. A lost reply is resolved by a linearizable member list
// before another attempt.
func (s *Sim) changeMembership() {
	w := &s.membership
	for {
		s.mu.Lock()
		if w.stage == 4 || s.now() >= s.stopAt {
			s.mu.Unlock()
			return
		}
		i := s.leaderIdx()
		if i < 0 {
			s.mu.Unlock()
			if !s.sleep(100 * Millisecond) {
				return
			}
			continue
		}
		var req proto.Message
		if w.list {
			req = &pb.MemberListRequest{Linearizable: true}
		} else {
			switch w.stage {
			case 0:
				req = &pb.MemberAddRequest{PeerURLs: []string{memberURL}, IsLearner: true}
			case 1:
				req = &pb.MemberUpdateRequest{ID: w.id, PeerURLs: []string{updatedMemberURL}}
			case 2:
				req = &pb.MemberPromoteRequest{ID: w.id}
			case 3:
				req = &pb.MemberRemoveRequest{ID: w.id}
			}
		}
		s.stats.MembershipAttempts++
		// An add has no client-chosen ID. Wait for its core reply
		// (including the core's own timeout) or a lost connection before
		// retrying. A shorter caller deadline could leave an unsubmitted
		// add queued; after a later URL update it could legitimately add
		// a fifth member.
		timeout := s.cfg.ClientTimeout
		if w.stage == 0 && !w.list {
			timeout = 0
		}
		s.mu.Unlock()
		resp, err := s.call(s.nodes[i], req, timeout)
		if !s.memberReply(req, resp, err) || !s.sleep(500*Millisecond) {
			return
		}
	}
}

// memberReply checks the reply to a membership call and moves on to the
// next stage once it matches. It reports false once the run failed.
func (s *Sim) memberReply(req, resp proto.Message, err error) bool {
	s.mu.Lock()
	w := &s.membership
	if err != nil {
		s.tracef("membership: stage %d: %v", w.stage, err)
		w.last = fmt.Sprintf("%T at %s: %v", req, seconds(s.now()), err)
		w.list = true
		s.mu.Unlock()
		return true
	}
	list, ok := resp.(interface{ GetMembers() []*pb.Member })
	if !ok {
		s.fail("membership: %T answered with %T", req, resp)
		s.mu.Unlock()
		return false
	}
	ms := list.GetMembers()
	match := w.matches(ms)
	if !match && !w.list {
		phase := "after healing"
		if !s.healed {
			phase = "during faults"
		}
		s.fail("membership: stage %d acknowledged %v but returned %v %s", w.stage, req, ms, phase)
		s.mu.Unlock()
		return false
	}
	w.list = false
	if !match {
		w.last = fmt.Sprintf("the member list at %s: %s", seconds(s.now()), describeMembers(list.GetMembers()))
		s.mu.Unlock()
		return true
	}
	s.stats.MembershipChanges[w.stage]++
	if !s.healed {
		s.stats.MembershipFaults[w.stage]++
	}
	s.tracef("membership: stage %d done", w.stage)
	if w.stage == 3 {
		w.removedAt = s.now()
	}
	w.stage++
	if w.stage > 1 {
		s.mu.Unlock()
		return true
	}
	w.id = ms[len(ms)-1].ID
	n := s.nodes[3]
	n.id, n.inactive = w.id, false
	s.mu.Unlock()
	if err := s.start(n); err != nil {
		s.mu.Lock()
		s.fail("membership: start learner: %w", err)
		s.mu.Unlock()
		return false
	}
	return true
}

func (w *membershipWork) matches(ms []*pb.Member) bool {
	count := 4
	if w.stage >= 3 {
		count = 3
	}
	if len(ms) != count {
		return false
	}
	for i := range 3 {
		if ms[i].ID != uint64(i+1) || ms[i].IsLearner || !slices.Equal(ms[i].PeerURLs, []string{peerURL(i)}) {
			return false
		}
	}
	if w.stage >= 3 {
		return true
	}
	m := ms[3]
	url := updatedMemberURL
	if w.stage == 0 {
		url = memberURL
	}
	return m.ID > 3 && (w.stage == 0 || m.ID == w.id) &&
		m.IsLearner == (w.stage < 2) && slices.Equal(m.PeerURLs, []string{url})
}

// membershipConfig keeps the standard fault mechanisms, with more time
// between the faults that break connections, so that changes can commit
// before healing: as etcd's, a node refuses a change unless it has been
// connected to the other voters for 5 s. For the same reason healing
// lasts 20 s: the first change after it can wait 10 s for that (seeds 14
// and 58), and three more follow. Corruption is left to the
// three-voter runs: a permanently fenced learner cannot catch up and be
// promoted, so that workload would require disk replacement too.
func membershipConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Membership = true
	cfg.Duration = 60 * Second
	cfg.Heal = 20 * Second
	cfg.NoLeases = true
	cfg.Clients = 3
	cfg.Faults.Corrupt, cfg.Faults.Rot = false, false
	cfg.Faults.CrashEvery = 30 * Second
	cfg.Faults.LinkEvery = 20 * Second
	cfg.Faults.DiskErrorEvery = 30 * Second
	cfg.Faults.HalfOpenEvery = 30 * Second
	return cfg
}

func (s *Sim) checkMembership() error {
	if w := s.membership; w.stage != 4 {
		return fmt.Errorf("membership: completed %d of 4 changes; the last answer that did not complete stage %d was %s", w.stage, w.stage, w.last)
	}
	for _, n := range s.live() {
		if c := n.proc.core; !s.membership.matches(c.Members()) || !slices.Contains(c.RemovedMembers(), s.membership.id) {
			return fmt.Errorf("membership: node %d lost the final members or removal tombstone", n.id)
		}
	}
	return nil
}

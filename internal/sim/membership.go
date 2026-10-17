package sim

import (
	"fmt"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// membershipWork serializes changes, but not KV traffic or faults. A lost
// reply is resolved by a linearizable member list before another attempt.
// The oracle uses the requested URLs and learner state, not the core's
// state hash: a mistake shared by every replica must still fail.
type membershipWork struct {
	stage      int
	id         uint64
	busy, list bool
}

const (
	memberURL        = "https://member:2380"
	updatedMemberURL = "https://updated-member:2380"
)

func (s *Sim) changeMembership() {
	w := &s.membership
	if w.stage == 4 || s.now >= s.stopAt {
		return
	}
	if w.busy {
		return
	}
	i := s.leaderIdx()
	if i < 0 {
		s.push(&event{at: s.now + 100*Millisecond, kind: evMembership})
		return
	}
	n := s.nodes[i]
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
	w.busy = true
	s.stats.MembershipAttempts++
	op := &clientOp{member: true, rec: &Record{Node: i, Call: s.now, req: req}}
	// An add has no client-chosen ID. Wait for its core reply (including
	// the core's own timeout) or a lost connection before retrying. A
	// shorter caller deadline could leave an unsubmitted add queued;
	// after a later URL update it could legitimately add a fifth member.
	if w.stage != 0 || w.list {
		s.push(&event{at: s.now + s.cfg.ClientTimeout, kind: evClientTimeout, op: op})
	}
	s.push(&event{at: s.now + s.clientLatency(), kind: evSubmit, node: i, gen: n.gen, op: op})
}

func (s *Sim) complete(op *clientOp, resp proto.Message, err error) {
	if !op.member {
		op.client.done(op, resp, err)
		return
	}
	if op.done {
		return
	}
	op.done = true
	w := &s.membership
	w.busy = false
	if err != nil {
		w.list = true
	} else {
		list, ok := resp.(interface{ GetMembers() []*pb.Member })
		if !ok {
			s.fail("membership: %T answered with %T", op.rec.req, resp)
			return
		}
		ms := list.GetMembers()
		match := w.matches(ms)
		if !match && !w.list {
			phase := "after healing"
			if !s.healed {
				phase = "during faults"
			}
			s.fail("membership: stage %d acknowledged %v but returned %v %s", w.stage, op.rec.req, ms, phase)
			return
		}
		if match {
			s.stats.MembershipChanges[w.stage]++
			if !s.healed {
				s.stats.MembershipFaults[w.stage]++
			}
			if w.stage == 0 {
				w.id = ms[len(ms)-1].ID
				n := s.nodes[3]
				n.id, n.inactive = w.id, false
				if err := s.start(n); err != nil {
					s.fail("membership: start learner: %w", err)
					return
				}
			}
			w.stage++
		}
		w.list = false
	}
	s.mix(0x6d, uint64(w.stage), w.id)
	s.push(&event{at: s.now + 500*Millisecond, kind: evMembership})
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
		if ms[i].ID != uint64(i+1) || ms[i].IsLearner || len(ms[i].PeerURLs) != 0 {
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
// between faults so changes can commit before healing. Corruption is left
// to the three-voter runs: a permanently fenced learner cannot catch up
// and be promoted, so that workload would require disk replacement too.
func membershipConfig(seed uint64) Config {
	cfg := DefaultConfig(seed)
	cfg.Membership = true
	cfg.Duration = 40 * Second
	cfg.NoLeases = true
	cfg.Clients = 3
	cfg.Faults.Corrupt, cfg.Faults.Rot = false, false
	cfg.Faults.CrashEvery = 8 * Second
	cfg.Faults.LinkEvery = 5 * Second
	return cfg
}

// A real sender dials even when Raft has no messages, with a retry
// backoff capped at one second. This matters for a removed learner:
// learners never campaign, so waiting for a vote request is not enough
// to deliver the peer's removed-member reply.
func (s *Sim) memberConnect(n *node) {
	for _, peer := range s.nodes {
		if peer == n || !peer.up || peer.pausedUntil > s.now {
			continue
		}
		out, back := &s.links[n.idx][peer.idx], &s.links[peer.idx][n.idx]
		if !out.up || !back.up || out.halfOpenUntil > s.now || back.halfOpenUntil > s.now || out.stallUntil > s.now {
			continue
		}
		if slices.Contains(peer.core.RemovedMembers(), n.id) {
			s.push(&event{at: max(s.now+out.base+back.base, back.stallUntil), kind: evRemoved, node: n.idx, from: peer.idx, gen: back.epoch})
		}
	}
}

func (s *Sim) checkMembership() error {
	if s.membership.stage != 4 {
		return fmt.Errorf("membership: completed %d of 4 changes", s.membership.stage)
	}
	for _, n := range s.live() {
		if !s.membership.matches(n.core.Members()) || !slices.Contains(n.core.RemovedMembers(), s.membership.id) {
			return fmt.Errorf("membership: node %d lost the final members or removal tombstone", n.id)
		}
	}
	return nil
}

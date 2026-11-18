package sim

import (
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// replace replaces n, fenced for good, the way an operator would
// (Config.Replace, #495): after 1 to 20 s, it removes n's member through
// the leader, empties n's disk, adds a member at n's peer URL, and starts
// n as that member, which joins the running cluster. Each call goes again
// until it is done; one that got no answer is resolved by a linearizable
// member list first, as the membership work does. With no quorum there
// is no leader, so nothing is replaced.
func (s *Sim) replace(n *node) {
	s.mu.Lock()
	wait, old, url := Second+s.rng.Int64N(19*Second), n.id, peerURL(n.idx)
	s.mu.Unlock()
	if !s.sleep(wait) {
		return
	}
	s.mu.Lock()
	s.tracef("node %d: an operator replaces it", old)
	s.mu.Unlock()
	if !s.memberCall(&pb.MemberRemoveRequest{ID: old}, func(ms []*pb.Member) bool {
		return !slices.ContainsFunc(ms, func(m *pb.Member) bool { return m.ID == old })
	}) {
		return
	}
	var id uint64
	if !s.memberCall(&pb.MemberAddRequest{PeerURLs: []string{url}}, func(ms []*pb.Member) bool {
		i := slices.IndexFunc(ms, func(m *pb.Member) bool { return slices.Contains(m.PeerURLs, url) })
		if i >= 0 {
			id = ms[i].ID
		}
		return i >= 0
	}) {
		return
	}
	s.mu.Lock()
	// The operator empties the disk: the node starts as a new member.
	disk := simfs.New()
	disk.Capacity = s.diskCapacity()
	disk.KeepIn(s.pages, keepPages)
	// The times the old member stalled or started again from its log stay,
	// for the checks of the calls made to it.
	*n = node{
		idx: n.idx, id: id, disk: disk, addrs: n.addrs, wake: n.wake, gen: n.gen, joins: true, since: s.now(),
		stalls: n.stalls, reloads: n.reloads, fromLog: n.fromLog,
	}
	s.traceDisk(n)
	s.stats.Replaced++
	s.tracef("node %d: replaced by member %d on an empty disk", old, id)
	if s.healed {
		// The new member joins after healing: the liveness rule waits
		// for it, as for a node that stopped then.
		s.lateStopAt = s.now()
		s.extendLiveness()
	}
	s.mu.Unlock()
	if err := s.start(n); err != nil {
		s.mu.Lock()
		s.fail("replacing node %d: start member %d: %w", old, id, err)
		s.mu.Unlock()
	}
}

// memberCall makes a membership call through the leader until done says
// the member list shows it, and reports false if the run ended first. A
// call that got no answer is resolved by a linearizable member list
// before it goes again. An add has no client-chosen ID, so it waits for
// the node's own answer, as the membership work's does.
func (s *Sim) memberCall(req proto.Message, done func([]*pb.Member) bool) bool {
	timeout := s.cfg.ClientTimeout
	if _, ok := req.(*pb.MemberAddRequest); ok {
		timeout = 0
	}
	list := false
	for {
		s.mu.Lock()
		i, stopped := s.leaderIdx(), s.now() >= s.stopAt
		s.mu.Unlock()
		if stopped {
			return false
		}
		if i < 0 {
			if !s.sleep(100 * Millisecond) {
				return false
			}
			continue
		}
		call := req
		if list {
			call = &pb.MemberListRequest{Linearizable: true}
		}
		resp, err := s.call(s.nodes[i], call, timeout)
		if err == nil {
			if r, ok := resp.(interface{ GetMembers() []*pb.Member }); ok && done(r.GetMembers()) {
				return true
			}
		}
		// Unanswered, or the list does not show it yet.
		list = err != nil
		s.mu.Lock()
		s.tracef("membership call %T: %v", call, err)
		s.mu.Unlock()
		if !s.sleep(500 * Millisecond) {
			return false
		}
	}
}

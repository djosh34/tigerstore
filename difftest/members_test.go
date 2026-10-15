//go:build difftest

package difftest

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// learners records the learners among members, and whether the header
// carries a revision: etcd's Cluster service sends none. The voters are
// left out, since etcd runs one node and tigerstore three.
func (r *run) learners(do string, header *pb.ResponseHeader, members []*pb.Member, err error) {
	r.t.Helper()
	var res any
	if err == nil {
		type learner struct {
			Name     string   `json:"name,omitempty"`
			PeerURLs []string `json:"peerURLs"`
		}
		var ls []learner
		for _, m := range members {
			if m.IsLearner {
				ls = append(ls, learner{m.Name, m.PeerURLs})
			}
		}
		res = struct {
			Revision bool      `json:"revision,omitempty"`
			Learners []learner `json:"learners,omitempty"`
		}{header.GetRevision() != 0, ls}
	}
	r.record(do, res, err)
}

// members adds, updates, promotes and removes a learner, which a cluster
// of one etcd node can do without losing its quorum, and records etcd's
// errors for the requests it refuses.
func members(r *run) {
	c := r.c.cluster
	list := func(do string) {
		resp, err := c.MemberList(r.ctx, &pb.MemberListRequest{Linearizable: true})
		r.learners(do, resp.GetHeader(), resp.GetMembers(), err)
	}
	list("list")
	add := func(do string, req *pb.MemberAddRequest) uint64 {
		resp, err := c.MemberAdd(r.ctx, req)
		r.learners(do, resp.GetHeader(), resp.GetMembers(), err)
		return resp.GetMember().GetID()
	}
	add("add a member with a bad URL", &pb.MemberAddRequest{PeerURLs: []string{"127.0.0.1:1"}, IsLearner: true})
	add("add a member with a path in its URL", &pb.MemberAddRequest{PeerURLs: []string{"https://127.0.0.1:1/x"}, IsLearner: true})
	id := add("add a learner", &pb.MemberAddRequest{PeerURLs: []string{"https://127.0.0.1:3", "https://127.0.0.2:1"}, IsLearner: true})
	add("add a second learner", &pb.MemberAddRequest{PeerURLs: []string{"https://127.0.0.1:4"}, IsLearner: true})
	add("add a learner with a taken peer URL", &pb.MemberAddRequest{PeerURLs: []string{"https://127.0.0.2:1"}, IsLearner: true})
	list("list with a learner")
	upd, err := c.MemberUpdate(r.ctx, &pb.MemberUpdateRequest{ID: id, PeerURLs: []string{"https://127.0.0.1:5"}})
	r.learners("update the learner's peer URLs", upd.GetHeader(), upd.GetMembers(), err)
	upd, err = c.MemberUpdate(r.ctx, &pb.MemberUpdateRequest{ID: id + 1000, PeerURLs: []string{"https://127.0.0.1:6"}})
	r.learners("update a member that does not exist", upd.GetHeader(), upd.GetMembers(), err)
	prom, err := c.MemberPromote(r.ctx, &pb.MemberPromoteRequest{ID: id})
	r.learners("promote the learner that never started", prom.GetHeader(), prom.GetMembers(), err)
	st, err := r.c.maint.Status(r.ctx, &pb.StatusRequest{})
	if err != nil {
		r.t.Fatal(err)
	}
	prom, err = c.MemberPromote(r.ctx, &pb.MemberPromoteRequest{ID: st.Header.MemberId})
	r.learners("promote a voter", prom.GetHeader(), prom.GetMembers(), err)
	prom, err = c.MemberPromote(r.ctx, &pb.MemberPromoteRequest{ID: id + 1000})
	r.learners("promote a member that does not exist", prom.GetHeader(), prom.GetMembers(), err)
	rm, err := c.MemberRemove(r.ctx, &pb.MemberRemoveRequest{ID: id + 1000})
	r.learners("remove a member that does not exist", rm.GetHeader(), rm.GetMembers(), err)
	rm, err = c.MemberRemove(r.ctx, &pb.MemberRemoveRequest{ID: id})
	r.learners("remove the learner", rm.GetHeader(), rm.GetMembers(), err)
	rm, err = c.MemberRemove(r.ctx, &pb.MemberRemoveRequest{ID: id})
	r.learners("remove the learner again", rm.GetHeader(), rm.GetMembers(), err)
	list("list after removing the learner")
}

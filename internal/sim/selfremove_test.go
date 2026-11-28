package sim

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simrun"
)

// The membership work adds and promotes a second voter, then removes it
// through whichever node leads. On main, seeds 9, 182, 228, 260, 279 and
// 295 of the 300-seed sweep stopped at three of four membership changes:
// a removing leader exited before its survivor learned the commit (#922).
// SIM_SEED=1 SIM_SEEDS=300 runs the qualification sweep under gosim.
func TestSelfRemovalOneVoterMembership(t *testing.T) {
	simrun.Seeds(t, 1, 16, func(t *testing.T, seed uint64) Stats {
		cfg := wlMembershipConfig(seed)
		cfg.Nodes = 1
		res := run(t, cfg)
		for stage, count := range res.Stats.MembershipChanges {
			if stage == 3 && count == 0 && res.Stats.StuckRemovalAt > 0 {
				// The removal left two voters without a quorum, as
				// etcd's does (stuckRemoval).
				continue
			}
			if count != 1 {
				t.Fatalf("seed %d: stage %d completed %d times", seed, stage, count)
			}
		}
		checkers(t, res)
		return res.Stats
	}, nil)
}

// TestARemovedLeaderReplaysItsRemovalBehindTheFence is #924: of two
// voters, the leader removes itself, and the survivor drops every message
// of the leader that carries the removal's commit. The leader crashes once
// that commit is durable, inside its wait for the survivor to know it, and
// starts again. Its start replays the removal; it must stay up, as Raft
// still counts it among the voters, until the survivor learned the commit,
// so that the survivor leads alone and takes a put. Before, the start
// stopped at once on the removal it replayed, and the survivor waited for
// its vote for good.
func TestARemovedLeaderReplaysItsRemovalBehindTheFence(t *testing.T) {
	if simrun.Drive(t, 924) {
		return
	}
	simrun.Bubble(t, func(t *testing.T) {
		cfg := DefaultConfig(924)
		cfg.Nodes = 1
		cfg.Membership = true
		cfg.Faults = Faults{}
		s, err := newSim(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if t.Failed() {
				explainSim(t, cfg, s)
			}
			s.shutdown()
		}()
		lead, survivor := s.nodes[0], s.nodes[1]
		survivor.inactive = true
		if err := s.start(lead); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// call retries req through n until it succeeds, for up to 20 s.
		call := func(n *node, req proto.Message) proto.Message {
			t.Helper()
			for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(500 * time.Millisecond) {
				cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
				resp, err := s.callWL(n.proc, req, cctx)
				ccancel()
				if err == nil {
					return resp
				}
				if time.Now().After(deadline) {
					s.mu.Lock()
					defer s.mu.Unlock()
					t.Fatalf("%T through node %d: %v (run: %v)", req, n.id, err, s.err)
				}
			}
		}
		time.Sleep(2 * time.Second)
		add := call(lead, &pb.MemberAddRequest{PeerURLs: []string{memberURL}, IsLearner: true}).(*pb.MemberAddResponse)
		s.mu.Lock()
		survivor.id, survivor.inactive = add.GetMember().GetID(), false
		s.mu.Unlock()
		if err := s.start(survivor); err != nil {
			t.Fatal(err)
		}
		call(lead, &pb.MemberPromoteRequest{ID: survivor.id})
		time.Sleep(2 * time.Second)

		s.mu.Lock()
		removal := lead.appended + 1
		survivor.drop = func(m *raftpb.Message) bool { return m.GetFrom() == lead.id && m.GetCommit() >= removal }
		s.mu.Unlock()
		go func() { _, _ = s.callWL(lead.proc, &pb.MemberRemoveRequest{ID: lead.id}, ctx) }()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
			s.mu.Lock()
			if lead.hs.commit >= removal {
				s.crash(lead)
				survivor.drop = nil
				s.mu.Unlock()
				break
			}
			s.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatalf("the removal at %d did not commit durably on node %d in 10 s", removal, lead.id)
			}
		}
		if err := s.start(lead); err != nil {
			t.Fatal(err)
		}
		call(survivor, &pb.PutRequest{Key: []byte("after"), Value: []byte("x")})
	})
}

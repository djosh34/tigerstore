package core

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

func (s *single) members(id uint64) []*pb.Member {
	s.t.Helper()
	r, ok := s.ok(id, &pb.MemberListRequest{Linearizable: true}).(*pb.MemberListResponse)
	if !ok {
		s.t.Fatal("not a member list")
	}
	return r.Members
}

// TestMemberChanges follows etcd v3.7.2's rules for member changes on a
// one-node cluster: a learner is added, updated and removed, each change
// answers the members after it, the errors are etcd's, a removed ID never
// comes back, and the members survive a restart from a checkpoint.
func TestMemberChanges(t *testing.T) {
	disk := simfs.New()
	s := startSingleWith(t, disk, 1, func(cfg *Config) {
		tinyCheckpoints(cfg)
		cfg.Members = []*pb.Member{{ID: 1, Name: "one", PeerURLs: []string{"https://a:1"}}}
		cfg.Name, cfg.ClientURLs = "one", []string{"https://a:2"}
	}).capable()
	for i := range 20 {
		s.n.Tick()
		s.run()
		if len(s.members(uint64(1 + i))[0].ClientURLs) > 0 {
			break
		}
	}
	one := &pb.Member{ID: 1, Name: "one", PeerURLs: []string{"https://a:1"}, ClientURLs: []string{"https://a:2"}}
	if got := s.members(30); !sameMembers(got, one) {
		t.Fatalf("members after publishing: %v", got)
	}
	add, ok := s.ok(31, &pb.MemberAddRequest{PeerURLs: []string{"https://b:1"}, IsLearner: true}).(*pb.MemberAddResponse)
	if !ok || add.Member.ID != 2 || !add.Member.IsLearner || add.Header.Revision != 0 || add.Header.MemberId != 1 {
		t.Fatalf("add: %v", add)
	}
	learner := &pb.Member{ID: 2, PeerURLs: []string{"https://b:1"}, IsLearner: true}
	if !sameMembers(add.Members, one, learner) {
		t.Fatalf("members after the add: %v", add.Members)
	}
	errs := []struct {
		op   proto.Message
		want error
	}{
		{&pb.MemberAddRequest{PeerURLs: []string{"https://b:1"}}, rpctypes.ErrGRPCPeerURLExist},
		{&pb.MemberAddRequest{PeerURLs: []string{"https://c:1"}, IsLearner: true}, rpctypes.ErrGRPCTooManyLearners},
		{&pb.MemberPromoteRequest{ID: 1}, rpctypes.ErrGRPCMemberNotLearner},
		{&pb.MemberPromoteRequest{ID: 2}, rpctypes.ErrGRPCLearnerNotReady},
		{&pb.MemberRemoveRequest{ID: 9}, rpctypes.ErrGRPCMemberNotFound},
		{&pb.MemberUpdateRequest{ID: 9, PeerURLs: []string{"https://d:1"}}, rpctypes.ErrGRPCMemberNotFound},
		{&pb.MemberUpdateRequest{ID: 2, PeerURLs: []string{"https://a:1"}}, rpctypes.ErrGRPCPeerURLExist},
	}
	for i, c := range errs {
		if r := s.do(uint64(40+i), c.op); !errors.Is(r.Err, c.want) {
			t.Fatalf("%T %v: %v, want %v", c.op, c.op, r.Err, c.want)
		}
	}
	upd, ok := s.ok(50, &pb.MemberUpdateRequest{ID: 2, PeerURLs: []string{"https://b:2"}}).(*pb.MemberUpdateResponse)
	learner.PeerURLs = []string{"https://b:2"}
	if !ok || !sameMembers(upd.Members, one, learner) {
		t.Fatalf("update: %v", upd)
	}
	if c := s.n.storage.confAt(^uint64(0)); len(c.Learners) != 1 || c.Learners[0] != 2 || len(c.Voters) != 1 {
		t.Fatalf("Raft's configuration: %v", s.n.storage.confAt(^uint64(0)))
	}

	// Compactions let the old values, and so the first segment, go.
	old := s
	s = restartFromCheckpoint(t, s, disk, func(i int) proto.Message {
		if i%5 == 4 {
			return &pb.CompactionRequest{Revision: old.n.kv.Rev()}
		}
		return &pb.PutRequest{Key: []byte("fill"), Value: make([]byte, 100)}
	})
	if got := s.members(60); !sameMembers(got, one, learner) {
		t.Fatalf("members after a restart from a checkpoint: %v", got)
	}
	rm, ok := s.ok(61, &pb.MemberRemoveRequest{ID: 2}).(*pb.MemberRemoveResponse)
	if !ok || !sameMembers(rm.Members, one) {
		t.Fatalf("remove: %v", rm)
	}
	if r := s.do(62, &pb.MemberRemoveRequest{ID: 2}); !errors.Is(r.Err, rpctypes.ErrGRPCMemberNotFound) {
		t.Fatalf("removing a removed member: %v", r.Err)
	}
	again, ok := s.ok(63, &pb.MemberAddRequest{PeerURLs: []string{"https://b:2"}, IsLearner: true}).(*pb.MemberAddResponse)
	if !ok || again.Member.ID != 3 {
		t.Fatalf("a new member got %v; a removed ID never comes back", again)
	}
}

// TestLastVoterStays removes the only voter, with and without
// --strict-reconfig-check. Raft cannot remove the last voter: the change
// is refused, the node goes on with its learner, and it starts again
// from its log.
func TestLastVoterStays(t *testing.T) {
	for _, strict := range []bool{false, true} {
		disk := simfs.New()
		change := func(cfg *Config) { cfg.StrictReconfigCheck = strict }
		s := startSingleWith(t, disk, 1, change).capable()
		s.ok(1, &pb.MemberAddRequest{PeerURLs: []string{"https://b:1"}, IsLearner: true})
		if r := s.do(2, &pb.MemberRemoveRequest{ID: 1}); !errors.Is(r.Err, rpctypes.ErrGRPCMemberNotEnoughStarted) {
			t.Fatalf("strict %v: removing the last voter: %v", strict, r.Err)
		}
		s.ok(3, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
		if err := s.log.Close(); err != nil {
			t.Fatal(err)
		}
		s = startSingleWith(t, disk, 2, change)
		if got := s.members(4); len(got) != 2 || got[0].ID != 1 || got[0].IsLearner {
			t.Fatalf("strict %v: members after a restart: %v", strict, got)
		}
		s.ok(5, &pb.MemberRemoveRequest{ID: 2})
	}
}

// TestStrictReconfigNeedsConnectedQuorum follows etcd's mayAddMember
// and mayRemoveMember: with three started voters and voter 3 down, no
// voter may be added or a connected one removed, as either would leave
// too few connected voters; voter 3 may go. A peer connected for less
// than 5 seconds counts as not connected, but not as down.
func TestStrictReconfigNeedsConnectedQuorum(t *testing.T) {
	up := map[uint64]int64{2: 600, 3: -1}
	w, rec, err := wal.Open(simfs.New())
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(1, []uint64{1, 2, 3})
	for id := uint64(1); id <= 3; id++ {
		cfg.Members = append(cfg.Members, &pb.Member{ID: id, Name: strconv.FormatUint(id, 10), PeerURLs: []string{fmt.Sprintf("https://%d:1", id)}})
	}
	cfg.ConnectedTicks = func(id uint64) int64 { return up[id] }
	n, err := New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	s := &single{t: t, log: w, n: n}
	s.run()
	if r := s.do(1, &pb.MemberAddRequest{PeerURLs: []string{"https://4:1"}}); !errors.Is(r.Err, rpctypes.ErrGRPCUnhealthy) {
		t.Fatalf("adding a voter with one of three down: %v", r.Err)
	}
	if r := s.do(2, &pb.MemberRemoveRequest{ID: 2}); !errors.Is(r.Err, rpctypes.ErrGRPCUnhealthy) {
		t.Fatalf("removing a connected voter with another down: %v", r.Err)
	}
	if r := s.do(3, &pb.MemberRemoveRequest{ID: 1}); !errors.Is(r.Err, rpctypes.ErrGRPCUnhealthy) {
		t.Fatalf("removing itself with another voter down: %v", r.Err)
	}
	// A change that passes the checks waits for a leader, without an
	// answer.
	refused := func(id uint64, op proto.Message) error {
		n.Submit(Request{ID: id, Op: op})
		for _, r := range s.run() {
			if r.ID == id {
				return r.Err
			}
		}
		return nil
	}
	up[3] = 10
	if r := s.do(4, &pb.MemberRemoveRequest{ID: 2}); !errors.Is(r.Err, rpctypes.ErrGRPCUnhealthy) {
		t.Fatalf("removing a voter with another connected for 1 s: %v", r.Err)
	}
	up[3] = -1
	if err := refused(5, &pb.MemberRemoveRequest{ID: 3}); err != nil {
		t.Fatalf("removing the voter that is down: %v", err)
	}
	up[3] = 600
	if err := refused(6, &pb.MemberAddRequest{PeerURLs: []string{"https://5:1"}}); err != nil {
		t.Fatalf("adding a voter with every voter connected: %v", err)
	}
}

func sameMembers(got []*pb.Member, want ...*pb.Member) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !proto.Equal(got[i], want[i]) {
			return false
		}
	}
	return true
}

// TestForceNewCluster restarts node 1 of a three-member cluster as
// etcd's --force-new-cluster does: the entries ForceNewCluster gives
// remove the other two, and the node, started again from its log,
// elects itself and takes writes alone.
func TestForceNewCluster(t *testing.T) {
	members := []*pb.Member{
		{ID: 1, Name: "one", PeerURLs: []string{"https://a:1"}},
		{ID: 2, Name: "two", PeerURLs: []string{"https://b:1"}},
		{ID: 3, Name: "three", PeerURLs: []string{"https://c:1"}},
	}
	three := func(cfg *Config) { cfg.Peers, cfg.Members = []uint64{1, 2, 3}, members }
	s := newMember(t, []uint64{1, 2, 3})
	s.run()
	disk := s.disk

	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(1, nil)
	three(&cfg)
	n, err := New(cfg, rec, w)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(n.Members()); got != 3 {
		t.Fatalf("%d members before, want 3", got)
	}
	ents, hs, err := n.ForceNewCluster()
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 || hs.Commit != ents[1].GetIndex() || ents[0].GetIndex() != n.Applied()+1 {
		t.Fatalf("%d entries, commit %d after applied %d", len(ents), hs.Commit, n.Applied())
	}
	w.Append(&hs, ents)
	if err := errors.Join(w.Sync(), w.Close()); err != nil {
		t.Fatal(err)
	}

	f := startSingleWith(t, disk, 2, three)
	if got := f.members(10); len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("members after: %v", got)
	}
	f.ok(11, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if ents, _, err := f.n.ForceNewCluster(); err != nil || len(ents) != 0 {
		t.Fatalf("a second force: %d entries, %v", len(ents), err)
	}
}

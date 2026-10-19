package core

import (
	"errors"
	"slices"
	"strings"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// The tests in this file check the guards etcd v3.7.2 keeps around Raft
// that #232 lists and tigerstore copies.

// sent runs the node until it is idle, as single.run does, and returns
// the messages it put out.
func (s *single) sent() []*raftpb.Message {
	s.t.Helper()
	var out []*raftpb.Message
	for range 100 {
		eff, err := s.n.Process()
		if err != nil {
			s.t.Fatalf("node stopped: %v", err)
		}
		out = append(out, eff.Send...)
		if len(eff.Persist) == 0 {
			return out
		}
		for _, j := range eff.Persist {
			s.log.Append(j.HardState, j.Entries)
		}
		if err := s.log.Sync(); err != nil {
			s.t.Fatal(err)
		}
		for _, j := range eff.Persist {
			s.n.Persisted(j)
		}
	}
	s.t.Fatal("the node never became idle")
	return nil
}

// TestForwardedProposalWaitsForLeader steps a proposal that peer 2
// forwards to a node that knows no leader, as #232 row 2 describes: Raft
// would drop it, etcd's raft.Node holds it until it knows a leader. The
// node holds it and forwards it once it hears of leader 3.
func TestForwardedProposalWaitsForLeader(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	p := &pending{req: Request{ID: 7, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}}}
	data, err := encodeBatch(s.n.state.version, 2, 1, []*pending{p}, writeSize(p))
	if err != nil {
		t.Fatal(err)
	}
	prop := &raftpb.Message{Type: raftpb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*raftpb.Entry{{Data: data}}}
	if err := s.n.Step(prop); err != nil {
		t.Fatal(err)
	}
	if got := s.sent(); len(got) != 0 {
		t.Fatalf("a node without a leader sent %v", got)
	}
	if err := s.n.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	got := s.sent()
	i := slices.IndexFunc(got, func(m *raftpb.Message) bool { return m.GetType() == raftpb.MsgProp })
	if i < 0 || got[i].GetTo() != 3 || len(got[i].Entries) != 1 || string(got[i].Entries[0].GetData()) != string(data) {
		t.Fatalf("after the leader came: sent %v, want the proposal forwarded to 3", got)
	}
}

// TestDroppedProposalAnswer submits a write to a leader that transfers
// its leadership, which Raft refuses: as etcd, the node answers with
// Raft's error under gRPC's Unknown, not with "no leader" (row 3).
func TestDroppedProposalAnswer(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	var got []Reply
	c.onReply = func(_ int, r Reply) { got = append(got, r) }
	c.down[1] = true
	c.nodes[0].rn.TransferLeader(2)
	c.step()
	c.nodes[0].Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	c.step()
	if len(got) != 1 || status.Code(got[0].Err) != codes.Unknown || status.Convert(got[0].Err).Message() != "raft proposal dropped" {
		t.Fatalf("a write during a leadership transfer: %+v", got)
	}
}

// TestFollowerHoldsSendsUntilConfApplies holds a follower's apply, as a
// write that waits for values from the log does, while the leader
// commits the removal of node 3. Like etcd's follower, which sends
// nothing until it applied a committed configuration change, node 2 then
// sends nothing; once its apply goes on, it applies the removal and sends
// what it held (row 9).
func TestFollowerHoldsSendsUntilConfApplies(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	for range 10 {
		c.step()
	}
	f := c.nodes[1]
	f.paused = &pausedWrite{}
	ctx, err := encodeConfContext(0, 0, 0, confRemove, &pb.Member{ID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.nodes[0].rn.ProposeConfChange(&raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(3)), Context: ctx}); err != nil {
		t.Fatal(err)
	}
	tick := func() {
		for _, n := range c.nodes {
			n.Tick()
		}
		c.step()
	}
	for range 5 {
		tick()
	}
	if f.confAt <= f.applied {
		t.Fatalf("node 2 has no committed configuration change waiting: confAt %d, applied %d; %s", f.confAt, f.applied, c.describe())
	}
	before := c.sent[1]
	for range 5 {
		tick()
	}
	if c.sent[1] != before || len(f.held) == 0 {
		t.Fatalf("node 2 sent %d messages and holds %d while the change waits", c.sent[1]-before, len(f.held))
	}
	f.paused = nil
	f.runApply()
	c.step()
	if !f.state.removed[3] || c.sent[1] == before || len(f.held) != 0 {
		t.Fatalf("after apply: removed %v, sent %d, held %d", f.state.removed[3], c.sent[1]-before, len(f.held))
	}
}

// TestStepRefusesRemovedAndMisrouted steps messages from a removed
// member and to another member, which etcd's Process refuses (rows 14
// and 15), and checks that a message to a removed member is dropped.
func TestStepRefusesRemovedAndMisrouted(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	n := c.nodes[1]
	n.state.removed[7] = true
	if err := n.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(7)), To: new(uint64(2)), Term: new(n.Term() + 1)}); !errors.Is(err, errFromRemoved) {
		t.Fatalf("a message from a removed member: %v", err)
	}
	if err := n.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(n.Term())}); !errors.Is(err, errNotForUs) {
		t.Fatalf("a message to another member: %v", err)
	}
	if n.Term() != c.nodes[0].Term() {
		t.Fatalf("a refused message moved the term to %d", n.Term())
	}
	n.send(&raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(7))})
	if len(n.out.Send) != 0 {
		t.Fatalf("a message to a removed member went out: %v", n.out.Send)
	}
}

// TestApplyRefusesGap hands apply an entry two past the applied one, which
// etcd stops on (row 30).
func TestApplyRefusesGap(t *testing.T) {
	s := newMember(t, []uint64{1})
	err := s.n.apply([]*raftpb.Entry{{Index: new(s.n.applied + 2), Term: new(uint64(1)), Type: raftpb.EntryNormal.Enum()}})
	if err == nil || !strings.Contains(err.Error(), "does not follow") {
		t.Fatalf("an entry after a gap: %v", err)
	}
}

// TestCancelDropsWriteWithoutLeader cancels a write that waits for a
// leader, which etcd's raft.Node drops when its context ends: it never
// goes out. A write cancelled while a leader is known goes on, as etcd
// proposed it already (row 32).
func TestCancelDropsWriteWithoutLeader(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	if r := s.run(); len(r) != 0 {
		t.Fatalf("answered at once: %+v", r)
	}
	s.n.Cancel(1)
	r := s.run()
	if len(r) != 1 || status.Code(r[0].Err) != codes.Canceled || len(s.n.proposals) != 0 || s.n.propBytes != 0 {
		t.Fatalf("after the cancel: replies %+v, %d writes and %d bytes queued", r, len(s.n.proposals), s.n.propBytes)
	}

	s = startSingle(t, simfs.New(), 1)
	s.n.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	s.n.Cancel(2)
	if r := s.run(); len(r) != 1 || r[0].Err != nil {
		t.Fatalf("a write cancelled while a leader is known: %+v", r)
	}
}

// TestTimeoutCause is etcd's parseProposeCtxErr: a proposed write that
// times out names a leader that failed just before, or a lost
// connection to the leader or of the leader to a quorum (row 34).
func TestTimeoutCause(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	lead, f := c.nodes[0], c.nodes[1]
	for range 5 {
		lead.Tick()
		f.Tick()
		c.step()
	}
	connected := int64(100)
	for _, n := range []*Node{lead, f} {
		n.cfg.ConnectedTicks = func(uint64) int64 { return connected }
	}
	for _, tc := range []struct {
		name      string
		n         *Node
		start     int64
		connected int64
		want      error
	}{
		{"leader, before it was elected", lead, lead.electedAt - 1, 100, rpctypes.ErrGRPCTimeoutDueToLeaderFail},
		{"leader, connected", lead, lead.clock, 100, rpctypes.ErrGRPCTimeout},
		{"leader, alone", lead, lead.clock, -1, rpctypes.ErrGRPCTimeoutDueToConnectionLost},
		{"leader, connected since after the start", lead, lead.clock - 3, 1, rpctypes.ErrGRPCTimeoutDueToConnectionLost},
		{"follower, connected", f, f.clock, 100, rpctypes.ErrGRPCTimeout},
		{"follower, cut off", f, f.clock, -1, rpctypes.ErrGRPCTimeoutDueToConnectionLost},
	} {
		connected = tc.connected
		if got := tc.n.timeoutCause(tc.start); !errors.Is(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}

	// Through a real timeout: the leader hears no acknowledgement and is
	// connected to nobody.
	connected = -1
	c.holdAcks = true
	var got []Reply
	c.onReply = func(_ int, r Reply) { got = append(got, r) }
	lead.Submit(Request{ID: 9, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for i := 0; len(got) == 0 && i <= lead.cfg.RequestTicks; i++ {
		lead.Tick()
		c.step()
	}
	if len(got) != 1 || !errors.Is(got[0].Err, rpctypes.ErrGRPCTimeoutDueToConnectionLost) {
		t.Fatalf("a write of a leader connected to nobody: %+v", got)
	}
}

// TestElapseTimesOutRequests moves the request clock past a write's
// deadline without a tick, as the driver does for the ticks it missed
// while busy: the write times out, by the wall clock as in etcd, and
// Raft's clock stays where it was (row 35).
func TestElapseTimesOutRequests(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	s.n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	s.run()
	now := s.n.now
	s.n.Elapse(int64(s.n.cfg.RequestTicks))
	r := s.run()
	if len(r) != 1 || status.Code(r[0].Err) != codes.DeadlineExceeded || s.n.now != now {
		t.Fatalf("after the clock moved on: %+v, ticks %d to %d", r, now, s.n.now)
	}
}

// TestReadFailsOnLeaderChange sends a linearizable read to a follower
// whose leader is down, so its read index never comes. When another
// leader is elected, the read fails with ErrGRPCLeaderChanged, which
// clients retry, as etcd's read loop fails its attempt (row 38).
func TestReadFailsOnLeaderChange(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	var got []Reply
	c.onReply = func(_ int, r Reply) { got = append(got, r) }
	c.down[0] = true
	c.nodes[1].Submit(Request{ID: 5, Op: &pb.RangeRequest{Key: []byte("k")}})
	for i := 0; len(got) == 0 && i < 100; i++ {
		c.nodes[1].Tick()
		c.nodes[2].Tick()
		c.step()
	}
	if len(got) != 1 || !errors.Is(got[0].Err, rpctypes.ErrGRPCLeaderChanged) {
		t.Fatalf("a read across an election: %+v; %s", got, c.describe())
	}
}

// TestFirstCommitAsksAgain applies a new leader's empty entry on a
// follower with a read whose index has not come: it asks for the index at
// once, as etcd's read loop does on the first commit in a term (row 39).
func TestFirstCommitAsksAgain(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	f := c.nodes[1]
	for range 5 {
		f.Tick()
	}
	b := &readBatch{ctx: 99, sent: 0}
	f.readBatches = append(f.readBatches, b)
	if err := f.applyEntry(&raftpb.Entry{Index: new(f.applied + 1), Term: new(f.Term()), Type: raftpb.EntryNormal.Enum()}); err != nil {
		t.Fatal(err)
	}
	if b.sent != f.now {
		t.Fatalf("the read index was last asked at tick %d, now is %d", b.sent, f.now)
	}
}

// TestVersionCountsLearners announces a version from both voters while
// the learner has not: as etcd counts every member's version, the cluster
// version waits for the learner (row 49).
func TestVersionCountsLearners(t *testing.T) {
	st := NewState(2, &mvcc.Heap{})
	st.members = map[uint64]*pb.Member{1: {ID: 1}, 2: {ID: 2}, 3: {ID: 3, IsLearner: true}}
	st.announce(1, 2, 1, true, st.versionMembers())
	st.announce(2, 2, 1, true, st.versionMembers())
	if st.version != 1 {
		t.Fatalf("version %d before the learner announced", st.version)
	}
	st.announce(3, 2, 1, true, st.versionMembers())
	if st.version != 2 {
		t.Fatalf("version %d after every member announced 2", st.version)
	}
}

// TestDowngradeEnableValidatesFirst asks for a downgrade to a version
// etcd refuses: as etcd's DowngradeEnable validates after a linearizable
// read, the node answers without writing anything to the log (row 50).
func TestDowngradeEnableValidatesFirst(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(*Config) {}).capable()
	last, err := s.n.storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	s.downgrade(1, pb.DowngradeRequest_ENABLE, "3.5", rpctypes.ErrGRPCInvalidDowngradeTargetVersion)
	if now, err := s.n.storage.LastIndex(); err != nil || now != last {
		t.Fatalf("an invalid ENABLE took the log from %d to %d: %v", last, now, err)
	}
	if got := s.downgrade(2, pb.DowngradeRequest_ENABLE, "3.6", nil); got != "3.7" || !s.n.state.downgradeEnabled {
		t.Fatalf("enable: version %q, enabled %v", got, s.n.state.downgradeEnabled)
	}
}

// TestTooManyRequests runs ahead the commit index by more than etcd's
// 5,000 entries: writes are refused with ErrGRPCRequestTooManyRequests,
// and with PriorityRequest a lease revoke only past twice that (row 52).
func TestTooManyRequests(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) { cfg.PriorityRequest = true })
	try := func(id uint64, gap uint64, op *pb.LeaseRevokeRequest) error {
		s.n.commit = s.n.applied + gap
		if op != nil {
			s.n.Submit(Request{ID: id, Op: op})
		} else {
			s.n.Submit(Request{ID: id, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
		}
		for _, r := range s.run() {
			if r.ID == id {
				return r.Err
			}
		}
		t.Fatalf("no reply to %d", id)
		return nil
	}
	if err := try(1, maxGap, nil); err != nil {
		t.Fatalf("a put at the limit: %v", err)
	}
	if err := try(2, maxGap+1, nil); !errors.Is(err, rpctypes.ErrGRPCRequestTooManyRequests) {
		t.Fatalf("a put past the limit: %v", err)
	}
	if err := try(3, maxGap+1, &pb.LeaseRevokeRequest{ID: 5}); !errors.Is(err, rpctypes.ErrGRPCLeaseNotFound) {
		t.Fatalf("a priority revoke past the limit: %v", err)
	}
	if err := try(4, 2*maxGap+1, &pb.LeaseRevokeRequest{ID: 5}); !errors.Is(err, rpctypes.ErrGRPCRequestTooManyRequests) {
		t.Fatalf("a revoke past twice the limit: %v", err)
	}
}

// TestOverQuotaAnswerWaitsForAlarm applies two puts that each fit the
// quota when taken, of which the second reaches it when it applies. As
// in etcd, that client's ErrNoSpace comes only once the NOSPACE alarm
// it raised applied (row 55).
func TestOverQuotaAnswerWaitsForAlarm(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) { cfg.QuotaBytes = 4000 }).capable()
	big := make([]byte, (4000-s.n.kv.Size())-256-2-1)
	s.n.SubmitAll([]Request{
		{ID: 1, Op: &pb.PutRequest{Key: []byte("a1"), Value: big}},
		{ID: 2, Op: &pb.PutRequest{Key: []byte("a2"), Value: big}},
	})
	for range 100 {
		eff, err := s.n.Process()
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range eff.Replies {
			if r.ID != 2 {
				continue
			}
			if !errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) || !s.n.ownNoSpace() {
				t.Fatalf("the put past the quota: %v, alarm applied %v", r.Err, s.n.ownNoSpace())
			}
			return
		}
		for _, j := range eff.Persist {
			s.log.Append(j.HardState, j.Entries)
		}
		if err := s.log.Sync(); err != nil {
			t.Fatal(err)
		}
		for _, j := range eff.Persist {
			s.n.Persisted(j)
		}
	}
	t.Fatal("no answer to the put past the quota")
}

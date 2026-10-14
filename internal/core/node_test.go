package core

import (
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// single runs a one-node cluster on a simulated disk.
type single struct {
	t     *testing.T
	disk  *simfs.Disk
	log   *wal.WAL
	n     *Node
	watch []WatchResponse
}

// newMember starts node 1 of peers on an empty simulated disk, without
// ticking it.
func newMember(t *testing.T, peers []uint64) *single {
	t.Helper()
	disk := simfs.New()
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(DefaultConfig(1, peers), rec)
	if err != nil {
		t.Fatal(err)
	}
	return &single{t: t, disk: disk, log: w, n: n}
}

func startSingle(t *testing.T, disk *simfs.Disk, boot uint64) *single {
	t.Helper()
	w, rec, err := wal.Open(disk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(1, []uint64{1})
	cfg.Boot = boot
	n, err := New(cfg, rec)
	if err != nil {
		t.Fatal(err)
	}
	s := &single{t: t, disk: disk, log: w, n: n}
	for range 50 {
		n.Tick()
		s.run()
		if n.Leader() == 1 {
			return s
		}
	}
	t.Fatal("the single node did not become leader")
	return nil
}

// run processes until the node is idle, persisting at once, and returns
// the replies.
func (s *single) run() []Reply {
	s.t.Helper()
	var out []Reply
	for range 100 {
		eff, err := s.n.Process()
		if err != nil {
			s.t.Fatalf("node stopped: %v", err)
		}
		out = append(out, eff.Replies...)
		s.watch = append(s.watch, eff.Watch...)
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

func (s *single) do(id uint64, op proto.Message) Reply {
	s.t.Helper()
	s.n.Submit(Request{ID: id, Op: op})
	for _, r := range s.run() {
		if r.ID == id {
			return r
		}
	}
	s.t.Fatalf("no reply to request %d", id)
	return Reply{}
}

// TestRejectedTxnKeepsNodeRunning commits a transaction that the index
// rejects. Every replica must answer it with the error and go on, also
// when it replays the entry after a restart.
func TestRejectedTxnKeepsNodeRunning(t *testing.T) {
	disk := simfs.New()
	s := startSingle(t, disk, 1)
	r := s.do(1, &pb.TxnRequest{Success: []*pb.RequestOp{
		{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("v")}}},
		{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("b"), Revision: 100}}},
	}})
	if !errors.Is(r.Err, rpctypes.ErrGRPCFutureRev) {
		t.Fatalf("txn with a future range = %v", r.Err)
	}
	if r := s.do(2, &pb.PutRequest{Key: []byte("c"), Value: []byte("1")}); r.Err != nil {
		t.Fatalf("put after the rejected txn: %v", r.Err)
	}

	s = startSingle(t, disk, 2)
	r = s.do(1, &pb.RangeRequest{Key: []byte("a"), RangeEnd: []byte("z")})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	got, _ := r.Resp.(*pb.RangeResponse)
	if got == nil || len(got.Kvs) != 1 || string(got.Kvs[0].Key) != "c" {
		t.Fatalf("after restart = %v", r.Resp)
	}
}

// TestStatusOneTerm checks that Status reports the same term in its header
// and body even before Process ran after a term change.
func TestStatusOneTerm(t *testing.T) {
	n, err := New(DefaultConfig(1, []uint64{1, 2, 3}), &wal.Recovered{})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Step(&raftpb.Message{Type: raftpb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(10))}); err != nil {
		t.Fatal(err)
	}
	st := n.Status()
	if st.RaftTerm != 10 || st.Header.RaftTerm != st.RaftTerm {
		t.Fatalf("status term %d, header term %d", st.RaftTerm, st.Header.RaftTerm)
	}
	if st.DowngradeInfo == nil {
		t.Fatal("status has no downgrade info")
	}
}

// TestWriteWaitsForLeader submits a write before the node has a leader.
// Like etcd, whose raft.Node takes no proposal without a leader, the write
// waits instead of failing with "no leader", and commits once the node is
// elected (#105). A write that finds no leader before its deadline gets
// what etcd answers then: DeadlineExceeded, not the request timeout.
func TestWriteWaitsForLeader(t *testing.T) {
	s := newMember(t, []uint64{1})
	n := s.n
	if n.Leader() != 0 {
		t.Fatal("the node has a leader before any tick")
	}
	n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	if r := s.run(); len(r) != 0 {
		t.Fatalf("a write without a leader was answered at once: %+v", r)
	}
	var got []Reply
	for range 50 {
		n.Tick()
		got = append(got, s.run()...)
		if len(got) > 0 {
			break
		}
	}
	if len(got) != 1 || got[0].ID != 1 || got[0].Err != nil {
		t.Fatalf("the write after the election: %+v", got)
	}

	// A member that knew a leader and lost it on its election timer, with
	// no Ready since: the write still waits.
	s = newMember(t, []uint64{1, 2, 3})
	n = s.n
	if err := n.Step(&raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}); err != nil {
		t.Fatal(err)
	}
	s.run()
	if n.Leader() != 2 {
		t.Fatalf("leader %d after a heartbeat from 2", n.Leader())
	}
	for i := 0; n.rn.BasicStatus().Lead != 0; i++ {
		if i > 100 {
			t.Fatal("the election timer never fired")
		}
		n.Tick()
	}
	n.Submit(Request{ID: 3, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	for _, r := range s.run() {
		if r.ID == 3 {
			t.Fatalf("a write after the leader was lost was answered at once: %+v", r)
		}
	}

	// A three-node member alone never gets a leader: its write times out.
	s = newMember(t, []uint64{1, 2, 3})
	n = s.n
	n.Submit(Request{ID: 2, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	got = s.run()
	for i := 0; len(got) == 0 && i < 2*n.cfg.RequestTicks; i++ {
		n.Tick()
		got = append(got, s.run()...)
	}
	if len(got) != 1 || status.Code(got[0].Err) != codes.DeadlineExceeded || status.Convert(got[0].Err).Message() != "context deadline exceeded" {
		t.Fatalf("a write that never finds a leader: %+v", got)
	}
}

// TestExpiredWritesLeaveQueue times out rounds of writes on a member that
// never finds a leader. Each round's writes leave the queue when they
// expire, so the queue does not grow with the rounds while clients retry.
func TestExpiredWritesLeaveQueue(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	n := s.n
	value := make([]byte, 16<<10)
	id := uint64(0)
	for round := range 5 {
		for range 100 {
			id++
			n.Submit(Request{ID: id, Op: &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id), Value: value}})
		}
		var got []Reply
		for range n.cfg.RequestTicks {
			n.Tick()
			got = append(got, s.run()...)
		}
		if len(got) != 100 {
			t.Fatalf("round %d: %d answers, want 100", round, len(got))
		}
		for _, r := range got {
			if status.Code(r.Err) != codes.DeadlineExceeded {
				t.Fatalf("round %d: write %d: %v", round, r.ID, r.Err)
			}
		}
		if len(n.proposals) != 0 || n.propBytes != 0 {
			t.Fatalf("round %d: %d writes and %d bytes still queued", round, len(n.proposals), n.propBytes)
		}
	}
}

// TestProposedWriteTimesOut keeps a proposed write from committing: a
// follower forwards it to a leader that never answers. Its deadline then
// gives etcd's request timeout, since it may still commit.
func TestProposedWriteTimesOut(t *testing.T) {
	s := newMember(t, []uint64{1, 2, 3})
	n := s.n
	heartbeat := func() {
		if err := n.Step(&raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}); err != nil {
			t.Fatal(err)
		}
	}
	heartbeat()
	s.run()
	n.Submit(Request{ID: 1, Op: &pb.PutRequest{Key: []byte("k"), Value: []byte("v")}})
	got := s.run()
	for i := 0; len(got) == 0 && i < 2*n.cfg.RequestTicks; i++ {
		heartbeat()
		n.Tick()
		got = append(got, s.run()...)
	}
	if len(got) != 1 || !errors.Is(got[0].Err, rpctypes.ErrGRPCTimeout) {
		t.Fatalf("a forwarded write that never commits: %+v", got)
	}
}

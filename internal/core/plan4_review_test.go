package core

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestPlan4FrameBoundary(t *testing.T) {
	const cap = 64 << 20
	p := &pb.PutRequest{Key: []byte("k"), Value: make([]byte, cap-128)}
	r, err := validate(p, 128)
	if err != nil {
		t.Fatal(err)
	}
	p.Value = append(p.Value, make([]byte, cap-proto.Size(r))...)
	if err := checkSize(r, 0, cap); err != nil {
		t.Fatal(err)
	}
	ps := []*pending{{req: Request{ID: 1, Op: p}}}
	data, err := encodeBatch(3, 1, 1, ps, 0)
	if err != nil {
		t.Fatal(err)
	}
	e := &raftpb.Entry{Term: new(uint64(1)), Index: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data}
	m := &raftpb.Message{Type: new(raftpb.MsgApp), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(1)), Entries: []*raftpb.Entry{e}}
	t.Logf("accepted request=%d entry data=%d WAL record body=%d peer message=%d limit=%d", proto.Size(r), len(data), len(data)+17, proto.Size(m), cap)
	if proto.Size(m) <= cap {
		t.Fatal("probe did not cross frame boundary")
	}
}

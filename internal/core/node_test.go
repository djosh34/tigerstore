package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

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

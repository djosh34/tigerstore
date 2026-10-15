package core

import (
	"errors"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestQuota follows etcd v3.7.2: a put that would reach the quota is
// refused before it is proposed and raises the node's NOSPACE alarm,
// which then refuses puts and grants while deletes and compactions go
// on; deactivating it after a compaction lets puts in again.
func TestQuota(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(cfg *Config) { cfg.QuotaBytes = 4000 }).capable()
	value := make([]byte, 500)
	var id uint64
	var refused Reply
	for {
		id++
		r := s.do(id, &pb.PutRequest{Key: fmt.Appendf(nil, "k%d", id%3), Value: value})
		if r.Err != nil {
			refused = r
			break
		}
		if id > 20 {
			t.Fatal("the quota never refused a put")
		}
	}
	if !errors.Is(refused.Err, rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("put past the quota: %v", refused.Err)
	}
	if got := s.alarms(100, &pb.AlarmRequest{}); !sameAlarms(got, &pb.AlarmMember{MemberID: 1, Alarm: pb.AlarmType_NOSPACE}) {
		t.Fatalf("alarms: %v", got)
	}
	st := s.n.Status()
	if st.DbSizeQuota != 4000 || st.DbSizeInUse != s.n.kv.Size() || st.DbSizeInUse+256+2+500 < 4000 {
		t.Fatalf("status: quota %d, in use %d", st.DbSizeQuota, st.DbSizeInUse)
	}
	if r := s.do(101, &pb.LeaseGrantRequest{ID: 9, TTL: 60}); !errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("grant under NOSPACE: %v", r.Err)
	}
	if r := s.do(102, &pb.PutRequest{Key: []byte("k1"), Value: []byte("x")}); !errors.Is(r.Err, rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("small put under NOSPACE: %v", r.Err)
	}
	if r := s.do(103, &pb.DeleteRangeRequest{Key: []byte("k0")}); r.Err != nil {
		t.Fatalf("delete under NOSPACE: %v", r.Err)
	}
	if r := s.do(104, &pb.CompactionRequest{Revision: s.n.kv.Rev()}); r.Err != nil {
		t.Fatalf("compact under NOSPACE: %v", r.Err)
	}
	s.alarms(105, alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_NOSPACE, 1))
	if r := s.do(106, &pb.PutRequest{Key: []byte("k1"), Value: value}); r.Err != nil {
		t.Fatalf("put after the compaction: %v", r.Err)
	}

	// Two puts that each fit, taken in one turn: the second reaches the
	// quota when it applies. It applies, as in etcd, but its client gets
	// ErrNoSpace, and the node raises its alarm again.
	free := 4000 - s.n.kv.Size()
	big := make([]byte, free-256-2-1)
	s.n.SubmitAll([]Request{
		{ID: 200, Op: &pb.PutRequest{Key: []byte("a1"), Value: big}},
		{ID: 201, Op: &pb.PutRequest{Key: []byte("a2"), Value: big}},
	})
	got := map[uint64]error{}
	for _, r := range s.run() {
		got[r.ID] = r.Err
	}
	if got[200] != nil || !errors.Is(got[201], rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("the two puts: %v", got)
	}
	if r := s.do(202, &pb.RangeRequest{Key: []byte("a2")}); r.Err != nil || len(r.Resp.(*pb.RangeResponse).Kvs) != 1 {
		t.Fatalf("the put that reached the quota did not apply: %v", r)
	}
	if got := s.alarms(203, &pb.AlarmRequest{}); len(got) != 1 {
		t.Fatalf("alarms after the apply-time refusal: %v", got)
	}
}

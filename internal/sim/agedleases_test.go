package sim

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/history"
)

// A lost attachment reply can put both attempts in the committed log,
// even when the second create comparison fails. Each needs its own value.
func TestImageLeaseAttachmentRetries(t *testing.T) {
	first := imageLeaseAttachment(0, 9, 0)
	retry := imageLeaseAttachment(0, 9, 1)
	res := &Result{
		History: []*Record{
			{Call: Second, Return: 2 * Second, req: first, Err: errClientTimeout, Ambiguous: true},
			{Call: 3 * Second, Return: 4 * Second, req: retry, Resp: &pb.TxnResponse{Succeeded: false}},
		},
		Log: []proto.Message{first, retry},
	}
	if err := checkLog(res); err != nil {
		t.Fatal(err)
	}
	for _, req := range []*pb.TxnRequest{first, retry} {
		if len(req.Compare) != 1 || !proto.Equal(req.Compare[0], createdNever(&kubeObject{key: []byte("/leases/00000000")})) {
			t.Fatalf("attachment lost its create comparison: %v", req)
		}
		if req.Success[0].GetRequestPut().Lease != 9 {
			t.Fatalf("attachment lost its lease: %v", req)
		}
	}
}

// An expiry hook records a decision, which a deposed leader may never
// commit. Only the log can show that the imaged lease was removed.
func TestImageLeaseExpiryNeedsCommittedRevoke(t *testing.T) {
	for _, c := range []struct {
		name string
		log  []proto.Message
		want int
	}{
		{"uncommitted decision", nil, 0},
		{"committed revoke", []proto.Message{&pb.LeaseRevokeRequest{ID: 9}}, 1},
		{"grant after revoke", []proto.Message{&pb.LeaseRevokeRequest{ID: 9}, &pb.LeaseGrantRequest{ID: 9, TTL: 120}}, 0},
		{"replayed revoke", []proto.Message{&pb.LeaseRevokeRequest{ID: 9}, &pb.LeaseRevokeRequest{ID: 9}}, 1},
		{"another lease revoked", []proto.Message{&pb.LeaseRevokeRequest{ID: 10}}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := &Sim{
				cfg: Config{from: &image{start: &history.Start{Leases: []int64{9}}}}, endAt: 200 * Second,
				expiredAt: map[int64]int64{9: 130 * Second}, expiredOrder: []int64{9},
			}
			res := &Result{Log: append([]proto.Message{&pb.LeaseGrantRequest{ID: 9, TTL: 120}}, c.log...), base: 1}
			if err := s.checkLeasesExpire(res, aliveLeases(nil, res.Log)); err != nil {
				t.Fatal(err)
			}
			if got := res.Stats.ImageRemovals; got != c.want {
				t.Errorf("imaged leases removed: %d, want %d", got, c.want)
			}
			if got := res.Stats.ImageExpiryDecisions; got != 1 {
				t.Errorf("expiry decisions: %d, want 1 even when the revoke never committed", got)
			}
		})
	}
}

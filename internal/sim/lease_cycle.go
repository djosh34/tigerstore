package sim

import (
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"
)

// leaseCycle deliberately crosses lease and compare state: renew a lease
// on one call, attach two keys in a compared Txn on the next, then read
// and revoke. Alternating cycles abandon their lease instead, allowing
// expiry while elections and snapshot installs run.
func (c *client) leaseCycle() proto.Message {
	phase := (c.n - 1) % 12
	if phase == 0 || len(c.leases) == 0 {
		return &pb.LeaseGrantRequest{TTL: 2}
	}
	id := c.leases[len(c.leases)-1]
	switch phase {
	case 1, 3, 5:
		return &pb.LeaseKeepAliveRequest{ID: id}
	case 2, 4, 6:
		return &pb.TxnRequest{
			Compare: []*pb.Compare{{Key: key(0), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL,
				TargetUnion: &pb.Compare_ModRevision{ModRevision: c.mods[0]}}},
			Success: []*pb.RequestOp{
				{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key(0), Value: fmt.Appendf(nil, "lease-%d-%d-a", id, c.n), Lease: id, PrevKv: true}}},
				{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: key(1), Value: fmt.Appendf(nil, "lease-%d-%d-b", id, c.n), Lease: id, PrevKv: true}}},
			},
			Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: key(0)}}}},
		}
	case 10:
		if (c.n/12)%2 == 0 {
			return &pb.LeaseRevokeRequest{ID: id}
		}
	}
	return &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}
}

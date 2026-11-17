package core

import (
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"
)

// TestLeaseTTLBuckets checks the buckets of etcd's lease TTL histogram:
// [i] holds the TTLs over 2^(i-1) and at most 2^i, the last the rest.
func TestLeaseTTLBuckets(t *testing.T) {
	for ttl, want := range map[int64]int{1: 0, 2: 1, 3: 2, 4: 2, 5: 3, 60: 6, 1 << 23: 23, 1<<23 + 1: LeaseTTLBuckets, maxLeaseTTL: LeaseTTLBuckets} {
		var c Counters
		c.granted(ttl)
		if c.LeaseTTLs[want] != 1 || c.LeaseTTLSum != ttl || c.LeaseGranted != 1 {
			t.Errorf("TTL %d: buckets %v, sum %d; want bucket %d", ttl, c.LeaseTTLs, c.LeaseTTLSum, want)
		}
	}
}

// TestInternalName checks the names etcd's apply_duration_seconds gives
// InternalRaftRequests.
func TestInternalName(t *testing.T) {
	for want, r := range map[string]*pb.InternalRaftRequest{
		"AuthUserAdd":       {Header: &pb.RequestHeader{ID: 1}, AuthUserAdd: &pb.AuthUserAddRequest{Name: "a"}},
		"AuthEnable":        {AuthEnable: &pb.AuthEnableRequest{}},
		"Authenticate":      {Authenticate: &pb.InternalAuthenticateRequest{}},
		"ClusterVersionSet": {ClusterVersionSet: &membershippb.ClusterVersionSetRequest{Ver: "3.7.0"}},
		"DowngradeInfoSet":  {DowngradeInfoSet: &membershippb.DowngradeInfoSetRequest{}},
		"Unknown":           {Header: &pb.RequestHeader{}},
	} {
		if got := internalName(r); got != want {
			t.Errorf("internalName = %q, want %q", got, want)
		}
	}
}

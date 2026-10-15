package core

import (
	"slices"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/membershippb"

	"github.com/djosh34/tigerstore/internal/simfs"
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

// TestAlarmGauges follows etcd's etcd_debugging_server_alarms: each
// activation applied adds one, a second of an active alarm too, and each
// deactivation of an active alarm takes one away.
func TestAlarmGauges(t *testing.T) {
	s := startSingle(t, simfs.New(), 1).capable()
	for i, r := range []*pb.AlarmRequest{
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NOSPACE, 7),
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NOSPACE, 7),
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_NONE, 7),
		alarmReq(pb.AlarmRequest_ACTIVATE, pb.AlarmType_CORRUPT, 3),
		alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_NOSPACE, 7),
		alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_CORRUPT, 3),
		alarmReq(pb.AlarmRequest_DEACTIVATE, pb.AlarmType_CORRUPT, 3),
	} {
		s.alarms(uint64(10+i), r)
	}
	want := []AlarmGauge{{MemberID: 7, Alarm: pb.AlarmType_NOSPACE, Value: 1}, {MemberID: 3, Alarm: pb.AlarmType_CORRUPT}}
	if got := s.n.Metrics().AlarmGauges; !slices.Equal(got, want) {
		t.Fatalf("alarm gauges %v, want %v", got, want)
	}
}

// TestMembershipSnapshotCounts checks the v2 store reads and writes
// counted as etcd builds a v2 store from the membership every 100
// entries: none until then, and one get, a create for the member and a
// set for the cluster version each time.
func TestMembershipSnapshotCounts(t *testing.T) {
	s := startSingle(t, simfs.New(), 1).capable()
	counts := func() [3]int64 {
		m := s.n.Metrics()
		return [3]int64{m.StoreGets, m.StoreCreates, m.StoreSets}
	}
	if got := counts(); got != [3]int64{} {
		t.Fatalf("counts %v before 100 entries", got)
	}
	for i := uint64(1); s.n.Applied() <= 2*membershipSnapshotEntries+1; i++ {
		s.ok(100+i, &pb.PutRequest{Key: []byte("a")})
	}
	if got, want := counts(), [3]int64{2, 2, 2}; got != want {
		t.Fatalf("counts %v after %d entries, want %v", got, s.n.Applied(), want)
	}
}

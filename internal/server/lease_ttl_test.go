package server_test

import (
	"context"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestTimeToLiveOnFollowers asks every member for the time left on a
// lease of 60 s, some ticks after its grant. Only the leader counts it;
// the followers ask the leader, as etcd's do, so none may answer the
// granted TTL.
func TestTimeToLiveOnFollowers(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 95)
	if err != nil {
		t.Fatal(err)
	}
	resp := retry(t, func(ctx context.Context) (*pb.LeaseGrantResponse, error) {
		return pb.NewLeaseClient(c.dial(1, cert)).LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 60})
	})
	time.Sleep(300 * time.Millisecond)
	for id := uint64(1); id <= 3; id++ {
		ttl := retry(t, func(ctx context.Context) (*pb.LeaseTimeToLiveResponse, error) {
			return pb.NewLeaseClient(c.dial(id, cert)).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: resp.ID})
		})
		if ttl.GrantedTTL != 60 || ttl.TTL < 1 || ttl.TTL > 59 || ttl.Header.MemberId != id {
			t.Errorf("node %d: TTL %d of %d, member %d", id, ttl.TTL, ttl.GrantedTTL, ttl.Header.MemberId)
		}
	}
}

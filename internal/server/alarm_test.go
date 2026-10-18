package server_test

import (
	"context"
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestAlarm checks Maintenance.Alarm across members: an alarm one member
// activates is listed by another, refuses puts on every member until it
// is deactivated, and shows in Status.
func TestAlarm(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 93)
	if err != nil {
		t.Fatal(err)
	}
	put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	m2, m3 := pb.NewMaintenanceClient(c.dial(2, cert)), pb.NewMaintenanceClient(c.dial(3, cert))
	ctx := context.Background()
	resp, err := m2.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET})
	if err != nil || len(resp.Alarms) != 0 || resp.Header.Revision < put.Header.Revision || resp.Header.MemberId != 2 {
		t.Fatalf("list with no alarm: %v, %v", resp, err)
	}
	nospace := &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: 2}
	// Alarm changes need the cluster version every member announces
	// after the first election; until then they are not capable.
	resp = retry(t, func(ctx context.Context) (*pb.AlarmResponse, error) { return m2.Alarm(ctx, nospace) })
	if len(resp.Alarms) != 1 || resp.Alarms[0].MemberID != 2 {
		t.Fatalf("activate: %v", resp)
	}
	if resp, err = m3.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET}); err != nil || len(resp.Alarms) != 1 {
		t.Fatalf("list on another member: %v, %v", resp, err)
	}
	if _, err := c.client(3, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}); !errors.Is(err, rpctypes.ErrGRPCNoSpace) {
		t.Fatalf("put under NOSPACE: %v", err)
	}
	st, err := m3.Status(ctx, &pb.StatusRequest{})
	if err != nil || len(st.Errors) != 1 || st.Errors[0] != "memberID:2 alarm:NOSPACE" {
		t.Fatalf("status: %v, %v", st.GetErrors(), err)
	}
	nospace.Action = pb.AlarmRequest_DEACTIVATE
	if _, err := m3.Alarm(ctx, nospace); err != nil {
		t.Fatal(err)
	}
	// Member 2 refuses puts until it applied the disarm itself. Its list
	// is a linearizable read, so it answers once it has (#254).
	if resp, err = m2.Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_GET}); err != nil || len(resp.Alarms) != 0 {
		t.Fatalf("list on member 2 after deactivating: %v, %v", resp, err)
	}
	if _, err := c.client(2, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}); err != nil {
		t.Fatalf("put after deactivating: %v", err)
	}
}

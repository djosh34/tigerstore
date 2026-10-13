package server_test

import (
	"context"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestAlarm checks Maintenance.Alarm against what etcd v3.7.2 answers
// with no alarm active: no alarms for a GET, a DEACTIVATE or an ACTIVATE
// of NONE, with a linearizable header. Activating NOSPACE by hand, which
// etcd allows, is not supported here and answers Unimplemented.
func TestAlarm(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 93)
	if err != nil {
		t.Fatal(err)
	}
	put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return c.client(1, cert).Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	m := pb.NewMaintenanceClient(c.dial(2, cert))
	for _, r := range []*pb.AlarmRequest{
		{Action: pb.AlarmRequest_GET},
		{Action: pb.AlarmRequest_GET, Alarm: pb.AlarmType_NOSPACE},
		{Action: pb.AlarmRequest_DEACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: 1},
		{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_NONE},
	} {
		resp, err := m.Alarm(context.Background(), r)
		if err != nil {
			t.Fatalf("%v: %v", r, err)
		}
		if len(resp.Alarms) != 0 || resp.Header.Revision < put.Header.Revision || resp.Header.MemberId != 2 || resp.Header.ClusterId == 0 {
			t.Fatalf("%v: %v", r, resp)
		}
	}
	_, err = m.Alarm(context.Background(), &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: pb.AlarmType_NOSPACE, MemberID: 1})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("activating NOSPACE: %v", err)
	}
}

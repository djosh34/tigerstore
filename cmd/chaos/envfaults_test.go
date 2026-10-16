package main

import (
	"context"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A MemberRemove that ran out of time while the cluster had no leader
// applied once one was elected, and the next try failed the run on
// "member not found" (CI run 38963063634). clientv3 hands the timeout
// back as the plain context error, so it must count as ambiguous.
func TestMayHaveApplied(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	timedOut := clientv3.ContextError(ctx, status.Error(codes.Canceled, "context canceled"))
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"client timeout", context.DeadlineExceeded, true},
		{"canceled through clientv3", timedOut, true},
		{"no leader", rpctypes.ErrNoLeader, true},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), true},
		{"member not found", rpctypes.ErrMemberNotFound, false},
		{"peer URL exists", rpctypes.ErrPeerURLExist, false},
	} {
		if got := mayHaveApplied(tc.err); got != tc.want {
			t.Errorf("%s: mayHaveApplied(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

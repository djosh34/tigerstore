package core

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MoveLeader follows etcd v3.7.2's (EtcdServer.MoveLeader): only the
// leader takes it, the transferee must be a voting member, and Raft's
// leadership transfer runs until this node sees the transferee lead.
// Like etcd, the node waits for that as long as the client does, without
// a deadline of its own: when the client goes away (Cancel), the request
// fails with ErrTimeoutLeaderTransfer.

// ErrTimeoutLeaderTransfer is etcd's answer to a transfer that did not
// finish in time, which etcd's gRPC layer has no code for.
var ErrTimeoutLeaderTransfer = status.Error(codes.Unknown, "etcdserver: request timed out, leader transfer took too long")

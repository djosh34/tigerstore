package server

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// method serves one gRPC method. A request the node runs (op) is decoded
// by the connection reader and handed to the node in a batch. Other unary
// calls (unary) and bidirectional streams (stream) run in a goroutine.
type method struct {
	op      func() proto.Message
	wirePut bool // op is a put that stays in its wire form
	// compact: op is a range whose answer the writer encodes from the
	// index's values, without building a RangeResponse.
	compact bool
	unary   func(ctx context.Context, s *Server, caller core.Caller, req []byte) (proto.Message, error)
	stream  func(s *Server, st *stream) error
	// serverStream: stream is a server-streaming call, whose client sends
	// one request.
	serverStream bool
	// idx is the method's place in methodPaths, for its metrics,
	// reqType its requestType and labels its labels.
	idx     int
	reqType string
	labels  [3]string
	// learner says what a learner serves of the method, as etcd's
	// isRPCSupportedForLearner: nothing by default.
	learner learnerUse
}

// learnerUse is what a learner serves of a method.
type learnerUse byte

const (
	learnerNever learnerUse = iota
	learnerAlways
	// learnerSerializable serves serializable ranges only.
	learnerSerializable
)

// methodPaths are the paths of methods, sorted.
var methodPaths = func() []string {
	paths := make([]string, 0, len(methods))
	for p := range methods {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	for i, p := range paths {
		methods[p].idx = i
		methods[p].reqType = requestType(p)
		methods[p].labels = labels(p, methods[p])
	}
	return paths
}()

// requestType is the type etcd's etcd_server_request_duration_seconds
// gives the requests of the method at path, "" for one it does not time:
// those that go through Raft and Range. A read-only Txn is a ReadonlyTxn.
func requestType(path string) string {
	service, name, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	switch service {
	case "etcdserverpb.KV":
		switch name {
		case "Range", "Put", "DeleteRange", "Txn":
			return name
		case "Compact":
			return "Compaction"
		}
	case "etcdserverpb.Lease":
		if name == "LeaseGrant" || name == "LeaseRevoke" {
			return name
		}
	case "etcdserverpb.Maintenance":
		if name == "Alarm" {
			return name
		}
	case "etcdserverpb.Auth":
		if strings.HasPrefix(name, "User") || strings.HasPrefix(name, "Role") {
			return "Auth" + name
		}
		return name
	}
	return ""
}

// methods are the methods that are served: etcd's, with the Auth
// service's in authMethods, and the gRPC health service's. Every other
// method of etcd's services answers Unimplemented, as grpc-go's generated
// Unimplemented servers do.
var methods = withAuth(map[string]*method{
	"/etcdserverpb.KV/Range":               {op: newOp[pb.RangeRequest], compact: true, learner: learnerSerializable},
	"/etcdserverpb.KV/RangeStream":         {stream: (*Server).rangeStream, serverStream: true},
	"/etcdserverpb.KV/Put":                 {op: newOp[pb.PutRequest], wirePut: true},
	"/etcdserverpb.KV/DeleteRange":         {op: newOp[pb.DeleteRangeRequest]},
	"/etcdserverpb.KV/Txn":                 {op: newOp[pb.TxnRequest]},
	"/etcdserverpb.KV/Compact":             {op: newOp[pb.CompactionRequest]},
	"/etcdserverpb.Lease/LeaseGrant":       {op: newOp[pb.LeaseGrantRequest]},
	"/etcdserverpb.Lease/LeaseRevoke":      {op: newOp[pb.LeaseRevokeRequest]},
	"/etcdserverpb.Lease/LeaseTimeToLive":  {op: newOp[pb.LeaseTimeToLiveRequest]},
	"/etcdserverpb.Lease/LeaseLeases":      {op: newOp[pb.LeaseLeasesRequest]},
	"/etcdserverpb.Lease/LeaseKeepAlive":   {stream: (*Server).keepAlive},
	"/etcdserverpb.Watch/Watch":            {stream: (*Server).watch},
	"/etcdserverpb.Cluster/MemberList":     {unary: nodeCall[pb.MemberListRequest]},
	"/etcdserverpb.Cluster/MemberAdd":      {unary: memberAdd},
	"/etcdserverpb.Cluster/MemberRemove":   {unary: nodeCall[pb.MemberRemoveRequest]},
	"/etcdserverpb.Cluster/MemberUpdate":   {unary: nodeCall[pb.MemberUpdateRequest]},
	"/etcdserverpb.Cluster/MemberPromote":  {unary: memberPromote},
	"/etcdserverpb.Maintenance/Status":     {unary: maintenanceStatus, learner: learnerAlways},
	"/etcdserverpb.Maintenance/Alarm":      {unary: nodeCall[pb.AlarmRequest]},
	"/etcdserverpb.Maintenance/Hash":       {unary: nodeCall[pb.HashRequest]},
	"/etcdserverpb.Maintenance/HashKV":     {unary: nodeCall[pb.HashKVRequest]},
	"/etcdserverpb.Maintenance/Snapshot":   {stream: (*Server).snapshot, serverStream: true, learner: learnerAlways},
	"/etcdserverpb.Maintenance/Defragment": {unary: defragment},
	"/etcdserverpb.Maintenance/MoveLeader": {unary: nodeCall[pb.MoveLeaderRequest]},
	"/etcdserverpb.Maintenance/Downgrade":  {unary: nodeCall[pb.DowngradeRequest]},
	"/v3lockpb.Lock/Lock":                  {unary: lock},
	"/v3lockpb.Lock/Unlock":                {unary: unlock},
	"/v3electionpb.Election/Campaign":      {unary: campaign},
	"/v3electionpb.Election/Proclaim":      {unary: electionProclaim},
	"/v3electionpb.Election/Leader":        {unary: electionLeader},
	"/v3electionpb.Election/Observe":       {stream: (*Server).observe, serverStream: true},
	"/v3electionpb.Election/Resign":        {unary: electionResign},
	"/grpc.health.v1.Health/Check":         {unary: healthCheck},
	"/grpc.health.v1.Health/List":          {unary: healthList},
	"/grpc.health.v1.Health/Watch":         {stream: (*Server).healthWatch, serverStream: true},
})

// withAuth adds authMethods to ms.
func withAuth(ms map[string]*method) map[string]*method {
	maps.Copy(ms, authMethods)
	return ms
}

// services are the gRPC services etcd registers.
var services = []string{"etcdserverpb.KV", "etcdserverpb.Watch", "etcdserverpb.Lease", "etcdserverpb.Cluster", "etcdserverpb.Maintenance", "etcdserverpb.Auth", "v3lockpb.Lock", "v3electionpb.Election", healthService}

// unknownMethod is grpc-go's answer for a method that is not served.
func unknownMethod(path string) error {
	service, name, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	if !ok {
		return status.Errorf(codes.Unimplemented, "malformed method name: %q", path)
	}
	if slices.Contains(services, service) {
		return status.Errorf(codes.Unimplemented, "unknown method %v for service %v", name, service)
	}
	return status.Errorf(codes.Unimplemented, "unknown service %v", service)
}

// unmarshal decodes a request message as grpc-go does, with its error.
func unmarshal(b []byte, m proto.Message) error {
	if err := proto.Unmarshal(b, m); err != nil {
		return unmarshalError(err)
	}
	return nil
}

// unmarshalError is grpc-go's error for a request it cannot decode.
// protobuf puts a no-break space after "proto:" in some builds, to keep
// programs from comparing its messages; etcd v3.7.2's build has a space.
func unmarshalError(err error) error {
	msg := strings.Replace(err.Error(), "proto:\u00a0", "proto: ", 1)
	return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %s", msg)
}

// newOp returns a new request of type T.
func newOp[T any, P interface {
	*T
	proto.Message
}]() proto.Message {
	return P(new(T))
}

// nodeCall decodes a request of type T and has the node answer it.
func nodeCall[T any, P interface {
	*T
	proto.Message
}](ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	req := P(new(T))
	if err := unmarshal(b, req); err != nil {
		return nil, err
	}
	resp, err := s.n.DoAs(ctx, caller, req)
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

func maintenanceStatus(ctx context.Context, s *Server, caller core.Caller, b []byte) (proto.Message, error) {
	var r pb.StatusRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if err := s.n.Authorize(ctx, caller, &r); err != nil {
		return nil, toStatus(err)
	}
	st, err := s.n.Status(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return st, nil
}

// toStatus turns context errors into the status etcd returns for them.
// Errors from the core are already etcd's gRPC errors.
func toStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unknown, err.Error())
}

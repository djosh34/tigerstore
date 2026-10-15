package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
)

// The gRPC health service answers as grpc-go's health.Server does in
// etcd v3.7.2: the server as a whole, service "", is SERVING, and no other
// service has a status. etcd sets that status at start and changes it only
// while a defragmentation runs with the StopGRPCServiceOnDefrag feature,
// which is off by default.
const healthService = "grpc.health.v1.Health"

func healthCheck(_ context.Context, s *Server, _ core.Caller, b []byte) (proto.Message, error) {
	var r healthpb.HealthCheckRequest
	if err := unmarshal(b, &r); err != nil {
		return nil, err
	}
	if r.Service != "" {
		return nil, status.Error(codes.NotFound, "unknown service")
	}
	st, _ := s.serving.status()
	return &healthpb.HealthCheckResponse{Status: st}, nil
}

func healthList(_ context.Context, s *Server, _ core.Caller, b []byte) (proto.Message, error) {
	if err := unmarshal(b, &healthpb.HealthListRequest{}); err != nil {
		return nil, err
	}
	st, _ := s.serving.status()
	return &healthpb.HealthListResponse{Statuses: map[string]*healthpb.HealthCheckResponse{
		"": {Status: st},
	}}, nil
}

// healthWatch sends the service's status, SERVICE_UNKNOWN for one without
// a status, and then each change of it until the client goes.
func (s *Server) healthWatch(st *stream) error {
	var r healthpb.HealthCheckRequest
	if err := st.RecvOne(&r); err != nil {
		return err
	}
	if r.Service != "" {
		if err := st.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVICE_UNKNOWN}); err != nil {
			return err
		}
		<-st.ctx.Done()
		return status.Error(codes.Canceled, "Stream has ended.")
	}
	last := healthpb.HealthCheckResponse_SERVICE_UNKNOWN
	for {
		cur, changed := s.serving.status()
		if cur != last {
			if err := st.Send(&healthpb.HealthCheckResponse{Status: cur}); err != nil {
				return err
			}
			last = cur
		}
		select {
		case <-changed:
		case <-st.ctx.Done():
			return status.Error(codes.Canceled, "Stream has ended.")
		}
	}
}

// /readyz and /livez run named checks, as etcd v3.7.2's CheckRegistry
// does (server/etcdserver/api/etcdhttp/health.go). The root path runs
// every check but those in ?exclude, and each check also has a path of
// its own. A failure answers 503 with a line per check; success answers
// "ok", after those lines with ?verbose. etcd runs the checks of a root
// path in map order, which varies; tigerstore runs them in etcd's
// registration order.

// healthCheckFunc is one check; a nil error passes.
type healthCheckFunc func(s *Server, ctx context.Context) error

type namedCheck struct {
	name  string
	check healthCheckFunc
}

var (
	livezChecks = []namedCheck{
		{"serializable_read", serializableRead},
	}
	readyzChecks = []namedCheck{
		{"data_corruption", noCorruptAlarm},
		{"serializable_read", serializableRead},
		{"linearizable_read", linearizableRead},
		{"non_learner", nonLearner},
	}
)

// checkPath is what one path of /readyz or /livez runs.
type checkPath struct {
	checks []namedCheck
	root   bool // only the root path takes ?exclude
}

// checkPaths are the paths of /readyz and /livez.
var checkPaths = func() map[string]checkPath {
	paths := map[string]checkPath{}
	for root, checks := range map[string][]namedCheck{"/livez": livezChecks, "/readyz": readyzChecks} {
		paths[root] = checkPath{checks: checks, root: true}
		for _, c := range checks {
			paths[root+"/"+c.name] = checkPath{checks: []namedCheck{c}}
		}
	}
	return paths
}()

// runChecks answers one path of /readyz or /livez. An excluded name that
// is not a check is ignored, as in etcd.
func (s *Server) runChecks(ctx context.Context, p checkPath, q url.Values) httpAnswer {
	var lines strings.Builder
	failed := false
	for _, c := range p.checks {
		if p.root && slices.Contains(q["exclude"], c.name) {
			continue
		}
		if err := c.check(s, ctx); err != nil {
			failed = true
			fmt.Fprintf(&lines, "[-]%s failed: %v\n", c.name, err)
		} else {
			fmt.Fprintf(&lines, "[+]%s ok\n", c.name)
		}
	}
	if failed {
		return httpError(http.StatusServiceUnavailable, lines.String())
	}
	body := "ok\n"
	if q.Has("verbose") {
		body = lines.String() + body
	}
	return httpAnswer{code: http.StatusOK, contentType: textPlain, body: body, nosniff: true}
}

// noCorruptAlarm passes while no CORRUPT alarm is active. Tigerstore
// raises no alarms (see alarm).
func noCorruptAlarm(*Server, context.Context) error { return nil }

// nonLearner passes on a voting member. Tigerstore has no learners.
func nonLearner(*Server, context.Context) error { return nil }

func serializableRead(s *Server, ctx context.Context) error { return s.healthRead(ctx, true) }

func linearizableRead(s *Server, ctx context.Context) error { return s.healthRead(ctx, false) }

// healthRead makes the one-key keys-only read of etcd's health checks.
// It fails after etcd's request timeout, as etcd's read-index wait does
// with ErrTimeout. Its error carries the error text etcd reports, without
// gRPC's wrapper.
func (s *Server) healthRead(ctx context.Context, serializable bool) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	_, err := s.n.DoAs(ctx, core.RootCaller(), &pb.RangeRequest{Key: []byte("health"), KeysOnly: true, Limit: 1, Serializable: serializable})
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok {
		return errors.New(st.Message())
	}
	return err
}

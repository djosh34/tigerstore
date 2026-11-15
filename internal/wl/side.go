package wl

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Side calls: the methods the loop does not serve itself, but hands to a
// goroutine of their own. They run etcd's own servers: Lock and Election,
// which etcd too runs as clients of itself, and gRPC's health service.
// Their client is the node's own, over loopback TLS, as etcd's gateway's
// is (selfClient), so what they ask the store reaches the loop as any
// other client's requests; nothing is added to its path. The loop reads a
// side call's one request, starts it, writes what it sends, and ends its
// context when its stream or connection goes.

// sideMethod is one method of a side service.
type sideMethod struct {
	path   string
	impl   func(*sideServers) any
	unary  *grpc.MethodDesc
	stream *grpc.StreamDesc
}

// sideServers are the servers the side calls run.
type sideServers struct {
	lock     v3lockpb.LockServer
	election v3electionpb.ElectionServer
	health   *health.Server
}

// sideServices are the services served by side calls, with their
// servers.
var sideServices = []struct {
	desc *grpc.ServiceDesc
	impl func(*sideServers) any
}{
	{&v3lockpb.Lock_ServiceDesc, func(s *sideServers) any { return s.lock }},
	{&v3electionpb.Election_ServiceDesc, func(s *sideServers) any { return s.election }},
	{&healthpb.Health_ServiceDesc, func(s *sideServers) any { return s.health }},
}

// maxSideMethods bounds the side methods, which take the kinds from kSide.
const maxSideMethods = 16

// sideMethods are the side services' methods; method i is kind kSide+i.
var sideMethods = func() []sideMethod {
	var out []sideMethod
	for _, svc := range sideServices {
		for i := range svc.desc.Methods {
			m := &svc.desc.Methods[i]
			out = append(out, sideMethod{path: "/" + svc.desc.ServiceName + "/" + m.MethodName, impl: svc.impl, unary: m})
		}
		for i := range svc.desc.Streams {
			s := &svc.desc.Streams[i]
			out = append(out, sideMethod{path: "/" + svc.desc.ServiceName + "/" + s.StreamName, impl: svc.impl, stream: s})
		}
	}
	return out[:min(len(out), maxSideMethods)]
}()

func init() {
	for i, m := range sideMethods {
		paths[m.path] = kSide + uint8(i)
	}
}

// sideKind reports whether kind is a side method's.
func sideKind(kind uint8) bool { return kind >= kSide && int(kind-kSide) < len(sideMethods) }

// selfClient is the node's client of itself: it dials addr, a client
// listener of the node, with the client port's TLS configuration, as
// etcd's gateway does, trusting the node it dials.
func selfClient(addr string, tlsCfg *tls.Config) (*clientv3.Client, error) {
	dial := tlsCfg.Clone()
	dial.InsecureSkipVerify = true //nolint:gosec // etcd's gateway trusts the node it runs in, which it dials over loopback
	dial.GetConfigForClient = nil
	return clientv3.New(clientv3.Config{
		Endpoints: []string{addr}, TLS: dial, Logger: zap.NewNop(),
		MaxCallRecvMsgSize: math.MaxInt32,
	})
}

func newSideServers(cli *clientv3.Client) *sideServers {
	h := health.NewServer()
	// etcd's: serving for every service, named by "".
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return &sideServers{lock: v3lock.NewLockServer(cli), election: v3election.NewElectionServer(cli), health: h}
}

// sideRequest starts the side call of stream s with its request, unless
// it already has one: a call takes one request.
func (n *node) sideRequest(c *client, s *stream, msg []byte) {
	h := mkHandle(c, s.id)
	if _, ok := n.sideCalls[h]; ok || n.shell == nil {
		return
	}
	n.sideCalls[h] = n.shell.sideCall(h, &sideMethods[s.kind-kSide], bytes.Clone(msg))
}

// cancelSide ends the context of the side call of h, if it has one.
func (n *node) cancelSide(h handle) {
	if cancel, ok := n.sideCalls[h]; ok {
		cancel()
		delete(n.sideCalls, h)
	}
}

// cancelSides ends the side calls of client socket idx, which closed.
func (n *node) cancelSides(idx int) {
	for h := range n.sideCalls {
		if i, _, _ := h.parts(); i == idx {
			n.cancelSide(h)
		}
	}
}

// sideSend writes a message a side call sent on its stream.
func (n *node) sideSend(h handle, msg []byte) {
	c, s := n.clientOf(h)
	if c == nil {
		n.cancelSide(h)
		return
	}
	n.openStream(c, s)
	var w msgW
	w.begin(c, s, len(msg))
	w.write(msg)
	w.end()
	n.markDirty(&c.sock)
}

// sideEnd ends the stream of a side call that returned: with its reply
// if it is unary, or with its error's status.
func (n *node) sideEnd(h handle, unary bool, reply []byte, err error) {
	n.cancelSide(h)
	c, s := n.clientOf(h)
	if c == nil {
		return
	}
	if err != nil {
		st := status.Convert(err)
		if errors.Is(err, context.Canceled) {
			st = status.New(codes.Canceled, err.Error())
		}
		n.replyError(c, s.id, int(st.Code()), st.Message())
		return
	}
	if !unary {
		n.openStream(c, s)
		n.metrics.count(s.kind, 0)
		n.stats.replies++
		n.endStreamOK(c, s)
		return
	}
	var w msgW
	n.beginReply(c, s, &w, len(reply))
	w.write(reply)
	n.endReply(c, s, &w)
}

// sideCall runs a side call in a goroutine of its own and posts what it
// sends and how it ends to the loop. It returns what ends the call's
// context, which the node's stop ends too.
func (l *loop) sideCall(h handle, m *sideMethod, req []byte) context.CancelFunc {
	ctx, cancel := context.WithCancel(l.ctx)
	impl := m.impl(l.side)
	go func() {
		var reply []byte
		var err error
		if m.unary != nil {
			var out any
			out, err = m.unary.Handler(impl, ctx, func(v any) error { return unmarshalSide(req, v) }, nil)
			if err == nil {
				reply, err = marshalSide(out)
			}
		} else {
			err = m.stream.Handler(impl, &sideStream{ctx: ctx, req: req, send: func(b []byte) {
				l.post(func() error { l.n.sideSend(h, b); return nil })
			}})
		}
		l.post(func() error { l.n.sideEnd(h, m.unary != nil, reply, err); return nil })
	}()
	return cancel
}

var errNotProto = errors.New("a side call's message is not a protobuf message")

func unmarshalSide(b []byte, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return errNotProto
	}
	if err := proto.Unmarshal(b, m); err != nil {
		return status.Error(codes.Internal, "grpc: error unmarshalling request: "+err.Error())
	}
	return nil
}

func marshalSide(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, errNotProto
	}
	return proto.Marshal(m)
}

// sideStream is a side call's stream as its server sees it: the one
// request it was sent, and the loop to send to.
type sideStream struct {
	ctx  context.Context
	req  []byte
	read bool
	send func([]byte)
}

func (s *sideStream) Context() context.Context     { return s.ctx }
func (s *sideStream) SetHeader(metadata.MD) error  { return nil }
func (s *sideStream) SendHeader(metadata.MD) error { return nil }
func (s *sideStream) SetTrailer(metadata.MD)       {}

func (s *sideStream) SendMsg(m any) error {
	if err := s.ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	b, err := marshalSide(m)
	if err != nil {
		return err
	}
	s.send(b)
	return nil
}

func (s *sideStream) RecvMsg(m any) error {
	if s.read {
		return io.EOF
	}
	s.read = true
	return unmarshalSide(s.req, m)
}

package wl

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
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

// selfTLS is the TLS configuration of the node's own client: the client
// port's, showing the node's certificate, and trusting the node it
// dials, as etcd's gateway does.
func selfTLS(tlsCfg *tls.Config) *tls.Config {
	dial := tlsCfg.Clone()
	dial.InsecureSkipVerify = true //nolint:gosec // etcd's gateway trusts the node it runs in, which it dials over loopback
	dial.GetConfigForClient = nil
	return dial
}

// sideCallerHeader names the client a call of the side calls' own client
// is for: the node's secret, a colon and the client's certificate name.
const sideCallerHeader = "tigerstore-side-caller"

// newSideSecret is a node's secret for sideCallerHeader.
func newSideSecret() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// endpointOf is the endpoint a client dials ln at: its address, or a unix
// socket's path as a unix URL.
func endpointOf(ln net.Listener) string {
	if a := ln.Addr(); a.Network() == "unix" {
		return "unix://" + a.String()
	}
	return ln.Addr().String()
}

// selfClient is the node's client of itself: it dials addr, a client
// listener of the node, with dial, or without TLS when dial is nil.
func selfClient(sys sysEnv, addr string, dial *tls.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints: []string{addr}, TLS: dial, Logger: zap.NewNop(),
		MaxCallRecvMsgSize: math.MaxInt32, DialOptions: sys.grpcDial(),
	})
}

func newSideServers(cli *clientv3.Client) *sideServers {
	h := health.NewServer()
	// etcd's: serving for every service, named by "".
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	return &sideServers{lock: v3lock.NewLockServer(cli), election: v3election.NewElectionServer(cli), health: h}
}

// sideRequest keeps the request of side call s until its client ends
// its side. As grpc-go's, a call takes exactly one request. The request
// kept counts in its client's share of requests sent in part, and is
// refused past it as one sent in part is: kept uncounted, the Lock or
// Election calls a client never ended held any memory it sent (#689's
// area-4 review).
func (n *node) sideRequest(c *client, s *stream, msg []byte) {
	h := mkHandle(c, s.id)
	if _, ok := n.takeSideReq(h); ok {
		n.replyError(c, s.id, codeInternal, "cardinality violation: received multiple request messages for non-client-streaming RPC")
		return
	}
	k := int64(len(msg))
	if c.partial+k > n.connIn || n.cpool+k > n.sizes.clientInBufs() {
		n.reject(c, s, errTooManyRequests)
		return
	}
	n.sideReqs[h] = bytes.Clone(msg)
	c.partial += k
	c.grow(int(k))
}

// takeSideReq takes the request kept for the side call of h out of its
// client's share.
func (n *node) takeSideReq(h handle) ([]byte, bool) {
	req, ok := n.sideReqs[h]
	if !ok {
		return nil, false
	}
	delete(n.sideReqs, h)
	if c, _ := n.clientOf(h); c != nil {
		c.partial -= int64(len(req))
		c.grow(-len(req))
	}
	return req, true
}

// sideEnded starts side call s, whose client ended its side, with its
// one request.
func (n *node) sideEnded(c *client, s *stream) {
	h := mkHandle(c, s.id)
	req, ok := n.takeSideReq(h)
	switch {
	case !ok:
		n.replyError(c, s.id, codeInternal, "cardinality violation: received no request message from non-client-streaming RPC")
	case n.shell != nil:
		n.sideCalls[h] = n.shell.sideCall(h, &sideMethods[s.kind-kSide], req)
	}
}

// cancelSide ends the context of the side call of h, if it has one.
func (n *node) cancelSide(h handle) {
	n.takeSideReq(h)
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
	// The closed client's share went with it (client.dropAll).
	for h := range n.sideReqs {
		if i, _, _ := h.parts(); i == idx {
			delete(n.sideReqs, h)
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
		n.callEnded(s, 0)
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
	// The server's calls of the node are for the client of h: they carry
	// its certificate name, with the secret that marks the node's own
	// client, and its token, as etcd's in-process client carries its
	// context.
	if c, s := l.n.clientOf(h); c != nil {
		ctx = metadata.AppendToOutgoingContext(ctx, sideCallerHeader, l.n.sideSecret+":"+c.nameOf(s))
		if s.token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "token", s.token)
		}
	}
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

// A follower hands a MemberPromote to the leader as etcd's does over its
// peer API (#783): on a peer connection of its own, whose hello is
// promoteAsk, with the peer TLS, it sends the caller's token and the
// request. The leader runs it through its own client with that token, as
// a side call's, so it checks the caller's auth again where the auth
// store is newest, as etcd v3.7.2's leader checks the token its peer
// forwards. As there, a caller known only by its certificate has no
// identity on the leader, which takes no name from a peer, and is
// refused while auth is on (#938). The follower gives up after
// forwardTimeout, the leader's call a second before.

// promoteAsk is the hello of a promotion forwarded to the leader.
const promoteAsk = ^uint64(1)

// forwardTimeout bounds a forwarded promotion, as etcd's ReqTimeout.
const forwardTimeout = requestTimeout + time.Second

// errForwardTimeout is etcd's answer to a forwarded promotion that
// timed out.
var errForwardTimeout = status.Error(codes.Unavailable, msgTimeout)

// forwardPromote has the leader at peer address addr run the
// MemberPromote req for the client of h, in a goroutine of its own, and
// answers that client with the leader's answer. It returns what ends the
// forward.
func (l *loop) forwardPromote(h handle, addr string, req []byte) context.CancelFunc {
	var token string
	if c, s := l.n.clientOf(h); c != nil {
		token = s.token
	}
	ctx, cancel := context.WithTimeout(l.ctx, forwardTimeout)
	go func() {
		reply, err := l.askPromote(ctx, addr, []byte(token), req)
		l.post(func() error { l.n.sideEnd(h, true, reply, err); return nil })
	}()
	return cancel
}

// askPromote sends the leader at addr the frames of a promotion, and
// returns its answer: the reply, or the error it answered with.
func (l *loop) askPromote(ctx context.Context, addr string, frames ...[]byte) ([]byte, error) {
	b, err := l.exchangePromote(ctx, addr, frames)
	code, k := binary.Uvarint(b)
	if err == nil && k <= 0 {
		err = errRecord
	}
	switch {
	case err != nil:
		l.logger.Printf("forward a promotion to %s: %v", addr, err)
		return nil, errForwardTimeout
	case code != 0:
		return nil, status.Error(codes.Code(code), string(b[k:]))
	}
	return b[k:], nil
}

// exchangePromote sends frames on a peer connection to addr and reads
// the answer, on Go's TLS: ctx's deadline bounds the dial, the handshake
// and every read and write, and ctx's end ends them.
func (l *loop) exchangePromote(ctx context.Context, addr string, frames [][]byte) ([]byte, error) {
	nc, err := l.n.sys.dial(ctx, addr, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = nc.Close() }()
	deadline, _ := ctx.Deadline()
	if err := nc.SetDeadline(deadline); err != nil {
		return nil, err
	}
	defer context.AfterFunc(ctx, func() { _ = nc.SetDeadline(time.Now()) })()
	cfg := l.peerTLS.Clone()
	cfg.ServerName, _, _ = net.SplitHostPort(addr)
	cfg.MinVersion = tls.VersionTLS13
	tc := tls.Client(nc, cfg)
	b := peerHelloBytes(promoteAsk, l.n.clusterID, "")
	for _, f := range frames {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(f)))
		b = append(b, f...)
	}
	if _, err := tc.Write(b); err != nil {
		return nil, err
	}
	var h [4]byte
	if _, err := io.ReadFull(tc, h[:]); err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint32(h[:])
	if size > 2*snapFrame {
		return nil, errRecord
	}
	answer := make([]byte, size)
	if _, err := io.ReadFull(tc, answer); err != nil {
		return nil, err
	}
	return answer, nil
}

// answerPromote runs the promotion a peer forwards, in the accept
// goroutine, with the caller's token, and answers with a code, 0 or the
// gRPC error's, then the reply or the error's message.
func (l *loop) answerPromote(s *sock) error {
	var frames [2]string // the caller's token and the request
	for i := range frames {
		b, err := readFrame(s)
		if err != nil {
			return err
		}
		frames[i] = string(b)
	}
	r := &etcdpb.MemberPromoteRequest{}
	if err := proto.Unmarshal([]byte(frames[1]), r); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(l.ctx, requestTimeout)
	defer cancel()
	// The call is for a caller of no certificate name, not for the
	// node's own.
	md := []string{sideCallerHeader, l.n.sideSecret + ":"}
	if frames[0] != "" {
		md = append(md, "token", frames[0])
	}
	ctx = metadata.AppendToOutgoingContext(ctx, md...)
	resp, err := etcdpb.NewClusterClient(l.self.ActiveConnection()).MemberPromote(ctx, r)
	var answer []byte
	switch {
	case ctx.Err() != nil:
		err = errForwardTimeout
	case err == nil:
		answer, err = proto.Marshal(resp)
	}
	if err != nil {
		st := status.Convert(err)
		answer = append(binary.AppendUvarint(nil, uint64(st.Code())), st.Message()...)
	} else {
		answer = append([]byte{0}, answer...)
	}
	putFrame(s, answer)
	return flushAll(s)
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

package wl

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/testcert"
)

// A follower hands a MemberPromote to the leader, as etcd's does, over
// the peer link (#783). These tests promote, through a follower, a
// learner that never started, which the leader refuses as not ready: an
// answer only the leader gives.

// idleLearner adds a learner that never starts, once the three nodes
// published their client URLs, and returns its ID and the indexes of the
// leader and of a follower.
func idleLearner(t *testing.T, tc *testCluster, c *clientv3.Client) (id uint64, leader, follower int) {
	t.Helper()
	waitMembers(t, c, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	added, err := c.MemberAddAsLearner(ctx, []string{"https://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	return added.Member.ID, slices.Index(tc.endpoints, tc.leaderEndpoint(t, c)), slices.Index(tc.endpoints, tc.followerEndpoint(t, c))
}

// fakeClientPort is a gRPC server with a certificate no node trusts. It
// answers every call as unimplemented and keeps the metadata of the
// first.
type fakeClientPort struct {
	addr string
	mu   sync.Mutex
	md   metadata.MD
}

func newFakeClientPort(t *testing.T) *fakeClientPort {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, _, _, err := ca.Issue("rogue", 1)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeClientPort{addr: ln.Addr().String()}
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})),
		grpc.UnknownServiceHandler(func(_ any, st grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(st.Context())
			f.mu.Lock()
			if f.md == nil {
				f.md = md
			}
			f.mu.Unlock()
			return status.Error(codes.Unimplemented, "a fake client port")
		}))
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return f
}

func (f *fakeClientPort) got() metadata.MD {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.md
}

// TestAForwardedPromotionTrustsNoClientURL is #783's points 1 and 2: each
// node's client URL is a server whose certificate no node can check, and
// the client port has no CA to check it with. The follower forwarded
// over the leader's client URLs, with its client TLS, which skipped the
// check: it called that server with the header and secret of its own
// side calls.
func TestAForwardedPromotionTrustsNoClientURL(t *testing.T) {
	fake := newFakeClientPort(t)
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			c.ClientURLs = []string{"https://" + fake.addr}
			c.ClientTLS = c.ClientTLS.Clone()
			c.ClientTLS.ClientCAs, c.ClientTLS.ClientAuth = nil, tls.NoClientCert
		}
	})
	c := tc.client(t)
	id, _, f := idleLearner(t, tc, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := tc.rawClient(t, tc.endpoints[f]).MemberPromote(ctx, id)
	if md := fake.got(); md != nil {
		if len(md.Get(sideCallerHeader)) > 0 {
			t.Errorf("the follower sent its side calls' secret to another server: %v", md)
		}
		t.Errorf("the follower called a server whose certificate it could not check: %v", md)
	}
	if !errors.Is(err, rpctypes.ErrMemberLearnerNotReady) {
		t.Errorf("a promotion of a learner that never started, through a follower: %v; want the leader's %v", err, rpctypes.ErrMemberLearnerNotReady)
	}
}

// enableAuth adds root, with the root role, and turns auth on, through c.
func enableAuth(t *testing.T, ctx context.Context, c *clientv3.Client) {
	t.Helper()
	for _, step := range []func() error{
		func() error { _, err := c.UserAdd(ctx, "root", "rootpw"); return err },
		func() error { _, err := c.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := c.AuthEnable(ctx); return err },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
}

// certClient is a client of endpoint whose certificate names cn, which
// authenticates as user with password if user is set.
func (tc *testCluster) certClient(t *testing.T, endpoint, cn, user, password string) *clientv3.Client {
	t.Helper()
	cert, _, _, err := tc.ca.Issue(cn, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second, Username: user, Password: password,
		TLS: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestAForwardedPromotionKeepsTheCallersIdentity is #783's point 3, as
// etcd v3.7.2 has it (#938): with auth on, root promotes through a
// follower. By its token it gets the leader's answer; the leader checked
// the follower's own certificate name instead, and refused it. By its
// certificate alone it is refused as etcd's leader refuses it: a
// certificate is the follower's to check, and the leader takes no name
// from a peer.
func TestAForwardedPromotionKeepsTheCallersIdentity(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	id, _, f := idleLearner(t, tc, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	enableAuth(t, ctx, c)
	for _, who := range []struct {
		what, cn, user, password string
		want                     error
	}{
		{"by its token", "nobody", "root", "rootpw", rpctypes.ErrMemberLearnerNotReady},
		{"by its certificate", "root", "", "", rpctypes.ErrUserEmpty},
	} {
		cl := tc.certClient(t, tc.endpoints[f], who.cn, who.user, who.password)
		if _, err := cl.MemberPromote(ctx, id); !errors.Is(err, who.want) {
			t.Errorf("root's promotion %s of a learner that never started, through a follower: %v; want %v", who.what, err, who.want)
		}
	}
}

// TestAForwardSendsNoTokenOverUnverifiedTLS is #938's item 2: with peer
// TLS that checks no certificate, as --peer-auto-tls's, a follower sent
// the caller's token to whatever answered at the leader's peer URL. It
// must answer that it does not lead instead.
func TestAForwardSendsNoTokenOverUnverifiedTLS(t *testing.T) {
	tc := newTestClusterWith(t, func(tc *testCluster) {
		tc.config = func(c *Config) {
			c.PeerClientTLS = c.PeerClientTLS.Clone()
			c.PeerClientTLS.InsecureSkipVerify = true
		}
	})
	c := tc.client(t)
	id, _, f := idleLearner(t, tc, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	enableAuth(t, ctx, c)
	root := tc.certClient(t, tc.endpoints[f], "nobody", "root", "rootpw")
	const want = "etcdserver: not leader"
	if _, err := root.MemberPromote(ctx, id); rpctypes.ErrorDesc(err) != want {
		t.Fatalf("root's promotion by its token through a follower whose peer TLS checks nothing: %v; want %q", err, want)
	}
}

// TestAForwardedPromotionIsCheckedOnTheLeader is #783's review: alice
// loses the root role while a follower's loop is held, and the follower,
// which has not applied that yet, forwards her promotion. The leader must
// check her again and refuse it, as etcd's leader checks the token its
// peer forwards. The follower records each promotion it forwards
// (testAdmit); one it refused itself, after it applied the revocation,
// tests nothing, and the test runs again.
func TestAForwardedPromotionIsCheckedOnTheLeader(t *testing.T) {
	loopOf := watchLoops(t)
	var fid atomic.Uint64
	forwarded := make(chan struct{}, 64)
	hook := func(n *node, kind uint8, _ []byte) {
		if kind == kMemberPromote && n.id == fid.Load() {
			forwarded <- struct{}{}
		}
	}
	testAdmit.Store(&hook)
	t.Cleanup(func() { testAdmit.Store(nil) })
	tc := newTestCluster(t)
	c := tc.client(t)
	id, li, f := idleLearner(t, tc, c)
	fid.Store(tc.ids[f])
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := c.UserAdd(ctx, "alice", "alicepw"); err != nil {
		t.Fatal(err)
	}
	enableAuth(t, ctx, c)
	root := tc.certClient(t, tc.endpoints[li], "nobody", "root", "rootpw")
	alice := tc.certClient(t, tc.endpoints[f], "nobody", "alice", "alicepw")
	for attempt := range 5 {
		if _, err := root.UserGrantRole(ctx, "alice", "root"); err != nil {
			t.Fatal(err)
		}
		// The follower applied the grant once it forwards her promotion.
		for {
			_, err := alice.MemberPromote(ctx, id)
			if errors.Is(err, rpctypes.ErrMemberLearnerNotReady) {
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("alice, granted root, never promoted through the follower: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		for len(forwarded) > 0 {
			<-forwarded
		}
		release := holdLoop(t, loopOf(tc.ids[f]))
		if _, err := root.UserRevokeRole(ctx, "alice", "root"); err != nil {
			release()
			t.Fatal(err)
		}
		promoted := make(chan error, 1)
		go func() {
			_, err := alice.MemberPromote(ctx, id)
			promoted <- err
		}()
		// The promotion reaches the follower's socket while its loop is
		// held, so its next turn reads it before it applies the revocation.
		time.Sleep(200 * time.Millisecond)
		release()
		err := <-promoted
		select {
		case <-forwarded:
		default:
			t.Logf("attempt %d: the follower applied the revocation before it took the promotion, which tests nothing: again (%v)", attempt, err)
			continue
		}
		if !errors.Is(err, rpctypes.ErrPermissionDenied) {
			t.Fatalf("alice's promotion, forwarded after she lost the root role on the leader: %v; want %v", err, rpctypes.ErrPermissionDenied)
		}
		return
	}
	t.Fatal("in 5 attempts the follower never forwarded a promotion before it applied the revocation")
}

// TestAForwardedPromotionIsBounded is #783's point 5: the leader's loop
// stands still while a follower forwards a promotion to it. The forward
// had no deadline of its own and waited for the leader as long as the
// client did; it must give up as a write does, in about requestTimeout.
func TestAForwardedPromotionIsBounded(t *testing.T) {
	loopOf := watchLoops(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	id, li, f := idleLearner(t, tc, c)
	fc := tc.rawClient(t, tc.endpoints[f])
	if _, err := fc.MemberList(context.Background()); err != nil {
		t.Fatal(err)
	}
	const hold = 3 * requestTimeout
	release := holdLoop(t, loopOf(tc.ids[li]))
	time.AfterFunc(hold, release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*hold)
	defer cancel()
	start := time.Now()
	_, err := fc.MemberPromote(ctx, id)
	if took := time.Since(start); took > requestTimeout+3*time.Second {
		t.Fatalf("a promotion forwarded to a leader that stood still answered after %v (%v); want about %v", took, err, requestTimeout)
	}
	if !errors.Is(err, rpctypes.ErrTimeout) {
		t.Fatalf("a promotion forwarded to a leader that stood still: %v; want %v", err, rpctypes.ErrTimeout)
	}
}

// TestAForwardStopsAtItsDeadline is #783's review: a leader's peer port
// that takes the connection and never answers, not even the TLS
// handshake. The forward must give up at its deadline, and at once when
// its client goes; the handshake kept a limit of its own, 10 s.
func TestAForwardStopsAtItsDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	l := &loop{n: &node{}, logger: log.New(io.Discard, "", 0), peerTLS: &tls.Config{MinVersion: tls.VersionTLS13}}
	for _, c := range []struct {
		what   string
		cancel time.Duration
	}{{"its deadline", 0}, {"its client goes", 200 * time.Millisecond}} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if c.cancel > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
			time.AfterFunc(c.cancel, cancel)
		}
		start := time.Now()
		_, err := l.askPromote(ctx, ln.Addr().String(), []byte("req"))
		cancel()
		if took := time.Since(start); took > 3*time.Second || !errors.Is(err, errForwardTimeout) {
			t.Errorf("a forward to a silent peer, ended by %s: %v after %v", c.what, err, took)
		}
	}
}

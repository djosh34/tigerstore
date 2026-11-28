package wl

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"slices"
	"sync"
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

// TestAForwardedPromotionKeepsTheCallersCertificateName is #783's point 3:
// with auth on, a client whose certificate names root promotes through a
// follower. The leader checked the follower's own certificate name, not
// the client's, and refused it.
func TestAForwardedPromotionKeepsTheCallersCertificateName(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	id, _, f := idleLearner(t, tc, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, step := range []func() error{
		func() error { _, err := c.UserAdd(ctx, "root", "rootpw"); return err },
		func() error { _, err := c.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := c.AuthEnable(ctx); return err },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	cert, _, _, err := tc.ca.Issue("root", 2)
	if err != nil {
		t.Fatal(err)
	}
	root, err := clientv3.New(clientv3.Config{
		Endpoints: []string{tc.endpoints[f]}, DialTimeout: 5 * time.Second,
		TLS: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if _, err := root.MemberPromote(ctx, id); !errors.Is(err, rpctypes.ErrMemberLearnerNotReady) {
		t.Fatalf("root's promotion of a learner that never started, through a follower: %v; want the leader's %v", err, rpctypes.ErrMemberLearnerNotReady)
	}
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

package server_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
	"github.com/djosh34/tigerstore/internal/testcert"
)

type cluster struct {
	t         *testing.T
	ca        *testcert.CA
	peerAddrs map[uint64]string
	dirs      map[uint64]string
	running   map[uint64]*running
}

type running struct {
	addr string
	stop func()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // only the port number is needed
	return ln.Addr().String()
}

func newCluster(t *testing.T) *cluster {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, ca: ca, peerAddrs: map[uint64]string{}, dirs: map[uint64]string{}, running: map[uint64]*running{}}
	for id := uint64(1); id <= 3; id++ {
		c.peerAddrs[id] = freeAddr(t)
		c.dirs[id] = t.TempDir()
	}
	for id := uint64(1); id <= 3; id++ {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, r := range c.running {
			r.stop()
		}
	})
	return c
}

func (c *cluster) start(id uint64) {
	t := c.t
	cert, _, _, err := c.ca.Issue("node", int64(id+10))
	if err != nil {
		t.Fatal(err)
	}
	n, err := node.New(node.Config{Core: core.DefaultConfig(id, []uint64{1, 2, 3}), DataDir: c.dirs[id], Tick: 10 * time.Millisecond, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	peers := map[uint64]string{}
	for pid, a := range c.peerAddrs {
		if pid != id {
			peers[pid] = a
		}
	}
	tr, err := peer.Start(peer.Config{ID: id, Listen: c.peerAddrs[id], Peers: peers, Certificate: cert, CA: c.ca.Pool, Deliver: n.Deliver, Unreachable: n.Unreachable})
	if err != nil {
		t.Fatal(err)
	}
	g := server.New(n, server.Config{TLS: &tls.Config{
		Certificates: []tls.Certificate{cert}, ClientCAs: c.ca.Pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	served := make(chan error, 1)
	go func() { ran <- n.Run(ctx, tr) }()
	go func() { served <- g.Serve(ln) }()
	c.running[id] = &running{addr: ln.Addr().String(), stop: func() {
		cancel()
		if err := <-ran; err != nil {
			t.Errorf("node %d: %v", id, err)
		}
		g.Stop()
		if err := errors.Join(<-served, tr.Close()); err != nil {
			t.Errorf("node %d: %v", id, err)
		}
	}}
}

func (c *cluster) client(id uint64, cert tls.Certificate) pb.KVClient {
	conn, err := grpc.NewClient(c.running[id].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { conn.Close() }) //nolint:errcheck // closing a test client
	return pb.NewKVClient(conn)
}

// retry runs fn until it succeeds or 10 s pass; the cluster needs a moment
// to elect a leader.
func retry[T any](t *testing.T, fn func(ctx context.Context) (T, error)) T {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		v, err := fn(ctx)
		cancel()
		if err == nil {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("still failing after 10 s: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestThreeNodesOverTLS(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	kv1, kv2, kv3 := c.client(1, cert), c.client(2, cert), c.client(3, cert)

	put := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv1.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	rev := put.Header.Revision
	if put.Header.MemberId != 1 || put.Header.RaftTerm == 0 {
		t.Fatalf("put header = %v", put.Header)
	}
	got, err := kv2.Range(context.Background(), &pb.RangeRequest{Key: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "1" || got.Header.Revision < rev {
		t.Fatalf("linearizable read on node 2 = %v", got)
	}

	cas := &pb.TxnRequest{
		Compare: []*pb.Compare{{Key: []byte("a"), Target: pb.Compare_MOD, Result: pb.Compare_EQUAL, TargetUnion: &pb.Compare_ModRevision{ModRevision: rev}}},
		Success: []*pb.RequestOp{{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte("a"), Value: []byte("2")}}}},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte("a")}}}},
	}
	txn, err := kv3.Txn(context.Background(), cas)
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Succeeded {
		t.Fatalf("first compare-and-swap failed: %v", txn)
	}
	txn, err = kv1.Txn(context.Background(), cas)
	if err != nil {
		t.Fatal(err)
	}
	if txn.Succeeded || string(txn.Responses[0].GetResponseRange().Kvs[0].Value) != "2" {
		t.Fatalf("second compare-and-swap = %v", txn)
	}

	if _, err := kv1.Put(context.Background(), &pb.PutRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("put without key: %v", err)
	}

	// Restart node 2 from its disk; it must still have the data.
	c.running[2].stop()
	delete(c.running, 2)
	// Node 2 may have been the leader, so the delete may time out until
	// nodes 1 and 3 elect a new one. Deleting twice is harmless.
	retry(t, func(ctx context.Context) (*pb.DeleteRangeResponse, error) {
		return kv1.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("a")})
	})
	c.start(2)
	kv2 = c.client(2, cert)
	got = retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
		return kv2.Range(ctx, &pb.RangeRequest{Key: []byte("a"), Revision: rev + 1})
	})
	if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "2" {
		t.Fatalf("node 2 after restart, read at %d = %v", rev+1, got)
	}
	got = retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
		return kv2.Range(ctx, &pb.RangeRequest{Key: []byte("a")})
	})
	if len(got.Kvs) != 0 {
		t.Fatalf("node 2 after restart still has the deleted key: %v", got)
	}
}

func TestClientCertificateRequired(t *testing.T) {
	c := newCluster(t)
	other, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	bad, _, _, err := other.Issue("client", 5)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, bad)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("a"), Serializable: true}); err == nil {
		t.Fatal("a client certificate from another CA was accepted")
	}
}

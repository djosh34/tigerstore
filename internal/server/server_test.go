package server_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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
	// via, if set, is the address node from dials to reach node to.
	via map[[2]uint64]string
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

func newCluster(t *testing.T) *cluster { return newClusterWith(t, nil) }

// newClusterWith runs setup on the cluster before its nodes start.
func newClusterWith(t *testing.T, setup func(*cluster)) *cluster {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, ca: ca, peerAddrs: map[uint64]string{}, dirs: map[uint64]string{}, running: map[uint64]*running{}}
	for id := uint64(1); id <= 3; id++ {
		c.peerAddrs[id] = freeAddr(t)
		c.dirs[id] = t.TempDir()
	}
	if setup != nil {
		setup(c)
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
			if v, ok := c.via[[2]uint64{id, pid}]; ok {
				peers[pid] = v
			}
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
		stopErr := g.Stop()
		if err := errors.Join(stopErr, <-served, tr.Close()); err != nil {
			t.Errorf("node %d: %v", id, err)
		}
	}}
}

func (c *cluster) client(id uint64, cert tls.Certificate) pb.KVClient {
	return pb.NewKVClient(c.dial(id, cert))
}

// dial opens a gRPC client connection to node id, closed with the test.
func (c *cluster) dial(id uint64, cert tls.Certificate) *grpc.ClientConn {
	conn, err := grpc.NewClient(c.running[id].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { conn.Close() }) //nolint:errcheck // closing a test client
	return conn
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

func TestWatchOverTLS(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 98)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	first := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("w/a"), Value: []byte("1")})
	})
	conn, err := grpc.NewClient(c.running[2].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader)
	ws, err := pb.NewWatchClient(conn).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{
		Key: []byte("w/"), RangeEnd: []byte("w0"), StartRevision: first.Header.Revision, PrevKv: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("w/a"), Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("w/a")}); err != nil {
		t.Fatal(err)
	}
	var got []string
	created := false
	for len(got) < 3 {
		r, err := ws.Recv()
		if err != nil {
			t.Fatal(err)
		}
		created = created || r.Created
		for _, ev := range r.Events {
			s := ev.Type.String() + " " + string(ev.Kv.Value)
			if ev.PrevKv != nil {
				s += " prev " + string(ev.PrevKv.Value)
			}
			got = append(got, s)
		}
	}
	if !created || strings.Join(got, ", ") != "PUT 1, PUT 2 prev 1, DELETE  prev 2" {
		t.Fatalf("created %v, events %q", created, got)
	}
	if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}}); err != nil {
		t.Fatal(err)
	}
	r, err := ws.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if r.WatchId != -1 || r.Header.Revision < first.Header.Revision+2 || len(r.Events) != 0 {
		t.Fatalf("progress response = %v", r)
	}
}

func TestStatusHealthVersionRangeStream(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 97)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")})
	})
	conn, err := grpc.NewClient(c.running[1].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	st, err := pb.NewMaintenanceClient(conn).Status(context.Background(), &pb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Version != "3.7.2" || st.StorageVersion != "3.7.0" || st.Leader == 0 || st.DbSize <= 0 ||
		st.RaftTerm == 0 || st.RaftIndex == 0 || st.RaftAppliedIndex == 0 || st.Header.MemberId != 1 || st.Header.Revision < 2 {
		t.Fatalf("status = %v", st)
	}

	stream, err := pb.NewKVClient(conn).RangeStream(context.Background(), &pb.RangeRequest{Key: []byte("a")})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("RangeStream: %v, want Unimplemented", err)
	}

	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	}, ForceAttemptHTTP2: true}}
	get := func(path string) (int, string) {
		resp, err := hc.Get("https://" + c.running[1].addr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck // closing a test response
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	if code, body := get("/version"); code != 200 || body != `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}` {
		t.Fatalf("/version = %d %q", code, body)
	}
	if code, body := get("/health"); code != 200 || body != `{"health":"true","reason":""}` {
		t.Fatalf("/health = %d %q", code, body)
	}
	if resp, err := hc.Head("https://" + c.running[1].addr + "/version"); err != nil || resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 {
		t.Fatalf("HEAD /version over HTTP/2: %v %v", resp, err)
	} else if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	// Without a quorum there is no leader for long, and /health says so.
	c.running[2].stop()
	c.running[3].stop()
	delete(c.running, 2)
	delete(c.running, 3)
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, body := get("/health")
		if code == 503 && body == "{\"health\":\"false\",\"reason\":\"RAFT NO LEADER\"}\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("/health without a quorum = %d %q", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if code, _ := get("/version"); code != 200 {
		t.Fatalf("/version without a quorum = %d", code)
	}
}

// TestManyStreamsOnOneConnection opens more watch streams on one
// connection than net/http's default HTTP/2 limit of 250; kube-apiserver
// runs many watches over one connection.
func TestManyStreamsOnOneConnection(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 96)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(c.running[1].addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: c.ca.Pool, ServerName: "localhost",
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // closing a test client
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wc := pb.NewWatchClient(conn)
	for i := range 300 {
		ws, err := wc.Watch(ctx)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		if err := ws.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{CreateRequest: &pb.WatchCreateRequest{Key: []byte("a")}}}); err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		r, err := ws.Recv()
		if err != nil || !r.Created {
			t.Fatalf("stream %d: %v %v", i, r, err)
		}
	}
}

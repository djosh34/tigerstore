package server_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
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
	// tick is the Raft tick, 10 ms unless setup changes it.
	tick time.Duration
	// stallElections, if set, is the nodes' CommitStallElections.
	stallElections int
	// coreConfig, if set, changes the nodes' core configuration, and
	// segmentBytes their segment size.
	coreConfig   func(*core.Config)
	segmentBytes int64
	// beforeRead, if set, runs before each value read from a log.
	beforeRead func()
	// join marks the nodes that join the running cluster instead of
	// starting it, and removed those that may stop as removed.
	join, removed map[uint64]bool
	// nodeConfig, if set, changes the nodes' driver configuration, and
	// wrap the transport each node runs with.
	nodeConfig func(*node.Config)
	wrap       func(id uint64, t peer.Transport) peer.Transport
	// jwt, if set, makes the auth tokens, and serverConfig, if set,
	// changes the servers' configuration.
	jwt          *server.JWT
	serverConfig func(*server.Config)
	// nodes and transports are the running nodes and their transports by
	// ID.
	nodes      map[uint64]*node.Node
	transports map[uint64]*peer.TCP
	servers    map[uint64]*server.Server
	// peerLns are the peer listeners of nodes not started yet, held open
	// from the start so that no other process takes their ports.
	peerLns map[uint64]net.Listener
}

type running struct {
	addr string
	stop func()
	// stopNode stops the node but leaves its client port open.
	stopNode func()
}

// listenPeer listens on addr, or on a free port if addr is empty. A port
// that a stopped node just released can be busy for a moment, so it
// retries for a while.
func listenPeer(t *testing.T, addr string) net.Listener {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newCluster(t *testing.T) *cluster { return newClusterWith(t, nil) }

// newClusterWith runs setup on the cluster before its nodes start.
func newClusterWith(t *testing.T, setup func(*cluster)) *cluster {
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, ca: ca, peerAddrs: map[uint64]string{}, dirs: map[uint64]string{}, running: map[uint64]*running{}, tick: 10 * time.Millisecond, peerLns: map[uint64]net.Listener{}}
	for id := uint64(1); id <= 3; id++ {
		ln := listenPeer(t, "")
		c.peerLns[id] = ln
		c.peerAddrs[id] = ln.Addr().String()
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

// viaPeers keeps node from dialing its peers through the addresses in
// cluster.via when the members change.
type viaPeers struct {
	peer.Transport
	c    *cluster
	from uint64
}

func (v viaPeers) SetPeers(peers map[uint64]string, removed []uint64) {
	for to := range peers {
		if a, ok := v.c.via[[2]uint64{v.from, to}]; ok {
			peers[to] = a
		}
	}
	v.Transport.SetPeers(peers, removed)
}

// joinNode starts node id, which joins the running cluster.
func (c *cluster) joinNode(id uint64) {
	if c.join == nil {
		c.join, c.removed = map[uint64]bool{}, map[uint64]bool{}
	}
	c.join[id] = true
	c.dirs[id] = c.t.TempDir()
	c.start(id)
}

// members are the cluster's three members with their peer URLs.
func (c *cluster) members() []*pb.Member {
	var ms []*pb.Member
	for id := uint64(1); id <= 3; id++ {
		ms = append(ms, &pb.Member{ID: id, Name: fmt.Sprintf("node%d", id), PeerURLs: []string{"https://" + c.peerAddrs[id]}})
	}
	return ms
}

func (c *cluster) start(id uint64) {
	t := c.t
	cert, _, _, err := c.ca.Issue("node", int64(id+10))
	if err != nil {
		t.Fatal(err)
	}
	logf := func(format string, args ...any) { t.Logf("node %d: %s", id, fmt.Sprintf(format, args...)) }
	cfg := core.DefaultConfig(id, []uint64{1, 2, 3})
	cfg.Members = c.members()
	cfg.Name = fmt.Sprintf("node%d", id)
	cfg.Join = c.join[id]
	cfg.CommitStallElections = c.stallElections
	if c.coreConfig != nil {
		c.coreConfig(&cfg)
	}
	ncfg := node.Config{Core: cfg, DataDir: c.dirs[id], Tick: c.tick, Logf: logf, SegmentBytes: c.segmentBytes, BeforeRead: c.beforeRead}
	if c.nodeConfig != nil {
		c.nodeConfig(&ncfg)
	}
	n, err := node.New(ncfg)
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
	pln, ok := c.peerLns[id]
	delete(c.peerLns, id)
	if !ok {
		// A restart: the stopped transport closed its listener.
		pln = listenPeer(t, c.peerAddrs[id])
	}
	tr, err := peer.Start(peer.Config{ID: id, Listeners: []net.Listener{pln}, Peers: peers, Certificate: cert, CA: c.ca.Pool, Deliver: n.Deliver, Unreachable: n.Unreachable, Serve: n.Serve, Answer: n.Answer, Removed: n.Removed, Logf: logf})
	if err != nil {
		t.Fatal(err)
	}
	scfg := server.Config{
		TLS: &tls.Config{
			Certificates: []tls.Certificate{cert}, ClientCAs: c.ca.Pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
		},
		PeerVersions: tr.Versions, JWT: c.jwt,
		// bcrypt's default cost takes seconds of CPU under the race
		// detector, which starves the nodes.
		BcryptCost: bcrypt.MinCost,
	}
	if c.serverConfig != nil {
		c.serverConfig(&scfg)
	}
	g, err := server.New(n, scfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	served := make(chan error, 1)
	var run peer.Transport = tr
	if c.via != nil {
		run = viaPeers{Transport: run, c: c, from: id}
	}
	if c.wrap != nil {
		run = c.wrap(id, run)
	}
	if c.nodes == nil {
		c.nodes, c.transports, c.servers = map[uint64]*node.Node{}, map[uint64]*peer.TCP{}, map[uint64]*server.Server{}
	}
	c.nodes[id], c.transports[id], c.servers[id] = n, tr, g
	go func() { ran <- n.Run(ctx, run) }()
	go func() { served <- g.Serve(ln) }()
	var once sync.Once
	stopNode := func() {
		once.Do(func() {
			cancel()
			if err := <-ran; err != nil && (!c.removed[id] || !errors.Is(err, core.ErrRemoved)) {
				t.Errorf("node %d: %v", id, err)
			}
		})
	}
	c.running[id] = &running{addr: ln.Addr().String(), stopNode: stopNode, stop: func() {
		stopNode()
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
	if err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || len(r.RangeResponse.Kvs) != 1 || string(r.RangeResponse.Kvs[0].Value) != "1" {
		t.Fatalf("RangeStream: %v, %v", r, err)
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
	// etcd's /version answers GET only.
	if resp, err := hc.Head("https://" + c.running[1].addr + "/version"); err != nil || resp.StatusCode != http.StatusMethodNotAllowed || resp.ProtoMajor != 2 {
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

// TestSlowRequestLogged checks that a Put, which the node runs, is logged
// when it takes more than --warning-unary-request-duration, as a call the
// server runs itself is.
func TestSlowRequestLogged(t *testing.T) {
	var mu sync.Mutex
	var logged []string
	c := newClusterWith(t, func(c *cluster) {
		c.serverConfig = func(cfg *server.Config) {
			cfg.WarningUnaryRequestDuration = time.Nanosecond
			cfg.Logf = func(format string, args ...any) {
				mu.Lock()
				defer mu.Unlock()
				logged = append(logged, fmt.Sprintf(format, args...))
			}
		}
	})
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
		return kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	})
	mu.Lock()
	defer mu.Unlock()
	for _, l := range logged {
		if strings.Contains(l, "/etcdserverpb.KV/Put") {
			return
		}
	}
	t.Fatalf("no slow Put in the log: %q", logged)
}

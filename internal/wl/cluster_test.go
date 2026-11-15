package wl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/testport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// portPair finds a free peer port whose port below is free too: the loop
// takes a member's client port to be its peer port less one.
func portPair(t *testing.T) int {
	t.Helper()
	for range 100 {
		ln, p, err := testport.Listen()
		if err != nil {
			t.Fatal(err)
		}
		below, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p-1))
		_ = ln.Close()
		if err == nil {
			_ = below.Close()
			return p
		}
	}
	t.Fatal("no free pair of ports")
	return 0
}

// testCluster is three nodes running in this process, and any that join
// them.
type testCluster struct {
	dir                       string
	certFile, keyFile, caFile string
	peers, endpoints          []string
	cert                      tls.Certificate
	ca                        *testcert.CA
	cancels                   []context.CancelFunc
	stopped                   []chan error
	// members are the members each node starts with; ids and joins are
	// each node's ID and whether it joins a running cluster.
	members []Member
	ids     []uint64
	joins   []bool
}

// newTestCluster starts a 3-node cluster in this process; the test's end
// stops it.
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	return newTestClusterWith(t, nil)
}

// newTestClusterWith starts a 3-node cluster as newTestCluster does, after
// before, if any, prepares it.
func newTestClusterWith(t *testing.T, before func(tc *testCluster)) *testCluster {
	t.Helper()
	tc := &testCluster{dir: t.TempDir()}
	var err error
	if tc.ca, err = testcert.NewCA(); err != nil {
		t.Fatal(err)
	}
	cert, certPEM, keyPEM, err := tc.ca.Issue("node", 1)
	if err != nil {
		t.Fatal(err)
	}
	tc.cert = cert
	files := map[string][]byte{"ca.pem": tc.ca.PEM, "cert.pem": certPEM, "key.pem": keyPEM}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(tc.dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tc.certFile, tc.keyFile, tc.caFile = filepath.Join(tc.dir, "cert.pem"), filepath.Join(tc.dir, "key.pem"), filepath.Join(tc.dir, "ca.pem")
	for i := range 3 {
		tc.addPorts(t, uint64(i+1), false)
	}
	if before != nil {
		before(tc)
	}
	tc.start(t)
	t.Cleanup(func() { tc.stop(t) })
	return tc
}

// addPorts gives the next node ports, its ID and its place among the
// members.
func (tc *testCluster) addPorts(t *testing.T, id uint64, join bool) int {
	t.Helper()
	p := portPair(t)
	tc.peers = append(tc.peers, fmt.Sprintf("https://127.0.0.1:%d", p))
	tc.endpoints = append(tc.endpoints, fmt.Sprintf("127.0.0.1:%d", p-1))
	tc.cancels, tc.stopped = append(tc.cancels, nil), append(tc.stopped, nil)
	tc.ids, tc.joins = append(tc.ids, id), append(tc.joins, join)
	i := len(tc.ids) - 1
	tc.members = append(tc.members, Member{ID: id, Name: fmt.Sprintf("n%d", i+1), PeerURLs: []string{tc.peers[i]}})
	return i
}

// start runs the three nodes on their data dirs.
func (tc *testCluster) start(t *testing.T) {
	t.Helper()
	for i := range tc.ids {
		tc.startNode(t, i)
	}
}

// startNode runs node i on its data dir.
func (tc *testCluster) startNode(t *testing.T, i int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	tc.cancels[i], tc.stopped[i] = cancel, make(chan error, 1)
	out := io.Discard
	if os.Getenv("WL_TEST_LOG") != "" {
		out = os.Stderr
	}
	cfg := Config{
		Name: fmt.Sprintf("n%d", i+1), ID: tc.ids[i], ClusterID: 0x7769726531, DataDir: tc.dataDir(i), Join: tc.joins[i],
		Logger:     log.New(out, fmt.Sprintf("n%d ", i+1), log.Lmicroseconds),
		ClientURLs: []string{"https://" + tc.endpoints[i]},
	}
	cfg.Members = tc.members
	var err error
	if cfg.ClientTLS, err = loadTLS(tc.certFile, tc.keyFile, tc.caFile, true); err != nil {
		t.Fatal(err)
	}
	cfg.PeerServerTLS = cfg.ClientTLS
	if cfg.PeerClientTLS, err = loadTLS(tc.certFile, tc.keyFile, tc.caFile, false); err != nil {
		t.Fatal(err)
	}
	cl, err := net.Listen("tcp", tc.endpoints[i])
	if err != nil {
		t.Fatal(err)
	}
	pl, err := net.Listen("tcp", strings.TrimPrefix(tc.peers[i], "https://"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ClientListeners, cfg.PeerListeners = []net.Listener{cl}, []net.Listener{pl}
	go func() { tc.stopped[i] <- run(ctx, cfg) }()
}

func loadTLS(cert, key, ca string, server bool) (*tls.Config, error) {
	c, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	cfg := &tls.Config{Certificates: []tls.Certificate{c}, MinVersion: tls.VersionTLS13}
	if server {
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func (tc *testCluster) dataDir(i int) string { return filepath.Join(tc.dir, fmt.Sprintf("n%d", i+1)) }

// stop stops the nodes and waits for them; a node that failed on its own
// fails the test.
func (tc *testCluster) stop(t *testing.T) {
	t.Helper()
	for i := range tc.cancels {
		tc.stopNode(t, i)
	}
}

// stopNode stops node i and waits for it.
func (tc *testCluster) stopNode(t *testing.T, i int) {
	t.Helper()
	if tc.cancels[i] == nil {
		return
	}
	tc.cancels[i]()
	tc.cancels[i] = nil
	if err := <-tc.stopped[i]; !errors.Is(err, errStopped) {
		t.Errorf("node %d failed: %v", i+1, err)
	}
}

// client returns a client of the cluster once it takes a write.
func (tc *testCluster) client(t *testing.T) *clientv3.Client {
	t.Helper()
	return tc.clientOf(t, tc.endpoints...)
}

// rawClient returns a client of the given endpoints.
func (tc *testCluster) rawClient(t *testing.T, endpoints ...string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{Certificates: []tls.Certificate{tc.cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// clientOf returns a client of the given endpoints once the cluster takes
// a write through them.
func (tc *testCluster) clientOf(t *testing.T, endpoints ...string) *clientv3.Client {
	t.Helper()
	c := tc.rawClient(t, endpoints...)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if _, err := c.Put(ctx, "/ready", "1"); err == nil {
			return c
		}
		if ctx.Err() != nil {
			t.Fatal("the cluster took no write in 30 s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// smallCheckpoints makes the nodes the test starts take a checkpoint
// every few writes, take in the values of the log before the checkpoint
// before, and keep few entries for followers that lag.
func smallCheckpoints(t *testing.T) {
	t.Helper()
	takeEveryValue(t)
	minWas, windowWas := ckptMin, storageWindow
	ckptMin, storageWindow = 4<<10, 10
	t.Cleanup(func() { ckptMin, storageWindow = minWas, windowWas })
}

func TestClusterComesBackAfterARestart(t *testing.T) {
	smallCheckpoints(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	g, err := c.Grant(ctx, 600)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if _, err := c.Put(ctx, fmt.Sprintf("/r/%03d", i), strconv.Itoa(i), clientv3.WithLease(g.ID)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := c.Get(ctx, "/r/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	tc.stop(t)
	for i := range 3 {
		if p, err := readPointer(filepath.Join(tc.dataDir(i), "ckpt")); p == nil || err != nil {
			t.Fatalf("node %d took no checkpoint: %v", i+1, err)
		}
	}
	tc.start(t)
	c2 := tc.client(t)
	after, err := c2.Get(ctx, "/r/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Kvs) != 100 || after.Kvs[99].ModRevision != before.Kvs[99].ModRevision || string(after.Kvs[42].Value) != "42" {
		t.Fatalf("after the restart %d keys, last at %d, want 100 at %d", len(after.Kvs), after.Kvs[len(after.Kvs)-1].ModRevision, before.Kvs[99].ModRevision)
	}
	ttl, err := c2.TimeToLive(ctx, g.ID, clientv3.WithAttachedKeys())
	if err != nil || len(ttl.Keys) != 100 {
		t.Fatalf("the lease after the restart: %v %v", ttl, err)
	}
}

func TestClusterServesTheEtcdAPI(t *testing.T) {
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("put, get and delete", func(t *testing.T) {
		if _, err := c.Put(ctx, "/kv/a", "1"); err != nil {
			t.Fatal(err)
		}
		r, err := c.Get(ctx, "/kv/a")
		if err != nil || len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "1" {
			t.Fatalf("get /kv/a: %v %v", r, err)
		}
		d, err := c.Delete(ctx, "/kv/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		if err != nil || d.Deleted != 1 || len(d.PrevKvs) != 1 {
			t.Fatalf("delete /kv/: %v %v", d, err)
		}
	})

	t.Run("txn", func(t *testing.T) {
		r, err := c.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision("/txn/k"), "=", 0)).
			Then(clientv3.OpPut("/txn/k", "v")).Else(clientv3.OpGet("/txn/k")).Commit()
		if err != nil || !r.Succeeded {
			t.Fatalf("first txn: %v %v", r, err)
		}
		r, err = c.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision("/txn/k"), "=", 0)).
			Then(clientv3.OpPut("/txn/k", "w")).Else(clientv3.OpGet("/txn/k")).Commit()
		if err != nil || r.Succeeded || string(r.Responses[0].GetResponseRange().Kvs[0].Value) != "v" {
			t.Fatalf("second txn: %v %v", r, err)
		}
		if _, err := c.Txn(ctx).Then(clientv3.OpPut("/txn/d", "1"), clientv3.OpPut("/txn/d", "2")).Commit(); err == nil {
			t.Fatal("a txn putting one key twice was taken")
		}
	})

	t.Run("watch from a past revision", func(t *testing.T) {
		p, err := c.Put(ctx, "/w/a", "1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/w/b", "2"); err != nil {
			t.Fatal(err)
		}
		wctx, wcancel := context.WithCancel(ctx)
		defer wcancel()
		ch := c.Watch(wctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(p.Header.Revision))
		if _, err := c.Put(ctx, "/w/c", "3"); err != nil {
			t.Fatal(err)
		}
		var got []string
		for len(got) < 3 {
			select {
			case r := <-ch:
				for _, ev := range r.Events {
					got = append(got, string(ev.Kv.Key))
				}
			case <-ctx.Done():
				t.Fatalf("events %q before the deadline", got)
			}
		}
		if strings.Join(got, " ") != "/w/a /w/b /w/c" {
			t.Fatalf("events %q", got)
		}
	})

	t.Run("leases", func(t *testing.T) {
		g, err := c.Grant(ctx, 60)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/lease/k", "v", clientv3.WithLease(g.ID)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.KeepAliveOnce(ctx, g.ID); err != nil {
			t.Fatal(err)
		}
		ttl, err := c.TimeToLive(ctx, g.ID, clientv3.WithAttachedKeys())
		if err != nil || len(ttl.Keys) != 1 || string(ttl.Keys[0]) != "/lease/k" {
			t.Fatalf("time to live: %v %v", ttl, err)
		}
		if _, err := c.Revoke(ctx, g.ID); err != nil {
			t.Fatal(err)
		}
		r, err := c.Get(ctx, "/lease/k")
		if err != nil || len(r.Kvs) != 0 {
			t.Fatalf("the leased key after the revoke: %v %v", r, err)
		}
		if _, err := c.Put(ctx, "/lease/k", "v", clientv3.WithLease(g.ID)); err == nil {
			t.Fatal("a put to a revoked lease was taken")
		}
	})

	t.Run("large range", func(t *testing.T) {
		v := strings.Repeat("x", 1000)
		for i := range 300 {
			if _, err := c.Put(ctx, fmt.Sprintf("/big/%04d", i), v); err != nil {
				t.Fatal(err)
			}
		}
		r, err := c.Get(ctx, "/big/", clientv3.WithPrefix())
		if err != nil || len(r.Kvs) != 300 {
			t.Fatalf("a 300 KB range: %d keys, %v", len(r.Kvs), err)
		}
	})

	t.Run("compaction", func(t *testing.T) {
		p, err := c.Put(ctx, "/c/k", "1")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Put(ctx, "/c/k", "2"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Compact(ctx, p.Header.Revision+1); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(ctx, "/c/k", clientv3.WithRev(p.Header.Revision)); err == nil {
			t.Fatal("a read before the compacted revision was answered")
		}
	})

	t.Run("version and health over HTTP/1.1", func(t *testing.T) {
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{tc.cert}, RootCAs: tc.ca.Pool, MinVersion: tls.VersionTLS13,
		}}}
		for path, want := range map[string]string{
			"/version": `{"etcdserver":"3.7.2","etcdcluster":"3.7.0","storage":"3.7.0"}`,
			"/health":  `{"health":"true","reason":""}`,
		} {
			resp, err := hc.Get("https://" + tc.endpoints[0] + path)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil || string(b) != want {
				t.Errorf("%s: %q %v, want %q", path, b, err, want)
			}
		}
	})

	t.Run("status and members", func(t *testing.T) {
		m, err := c.MemberList(ctx)
		if err != nil || len(m.Members) != 3 {
			t.Fatalf("member list: %v %v", m, err)
		}
		s, err := c.Status(ctx, c.Endpoints()[0])
		if err != nil || s.Leader == 0 {
			t.Fatalf("status: %v %v", s, err)
		}
	})
}

// A follower further behind than the leader keeps entries for gets the
// leader's checkpoint as a Raft snapshot, and goes on from it (#602).
func TestAFollowerThatLagsCatchesUpByASnapshot(t *testing.T) {
	smallCheckpoints(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// A follower lags: a write a follower forwarded to a leader that
	// stopped is lost and times out, in etcd too.
	st, err := c.Status(ctx, tc.endpoints[0])
	if err != nil {
		t.Fatal(err)
	}
	lag := 2
	if st.Leader == 3 {
		lag = 1
	}
	tc.stopNode(t, lag)
	// Twenty keys written again and again, and the history compacted: the
	// log grows past twice the values a checkpoint holds, and the nodes
	// take checkpoints.
	value := strings.Repeat("v", 1000)
	for i := range 300 {
		r, err := c.Put(ctx, fmt.Sprintf("/lag/%03d", i%20), value+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		if i == 200 {
			if _, err := c.Compact(ctx, r.Header.Revision); err != nil {
				t.Fatal(err)
			}
		}
	}
	tc.startNode(t, lag)
	c3 := tc.clientOf(t, tc.endpoints[lag])
	for {
		r, err := c3.Get(ctx, "/lag/", clientv3.WithPrefix(), clientv3.WithSerializable())
		if err == nil && len(r.Kvs) == 20 && string(r.Kvs[19].Value) == value+"299" {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the follower did not catch up: %v %v", r, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// And it goes on with the writes after.
	if _, err := c.Put(ctx, "/lag/after", "a"); err != nil {
		t.Fatal(err)
	}
	for {
		r, err := c3.Get(ctx, "/lag/after", clientv3.WithSerializable())
		if err == nil && len(r.Kvs) == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the follower took no write after the snapshot: %v %v", r, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

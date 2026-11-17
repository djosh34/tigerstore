//go:build simio

package simos

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/testcert"
	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

// simCluster is three wl nodes on one OS: their files on simfs disks,
// their sockets on a network in memory.
type simCluster struct {
	os      *OS
	nw      *memNet
	disks   []*simfs.Disk
	members []wl.Member
	cert    tls.Certificate
	pool    *x509.CertPool
	cancels []context.CancelFunc
	stopped []chan error
	// fs and hooks, where set, are node i's disk instead of disks[i] and
	// its hooks.
	fs    []wal.FS
	hooks []wl.Hooks
	// budget is each node's memory budget, 0 for none.
	budget int64
	// ckptLog and catchup are each node's CheckpointLog and
	// CatchupEntries, 0 for wl's.
	ckptLog int64
	catchup uint64
}

// testOS is the descriptor table of every cluster of the tests: wl sends
// the calls that name only a descriptor to the first node's (wl.Sys).
var testOS = New()

func newSimCluster(t *testing.T) *simCluster {
	t.Helper()
	ca, err := testcert.NewCA()
	if err != nil {
		t.Fatal(err)
	}
	var hosts []string
	for i := range 3 {
		hosts = append(hosts, fmt.Sprintf("n%d", i+1))
	}
	cert, _, _, err := ca.IssueFor("node", 1, nil, hosts)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	c := &simCluster{os: testOS, nw: newMemNet(), cert: cert, pool: pool}
	for i, h := range hosts {
		c.disks = append(c.disks, simfs.New())
		c.members = append(c.members, wl.Member{ID: uint64(i + 1), Name: h, PeerURLs: []string{"https://" + h + ":2380"}, ClientURLs: []string{"https://" + h + ":2379"}})
	}
	c.cancels, c.stopped = make([]context.CancelFunc, 3), make([]chan error, 3)
	c.fs, c.hooks = make([]wal.FS, 3), make([]wl.Hooks, 3)
	t.Cleanup(func() {
		for i := range c.cancels {
			c.stop(t, i)
		}
	})
	return c
}

func (c *simCluster) tls(server bool) *tls.Config {
	cfg := &tls.Config{Certificates: []tls.Certificate{c.cert}, RootCAs: c.pool, MinVersion: tls.VersionTLS12}
	if server {
		cfg.ClientCAs, cfg.ClientAuth = c.pool, tls.RequireAndVerifyClientCert
	}
	return cfg
}

// start runs node i on its disk.
func (c *simCluster) start(t *testing.T, i int) {
	t.Helper()
	host := c.members[i].Name
	dataDir := "/data/" + host
	ctx, cancel := context.WithCancel(context.Background())
	c.cancels[i], c.stopped[i] = cancel, make(chan error, 1)
	out := io.Discard
	if os.Getenv("SIMOS_TEST_LOG") != "" {
		out = os.Stderr
	}
	cfg := wl.Config{
		Name: host, ID: uint64(i + 1), ClusterID: 0x51, DataDir: dataDir, Members: c.members,
		ClientURLs: c.members[i].ClientURLs, Logger: log.New(out, host+" ", log.Lmicroseconds),
		ClientTLS: c.tls(true), PeerServerTLS: c.tls(true), PeerClientTLS: c.tls(false),
		ClientListeners: []net.Listener{c.nw.listen(host + ":2379")},
		PeerListeners:   []net.Listener{c.nw.listen(host + ":2380")},
		Sys:             c.os.NewNode(dataDir, c.disk(i), c.nw.from(host)),
		Hooks:           c.hooks[i],
		MemoryBudget:    c.budget,
		CheckpointLog:   c.ckptLog,
		CatchupEntries:  c.catchup,
	}
	go func() { c.stopped[i] <- wl.Run(ctx, cfg) }()
}

// disk is node i's disk: its own FS if a test set one, or else its simfs
// disk behind one lock. simfs is not safe for concurrent use, and outside
// gosim wl's goroutines call it at once: the log's helper, the remover
// and the checkpoint writer (the race CI's -race run found on 1c158211).
func (c *simCluster) disk(i int) wal.FS {
	if c.fs[i] == nil {
		c.fs[i] = &lockedDisk{d: c.disks[i]}
	}
	return c.fs[i]
}

func (c *simCluster) stop(t *testing.T, i int) {
	t.Helper()
	if c.cancels[i] == nil {
		return
	}
	c.cancels[i]()
	c.cancels[i] = nil
	if err := <-c.stopped[i]; err != nil {
		t.Errorf("node %d: %v", i+1, err)
	}
}

func (c *simCluster) client(t *testing.T) *clientv3.Client {
	t.Helper()
	return c.clientOf(t, "n1:2379", "n2:2379", "n3:2379")
}

// clientOf is a client of the nodes at endpoints alone.
func (c *simCluster) clientOf(t *testing.T, endpoints ...string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints, TLS: c.tls(false), Logger: zap.NewNop(),
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(c.nw.Dial)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// A cluster of wl nodes runs on the seam: every file on its simfs disk,
// every socket on the simulator's kind of connection, epoll and eventfd
// emulated. It serves writes and reads, and a node stopped and started
// again comes back with what it held, from its disk alone.
func TestAClusterRunsOnTheSeam(t *testing.T) {
	c := newSimCluster(t)
	for i := range 3 {
		c.start(t, i)
	}
	cli := c.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The first put waits for a leader.
	for {
		_, err := cli.Put(ctx, "/k/00", "0")
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("no leader took a put: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for k := range 50 {
		if _, err := cli.Put(ctx, fmt.Sprintf("/k/%02d", k), strconv.Itoa(k)); err != nil {
			t.Fatalf("put %d: %v", k, err)
		}
	}
	c.stop(t, 2)
	c.start(t, 2)
	// Node 3 serves, after its restart, every key it held.
	cli3, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"n3:2379"}, TLS: c.tls(false), Logger: zap.NewNop(),
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(c.nw.Dial)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli3.Close() }()
	for {
		resp, err := cli3.Get(ctx, "/k/", clientv3.WithPrefix(), clientv3.WithSerializable())
		got := 0
		if err == nil {
			got = len(resp.Kvs)
		}
		if got == 50 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("node 3 after its restart: %d keys, %v", got, errors.Join(err, ctx.Err()))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A watch catches up through values read off the loop when one turn's
// catch-up needs more of them than the loop keeps (fetchedMax, a
// sixteenth of the budget), with a page cache that never holds them, as
// one under pressure: here 300 values of history, 9.6 MiB, past the 6 MiB
// a budget of 96 MiB keeps. Trimmed before the watch caught up, the
// values it needed went, it fetched them again, and the trim took them
// again, turn after turn, for good (#689's area-3 review).
func TestAWatchCatchesUpWithAPageCacheThatHoldsNothing(t *testing.T) {
	c := newSimCluster(t)
	c.budget = 96 << 20
	for i := range 3 {
		c.start(t, i)
	}
	cli := c.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		if _, err := cli.Put(ctx, "/leader", "v"); err == nil {
			break
		} else if ctx.Err() != nil {
			t.Fatalf("no leader took a put: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	const keys, size = 300, 32 << 10
	value := func(i, round int) string { return fmt.Sprintf("%04d-%d", i, round) + strings.Repeat("v", size) }
	var first int64
	for round := range 2 {
		for i := range keys {
			r, err := cli.Put(ctx, fmt.Sprintf("/w/%04d", i), value(i, round))
			if err != nil {
				t.Fatal(err)
			}
			if first == 0 {
				first = r.Header.Revision
			}
		}
	}
	// No read that must not wait finds its bytes in the page cache.
	c.os.SetUncached(func() bool { return true })
	t.Cleanup(func() { c.os.SetUncached(nil) })
	cli1, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"n1:2379"}, TLS: c.tls(false), Logger: zap.NewNop(),
		DialOptions: []grpc.DialOption{grpc.WithContextDialer(c.nw.Dial)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli1.Close() }()
	wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
	defer wcancel()
	got := 0
	for r := range cli1.Watch(wctx, "/w/", clientv3.WithPrefix(), clientv3.WithRev(first)) {
		if err := r.Err(); err != nil {
			t.Fatalf("after %d events: %v", got, err)
		}
		for _, ev := range r.Events {
			if want := value(got%keys, got/keys); string(ev.Kv.Value) != want {
				t.Fatalf("event %d holds %d bytes of %s, not its value", got, len(ev.Kv.Value), ev.Kv.Key)
			}
			got++
		}
		if got == 2*keys {
			return
		}
	}
	t.Fatalf("the watch from revision %d caught up %d of %d events in 30 s", first, got, 2*keys)
}

// isolate cuts host off the network, until heal.
func (nw *memNet) isolate(h string) {
	nw.mu.Lock()
	if nw.cut == nil {
		nw.cut = map[string]bool{}
	}
	nw.cut[h] = true
	var closing []hostConn
	k := 0
	for _, c := range nw.conns {
		if c.from == h || c.to == h {
			closing = append(closing, c)
			continue
		}
		nw.conns[k] = c
		k++
	}
	nw.conns = nw.conns[:k]
	nw.mu.Unlock()
	for _, c := range closing {
		_ = c.a.Close()
		_ = c.b.Close()
	}
}

// heal puts host h back on the network.
func (nw *memNet) heal(h string) {
	nw.mu.Lock()
	delete(nw.cut, h)
	nw.mu.Unlock()
}

// from is the network as host h dials it.
func (nw *memNet) from(h string) Net { return hostNet{nw: nw, host: h} }

type hostNet struct {
	nw   *memNet
	host string
}

func (n hostNet) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return n.nw.dial(ctx, n.host, addr)
}

// state is each open connection's bytes in each direction: written in
// all, when the last came, and still unread, for a test that stalled.
func (nw *memNet) state() string {
	nw.mu.Lock()
	conns := slices.Clone(nw.conns)
	nw.mu.Unlock()
	var b strings.Builder
	now := time.Now()
	for _, hc := range conns {
		hc.a.mu.Lock()
		from := hc.from
		if from == "" {
			from = "client"
		}
		fmt.Fprintf(&b, "%s->%s: out %d B, last %v ago, unread %d, closed %v/%v, notify %v | back %d B, last %v ago, unread %d, closed %v/%v, notify %v\n",
			from, hc.a.remote,
			hc.a.out.total, ago(now, hc.a.out.last), len(hc.a.out.buf), hc.a.closed, hc.a.out.closed, hc.b.notify != nil,
			hc.b.out.total, ago(now, hc.b.out.last), len(hc.b.out.buf), hc.b.closed, hc.b.out.closed, hc.a.notify != nil)
		hc.a.mu.Unlock()
	}
	return b.String()
}

func ago(now, t time.Time) time.Duration {
	if t.IsZero() {
		return -1
	}
	return now.Sub(t).Round(time.Millisecond)
}

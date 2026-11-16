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
	"strconv"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/testcert"
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
}

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
	c := &simCluster{os: New(), nw: newMemNet(), cert: cert, pool: pool}
	for i, h := range hosts {
		c.disks = append(c.disks, simfs.New())
		c.members = append(c.members, wl.Member{ID: uint64(i + 1), Name: h, PeerURLs: []string{"https://" + h + ":2380"}, ClientURLs: []string{"https://" + h + ":2379"}})
	}
	c.cancels, c.stopped = make([]context.CancelFunc, 3), make([]chan error, 3)
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
		Sys:             c.os.NewNode(dataDir, c.disks[i], c.nw),
	}
	go func() { c.stopped[i] <- wl.Run(ctx, cfg) }()
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
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"n1:2379", "n2:2379", "n3:2379"}, TLS: c.tls(false), Logger: zap.NewNop(),
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

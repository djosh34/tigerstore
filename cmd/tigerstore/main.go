// Command tigerstore runs one node of a tigerstore cluster. Its flags use
// etcd's names.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
)

type flags struct {
	name, dataDir                             string
	initialCluster, clusterToken              string
	listenPeer, listenClient, advertiseClient string
	certFile, keyFile, caFile                 string
	clientCertAuth                            bool
	peerCertFile, peerKeyFile, peerCAFile     string
	progressInterval                          time.Duration
	memoryBudget                              int64
}

// minProcs is the fewest Go processors tigerstore runs with, even on one
// CPU or with GOMAXPROCS=1. An fsync blocks in a system call that can keep
// its processor until the runtime takes it back, which is not reliably
// fast; with only one, the goroutines that take in requests, send to
// peers and release replies all wait for the disk, and the leader writes
// about one request per fsync. The batch256 pre-work on #47 measured 1.2
// writes per fsync with one processor and 53 with two on the same CPU.
const minProcs = 2

// gcPercent is the GOGC that tigerstore runs with unless GOGC is set.
// Design E keeps values in the log on disk, so the heap is mostly the
// index, whose pointers every collection scans, and at Go's default of
// 100 a node under load collects every few seconds; docs/design.md has
// the measurements. The node's memory budget still caps the heap (run).
const gcPercent = 400

// tick is a node's Raft tick.
const tick = 100 * time.Millisecond

func main() {
	if runtime.GOMAXPROCS(0) < minProcs {
		runtime.GOMAXPROCS(minProcs)
	}
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(gcPercent)
	}
	logger := log.New(os.Stderr, "tigerstore ", log.LstdFlags|log.Lmicroseconds)
	if err := run(os.Args[1:], logger); err != nil {
		logger.Printf("error: %v", err)
		os.Exit(1)
	}
}

func parse(args []string) (*flags, error) {
	f := &flags{}
	fs := flag.NewFlagSet("tigerstore", flag.ContinueOnError)
	fs.StringVar(&f.name, "name", "", "name of this node in --initial-cluster")
	fs.StringVar(&f.dataDir, "data-dir", "", "data directory (default <name>.tigerstore)")
	fs.StringVar(&f.initialCluster, "initial-cluster", "", "all nodes as name=https://host:peerport, comma separated, the same on every node")
	fs.StringVar(&f.clusterToken, "initial-cluster-token", "tigerstore", "token that sets the cluster ID")
	fs.StringVar(&f.listenPeer, "listen-peer-urls", "", "URL to accept peers on (default this node's --initial-cluster URL)")
	fs.StringVar(&f.listenClient, "listen-client-urls", "https://127.0.0.1:2379", "URL to accept clients on")
	fs.StringVar(&f.advertiseClient, "advertise-client-urls", "", "client URL to report in MemberList (default --listen-client-urls)")
	fs.StringVar(&f.certFile, "cert-file", "", "client server certificate")
	fs.StringVar(&f.keyFile, "key-file", "", "client server key")
	fs.StringVar(&f.caFile, "trusted-ca-file", "", "CA for client certificates")
	fs.BoolVar(&f.clientCertAuth, "client-cert-auth", false, "require client certificates signed by --trusted-ca-file")
	fs.StringVar(&f.peerCertFile, "peer-cert-file", "", "peer certificate, used as server and client")
	fs.StringVar(&f.peerKeyFile, "peer-key-file", "", "peer key")
	fs.StringVar(&f.peerCAFile, "peer-trusted-ca-file", "", "CA for peer certificates")
	fs.DurationVar(&f.progressInterval, "watch-progress-notify-interval", 10*time.Minute, "interval of progress notifications to watchers that ask for them")
	fs.Int64Var(&f.memoryBudget, "memory-budget-bytes", 0, "memory this node may use (default GOMEMLIMIT, else the cgroup's memory limit, else the host's memory)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if f.name == "" || f.initialCluster == "" {
		return nil, errors.New("--name and --initial-cluster are required")
	}
	if f.dataDir == "" {
		f.dataDir = f.name + ".tigerstore"
	}
	if f.advertiseClient == "" {
		f.advertiseClient = f.listenClient
	}
	// etcd takes comma-separated lists in these flags; tigerstore serves
	// one URL each for now.
	for _, fl := range []struct{ name, v string }{
		{"listen-client-urls", f.listenClient}, {"advertise-client-urls", f.advertiseClient}, {"listen-peer-urls", f.listenPeer},
	} {
		if strings.Contains(fl.v, ",") {
			return nil, fmt.Errorf("--%s takes one URL; lists are not supported yet: %q", fl.name, fl.v)
		}
	}
	return f, nil
}

// member is one entry of --initial-cluster. Raft IDs are positions in the
// list, starting at 1, so every node must get the same list.
type member struct {
	id   uint64
	name string
	url  *url.URL
}

func parseCluster(s string) ([]member, error) {
	var out []member
	for i, part := range strings.Split(s, ",") {
		name, raw, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("--initial-cluster entry %q is not name=url", part)
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		if u.Scheme != "https" {
			return nil, fmt.Errorf("peer URL %s must be https: node-to-node traffic is always encrypted", raw)
		}
		out = append(out, member{id: uint64(i + 1), name: name, url: u})
	}
	return out, nil
}

func loadCA(file string) (*x509.CertPool, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("no certificates in %s", file)
	}
	return pool, nil
}

// clientTLS builds the client-side TLS config, or nil for http.
func clientTLS(f *flags, u *url.URL) (*tls.Config, error) {
	switch u.Scheme {
	case "http":
		return nil, nil
	case "https":
	default:
		return nil, fmt.Errorf("client URL %s must be http or https", u)
	}
	cert, err := tls.LoadX509KeyPair(f.certFile, f.keyFile)
	if err != nil {
		return nil, fmt.Errorf("client certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if f.clientCertAuth {
		if cfg.ClientCAs, err = loadCA(f.caFile); err != nil {
			return nil, err
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func run(args []string, logger *log.Logger) error {
	f, err := parse(args)
	if err != nil {
		return err
	}
	members, err := parseCluster(f.initialCluster)
	if err != nil {
		return err
	}
	var self *member
	var ids []uint64
	peers := map[uint64]string{}
	for i := range members {
		m := &members[i]
		ids = append(ids, m.id)
		if m.name == f.name {
			self = m
			continue
		}
		peers[m.id] = m.url.Host
	}
	if self == nil {
		return fmt.Errorf("--name %q is not in --initial-cluster", f.name)
	}
	listenPeer := self.url
	if f.listenPeer != "" {
		if listenPeer, err = url.Parse(f.listenPeer); err != nil {
			return err
		}
	}
	peerCert, err := tls.LoadX509KeyPair(f.peerCertFile, f.peerKeyFile)
	if err != nil {
		return fmt.Errorf("peer certificate (node-to-node traffic is always encrypted): %w", err)
	}
	peerCA, err := loadCA(f.peerCAFile)
	if err != nil {
		return fmt.Errorf("peer CA: %w", err)
	}
	clientURL, err := url.Parse(f.listenClient)
	if err != nil {
		return err
	}
	ctls, err := clientTLS(f, clientURL)
	if err != nil {
		return err
	}

	h := fnv.New64a()
	h.Write([]byte(f.clusterToken + "/" + f.initialCluster))
	ccfg := core.DefaultConfig(self.id, ids)
	ccfg.ClusterID = h.Sum64()
	// As in etcd, an interval of 0 or less keeps the default.
	if f.progressInterval > 0 {
		ccfg.ProgressTicks = max(1, int(f.progressInterval/tick))
	}
	budget, from, err := node.MemoryBudget(os.DirFS("/"), f.memoryBudget)
	if err != nil {
		return err
	}
	// The garbage collector works harder as the heap nears the budget;
	// the value cache takes what the rest leaves.
	debug.SetMemoryLimit(budget)
	ccfg.MemoryBytes = node.CacheMemory(budget)
	logger.Printf("memory budget %d bytes (from %s), %d of them for the index, the entries in memory and the value cache", budget, from, ccfg.MemoryBytes)
	n, err := node.New(node.Config{Core: ccfg, DataDir: f.dataDir, Tick: tick, Logf: logger.Printf})
	if err != nil {
		return err
	}
	t, err := peer.Start(peer.Config{
		ID: self.id, Listen: listenPeer.Host, Peers: peers,
		Certificate: peerCert, CA: peerCA,
		Deliver: n.Deliver, Unreachable: n.Unreachable, Serve: n.Serve, Logf: logger.Printf,
	})
	if err != nil {
		return err
	}

	var sm []server.Member
	for _, m := range members {
		sm = append(sm, server.Member{ID: m.id, Name: m.name, PeerURLs: []string{m.url.String()}})
		if m.id == self.id {
			sm[len(sm)-1].ClientURLs = []string{f.advertiseClient}
		}
	}
	g := server.New(n, server.Config{ClusterID: ccfg.ClusterID, Members: sm, TLS: ctls})
	ln, err := net.Listen("tcp", clientURL.Host)
	if err != nil {
		return errors.Join(err, t.Close())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	served := make(chan error, 1)
	go func() {
		// A node that cannot serve clients stops instead of staying a
		// silent Raft member.
		served <- g.Serve(ln)
		cancel()
	}()
	logger.Printf("node %s (raft ID %d) serving clients on %s, peers on %s", f.name, self.id, clientURL, listenPeer.Host)
	runErr := n.Run(ctx, t)
	stopErr := g.Stop()
	return errors.Join(runErr, stopErr, <-served, t.Close())
}

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
	peerAllowedCNs, peerAllowedHostnames      []string
	progressInterval                          time.Duration
	quotaBackendBytes                         int64
	corruptCheck, compactHashCheck            time.Duration
	features                                  featureGates
	diskBudget                                int64
	memoryBudget                              int64
	clusterState                              string
	maxLearners                               int
	strictReconfig                            bool
	authToken                                 string
	jwt                                       *server.JWT
	bcryptCost, authTokenTTL                  uint
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
	args := os.Args[1:]
	run := run
	if len(args) > 0 && args[0] == "snapshot" {
		run, args = snapshotCommand, args[1:]
	}
	if err := run(args, logger); err != nil {
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
	fs.StringVar(&f.clusterState, "initial-cluster-state", "new", "new for a node of a new cluster, existing for one that joins a running cluster after MemberAdd")
	fs.String("initial-advertise-peer-urls", "", "accepted for etcd's sake; a node's peer URL is its entry in --initial-cluster")
	fs.StringVar(&f.authToken, "auth-token", "simple", "auth token type: simple, or jwt,pub-key=<file>,priv-key=<file>,sign-method=<alg>,ttl=<duration>")
	fs.UintVar(&f.bcryptCost, "bcrypt-cost", 10, "bcrypt cost factor of hashed passwords")
	fs.UintVar(&f.authTokenTTL, "auth-token-ttl", 300, "seconds an unused simple token lives")
	fs.IntVar(&f.maxLearners, "max-learners", 1, "the most learners the cluster may have")
	fs.BoolVar(&f.strictReconfig, "strict-reconfig-check", true, "refuse member changes that would leave fewer started voters than a quorum")
	f.features = newFeatureGates()
	fs.Var(f.features, "feature-gates", "comma-separated etcd server feature gates, such as InitialCorruptCheck=true,CompactHashCheck=true")
	fs.DurationVar(&f.corruptCheck, "corrupt-check-time", 0, "interval of the leader's corruption checks of every member; 0 turns them off")
	fs.DurationVar(&f.compactHashCheck, "compact-hash-check-time", time.Minute, "interval of the leader's checks of the hashes compactions took, with the CompactHashCheck feature gate")
	fs.Int64Var(&f.quotaBackendBytes, "quota-backend-bytes", 0, "raise the NOSPACE alarm when the data reaches this size; 0 means 2 GiB, a negative value no quota")
	// As in etcd, each takes a comma-separated list, and a repeated flag
	// replaces the list.
	fs.Func("peer-cert-allowed-cn", "comma-separated CommonNames of the peer certificates to accept", func(s string) error {
		f.peerAllowedCNs = strings.Split(s, ",")
		return nil
	})
	fs.Func("peer-cert-allowed-hostname", "comma-separated SAN hostnames of the peer certificates to accept", func(s string) error {
		f.peerAllowedHostnames = strings.Split(s, ",")
		return nil
	})
	fs.DurationVar(&f.progressInterval, "watch-progress-notify-interval", 10*time.Minute, "interval of progress notifications to watchers that ask for them")
	fs.Int64Var(&f.diskBudget, "disk-budget-bytes", 0, "bytes of the log's files at which writes get NOSPACE; cleaning keeps them below. Leave room above it for 64 MiB of writes in flight, a 64 MiB pack and a checkpoint; below two segments (128 MiB) plus the checkpoints and live values, writes stay at NOSPACE (0 means no budget)")
	fs.Int64Var(&f.memoryBudget, "memory-budget-bytes", 0, "memory this node may use (default GOMEMLIMIT, else the cgroup's memory limit, else the host's memory)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	var err error
	if f.jwt, err = server.ParseAuthToken(f.authToken); err != nil {
		return nil, fmt.Errorf("--auth-token %q: %w", f.authToken, err)
	}
	if f.clusterState != "new" && f.clusterState != "existing" {
		return nil, fmt.Errorf("--initial-cluster-state must be new or existing, not %q", f.clusterState)
	}
	if f.compactHashCheck <= 0 {
		return nil, fmt.Errorf("--compact-hash-check-time must be >0 (set to %v)", f.compactHashCheck)
	}
	if f.name == "" || f.initialCluster == "" {
		return nil, errors.New("--name and --initial-cluster are required")
	}
	if len(f.peerAllowedCNs) > 0 && len(f.peerAllowedHostnames) > 0 {
		return nil, errors.New("--peer-cert-allowed-cn and --peer-cert-allowed-hostname cannot both be set")
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
	for i := range members {
		if members[i].name == f.name {
			self = &members[i]
		}
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

	pcfg := peer.Config{
		Listen: listenPeer.Host, Certificate: peerCert, CA: peerCA,
		AllowedCNs: f.peerAllowedCNs, AllowedHostnames: f.peerAllowedHostnames, Logf: logger.Printf,
	}
	ident, err := loadIdentity(f, members, self, pcfg)
	if err != nil {
		return err
	}
	ccfg := core.DefaultConfig(ident.id, nil)
	ccfg.ClusterID = ident.cluster
	if f.clusterState == "new" {
		ccfg.Members = initialMembers(members)
		for _, m := range ccfg.Members {
			ccfg.Peers = append(ccfg.Peers, m.ID)
		}
	} else {
		ccfg.Join = true
	}
	ccfg.Name, ccfg.ClientURLs = f.name, []string{f.advertiseClient}
	ccfg.MaxLearners, ccfg.StrictReconfigCheck = f.maxLearners, f.strictReconfig
	ccfg.AuthTokenTicks = int(f.authTokenTTL) * int(time.Second/tick)
	ccfg.QuotaBytes = f.quotaBackendBytes
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
	ncfg := node.Config{Core: ccfg, DataDir: f.dataDir, Tick: tick, DiskBudget: f.diskBudget, CorruptCheckTime: f.corruptCheck, Logf: logger.Printf}
	if f.features["CompactHashCheck"] {
		ncfg.CompactHashCheckTime = f.compactHashCheck
	}
	n, err := node.New(ncfg)
	if err != nil {
		return err
	}
	// The node hands the transport the members once it runs; until then,
	// those of --initial-cluster.
	pcfg.ID, pcfg.Peers = ident.id, node.PeerAddrs(initialMembers(members))
	pcfg.Deliver, pcfg.Unreachable, pcfg.Serve, pcfg.Answer, pcfg.Removed = n.Deliver, n.Unreachable, n.Serve, n.Answer, n.Removed
	t, err := peer.Start(pcfg)
	if err != nil {
		return err
	}
	g := server.New(n, server.Config{ClusterID: ccfg.ClusterID, TLS: ctls, PeerVersions: t.Versions, BcryptCost: int(f.bcryptCost), JWT: f.jwt})
	ln, err := net.Listen("tcp", clientURL.Host)
	if err != nil {
		return errors.Join(err, t.Close())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ran := make(chan error, 1)
	go func() {
		ran <- n.Run(ctx, t)
		cancel()
	}()
	if f.features["InitialCorruptCheck"] {
		// As in etcd, before it serves any client.
		if err := n.InitialCheck(ctx, t); err != nil {
			cancel()
			return errors.Join(err, <-ran, ln.Close(), t.Close())
		}
		logger.Printf("initial corruption check passed")
	}
	served := make(chan error, 1)
	go func() {
		// A node that cannot serve clients stops instead of staying a
		// silent Raft member.
		served <- g.Serve(ln)
		cancel()
	}()
	logger.Printf("node %s (raft ID %d) serving clients on %s, peers on %s", f.name, ident.id, clientURL, listenPeer.Host)
	runErr := <-ran
	stopErr := g.Stop()
	return errors.Join(runErr, stopErr, <-served, t.Close())
}

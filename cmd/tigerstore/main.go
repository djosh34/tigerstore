// Command tigerstore runs one node of a tigerstore cluster. Its command
// line is etcd v3.7.2's (config.go).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"slices"
	"syscall"
	"time"

	"go.etcd.io/etcd/pkg/v3/featuregate"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdmain"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/features"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/server"
)

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
// GOGC 100 cost 1.6% to 6.4% of max throughput and put its Put p99 at
// half load from 2.6 to 36 ms (#157).
const gcPercent = 400

func main() {
	if runtime.GOMAXPROCS(0) < minProcs {
		runtime.GOMAXPROCS(minProcs)
	}
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(gcPercent)
	}
	args := os.Args[1:]
	if len(args) > 0 && (args[0] == "grpc-proxy" || args[0] == "gateway") {
		// etcd's own commands, with etcd's flags: a gRPC proxy in front
		// of the cluster's client URLs (--namespace among them), and a
		// TCP gateway to them.
		etcdmain.Main(os.Args)
		return
	}
	if len(args) > 0 && args[0] == "snapshot" {
		logger := log.New(os.Stderr, "tigerstore ", log.LstdFlags|log.Lmicroseconds)
		if err := snapshotCommand(args[1:], logger); err != nil {
			logger.Printf("error: %v", err)
			os.Exit(1)
		}
		return
	}
	os.Exit(run(args))
}

// run runs a node with the command line args and returns the exit code:
// 0 after --help or --version or a clean stop, 2 for flags that do not
// parse, 1 for any other error, as etcd's.
func run(args []string) int {
	c := newConfig(os.Stderr)
	err := c.parse(args, os.Stdout)
	switch {
	case errors.Is(err, errHelp):
		return 0
	case errors.Is(err, errUsage):
		return 2
	}
	lg := c.ec.GetLogger()
	if lg == nil {
		var lerr error
		if lg, lerr = zap.NewProduction(); lerr != nil {
			fmt.Fprintf(os.Stderr, "error creating zap logger %v\n", lerr)
			return 1
		}
	}
	lg.Info("Running: ", zap.Strings("args", os.Args))
	if err != nil {
		lg.Warn("failed to verify flags", zap.Error(err))
		return 1
	}
	c.ec.SetupGlobalLoggers()
	if err := start(c, lg); err != nil {
		lg.Error("tigerstore stopped", zap.Error(err))
		return 1
	}
	return 0
}

// start runs the node until a signal or an error stops it.
func start(c *config, lg *zap.Logger) error {
	ec := &c.ec
	logf := lg.Sugar().Infof
	if host, err := ec.UpdateDefaultClusterFromName(c.defaultCluster); host != "" || err != nil {
		lg.Info("detected default host for advertise", zap.String("host", host), zap.Error(err))
	}
	if ec.Dir == "" {
		ec.Dir = ec.Name + ".etcd"
		lg.Warn("'data-dir' was empty; using default", zap.String("data-dir", ec.Dir))
	}
	if ec.MemoryMlock {
		// etcd locks its database's pages in memory; tigerstore's index
		// and buffers are in its heap.
		if err := unix.Mlockall(unix.MCL_CURRENT | unix.MCL_FUTURE); err != nil {
			return fmt.Errorf("--memory-mlock: %w", err)
		}
	}
	ident, started, err := readIdentity(ec.Dir)
	if err != nil {
		return err
	}
	var cluster clusterSpec
	members := ident.members
	if !started {
		if cluster, err = resolveCluster(ec, lg); err != nil {
			return err
		}
		members = initialMembers(cluster.members)
	}
	if err := checkPeerURLs(ec, members); err != nil {
		return err
	}
	if err := ec.ClientSelfCert(); err != nil {
		return err
	}
	// etcd ignores --peer-auto-tls when certificates are given.
	selfSigned := ec.PeerAutoTLS && ec.PeerTLSInfo.Empty()
	if err := ec.PeerSelfCert(); err != nil {
		return err
	}
	pcfg, err := peerConfig(ec, selfSigned)
	if err != nil {
		return err
	}
	pcfg.Logf = logf
	ctls, err := clientTLS(ec)
	if err != nil {
		return err
	}
	clientLns, err := clientListeners(ec, ctls != nil, lg)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			closeListeners(clientLns, pcfg.Listeners, lg)
		}
	}()
	if !started {
		if ident, err = newIdentity(ec, cluster, pcfg); err != nil {
			return err
		}
	}
	ccfg := coreConfig(ec, ident)
	if w := c.own.UnsafeWindow; w > 0 {
		ccfg.WindowBytes, ccfg.WindowMaxBytes = w, 2*w
	}
	if b := c.own.UnsafeCheckpoint; b > 0 {
		ccfg.CheckpointBytes = b
	}
	budget, from, err := node.MemoryBudget(os.DirFS("/"), c.own.MemoryBudget)
	if err != nil {
		return err
	}
	// The garbage collector works harder as the heap nears the Go limit;
	// the value cache takes what the rest of the process leaves.
	limit := node.GoMemory(budget, from)
	debug.SetMemoryLimit(limit)
	ccfg.MemoryBytes = node.ProcessMemory(limit)
	logf("memory budget %d bytes (from %s), Go memory limit %d, %d of them for the whole process with its value cache", budget, from, limit, ccfg.MemoryBytes)
	compaction, err := node.ParseCompaction(ec.AutoCompactionMode, ec.AutoCompactionRetention)
	if err != nil {
		return err
	}
	gate := ec.ServerFeatureGate
	ncfg := node.Config{
		Core: ccfg, DataDir: ec.Dir, WalDir: ec.WalDir, Tick: time.Duration(ec.TickMs) * time.Millisecond,
		DiskBudget: c.own.DiskBudget, SegmentBytes: c.own.UnsafeSegment, CorruptCheckTime: ec.CorruptCheckTime, Logf: logf, AutoCompaction: compaction,
		UnsafeNoFsync: ec.UnsafeNoFsync, WarningApplyDuration: ec.WarningApplyDuration, ForceNewCluster: ec.ForceNewCluster,
	}
	if gate.Enabled(features.CompactHashCheck) {
		ncfg.CompactHashCheckTime = ec.CompactHashCheckTime
	}
	if ncfg.Metrics, err = metrics.New(featureMetrics(gate)); err != nil {
		return err
	}
	pcfg.Metrics = ncfg.Metrics
	jwt, err := server.ParseAuthToken(ec.AuthToken)
	if err != nil {
		return fmt.Errorf("--auth-token %q: %w", ec.AuthToken, err)
	}
	var tp *sdktrace.TracerProvider
	if ec.EnableDistributedTracing {
		if tp, err = newTracerProvider(context.Background(), ec); err != nil {
			return err
		}
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				lg.Warn("stop distributed tracing", zap.Error(err))
			}
		}()
	}
	n, err := node.New(ncfg)
	if err != nil {
		return err
	}
	// The node hands the transport the members once it runs; until then,
	// those it started with.
	pcfg.ID, pcfg.Peers = ident.id, node.PeerAddrs(ident.members)
	if len(ec.AdvertisePeerUrls) > 0 {
		pcfg.Addr = ec.AdvertisePeerUrls[0].Host
	}
	pcfg.Deliver, pcfg.Unreachable, pcfg.Serve, pcfg.Answer, pcfg.Removed = n.Deliver, n.Unreachable, n.Serve, n.Answer, n.Removed
	t, err := peer.Start(pcfg)
	if err != nil {
		return err
	}
	// etcd hands grpc-go each keepalive setting only when it is above 0.
	var kaMin, kaTime, kaTimeout time.Duration
	if ec.GRPCKeepAliveMinTime > 0 {
		kaMin = ec.GRPCKeepAliveMinTime
	}
	if ec.GRPCKeepAliveInterval > 0 && ec.GRPCKeepAliveTimeout > 0 {
		kaTime, kaTimeout = ec.GRPCKeepAliveInterval, ec.GRPCKeepAliveTimeout
	}
	g, err := server.New(n, server.Config{
		ClusterID: ccfg.ClusterID, TLS: ctls, PeerVersions: t.Versions, BcryptCost: int(ec.BcryptCost), JWT: jwt,
		StopServiceOnDefrag: gate.Enabled(features.StopGRPCServiceOnDefrag),
		NoGateway:           !ec.EnableGRPCGateway, CORS: ec.CORS, HostWhitelist: ec.HostWhitelist,
		ExtensiveMetrics: ec.Metrics == "extensive", Pprof: ec.EnablePprof,
		TracerProvider:  tracerProvider(tp),
		MaxRequestBytes: int(ec.MaxRequestBytes), MaxConcurrentStreams: ec.MaxConcurrentStreams,
		WarningUnaryRequestDuration: ec.WarningUnaryRequestDuration, Logf: logf,
		KeepaliveMinTime: kaMin, KeepaliveTime: kaTime, KeepaliveTimeout: kaTimeout,
	})
	if err != nil {
		return errors.Join(err, t.Close())
	}
	ok = true
	msrvs, err := serveMetricsURLs(ec, ctls, g.MetricsHandler(), lg)
	if err != nil {
		closeListeners(clientLns, nil, lg)
		return errors.Join(err, g.Stop(), t.Close())
	}
	defer func() {
		for _, srv := range msrvs {
			if err := srv.Close(); err != nil {
				lg.Warn("close the metrics listener", zap.String("address", srv.Addr), zap.Error(err))
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ran := make(chan error, 1)
	go func() {
		ran <- n.Run(ctx, t)
		cancel()
	}()
	go func() {
		if err := n.AdjustTicks(ctx, t.ActivePeers, ec.InitialElectionTickAdvance, rafthttp.ConnReadTimeout); err != nil && ctx.Err() == nil {
			lg.Warn("adjust election ticks", zap.Error(err))
		}
	}()
	if gate.Enabled(features.InitialCorruptCheck) {
		// As in etcd, before it serves any client.
		if err := n.InitialCheck(ctx, t); err != nil {
			cancel()
			closeListeners(clientLns, nil, lg)
			return errors.Join(err, <-ran, g.Stop(), t.Close())
		}
		logf("initial corruption check passed")
	}
	if err := bootstrapDefrag(ctx, n, ec.BootstrapDefragThresholdMegabytes, lg); err != nil {
		cancel()
		closeListeners(clientLns, nil, lg)
		return errors.Join(err, <-ran, g.Stop(), t.Close())
	}
	served := make(chan error, len(clientLns))
	for _, l := range clientLns {
		go func() {
			// A node that cannot serve clients stops instead of staying
			// a silent Raft member.
			served <- g.ServeListener(l.Listener)
			cancel()
		}()
		lg.Info("serving client traffic", zap.String("address", l.url.String()), zap.String("traffic", trafficName(l.Traffic)))
	}
	logf("node %s (raft ID %d) serving clients, peers on %v", ec.Name, ident.id, ec.ListenPeerUrls)
	runErr := <-ran
	stopErr := g.Stop()
	for range clientLns {
		stopErr = errors.Join(stopErr, <-served)
	}
	return errors.Join(runErr, stopErr, t.Close())
}

// featureMetrics are the feature gates for etcd_server_feature_enabled,
// AllAlpha and AllBeta among them.
func featureMetrics(gate featuregate.FeatureGate) []metrics.Feature {
	specs := map[featuregate.Feature]featuregate.FeatureSpec{
		"AllAlpha": {PreRelease: featuregate.Alpha},
		"AllBeta":  {PreRelease: featuregate.Beta},
	}
	maps.Copy(specs, features.DefaultEtcdServerFeatureGates)
	var out []metrics.Feature
	for name, spec := range specs {
		out = append(out, metrics.Feature{Name: string(name), Stage: string(spec.PreRelease), On: gate.Enabled(name)})
	}
	return out
}

// bootstrapDefrag is etcd's --bootstrap-defrag-threshold-megabytes: as it
// starts, a node defragments when at least that many megabytes of its
// files are free to reclaim.
func bootstrapDefrag(ctx context.Context, n *node.Node, thresholdMB uint, lg *zap.Logger) error {
	if thresholdMB == 0 {
		return nil
	}
	m, err := n.Metrics(ctx)
	if err != nil {
		return err
	}
	threshold := int64(thresholdMB) * 1024 * 1024
	if free := m.DbSize - m.DbSizeInUse; free < threshold {
		lg.Info("skipping defragmentation", zap.Int64("current-db-size-bytes", m.DbSize), zap.Int64("current-db-size-in-use-bytes", m.DbSizeInUse), zap.Int64("defrag-threshold-bytes", threshold))
		return nil
	}
	return n.Defragment(ctx)
}

// coreConfig is the core's configuration from etcd's.
func coreConfig(ec *embed.Config, ident identity) core.Config {
	ccfg := core.DefaultConfig(ident.id, nil)
	ccfg.ClusterID = ident.cluster
	if ec.IsNewCluster() {
		ccfg.Members = ident.members
		for _, m := range ccfg.Members {
			ccfg.Peers = append(ccfg.Peers, m.ID)
		}
	} else {
		ccfg.Join = true
	}
	ccfg.TickMillis = int64(ec.TickMs)
	ccfg.ElectionTicks = ec.ElectionTicks()
	ccfg.NoPreVote = !ec.PreVote
	// etcd's request timeout: 5 s plus two election timeouts.
	ccfg.RequestTicks = int((5000 + 2*int64(ec.ElectionMs)) / int64(ec.TickMs))
	ccfg.Name = ec.Name
	for _, u := range ec.AdvertiseClientUrls {
		ccfg.ClientURLs = append(ccfg.ClientURLs, u.String())
	}
	ccfg.MaxLearners, ccfg.StrictReconfigCheck = ec.MaxLearners, ec.StrictReconfigCheck
	ccfg.MaxRequestBytes, ccfg.MaxTxnOps = int(ec.MaxRequestBytes), int(ec.MaxTxnOps)
	tick := time.Duration(ec.TickMs) * time.Millisecond
	ccfg.AuthTokenTicks = int(time.Duration(ec.AuthTokenTTL) * time.Second / tick)
	ccfg.QuotaBytes = ec.QuotaBackendBytes
	if ec.ServerFeatureGate.Enabled(features.LeaseCheckpoint) {
		// etcd v3.7.2 checkpoints every 5 minutes; no flag changes it.
		ccfg.LeaseCheckpointTicks = int(5 * time.Minute / tick)
	}
	// As in etcd, an interval of 0 or less keeps the default.
	if ec.WatchProgressNotifyInterval > 0 {
		ccfg.ProgressTicks = max(1, int(ec.WatchProgressNotifyInterval/tick))
	} else {
		ccfg.ProgressTicks = int(10 * time.Minute / tick)
	}
	return ccfg
}

// closeListeners closes listeners that no server took.
func closeListeners(client []clientListener, peers []net.Listener, lg *zap.Logger) {
	lns := slices.Clone(peers)
	for _, l := range client {
		lns = append(lns, l)
	}
	if err := closeAll(lns); err != nil {
		lg.Warn("close the listeners", zap.Error(err))
	}
}

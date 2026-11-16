// Command tigerstore runs one node of a tigerstore cluster. Its command
// line is etcd v3.7.2's (config.go).
package main

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"slices"

	"go.etcd.io/etcd/pkg/v3/featuregate"
	"go.etcd.io/etcd/server/v3/etcdmain"
	"go.etcd.io/etcd/server/v3/features"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/djosh34/tigerstore/internal/logq"
	"github.com/djosh34/tigerstore/internal/metrics"
	"github.com/djosh34/tigerstore/internal/node"
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
	logf, stopLog := nodeLog(lg)
	defer stopLog()
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
	budget, from, err := node.MemoryBudget(os.DirFS("/"), c.own.MemoryBudget)
	if err != nil {
		return err
	}
	// The loop owns the listeners from here on.
	ok = true
	logf("memory budget %d bytes (from %s)", budget, from)
	return runWL(ec, ident, pcfg, ctls, clientLns, budget, lg)
}

// nodeLog returns what the node, its peers and its server log with, and
// how to stop it; until then the standard log package's lines, as x/net's
// HTTP/2 debug output, go the same way. Neither waits for the log's
// output, which may sit on the disk the node writes to: the node's
// committer logs as it works, and a committer that waits through a disk
// stall sends no heartbeats (#680).
func nodeLog(lg *zap.Logger) (func(format string, args ...any), func()) {
	q := logq.New(lg, logq.Size)
	flags, out := log.Flags(), log.Writer()
	log.SetFlags(0)
	log.SetOutput(q)
	return q.Infof, func() {
		log.SetOutput(out)
		log.SetFlags(flags)
		q.Stop()
	}
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

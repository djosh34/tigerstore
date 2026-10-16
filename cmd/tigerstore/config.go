package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"

	"go.etcd.io/etcd/pkg/v3/flags"
	cconfig "go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.uber.org/zap"
	"sigs.k8s.io/yaml"

	"github.com/djosh34/tigerstore/internal/core"
)

// The command line is etcd v3.7.2's (server/etcdmain/config.go): every
// flag of embed.Config with its default, the same names in ETCD_*
// environment variables for the flags not given, or a YAML
// --config-file instead of both, checked by embed.Config.Validate. On
// top come tigerstore's own flags, which a configuration file may set
// too.

// ignoredFlags are the flags etcd accepts and ignores, from etcd v2.
var ignoredFlags = []string{
	"cluster-active-size", "cluster-remove-delay", "cluster-sync-interval", "config", "force",
	"max-result-buffer", "max-retry-attempts", "peer-heartbeat-interval", "peer-election-timeout",
	"retry-interval", "snapshot", "v", "vv", "test.coverprofile", "test.outputdir",
}

// deprecatedFlags are etcd's flags that it warns about when set.
var deprecatedFlags = map[string]string{
	"max-snapshots":  "--max-snapshots is deprecated in 3.6 and will be decommissioned in 3.8.",
	"v2-deprecation": "--v2-deprecation is deprecated and scheduled for removal in v3.8. The default value is enforced, ignoring user input.",
}

// errHelp and errUsage end the command after --help, which exits 0, and a
// flag that does not parse, which exits 2, as in etcd.
var (
	errHelp  = errors.New("help requested")
	errUsage = errors.New("bad usage")
)

// config is a command line's configuration.
type config struct {
	ec           embed.Config
	fs           *flag.FlagSet
	configFile   string
	printVersion bool
	clusterState *flags.SelectiveStringValue
	fallback     *flags.SelectiveStringValue
	v2deprecated *flags.SelectiveStringsValue
	// defaultCluster is --initial-cluster's default, which etcd replaces
	// when only --name is set.
	defaultCluster string
	// own are tigerstore's flags.
	own ownFlags
}

// ownFlags are tigerstore's flags, also the keys of a configuration
// file.
type ownFlags struct {
	DiskBudget   int64 `json:"disk-budget-bytes"`
	MemoryBudget int64 `json:"memory-budget-bytes"`
	// The unsafe sizes are for tests that need a follower behind the
	// window, checkpoints and removed segments with little data, as
	// real-process chaos does.
	UnsafeWindow     int64 `json:"unsafe-window-bytes"`
	UnsafeCheckpoint int64 `json:"unsafe-checkpoint-bytes"`
	UnsafeSegment    int64 `json:"unsafe-segment-bytes"`
}

func newConfig(stderr io.Writer) *config {
	c := &config{
		ec:           *embed.NewConfig(),
		fs:           flag.NewFlagSet("tigerstore", flag.ContinueOnError),
		clusterState: flags.NewSelectiveStringValue(embed.ClusterStateFlagNew, embed.ClusterStateFlagExisting),
		fallback:     flags.NewSelectiveStringValue("exit", "proxy"),
		v2deprecated: flags.NewSelectiveStringsValue(string(cconfig.V2Depr1WriteOnly), string(cconfig.V2Depr1WriteOnlySkipCheck), string(cconfig.V2Depr1WriteOnlyDrop), string(cconfig.V2Depr2Gone)),
	}
	fs := c.fs
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprintln(stderr, usage) } //nolint:errcheck // usage goes to the terminal
	c.ec.AddFlags(fs)
	c.defaultCluster = c.ec.InitialCluster
	fs.StringVar(&c.configFile, "config-file", "", "Path to the server configuration file. Note that if a configuration file is provided, other command line flags and environment variables will be ignored.")
	fs.Var(c.fallback, "discovery-fallback", fmt.Sprintf("Valid values include %q", c.fallback.Valids()))
	fs.Var(c.clusterState, "initial-cluster-state", "Initial cluster state ('new' when bootstrapping a new cluster or 'existing' when adding new members to an existing cluster). After successful initialization (bootstrapping or adding), flag is ignored on restarts.")
	fs.Var(c.v2deprecated, "v2-deprecation", fmt.Sprintf("v2store deprecation stage: %q. Deprecated and scheduled for removal in v3.8. The default value is enforced, ignoring user input.", c.v2deprecated.Valids()))
	fs.BoolVar(&c.printVersion, "version", false, "Print the version and exit.")
	for _, name := range ignoredFlags {
		fs.Var(&flags.IgnoredFlag{Name: name}, name, "")
	}
	fs.Int64Var(&c.own.DiskBudget, "disk-budget-bytes", 0, "bytes of the log's files at which writes get NOSPACE; cleaning keeps them below. Leave room above it for 64 MiB of writes in flight, a 64 MiB pack and a checkpoint; below two segments (128 MiB) plus the checkpoints and live values, writes stay at NOSPACE (0 means no budget)")
	fs.Int64Var(&c.own.MemoryBudget, "memory-budget-bytes", 0, "memory this node may use (default GOMEMLIMIT, else the cgroup's memory limit, else the host's memory)")
	fs.Int64Var(&c.own.UnsafeWindow, "unsafe-window-bytes", 0, "for tests only: bytes of entries kept in memory for followers, twice that while one still needs them (0 means 64 MiB)")
	fs.Int64Var(&c.own.UnsafeCheckpoint, "unsafe-checkpoint-bytes", 0, "for tests only: bytes of log after which the node cuts a checkpoint (0 means 256 MiB)")
	fs.Int64Var(&c.own.UnsafeSegment, "unsafe-segment-bytes", 0, "for tests only: bytes after which the log starts a new segment file (0 means 64 MiB)")
	return c
}

const usage = `Usage:

  tigerstore [flags]
    Start a tigerstore member, with etcd v3.7.2's flags.

  tigerstore --version
    Show the version of tigerstore.

  tigerstore -h | --help
    Show the help information about tigerstore.

  tigerstore snapshot restore <file> [options]
    Restore a member's data directory from an etcd snapshot file.

  tigerstore grpc-proxy start [options]
    Start etcd v3.7.2's gRPC proxy in front of the cluster.

  tigerstore gateway start [options]
    Start etcd v3.7.2's TCP gateway to the cluster.`

// parse parses the command line, the environment or the configuration
// file, as etcd's config.parse does, and validates the result. With
// --version it writes the version to stdout and returns errHelp.
func (c *config) parse(args []string, stdout io.Writer) error {
	switch err := c.fs.Parse(args); {
	case errors.Is(err, flag.ErrHelp):
		fmt.Fprintln(stdout, usage)      //nolint:errcheck // help goes to the terminal
		fmt.Fprintln(stdout, "\nFlags:") //nolint:errcheck // help goes to the terminal
		c.fs.SetOutput(stdout)
		c.fs.PrintDefaults()
		return errHelp
	case err != nil:
		return errUsage
	}
	if len(c.fs.Args()) != 0 {
		return fmt.Errorf("%q is not a valid flag", c.fs.Arg(0))
	}
	if c.printVersion {
		fmt.Fprintf(stdout, "etcd Version: %s\nGit SHA: %s\nGo Version: %s\nGo OS/Arch: %s/%s\n", //nolint:errcheck // the version goes to the terminal
			core.ServerVersion, gitSHA(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
		return errHelp
	}
	// Whether a configuration file is used decides whether the other
	// environment variables are, so its own goes first.
	if c.configFile == "" {
		c.configFile = os.Getenv(flags.FlagToEnv("ETCD", "config-file"))
	}
	var err error
	if c.configFile != "" {
		err = c.fromFile(c.configFile)
	} else {
		err = c.fromCmdLine()
	}
	c.ec.V2Deprecation = cconfig.V2DeprDefault
	if c.ec.WarningUnaryRequestDuration == 0 {
		c.ec.WarningUnaryRequestDuration = embed.DefaultWarningUnaryRequestDuration
	}
	if lg := c.ec.GetLogger(); lg != nil {
		for name := range c.ec.FlagsExplicitlySet {
			if msg, ok := deprecatedFlags[name]; ok {
				lg.Warn(msg)
			}
		}
	}
	return err
}

// fromCmdLine is etcd's configFromCmdLine.
func (c *config) fromCmdLine() error {
	lg, err := zap.NewProduction()
	if err != nil {
		return err
	}
	if v := os.Getenv("ETCD_VERSION"); v != "" {
		if err := os.Unsetenv("ETCD_VERSION"); err != nil {
			return err
		}
		lg.Warn("cannot set special environment variable", zap.String("key", "ETCD_VERSION"), zap.String("value", v))
	}
	if err := flags.SetFlagsFromEnv(lg, "ETCD", c.fs); err != nil {
		return err
	}
	if rafthttp.ConnReadTimeout < rafthttp.DefaultConnReadTimeout {
		rafthttp.ConnReadTimeout = rafthttp.DefaultConnReadTimeout
		lg.Info(fmt.Sprintf("raft-read-timeout increased to minimum value: %v", rafthttp.DefaultConnReadTimeout))
	}
	if rafthttp.ConnWriteTimeout < rafthttp.DefaultConnWriteTimeout {
		rafthttp.ConnWriteTimeout = rafthttp.DefaultConnWriteTimeout
		lg.Info(fmt.Sprintf("raft-write-timeout increased to minimum value: %v", rafthttp.DefaultConnWriteTimeout))
	}
	fs := c.fs
	c.ec.ListenPeerUrls = flags.UniqueURLsFromFlag(fs, "listen-peer-urls")
	c.ec.AdvertisePeerUrls = flags.UniqueURLsFromFlag(fs, "initial-advertise-peer-urls")
	c.ec.ListenClientUrls = flags.UniqueURLsFromFlag(fs, "listen-client-urls")
	c.ec.ListenClientHttpUrls = flags.UniqueURLsFromFlag(fs, "listen-client-http-urls")
	c.ec.AdvertiseClientUrls = flags.UniqueURLsFromFlag(fs, "advertise-client-urls")
	c.ec.ListenMetricsUrls = flags.UniqueURLsFromFlag(fs, "listen-metrics-urls")
	c.ec.DiscoveryCfg.Endpoints = flags.UniqueStringsFromFlag(fs, "discovery-endpoints")
	c.ec.CORS = flags.UniqueURLsMapFromFlag(fs, "cors")
	c.ec.HostWhitelist = flags.UniqueStringsMapFromFlag(fs, "host-whitelist")
	c.ec.ClientTLSInfo.AllowedHostnames = flags.StringsFromFlag(fs, "client-cert-allowed-hostname")
	c.ec.PeerTLSInfo.AllowedCNs = flags.StringsFromFlag(fs, "peer-cert-allowed-cn")
	c.ec.PeerTLSInfo.AllowedHostnames = flags.StringsFromFlag(fs, "peer-cert-allowed-hostname")
	c.ec.CipherSuites = flags.StringsFromFlag(fs, "cipher-suites")
	c.ec.MaxConcurrentStreams = flags.Uint32FromFlag(fs, "max-concurrent-streams")
	c.ec.LogOutputs = flags.UniqueStringsFromFlag(fs, "log-outputs")
	c.ec.ClusterState = c.clusterState.String()
	// etcd drops its default advertise-client-urls when
	// listen-client-urls is set and it is not, and its default
	// initial-cluster when discovery is set and it is not.
	if flags.IsSet(fs, "listen-client-urls") && !flags.IsSet(fs, "advertise-client-urls") {
		c.ec.AdvertiseClientUrls = nil
	}
	if (c.ec.DNSCluster != "" || c.ec.DNSClusterServiceName != "" || len(c.ec.DiscoveryCfg.Endpoints) > 0) && !flags.IsSet(fs, "initial-cluster") {
		c.ec.InitialCluster = ""
	}
	fs.Visit(func(f *flag.Flag) { c.ec.FlagsExplicitlySet[f.Name] = true })
	if c.fallback.String() == "proxy" {
		return errors.New(`v2 proxy is deprecated, and --discovery-fallback can't be configured as "proxy"`)
	}
	return c.ec.Validate()
}

// fromFile is etcd's configFromFile, plus tigerstore's keys of the file.
func (c *config) fromFile(path string) error {
	ec, err := embed.ConfigFromFile(path)
	if err != nil {
		return err
	}
	c.ec = *ec
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	// The file replaces the command line, tigerstore's flags too.
	c.own = ownFlags{}
	return yaml.Unmarshal(b, &c.own)
}

// gitSHA is the commit the binary was built from, as etcd's version
// shows it.
func gitSHA() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value[:min(len(s.Value), 7)]
			}
		}
	}
	return "Not provided (use ./build instead of go build)"
}

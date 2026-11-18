package wl

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/djosh34/tigerstore/internal/authtoken"
	"github.com/djosh34/tigerstore/internal/metrics"
)

// Config is what a node runs with: its identity and its cluster's, as
// cmd/tigerstore keeps them in the data dir, and the listeners, TLS and
// memory it takes from etcd's command line.
type Config struct {
	Name          string
	ID, ClusterID uint64
	// Members are the cluster's members as the node starts: a new cluster
	// starts its log with them, and a node that joins a running cluster
	// (Join) dials them until it learns the list from the leader.
	Members []Member
	Join    bool
	// ClientURLs are the client URLs the node publishes.
	ClientURLs []string
	DataDir    string
	// The listeners of the client and peer URLs: ClientListeners and
	// PeerListeners serve TLS, with ClientTLS and PeerServerTLS;
	// PlainClientListeners, of http and unix client URLs, serve clients
	// without TLS, as etcd does: HTTP/2 with prior knowledge (h2c) and
	// HTTP/1.1 (plain.go).
	ClientListeners, PeerListeners []net.Listener
	PlainClientListeners           []net.Listener
	ClientTLS                      *tls.Config
	// PeerServerTLS checks the peers that connect; PeerClientTLS is how
	// this node connects to them.
	PeerServerTLS, PeerClientTLS *tls.Config
	// MemoryBudget is the memory the node may use, 0 for no limit.
	MemoryBudget int64
	Logger       *log.Logger
	// Sys, in a binary built for the simulator, stands in for the system
	// calls it cannot run; nil otherwise (sys.go).
	Sys Sys
	// Hooks, in a binary built for the simulator, let it watch and hold
	// the node; nil otherwise (sys.go).
	Hooks Hooks
	// CORS and HostWhitelist are etcd's --cors and --host-whitelist; Pprof
	// serves /debug/pprof/, as --enable-pprof.
	CORS, HostWhitelist map[string]struct{}
	Pprof               bool
	// Metrics is the node's set of etcd's metrics, a new one if nil;
	// ExtensiveMetrics is --metrics extensive.
	Metrics          *metrics.Set
	ExtensiveMetrics bool
	// MetricsListeners serve /metrics, /health, /livez and /readyz, as
	// etcd's --listen-metrics-urls do.
	MetricsListeners []net.Listener
	// QuotaBackendBytes is etcd's --quota-backend-bytes: 0 for etcd's
	// default, below 0 for none.
	QuotaBackendBytes int64
	// DiskBudget is --disk-budget-bytes, the most the node's data
	// directory takes: its log's ring takes half of it, and the
	// checkpoints the rest. 0 for none: the ring is then the quota's size.
	// LogRing, if above 0, is the ring's size in bytes in place of either:
	// tests and the simulator set it small (logring.go); below 0, the log
	// has no ring.
	DiskBudget int64
	LogRing    int64
	// JWT signs and checks auth tokens (--auth-token jwt,...); nil for
	// etcd's simple tokens. TokenTTL is --auth-token-ttl, BcryptCost
	// --bcrypt-cost; 0 for etcd's defaults.
	JWT *authtoken.JWT
	// MaxStreams is etcd's --max-concurrent-streams: the streams a client
	// connection may have open at once; 0, or above defaultMaxStreams, for
	// defaultMaxStreams.
	MaxStreams uint32
	// ProgressNotify is etcd's --experimental-watch-progress-notify-interval.
	ProgressNotify time.Duration
	TokenTTL       time.Duration
	BcryptCost     int
	// AutoCompactPeriod and AutoCompactRevisions are etcd's
	// --auto-compaction-mode periodic and revision with their retention;
	// both 0 for none.
	AutoCompactPeriod    time.Duration
	AutoCompactRevisions int64
	// CheckpointLog is the log written after the newest checkpoint before
	// the next is taken, 0 for ckptMin, and CatchupEntries the applied
	// entries Raft's storage keeps for followers that lag, as etcd's
	// --experimental-snapshot-catchup-entries, 0 for storageWindow. The
	// simulator, whose disks are small, sets both lower, so that the log
	// behind a checkpoint is cut.
	CheckpointLog  int64
	CatchupEntries uint64
	// CheckpointChunkKeys is the most keys a checkpoint writes in one
	// write, 0 for ckptChunkKeys. The simulator sets it lower, so that a
	// checkpoint of its few keys is several writes, which a crash may
	// keep only some of.
	CheckpointChunkKeys int
}

// Member is one member of the cluster.
type Member struct {
	ID         uint64
	Name       string
	PeerURLs   []string
	ClientURLs []string
}

var errConfig = errors.New("a node's configuration")

func (c *Config) check() error {
	switch {
	case c.ID == 0:
		return fmt.Errorf("%w: no member ID", errConfig)
	case c.PeerServerTLS == nil || c.PeerClientTLS == nil:
		return fmt.Errorf("%w: the peer URLs must be https", errConfig)
	case c.ClientTLS == nil && len(c.ClientListeners) > 0:
		return fmt.Errorf("%w: https client URLs need the client TLS", errConfig)
	case len(c.ClientListeners)+len(c.PlainClientListeners) == 0 || len(c.PeerListeners) == 0:
		return fmt.Errorf("%w: no client or peer listener", errConfig)
	case c.Logger == nil:
		return fmt.Errorf("%w: no logger", errConfig)
	}
	for _, m := range c.Members {
		if m.ID == c.ID {
			return nil
		}
	}
	return fmt.Errorf("%w: member %d is not among the members", errConfig, c.ID)
}

// member is m as the node keeps it.
func (m Member) member() (member, error) {
	if len(m.PeerURLs) == 0 {
		return member{}, fmt.Errorf("%w: member %d has no peer URL", errConfig, m.ID)
	}
	for _, raw := range m.PeerURLs {
		if _, err := url.Parse(raw); err != nil {
			return member{}, fmt.Errorf("%w: member %d's peer URL: %w", errConfig, m.ID, err)
		}
	}
	return member{id: m.ID, name: m.Name, peerURLs: m.PeerURLs, clientURLs: m.ClientURLs}, nil
}

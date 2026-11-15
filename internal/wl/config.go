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
	// The listeners of the client and peer URLs. Both serve TLS only.
	ClientListeners, PeerListeners []net.Listener
	ClientTLS                      *tls.Config
	// PeerServerTLS checks the peers that connect; PeerClientTLS is how
	// this node connects to them.
	PeerServerTLS, PeerClientTLS *tls.Config
	// MemoryBudget is the memory the node may use, 0 for no limit.
	MemoryBudget int64
	Logger       *log.Logger
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
	// JWT signs and checks auth tokens (--auth-token jwt,...); nil for
	// etcd's simple tokens. TokenTTL is --auth-token-ttl, BcryptCost
	// --bcrypt-cost; 0 for etcd's defaults.
	JWT        *authtoken.JWT
	TokenTTL   time.Duration
	BcryptCost int
	// AutoCompactPeriod and AutoCompactRevisions are etcd's
	// --auto-compaction-mode periodic and revision with their retention;
	// both 0 for none.
	AutoCompactPeriod    time.Duration
	AutoCompactRevisions int64
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
	case c.ClientTLS == nil || c.PeerServerTLS == nil || c.PeerClientTLS == nil:
		return fmt.Errorf("%w: the client and peer URLs must be https", errConfig)
	case len(c.ClientListeners) == 0 || len(c.PeerListeners) == 0:
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

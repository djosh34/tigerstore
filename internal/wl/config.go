package wl

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
)

// Config is what a node runs with: its identity and its cluster's, as
// cmd/tigerstore keeps them in the data dir, and the listeners, TLS and
// memory it takes from etcd's command line.
type Config struct {
	Name          string
	ID, ClusterID uint64
	// Members are the cluster's members as the node starts; a new cluster
	// starts its log with them.
	Members []Member
	DataDir string
	// The listeners of the client and peer URLs. Both serve TLS only.
	ClientListeners, PeerListeners []net.Listener
	ClientTLS                      *tls.Config
	// PeerServerTLS checks the peers that connect; PeerClientTLS is how
	// this node connects to them.
	PeerServerTLS, PeerClientTLS *tls.Config
	// MemoryBudget is the memory the node may use, 0 for no limit.
	MemoryBudget int64
	Logger       *log.Logger
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

// member is m as the node keeps it: where it dials m, and the client URL
// it lists for m. A member whose client URLs the node does not know has
// its peer port less one listed.
func (m Member) member(self bool) (member, error) {
	if len(m.PeerURLs) == 0 {
		return member{}, fmt.Errorf("%w: member %d has no peer URL", errConfig, m.ID)
	}
	u, err := url.Parse(m.PeerURLs[0])
	if err != nil {
		return member{}, fmt.Errorf("%w: member %d's peer URL: %w", errConfig, m.ID, err)
	}
	mem := member{id: m.ID, name: m.Name, peerURL: m.PeerURLs[0], peerAddr: u.Host}
	if len(m.ClientURLs) > 0 {
		mem.cliURL = m.ClientURLs[0]
		return mem, nil
	}
	if self {
		return member{}, fmt.Errorf("%w: this member has no client URL", errConfig)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return member{}, fmt.Errorf("%w: member %d's peer URL: %w", errConfig, m.ID, err)
	}
	pn, err := strconv.Atoi(port)
	if err != nil {
		return member{}, fmt.Errorf("%w: member %d's peer URL: %w", errConfig, m.ID, err)
	}
	mem.cliURL = "https://" + net.JoinHostPort(host, strconv.Itoa(pn-1))
	return mem, nil
}

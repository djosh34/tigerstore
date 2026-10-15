package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
)

// identity is a node's Raft ID and its cluster's ID. A node keeps them in
// <data-dir>/member from its first start on, so that a restart does not
// depend on --initial-cluster, which a joined node's differs from the
// first members'.
type identity struct{ id, cluster uint64 }

// identityFile is where a data directory keeps the node's identity.
func identityFile(dataDir string) string { return filepath.Join(dataDir, "member") }

// joinTimeout bounds how long a node that joins a running cluster asks
// the other members for theirs.
const joinTimeout = 30 * time.Second

// loadIdentity returns the identity the data directory keeps, or the one
// of a first start: in a new cluster, the node's place in the cluster
// and a hash of its token and key; in a running one
// (--initial-cluster-state existing), what the other members answer for
// any of the node's peer URLs.
func loadIdentity(ec *embed.Config, cluster clusterSpec, pcfg peer.Config) (identity, error) {
	var id identity
	b, err := os.ReadFile(identityFile(ec.Dir))
	switch {
	case err == nil:
		if _, err := fmt.Sscanf(string(b), "%x %x\n", &id.id, &id.cluster); err != nil {
			return id, fmt.Errorf("%s: %w", identityFile(ec.Dir), err)
		}
		return id, nil
	case !errors.Is(err, fs.ErrNotExist):
		return id, err
	}
	if cluster.self < 0 {
		return id, fmt.Errorf("couldn't find local name %q in the initial cluster configuration", ec.Name)
	}
	self := cluster.members[cluster.self]
	if ec.IsNewCluster() {
		h := fnv.New64a()
		h.Write([]byte(cluster.token + "/" + cluster.key))
		id = identity{id: self.id, cluster: h.Sum64()}
		return id, saveIdentity(ec.Dir, id)
	}
	var addrs []string
	for i, m := range cluster.members {
		if i == cluster.self {
			continue
		}
		for _, raw := range m.urls {
			u, err := url.Parse(raw)
			if err != nil {
				return id, err
			}
			addrs = append(addrs, u.Host)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
	defer cancel()
	ms, clusterID, err := node.RemoteMembers(ctx, pcfg, addrs)
	if err != nil {
		return id, err
	}
	id.cluster = clusterID
	for _, m := range ms {
		for _, u := range m.PeerURLs {
			if slices.Contains(self.urls, u) {
				id.id = m.ID
			}
		}
	}
	if id.id == 0 {
		return id, fmt.Errorf("the cluster has no member with peer URLs %v; add it with MemberAdd first", self.urls)
	}
	return id, saveIdentity(ec.Dir, id)
}

// saveIdentity writes the identity file and syncs it and its directory.
func saveIdentity(dataDir string, id identity) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	tmp := identityFile(dataDir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%x %x\n", id.id, id.cluster)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp, identityFile(dataDir)); err != nil {
		return err
	}
	d, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// initialMembers are the members of --initial-cluster with the Raft IDs
// of their places.
func initialMembers(members []member) []*pb.Member {
	var out []*pb.Member
	for _, m := range members {
		out = append(out, &pb.Member{ID: m.id, Name: m.name, PeerURLs: m.urls})
	}
	return out
}

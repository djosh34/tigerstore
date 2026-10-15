package main

import (
	"context"
	"encoding/json"
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

// identity is a node's Raft ID, its cluster's ID and the members it
// started with: those of a new cluster, or those a joining node was told
// of, with their real IDs. A node keeps it in <data-dir>/member from its
// first start on, so a restart depends neither on --initial-cluster, which
// a joined node's differs from the first members', nor on discovery; a
// first start that fails after writing it starts the same way again.
type identity struct {
	id, cluster uint64
	members     []*pb.Member
}

// identityJSON is the identity file's format.
type identityJSON struct {
	ID      uint64       `json:"id"`
	Cluster uint64       `json:"cluster"`
	Members []memberJSON `json:"members"`
}

type memberJSON struct {
	ID       uint64   `json:"id"`
	Name     string   `json:"name"`
	PeerURLs []string `json:"peerURLs"`
}

// identityFile is where a data directory keeps the node's identity.
func identityFile(dataDir string) string { return filepath.Join(dataDir, "member") }

// joinTimeout bounds how long a node that joins a running cluster asks
// the other members for theirs.
const joinTimeout = 30 * time.Second

// readIdentity returns the identity the data directory keeps, and
// whether it keeps one.
func readIdentity(dataDir string) (identity, bool, error) {
	b, err := os.ReadFile(identityFile(dataDir))
	if errors.Is(err, fs.ErrNotExist) {
		return identity{}, false, nil
	}
	if err != nil {
		return identity{}, false, err
	}
	var j identityJSON
	if err := json.Unmarshal(b, &j); err != nil {
		return identity{}, false, fmt.Errorf("%s: %w", identityFile(dataDir), err)
	}
	id := identity{id: j.ID, cluster: j.Cluster}
	for _, m := range j.Members {
		id.members = append(id.members, &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs})
	}
	return id, true, nil
}

// newIdentity returns and saves the identity of a first start: in a new
// cluster, the node's place in it, a hash of its token and key, and its
// members; in a running one (--initial-cluster-state existing), the
// members the others answer, the ID of the one with any of the node's
// peer URLs, and their cluster ID.
func newIdentity(ec *embed.Config, cluster clusterSpec, pcfg peer.Config) (identity, error) {
	if cluster.self < 0 {
		return identity{}, fmt.Errorf("couldn't find local name %q in the initial cluster configuration", ec.Name)
	}
	if ec.IsNewCluster() {
		id := clusterIdentity(cluster)
		return id, saveIdentity(ec.Dir, id)
	}
	self := cluster.members[cluster.self]
	var addrs []string
	for i, m := range cluster.members {
		if i == cluster.self {
			continue
		}
		for _, raw := range m.urls {
			u, err := url.Parse(raw)
			if err != nil {
				return identity{}, err
			}
			addrs = append(addrs, u.Host)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
	defer cancel()
	ms, clusterID, err := node.RemoteMembers(ctx, pcfg, addrs)
	if err != nil {
		return identity{}, err
	}
	id := identity{cluster: clusterID, members: ms}
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

// clusterIdentity is the identity of the node cluster.self of a new
// cluster.
func clusterIdentity(cluster clusterSpec) identity {
	h := fnv.New64a()
	h.Write([]byte(cluster.token + "/" + cluster.key))
	return identity{id: cluster.members[cluster.self].id, cluster: h.Sum64(), members: initialMembers(cluster.members)}
}

// saveIdentity writes the identity file and syncs it and its directory.
func saveIdentity(dataDir string, id identity) error {
	j := identityJSON{ID: id.id, Cluster: id.cluster}
	for _, m := range id.members {
		j.Members = append(j.Members, memberJSON{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs})
	}
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	tmp := identityFile(dataDir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
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

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/url"
	"slices"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/peer"
	"github.com/djosh34/tigerstore/internal/wl"
)

// identity is a node's Raft ID, its cluster's ID and the members it
// started with: those of a new cluster, or those a joining node was told
// of, with their real IDs. A node keeps it in <data-dir>/member from its
// first start on, so a restart depends neither on --initial-cluster, which
// a joined node's differs from the first members', nor on discovery; a
// first start that fails after writing it starts the same way again.
// first is whether the node bootstrapped the cluster, as one of its first
// members: every start takes it from here, not from
// --initial-cluster-state, which etcd ignores once a node has a member
// file. A file without it, from before it was kept, counts as a joiner.
type identity struct {
	id, cluster uint64
	members     []*pb.Member
	first       bool
}

// joinTimeout bounds how long a node that joins a running cluster asks
// the other members for theirs.
const joinTimeout = 30 * time.Second

// readIdentity returns the identity the data directory keeps, and
// whether it keeps one (wl.ReadIdentity).
func readIdentity(dataDir string) (identity, bool, error) {
	w, ok, err := wl.ReadIdentity(nil, dataDir)
	if err != nil || !ok {
		return identity{}, ok, err
	}
	id := identity{id: w.ID, cluster: w.Cluster, first: w.First}
	for _, m := range w.Members {
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
	ms, clusterID, err := remoteMembers(ctx, pcfg, addrs)
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
	return identity{id: cluster.members[cluster.self].id, cluster: h.Sum64(), members: initialMembers(cluster.members), first: true}
}

// saveIdentity writes the identity file durably (wl.SaveIdentity).
func saveIdentity(dataDir string, id identity) error {
	w := wl.Identity{ID: id.id, Cluster: id.cluster, First: id.first}
	for _, m := range id.members {
		w.Members = append(w.Members, wl.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs})
	}
	return wl.SaveIdentity(nil, dataDir, w)
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

// remoteMembers asks the running cluster's members at addrs for its ID
// and members, over the peer protocol of the node that runs.
func remoteMembers(ctx context.Context, pcfg peer.Config, addrs []string) ([]*pb.Member, uint64, error) {
	if !useWL() {
		return node.RemoteMembers(ctx, pcfg, addrs)
	}
	_, client, err := wlPeerTLS(pcfg)
	if err != nil {
		return nil, 0, err
	}
	ms, clusterID, err := wl.RemoteMembers(ctx, nil, client, addrs)
	if err != nil {
		return nil, 0, err
	}
	out := make([]*pb.Member, 0, len(ms))
	for _, m := range ms {
		out = append(out, &pb.Member{ID: m.ID, Name: m.Name, PeerURLs: m.PeerURLs, ClientURLs: m.ClientURLs})
	}
	return out, clusterID, nil
}

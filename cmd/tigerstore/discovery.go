package main

import (
	"fmt"
	"slices"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/types"
	cconfig "go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.uber.org/zap"
)

// clusterSpec is the cluster a node starts with: its members in Raft ID
// order, which of them this node is (-1 if none, as on a restart that
// does not need them), and the token and key that the cluster ID hashes.
// Every node of a new cluster must get the same members, in the same
// order, with the same names, and the same token and key.
type clusterSpec struct {
	members    []member
	self       int
	token, key string
}

// resolveCluster finds the cluster of a first start as etcd does: it
// takes --initial-cluster as it is, or with --discovery-endpoints
// registers with etcd's v3 discovery and waits for the cluster it
// reports, or with --discovery-srv looks the members up in DNS SRV
// records. Either way it first runs etcd's checks of the cluster. A
// restart takes the members its identity file keeps instead.
func resolveCluster(ec *embed.Config, lg *zap.Logger) (clusterSpec, error) {
	urls, token, err := ec.PeerURLsMapAndToken("etcd")
	if err != nil {
		return clusterSpec{}, fmt.Errorf("error setting up initial cluster: %w", err)
	}
	sc := &cconfig.ServerConfig{
		Name: ec.Name, InitialPeerURLsMap: urls, PeerURLs: ec.AdvertisePeerUrls, Logger: lg,
		DiscoveryCfg: ec.DiscoveryCfg,
	}
	if ec.IsNewCluster() {
		err = sc.VerifyBootstrap()
	} else {
		err = sc.VerifyJoinExisting()
	}
	if err != nil {
		return clusterSpec{}, err
	}
	switch {
	case sc.ShouldDiscover():
		// etcd registers under the member ID of its peer URLs and the
		// discovery token; the members come back in the order they
		// registered, the same for every node.
		id := membership.NewMember(ec.Name, types.URLs(ec.AdvertisePeerUrls), token, nil).ID
		lg.Info("Bootstrapping cluster using v3 discovery.")
		s, err := v3discovery.JoinCluster(lg, &ec.DiscoveryCfg, id, urls.String())
		if err != nil {
			return clusterSpec{}, fmt.Errorf("discovery failed: %w", err)
		}
		found, err := types.NewURLsMap(s)
		if err != nil {
			return clusterSpec{}, err
		}
		if cconfig.CheckDuplicateURL(found) {
			return clusterSpec{}, fmt.Errorf("discovery cluster %s has duplicate url", found)
		}
		return staticCluster(ec, token, s)
	case ec.DNSCluster != "":
		return srvCluster(urls, ec.Name, token), nil
	}
	return staticCluster(ec, token, ec.InitialCluster)
}

// staticCluster is the cluster of an --initial-cluster string.
func staticCluster(ec *embed.Config, token, s string) (clusterSpec, error) {
	spec := clusterSpec{self: -1, token: token, key: s}
	if s == "" {
		return spec, nil
	}
	var err error
	if spec.members, err = parseCluster(s); err != nil {
		return spec, err
	}
	spec.self = slices.IndexFunc(spec.members, func(m member) bool { return m.name == ec.Name })
	return spec, nil
}

// srvCluster is the cluster of DNS SRV records. etcd names the node's own
// entry after it and the others with numbers in the order the lookups
// gave them, which differ between nodes; so the members are ordered by
// their peer URLs, keep no names until they publish theirs, and the
// cluster ID hashes the URLs alone.
func srvCluster(urls types.URLsMap, name, token string) clusterSpec {
	var self []string
	for n, us := range urls {
		ss := us.StringSlice()
		slices.Sort(ss)
		if n == name {
			self = ss
		}
	}
	var lists [][]string
	for _, us := range urls {
		ss := us.StringSlice()
		slices.Sort(ss)
		lists = append(lists, ss)
	}
	slices.SortFunc(lists, slices.Compare)
	spec := clusterSpec{self: -1, token: token}
	var all []string
	for i, ss := range lists {
		spec.members = append(spec.members, member{id: uint64(i + 1), urls: ss})
		if slices.Equal(ss, self) {
			spec.self = i
		}
		all = append(all, ss...)
	}
	spec.key = strings.Join(all, ",")
	return spec
}

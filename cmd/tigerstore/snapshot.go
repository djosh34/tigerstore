package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/djosh34/tigerstore/internal/node"
)

// snapshotCommand runs `tigerstore snapshot restore <file>`, which writes
// a snapshot that Maintenance.Snapshot saved (etcdctl snapshot save) into
// a new data directory, as `etcdutl snapshot restore` does: the members
// of --initial-cluster replace the snapshot's. Every node of the new
// cluster restores the same file with its own --name and the same
// --initial-cluster and --initial-cluster-token, and starts with
// --initial-cluster-state new.
func snapshotCommand(args []string, logger *log.Logger) error {
	if len(args) == 0 || args[0] != "restore" {
		return errors.New("usage: tigerstore snapshot restore <file> --name <name> --initial-cluster <cluster> [--data-dir <dir>] [--skip-hash-check]")
	}
	args = args[1:]
	var path string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		path, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet("tigerstore snapshot restore", flag.ContinueOnError)
	name := fs.String("name", "default", "name of this node in --initial-cluster")
	dataDir := fs.String("data-dir", "", "data directory to create (default <name>.etcd)")
	cluster := fs.String("initial-cluster", "", "the new cluster as name=https://host:peerport, comma separated, the same on every node")
	token := fs.String("initial-cluster-token", "etcd-cluster", "token of the new cluster, which its ID comes from")
	fs.String("initial-advertise-peer-urls", "", "accepted for etcdutl's sake; peer URLs come from --initial-cluster")
	skipHash := fs.Bool("skip-hash-check", false, "do not check the snapshot's SHA-256")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if path == "" {
		path = fs.Arg(0)
	}
	if path == "" || *cluster == "" {
		return errors.New("a snapshot file and --initial-cluster are required")
	}
	members, err := parseCluster(*cluster)
	if err != nil {
		return err
	}
	self := slices.IndexFunc(members, func(m member) bool { return m.name == *name })
	if self < 0 {
		return fmt.Errorf("--name %q is not in --initial-cluster", *name)
	}
	if *dataDir == "" {
		*dataDir = *name + ".etcd"
	}
	info, err := node.Restore(path, *dataDir, initialMembers(members), *skipHash)
	if err != nil {
		return err
	}
	// The node starts from this identity whatever its cluster flags say.
	if err := saveIdentity(*dataDir, clusterIdentity(clusterSpec{members: members, self: self, token: *token, key: *cluster})); err != nil {
		return err
	}
	logger.Printf("restored the snapshot of revision %d (index %d, term %d) into %s", info.Revision, info.Index, info.Term, *dataDir)
	return nil
}

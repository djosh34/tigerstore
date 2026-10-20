// Command etcdwal writes the committed log of stopped tigerstore nodes as an
// etcd v3.7.2 WAL, so that etcd's robustness tests read the persisted
// requests with their own decoder (#356).
//
//	etcdwal -out dir node-data-dir...
//
// It creates dir/member/wal, as in an etcd data directory. Each write in the
// log becomes one entry with etcd's InternalRaftRequest, in log order. Lease
// renewals, lease checkpoints and member announcements change no key and are
// left out, as etcd keeps renewals out of its log and its tests skip the
// others. A lease expiry fails: it revokes the lease only if no renewal
// committed first, and the robustness presets grant leases that outlive
// their histories.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/djosh34/tigerstore/internal/committed"
	"github.com/djosh34/tigerstore/internal/core"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	out := flag.String("out", "", "etcd data directory to create")
	flag.Parse()
	if *out == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: etcdwal -out dir node-data-dir...")
		os.Exit(2)
	}
	n, err := run(*out, flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "etcdwal:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "etcdwal: wrote %d requests to %s\n", n, *out)
}

func run(out string, dirs []string) (int, error) {
	var nodes []committed.Node
	for _, dir := range dirs {
		n, err := committed.Read(dir)
		if err != nil {
			return 0, fmt.Errorf("node %s: %w", dir, err)
		}
		if err = n.Validate(); err != nil {
			return 0, fmt.Errorf("node %s: %w", dir, err)
		}
		nodes = append(nodes, n)
	}
	log, err := committed.Prefix(nodes)
	if err != nil {
		return 0, err
	}
	var ents []*raftpb.Entry
	for _, e := range log {
		if e.GetType() != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		err := core.DecodeBatch(e.Data, func(_, _, _ uint64, m proto.Message) error {
			req, err := internalRequest(m)
			if err != nil || req == nil {
				return err
			}
			data, err := proto.Marshal(req)
			if err != nil {
				return err
			}
			index := uint64(len(ents) + 1)
			ents = append(ents, &raftpb.Entry{Term: new(uint64(1)), Index: new(index), Type: raftpb.EntryNormal.Enum(), Data: data})
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("committed index %d: %w", e.GetIndex(), err)
		}
	}
	w, err := wal.Create(zap.NewNop(), filepath.Join(out, "member", "wal"), nil)
	if err != nil {
		return 0, err
	}
	state := &raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(len(ents)))}
	if err = w.Save(state, ents); err != nil {
		return 0, errors.Join(err, w.Close())
	}
	return len(ents), w.Close()
}

// internalRequest wraps one write as etcd logs it, or returns nil for a
// write that etcd keeps out of its log or its tests skip.
func internalRequest(m proto.Message) (*pb.InternalRaftRequest, error) {
	switch m := m.(type) {
	case *pb.PutRequest:
		return &pb.InternalRaftRequest{Put: m}, nil
	case *pb.DeleteRangeRequest:
		return &pb.InternalRaftRequest{DeleteRange: m}, nil
	case *pb.TxnRequest:
		return &pb.InternalRaftRequest{Txn: m}, nil
	case *pb.CompactionRequest:
		return &pb.InternalRaftRequest{Compaction: m}, nil
	case *pb.LeaseGrantRequest:
		return &pb.InternalRaftRequest{LeaseGrant: m}, nil
	case *pb.LeaseRevokeRequest:
		return &pb.InternalRaftRequest{LeaseRevoke: m}, nil
	case *pb.LeaseKeepAliveRequest, *pb.LeaseCheckpointRequest, *pb.Member:
		return nil, nil
	}
	return nil, fmt.Errorf("no etcd request for %T", m)
}

package core

import (
	"slices"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/wal"
)

// RepairTornBootstrap is DIAGNOSTIC REFERENCE CODE, not a mergeable fix.
// The initial identity is already durable before RawNode.Bootstrap, whose
// term-1 entries are committed by construction. Recreate a missing suffix
// only when every surviving entry exactly matches that identity's original
// bootstrap. Never apply a smaller membership or regress term/vote. Sync
// the complete log before New can apply it or send messages.
//
// Production integration needs a reviewed durable-identity contract and
// crash coverage; this deliberately uses synchronous startup I/O, outside
// the deterministic core and before the driver publishes its new WAL.
func RepairTornBootstrap(cfg Config, rec *wal.Recovered, w *wal.WAL) error {
	if rec.Root.Index != 0 || rec.HardState.Commit != 0 || rec.Last == 0 ||
		rec.Last >= uint64(len(cfg.Peers)) || !slices.Contains(cfg.Peers, cfg.ID) {
		return nil
	}
	peers, err := bootstrapPeers(cfg)
	if err != nil {
		return err
	}
	entries := make([]*raftpb.Entry, len(peers))
	for i, peer := range peers {
		cc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: new(peer.ID), Context: peer.Context}
		data, err := proto.Marshal(cc)
		if err != nil {
			return err
		}
		entries[i] = &raftpb.Entry{Type: raftpb.EntryConfChange.Enum(), Term: new(uint64(1)), Index: new(uint64(i + 1)), Data: data}
		if uint64(i+1) <= rec.Last {
			got, err := w.Entry(uint64(i + 1))
			if err != nil {
				return err
			}
			if !proto.Equal(got, entries[i]) {
				return nil
			}
		}
	}
	w.Append(nil, entries[rec.Last:])
	if err := w.Sync(); err != nil {
		return err
	}
	rec.Last = uint64(len(entries))
	return nil
}

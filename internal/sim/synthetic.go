package sim

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/djosh34/tigerstore/history"
	"github.com/djosh34/tigerstore/history/frometcd"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wl"
)

// A synthetic image (#502) is an image no run of writes reaches: a store
// whose revision, Raft index and term, lease IDs and member IDs are near
// their limits, put on every node's disk from one snapshot, as an
// operator restores a cluster from a snapshot (wl.WriteSnapshot and
// wl.RestoreSys). Its forks start from it as from an aged image's, and
// every checker runs on them: the record of committed entries starts
// after the snapshot's index (entryLog.from), and the model from the
// snapshot's store (replay).

// synthStore is the store a synthetic image's snapshot holds: its keys,
// one version each, at revision rev, compacted there, and its leases'
// TTLs by their IDs.
type synthStore struct {
	rev    int64
	seeds  []mvcc.Seed
	leases map[int64]int64
}

// synthSpec is what a synthetic image is made of: its store, the Raft
// index and term of its snapshot, and its members' IDs, one a voter.
type synthSpec struct {
	store       *synthStore
	index, term uint64
	ids         []uint64
}

// restoreImage puts spec's snapshot on a disk for each voter of s, a run
// that has not started, and returns the image its forks start from. s
// gives the image its certificate and its disks' kept pages, as an aging
// run does (capture).
func (s *Sim) restoreImage(spec *synthSpec) (*image, error) {
	st := spec.store
	if len(spec.ids) != s.cfg.voters() {
		return nil, fmt.Errorf("a synthetic image of %d members for %d voters", len(spec.ids), s.cfg.voters())
	}
	if s.cfg.Membership {
		// Its forks take a disk for each of its voters only (fork), and
		// the membership work's checks name the voters 1 to n.
		return nil, errors.New("a synthetic image has no member for the membership work to add")
	}
	var snap bytes.Buffer
	err := wl.WriteSnapshot(&snap, &wl.SnapStore{
		Cluster: wlCluster, Index: spec.index, Term: spec.term, Rev: st.rev, CompactRev: st.rev,
		Leases: st.leases, Keys: snapKeys(st.seeds),
	})
	if err != nil {
		return nil, err
	}
	ms := wlMembers(s.cfg.voters())
	for i := range ms {
		ms[i].ID = spec.ids[i]
	}
	src := &Sim{pages: s.pages, chunks: s.chunks}
	src.net.ca, src.net.cert, src.net.signer, src.net.hosts, src.net.expires, src.net.valid = s.net.ca, s.net.cert, s.net.signer, s.net.hosts, s.net.expires, s.net.valid
	img := &image{
		src: src, ids: spec.ids,
		log: &entryLog{from: spec.index, data: arena{store: s.chunks}}, ckpts: map[uint64]ckptRec{},
		maxLogged: spec.index, applied: spec.index, expiredAt: map[int64]int64{}, revokedAt: map[int64]int64{},
		start: st.start(), synth: st,
	}
	for i, n := range s.nodes[:s.cfg.voters()] {
		sys := s.os.NewNode(wlDataDir, n.disk, nil)
		if _, _, _, err := wl.RestoreSys(sys, bytes.NewReader(snap.Bytes()), int64(snap.Len()), wlDataDir, ms, false); err != nil {
			return nil, fmt.Errorf("restoring member %d's disk: %w", ms[i].ID, err)
		}
		img.disks = append(img.disks, n.disk.Durable())
	}
	return img, nil
}

// start is st as the checkers' history starts from it.
func (st *synthStore) start() *history.Start {
	out := &history.Start{Rev: st.rev, Leases: slices.Sorted(maps.Keys(st.leases))}
	for _, k := range st.seeds {
		out.KVs = append(out.KVs, frometcd.KeyValue(&mvccpb.KeyValue{
			Key: k.Key, Value: k.Value, CreateRevision: k.Create, ModRevision: k.Mod, Version: k.Version, Lease: k.Lease,
		}))
	}
	return out
}

// snapKeys are seeds as a snapshot's keys, each the one state a read at
// the store's revision sees.
func snapKeys(seeds []mvcc.Seed) iter.Seq2[wl.SnapKey, error] {
	return func(yield func(wl.SnapKey, error) bool) {
		for _, k := range seeds {
			state := wl.SnapState{Value: k.Value, Create: k.Create, Mod: k.Mod, Version: k.Version, Lease: k.Lease}
			if !yield(wl.SnapKey{Key: k.Key, States: []wl.SnapState{state}}, nil) {
				return
			}
		}
	}
}

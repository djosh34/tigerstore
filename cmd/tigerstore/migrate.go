package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"github.com/djosh34/tigerstore/internal/wl"
)

// mainSnapshotFormat is the snapshot format of main's engine, which the
// previous release ran.
const mainSnapshotFormat = 1

// restoreMain restores a snapshot of main's engine into dataDir for
// internal/wl: the offline upgrade from the previous release. Main's own
// restore makes a data dir of main's engine from it, in a temporary
// directory beside dataDir; its store, leases, alarms and auth store are
// then written as a snapshot of this engine, which wl.Restore takes. It
// returns the revision the store was compacted at: the history before it
// is gone, as after an etcd compaction.
func restoreMain(path, dataDir string, members []*pb.Member, ms []wl.Member, skipHash bool) (node.SnapshotInfo, int64, error) {
	parent := filepath.Dir(filepath.Clean(dataDir))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return node.SnapshotInfo{}, 0, err
	}
	tmp, err := os.MkdirTemp(parent, filepath.Base(dataDir)+".upgrade-")
	if err != nil {
		return node.SnapshotInfo{}, 0, err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck // only a temporary copy
	info, err := node.Restore(path, filepath.Join(tmp, "main"), members, skipHash)
	if err != nil {
		return info, 0, err
	}
	converted := filepath.Join(tmp, "wl.snapshot")
	compacted, err := convertMain(filepath.Join(tmp, "main", "wal"), converted, members[0].ID, info)
	if err != nil {
		return info, 0, fmt.Errorf("convert the snapshot of the previous engine: %w", err)
	}
	if _, _, _, err := wl.Restore(converted, dataDir, ms, false); err != nil {
		return info, 0, err
	}
	return info, compacted, nil
}

// convertMain writes the store of the main engine's log in walDir, at
// info's index, as a snapshot of internal/wl to out, and returns the
// revision it was compacted at.
func convertMain(walDir, out string, id uint64, info node.SnapshotInfo) (int64, error) {
	s, err := readMain(walDir, id, info)
	if err != nil {
		return 0, err
	}
	f, err := os.Create(out)
	if err != nil {
		return 0, err
	}
	err = wl.WriteSnapshot(f, s)
	if err == nil {
		err = f.Sync()
	}
	return s.CompactRev, errors.Join(err, f.Close())
}

// readMain reads the store of the main engine's log in walDir, at info's
// index, as a snapshot of internal/wl holds it.
func readMain(walDir string, id uint64, info node.SnapshotInfo) (*wl.SnapStore, error) {
	dir, err := osfs.New(walDir)
	if err != nil {
		return nil, err
	}
	log, rec, err := wal.Open(dir)
	if err != nil {
		return nil, err
	}
	defer log.Close() //nolint:errcheck // read only
	cfg := core.DefaultConfig(id, nil)
	cfg.Join = true
	n, err := core.New(cfg, rec, log)
	if err != nil {
		return nil, err
	}
	st := n.Store()
	if st.Rev() != info.Revision {
		return nil, fmt.Errorf("the restored store is at revision %d, the snapshot at %d", st.Rev(), info.Revision)
	}
	keys, changes, err := mainHistory(st, valueLog{log})
	if err != nil {
		return nil, err
	}
	s := &wl.SnapStore{
		Cluster: info.ClusterID, Index: info.Index, Term: info.Term,
		Rev: st.Rev(), CompactRev: max(st.CompactRev(), 0),
		Leases: n.LeaseTTLs(), Keys: func(yield func(wl.SnapKey, error) bool) {
			for _, k := range keys {
				if !yield(k, nil) {
					return
				}
			}
		},
		Changes: changes,
	}
	s.Auth.Enabled, s.Auth.Revision, s.Auth.Users, s.Auth.Roles = n.AuthState()
	alarms, _ := n.Alarms()
	for _, a := range alarms {
		s.Alarms = append(s.Alarms, [2]uint64{a.MemberID, uint64(a.Alarm)})
	}
	return s, nil
}

// mainPage is how many keys a read of the main engine's store takes at a
// time.
const mainPage = 1024

// mainHistory reads a store of main's engine as a snapshot of
// internal/wl holds it: each key with the states a read at the compacted
// revision or later sees, in name order, and the changes from the
// compacted revision on. Values only the log has are read from log.
func mainHistory(st *mvcc.Store, log core.Log) ([]wl.SnapKey, []wl.SnapChange, error) {
	compacted := max(st.CompactRev(), 0)
	states := map[string][]wl.SnapState{}
	// The live keys at the compacted revision, a page at a time.
	live := map[string]bool{}
	for from := []byte{0}; compacted > 0; {
		resp, err := st.Range(&pb.RangeRequest{Key: from, RangeEnd: []byte{0}, Revision: compacted, Limit: mainPage})
		if err == nil {
			err = core.ReadValues(log, st.TakeFills())
		}
		if err != nil {
			return nil, nil, err
		}
		for _, kv := range resp.Kvs {
			states[string(kv.Key)] = []wl.SnapState{{Value: kv.Value, Create: kv.CreateRevision, Mod: kv.ModRevision, Version: kv.Version, Lease: kv.Lease}}
			live[string(kv.Key)] = true
		}
		if !resp.More || len(resp.Kvs) == 0 {
			break
		}
		last := resp.Kvs[len(resp.Kvs)-1].Key
		from = append(append(make([]byte, 0, len(last)+1), last...), 0)
	}
	// Then each change from the compacted revision on, as its watch event.
	var events []*mvccpb.Event
	for pos := st.HistoryFrom(max(compacted, 1)); pos < st.HistoryEnd(); pos++ {
		ch := st.Changed(pos)
		if ch.Gone() {
			continue
		}
		ev, err := ch.Event(false)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, ev)
	}
	if err := core.ReadValues(log, st.TakeFills()); err != nil {
		return nil, nil, err
	}
	var changes []wl.SnapChange
	for _, ev := range events {
		kv, del := ev.Kv, ev.Type == mvccpb.DELETE
		key := string(kv.Key)
		changes = append(changes, wl.SnapChange{Rev: kv.ModRevision, Key: kv.Key, Deleted: del})
		switch {
		case kv.ModRevision > compacted && del:
			states[key] = append(states[key], wl.SnapState{Deleted: true, Mod: kv.ModRevision})
		case kv.ModRevision > compacted:
			states[key] = append(states[key], wl.SnapState{Value: kv.Value, Create: kv.CreateRevision, Mod: kv.ModRevision, Version: kv.Version, Lease: kv.Lease})
		case del && !live[key]:
			// A key deleted at the compacted revision keeps its deletion.
			states[key] = []wl.SnapState{{Deleted: true, Mod: compacted}}
		}
	}
	keys := make([]wl.SnapKey, 0, len(states))
	for _, name := range slices.Sorted(maps.Keys(states)) {
		keys = append(keys, wl.SnapKey{Key: []byte(name), States: states[name]})
	}
	return keys, changes, nil
}

// valueLog reads values from the main engine's log for core.ReadValues;
// it has no checkpoint to open.
type valueLog struct{ *wal.WAL }

func (valueLog) Checkpoint(uint64) (io.ReadCloser, error) {
	return nil, errors.New("the upgrade reads no checkpoint")
}

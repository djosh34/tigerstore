package main

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

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
// directory beside dataDir; its store's live keys and leases are then
// written as a snapshot of this engine, which wl.Restore takes.
func restoreMain(path, dataDir string, members []*pb.Member, ms []wl.Member, skipHash bool) (node.SnapshotInfo, error) {
	parent := filepath.Dir(filepath.Clean(dataDir))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return node.SnapshotInfo{}, err
	}
	tmp, err := os.MkdirTemp(parent, filepath.Base(dataDir)+".upgrade-")
	if err != nil {
		return node.SnapshotInfo{}, err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck // only a temporary copy
	info, err := node.Restore(path, filepath.Join(tmp, "main"), members, skipHash)
	if err != nil {
		return info, err
	}
	converted := filepath.Join(tmp, "wl.snapshot")
	if err := convertMain(filepath.Join(tmp, "main", "wal"), converted, members[0].ID, info); err != nil {
		return info, fmt.Errorf("convert the snapshot of the previous engine: %w", err)
	}
	if _, _, _, err := wl.Restore(converted, dataDir, ms, false); err != nil {
		return info, err
	}
	return info, nil
}

// convertMain writes the store of the main engine's log in walDir, at
// info's index, as a snapshot of internal/wl to out.
func convertMain(walDir, out string, id uint64, info node.SnapshotInfo) error {
	dir, err := osfs.New(walDir)
	if err != nil {
		return err
	}
	log, rec, err := wal.Open(dir)
	if err != nil {
		return err
	}
	defer log.Close() //nolint:errcheck // read only
	cfg := core.DefaultConfig(id, nil)
	cfg.Join = true
	n, err := core.New(cfg, rec, log)
	if err != nil {
		return err
	}
	st := n.Store()
	if st.Rev() != info.Revision {
		return fmt.Errorf("the restored store is at revision %d, the snapshot at %d", st.Rev(), info.Revision)
	}
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	err = wl.WriteSnapshot(f, info.ClusterID, info.Index, info.Term, info.Revision, n.LeaseTTLs(), mainKeys(st, valueLog{log}))
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// mainKeys yields the live keys of a store of main's engine in name
// order, a page at a time, with their values read from its log.
func mainKeys(st *mvcc.Store, log core.Log) iter.Seq2[wl.SnapKey, error] {
	return func(yield func(wl.SnapKey, error) bool) {
		from := []byte{0}
		for {
			resp, err := st.Range(&pb.RangeRequest{Key: from, RangeEnd: []byte{0}, Limit: 1024})
			if err == nil {
				err = core.ReadValues(log, st.TakeFills())
			}
			if err != nil {
				yield(wl.SnapKey{}, err)
				return
			}
			for _, kv := range resp.Kvs {
				k := wl.SnapKey{Key: kv.Key, Value: kv.Value, Create: kv.CreateRevision, Mod: kv.ModRevision, Version: kv.Version, Lease: kv.Lease}
				if !yield(k, nil) {
					return
				}
			}
			if !resp.More || len(resp.Kvs) == 0 {
				return
			}
			// The next page starts just after the last key, which may be
			// the store's own bytes: a copy.
			last := resp.Kvs[len(resp.Kvs)-1].Key
			from = append(append(make([]byte, 0, len(last)+1), last...), 0)
		}
	}
}

// valueLog reads values from the main engine's log for core.ReadValues;
// it has no checkpoint to open.
type valueLog struct{ *wal.WAL }

func (valueLog) Checkpoint(uint64) (io.ReadCloser, error) {
	return nil, errors.New("the upgrade reads no checkpoint")
}

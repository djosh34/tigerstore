package wl

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// A store of another engine, written with WriteSnapshot, restores into a
// cluster that serves its keys, revisions and leases, and goes on from
// its revision.
func TestASnapshotOfAnotherEngineRestores(t *testing.T) {
	const rev = 40
	keys := func(yield func(SnapKey, error) bool) {
		for i := range 20 {
			k := SnapKey{Key: fmt.Appendf(nil, "/m/%02d", i), Value: fmt.Appendf(nil, "v%d", i), Create: int64(2 + i), Mod: int64(20 + i), Version: 3}
			if i == 19 {
				k.Lease = 7
			}
			if !yield(k, nil) {
				return
			}
		}
	}
	path := filepath.Join(t.TempDir(), "converted.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteSnapshot(f, 99, 500, 4, rev, map[int64]int64{7: 600}, keys); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if format, err := SnapshotFormat(path); err != nil || format != clientSnapVersion {
		t.Fatalf("the snapshot is of format %d (%v)", format, err)
	}
	restored := newTestClusterWith(t, func(nc *testCluster) {
		for i := range 3 {
			if index, _, got, err := Restore(path, nc.dataDir(i), nc.members, false); err != nil || index != 500 || got != rev {
				t.Fatalf("restore: index %d revision %d: %v", index, got, err)
			}
		}
	})
	c := restored.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := c.Get(ctx, "/m/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 20 {
		t.Fatalf("%d keys restored, want 20", len(got.Kvs))
	}
	kv := got.Kvs[19]
	if string(kv.Value) != "v19" || kv.CreateRevision != 21 || kv.ModRevision != 39 || kv.Version != 3 || kv.Lease != 7 {
		t.Fatalf("the last key restored as %v", kv)
	}
	ttl, err := c.TimeToLive(ctx, 7, clientv3.WithAttachedKeys())
	if err != nil || ttl.GrantedTTL != 600 || len(ttl.Keys) != 1 {
		t.Fatalf("lease 7 restored as %v (%v)", ttl, err)
	}
	if got.Header.Revision <= rev {
		t.Fatalf("the restored cluster is at revision %d, before the snapshot's %d", got.Header.Revision, rev)
	}
}

func TestWriteSnapshotRefusesKeysOutOfOrder(t *testing.T) {
	keys := func(yield func(SnapKey, error) bool) {
		_ = yield(SnapKey{Key: []byte("/b"), Value: []byte("1"), Create: 2, Mod: 2, Version: 1}, nil) &&
			yield(SnapKey{Key: []byte("/a"), Value: []byte("1"), Create: 3, Mod: 3, Version: 1}, nil)
	}
	if err := WriteSnapshot(discard{}, 1, 1, 1, 3, nil, iter.Seq2[SnapKey, error](keys)); err == nil {
		t.Fatal("keys out of order were written")
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func TestANodeRefusesADataDirOfMainsEngine(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wal", "0000000000000001.wal"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkNotMain(dir); !errors.Is(err, ErrMainDataDir) {
		t.Fatalf("a data dir of main's engine gave %v", err)
	}
	if err := checkNotMain(t.TempDir()); err != nil {
		t.Fatalf("an empty data dir gave %v", err)
	}
}

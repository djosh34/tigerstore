package wl

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// A restore into a data dir that does not exist yet makes it and its
// missing parents, and syncs the directory each of them is in, outermost
// first (#842).
func TestARestoreMakesItsDataDirDurable(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "a", "b", "data")
	want := []string{root, filepath.Join(root, "a"), filepath.Join(root, "a", "b")}
	if got := missingDirs(sysEnv{}, dataDir); !slices.Equal(got, want) {
		t.Fatalf("the missing data dir's parents to sync are %v, want %v", got, want)
	}
	s := &SnapStore{Cluster: 99, Index: 500, Term: 4, Rev: 8, CompactRev: 8, Keys: func(func(SnapKey, error) bool) {}}
	path := filepath.Join(t.TempDir(), "snap.db")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteSnapshot(f, s); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Restore(path, dataDir, []Member{{ID: 1, PeerURLs: []string{"https://127.0.0.1:2380"}}}, false); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ckpt/CURRENT", "wal"} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err != nil {
			t.Errorf("the restored data dir lacks %s: %v", name, err)
		}
	}
	if got := missingDirs(sysEnv{}, dataDir); len(got) != 0 {
		t.Errorf("an existing data dir has parents to sync: %v", got)
	}
}

// A cluster restored from a snapshot whose Raft index, term or member IDs
// are near their limits elects a leader and takes a write (#502's
// synthetic images). An index or term past 2^63 is negative as an int64,
// one near 2^64 wraps on a sum. A member ID has 32 bits, as wl's own do
// (newMemberID); a larger one is #841.
func TestARestoreNearTheLimitsTakesWrites(t *testing.T) {
	for _, c := range []struct {
		name        string
		index, term uint64
		ids         []uint64
	}{
		{name: "small", index: 500, term: 4},
		{name: "index past 2^63", index: 1<<63 + 500, term: 4},
		{name: "index near 2^64", index: math.MaxUint64 - 1<<28, term: 4},
		{name: "term past 2^63", index: 500, term: 1<<63 + 4},
		{name: "term near 2^64", index: 500, term: math.MaxUint64 - 1<<19},
		{name: "member IDs near 2^32", index: 500, term: 4, ids: []uint64{math.MaxUint32 - 2, math.MaxUint32 - 1, math.MaxUint32}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := &SnapStore{
				Cluster: 99, Index: c.index, Term: c.term, Rev: 8, CompactRev: 8,
				Keys: func(yield func(SnapKey, error) bool) {
					yield(SnapKey{Key: []byte("/k"), States: []SnapState{{Value: []byte("v"), Create: 8, Mod: 8, Version: 1}}}, nil)
				},
			}
			path := filepath.Join(t.TempDir(), "snap.db")
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := WriteSnapshot(f, s); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			tc := newTestClusterWith(t, func(nc *testCluster) {
				for i, id := range c.ids {
					nc.ids[i], nc.members[i].ID = id, id
				}
				for i := range nc.ids {
					if _, _, _, err := Restore(path, nc.dataDir(i), nc.members, false); err != nil {
						t.Fatal(err)
					}
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cli := tc.rawClient(t, tc.endpoints...)
			put, err := cli.Put(ctx, "/after", "x")
			for err != nil && ctx.Err() == nil {
				time.Sleep(100 * time.Millisecond)
				put, err = cli.Put(ctx, "/after", "x")
			}
			if err != nil {
				t.Fatalf("no write was taken in 20 s: %v", err)
			}
			// The write goes on from the restored store, which a new
			// empty cluster would not have.
			if put.Header.Revision != 9 {
				t.Fatalf("the write after the restore is at revision %d, want 9", put.Header.Revision)
			}
			got, err := cli.Get(ctx, "/k")
			if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "v" || got.Kvs[0].ModRevision != 8 {
				t.Fatalf("the restored key reads as %v (%v)", got, err)
			}
		})
	}
}

package wl

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
			c := tc.rawClient(t, tc.endpoints...)
			_, err = c.Put(ctx, "/after", "x")
			for err != nil && ctx.Err() == nil {
				time.Sleep(100 * time.Millisecond)
				_, err = c.Put(ctx, "/after", "x")
			}
			if err != nil {
				t.Fatalf("no write was taken in 20 s: %v", err)
			}
		})
	}
}

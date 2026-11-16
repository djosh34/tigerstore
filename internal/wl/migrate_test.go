package wl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/bcrypt"
)

// A store of another engine, written with WriteSnapshot, restores into a
// cluster that serves its keys, its history from the compacted revision
// on, its leases and its auth store, and goes on from its revision.
func TestASnapshotOfAnotherEngineRestores(t *testing.T) {
	const compacted, rev = 5, 8
	keys := []SnapKey{
		// Put at 3, so a read at 5 sees it; put again at 7, deleted at 8.
		{Key: []byte("/m/a"), States: []SnapState{
			{Value: []byte("v3"), Create: 3, Mod: 3, Version: 1},
			{Value: []byte("v7"), Create: 3, Mod: 7, Version: 2},
			{Deleted: true, Mod: 8},
		}},
		{Key: []byte("/m/b"), States: []SnapState{{Value: []byte("v6"), Create: 6, Mod: 6, Version: 1, Lease: 7}}},
	}
	changes := []SnapChange{{Rev: 6, Key: []byte("/m/b")}, {Rev: 7, Key: []byte("/m/a")}, {Rev: 8, Key: []byte("/m/a"), Deleted: true}}
	hash, err := bcrypt.GenerateFromPassword([]byte("rootpw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	s := &SnapStore{
		Cluster: 99, Index: 500, Term: 4, Rev: rev, CompactRev: compacted,
		Leases: map[int64]int64{7: 600},
		Auth:   SnapAuth{Enabled: true, Revision: 9, Users: []*authpb.User{{Name: []byte("root"), Password: hash, Roles: []string{"root"}}}, Roles: []*authpb.Role{{Name: []byte("root")}}},
		Keys: func(yield func(SnapKey, error) bool) {
			for _, k := range keys {
				if !yield(k, nil) {
					return
				}
			}
		},
		Changes: changes,
	}
	path := filepath.Join(t.TempDir(), "converted.db")
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Auth is on: the client goes as root.
	c := restored.rawClient(t, restored.endpoints...)
	// The new cluster elects its leader first.
	st, err := c.AuthStatus(ctx)
	for err != nil && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
		st, err = c.AuthStatus(ctx)
	}
	if err != nil || !st.Enabled || st.AuthRevision != 9 {
		t.Fatalf("the auth status restored as %v (%v)", st, err)
	}
	if _, err := c.Authenticate(ctx, "root", "rootpw"); err != nil {
		t.Fatalf("root's password did not restore: %v", err)
	}
	root, err := clientv3.New(clientv3.Config{Endpoints: restored.endpoints, Username: "root", Password: "rootpw", DialTimeout: 5 * time.Second, TLS: restored.clientTLS()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	for _, at := range []struct {
		rev  int64
		want []string
	}{{5, []string{"v3"}}, {6, []string{"v3", "v6"}}, {7, []string{"v7", "v6"}}, {8, []string{"v6"}}} {
		got, err := root.Get(ctx, "/m/", clientv3.WithPrefix(), clientv3.WithRev(at.rev))
		if err != nil {
			t.Fatalf("a read at %d: %v", at.rev, err)
		}
		var vals []string
		for _, kv := range got.Kvs {
			vals = append(vals, string(kv.Value))
		}
		if !slices.Equal(vals, at.want) {
			t.Fatalf("a read at %d has %v, want %v", at.rev, vals, at.want)
		}
	}
	if _, err := root.Get(ctx, "/m/", clientv3.WithPrefix(), clientv3.WithRev(compacted-1)); !errors.Is(err, rpctypes.ErrCompacted) {
		t.Fatalf("a read before the compacted revision gave %v", err)
	}
	wc := root.Watch(ctx, "/m/", clientv3.WithPrefix(), clientv3.WithRev(6))
	var seen []string
	for len(seen) < 3 {
		wr := <-wc
		if err := wr.Err(); err != nil {
			t.Fatal(err)
		}
		for _, ev := range wr.Events {
			seen = append(seen, fmt.Sprintf("%s %s@%d", ev.Type, ev.Kv.Key, ev.Kv.ModRevision))
		}
	}
	if want := []string{"PUT /m/b@6", "PUT /m/a@7", "DELETE /m/a@8"}; !slices.Equal(seen, want) {
		t.Fatalf("a watch from 6 saw %v, want %v", seen, want)
	}
	ttl, err := root.TimeToLive(ctx, 7, clientv3.WithAttachedKeys())
	if err != nil || ttl.GrantedTTL != 600 || len(ttl.Keys) != 1 {
		t.Fatalf("lease 7 restored as %v (%v)", ttl, err)
	}
	put, err := root.Put(ctx, "/m/after", "x")
	if err != nil || put.Header.Revision != rev+1 {
		t.Fatalf("a put after the restore: %v (%v)", put, err)
	}
}

func TestWriteSnapshotRefusesKeysOutOfOrder(t *testing.T) {
	s := &SnapStore{Rev: 3, Keys: func(yield func(SnapKey, error) bool) {
		_ = yield(SnapKey{Key: []byte("/b"), States: []SnapState{{Value: []byte("1"), Create: 2, Mod: 2, Version: 1}}}, nil) &&
			yield(SnapKey{Key: []byte("/a"), States: []SnapState{{Value: []byte("1"), Create: 3, Mod: 3, Version: 1}}}, nil)
	}}
	if err := WriteSnapshot(discard{}, s); err == nil {
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
	if err := checkNotMain(sysEnv{}, dir); !errors.Is(err, ErrMainDataDir) {
		t.Fatalf("a data dir of main's engine gave %v", err)
	}
	if err := checkNotMain(sysEnv{}, t.TempDir()); err != nil {
		t.Fatalf("an empty data dir gave %v", err)
	}
}

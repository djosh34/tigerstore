package wl

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// A client's snapshot holds every key as of when it asked, ends with its
// SHA-256 as etcd's does, and a new cluster restored from it serves them.
func TestASnapshotRestoresIntoANewCluster(t *testing.T) {
	smallCheckpoints(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := range 300 {
		if _, err := c.Put(ctx, fmt.Sprintf("/snap/%03d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	last, err := c.Put(ctx, "/snap/last", "after the others")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := c.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(b)%512 != sha256.Size {
		t.Fatalf("a snapshot of %d bytes: not padded to 512 before its digest", len(b))
	}
	if sum := sha256.Sum256(b[:len(b)-sha256.Size]); string(sum[:]) != string(b[len(b)-sha256.Size:]) {
		t.Fatal("the snapshot's digest does not match")
	}
	path := filepath.Join(t.TempDir(), "snap.db")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	restored := newTestClusterWith(t, func(nc *testCluster) {
		for i := range 3 {
			if _, _, rev, err := Restore(path, nc.dataDir(i), nc.members, false); err != nil || rev < last.Header.Revision {
				t.Fatalf("restore: revision %d, want at least %d: %v", rev, last.Header.Revision, err)
			}
		}
	})
	rcl := restored.client(t)
	got, err := rcl.Get(ctx, "/snap/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 301 {
		t.Fatalf("the restored cluster has %d keys, want 301", len(got.Kvs))
	}
	r := waitMembers(t, rcl, 3)
	for i, m := range r.Members {
		if m.PeerURLs[0] != restored.peers[i] {
			t.Fatalf("the restored cluster's members are %v", r.Members)
		}
	}
	// A corrupt file is refused.
	b[100] ^= 1
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Restore(path, t.TempDir(), restored.members, false); err == nil {
		t.Fatal("a corrupt snapshot restored")
	}
}

// A restore never goes into a data dir that holds files, as etcdutl's
// and main's: the old log there would go on after the new checkpoint,
// and the restored node's store would part from its cluster's.
func TestARestoreRefusesADataDirThatHoldsFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "wal", "0000000000000000.seg"), []byte("an old log"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Restore(filepath.Join(t.TempDir(), "snap.db"), dir, nil, false); !errors.Is(err, ErrDataDirNotEmpty) {
		t.Fatalf("a restore into a data dir with a log gave %v, want ErrDataDirNotEmpty", err)
	}
}

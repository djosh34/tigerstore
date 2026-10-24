package core_test

import (
	"bytes"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	etcdmvcc "go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlan4EtcdPhysicalCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	b := backend.NewDefaultBackend(zap.NewNop(), path)
	defer b.Close()
	s := etcdmvcc.NewStore(zap.NewNop(), b, &lease.FakeLessor{}, etcdmvcc.StoreConfig{})
	defer s.Close()
	marker := bytes.Repeat([]byte("PLAN4_OLD_VALUE_"), 4096)
	s.Put([]byte("key"), marker, lease.NoLease)
	b.ForceCommit()
	rev := s.Put([]byte("key"), []byte("new"), lease.NoLease)
	b.ForceCommit()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	done, err := s.Compact(traceutil.TODO(), rev)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction timed out")
	}
	b.ForceCommit() // EtcdServer.Compact physical=true does this after waiting.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("db bytes before=%d after=%d obsolete value still on disk=%v", before.Size(), len(raw), bytes.Contains(raw, marker))
	if !bytes.Contains(raw, marker) {
		t.Fatal("probe did not retain old bytes")
	}
}

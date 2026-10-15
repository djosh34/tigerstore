package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/node"
	"github.com/djosh34/tigerstore/internal/wal"
)

// saveSnapshot downloads node id's snapshot as etcdctl snapshot save does
// and checks it as etcd's stream is checked: the messages carry the
// bytes still to come and the storage version, and the file ends, after a
// multiple of 512 bytes, with the SHA-256 of everything before.
func saveSnapshot(t *testing.T, c *cluster, id uint64) []byte {
	t.Helper()
	cert, _, _, err := c.ca.Issue("client", 90)
	if err != nil {
		t.Fatal(err)
	}
	s, err := pb.NewMaintenanceClient(c.dial(id, cert)).Snapshot(context.Background(), &pb.SnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var file []byte
	var remaining []uint64
	for {
		m, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if m.Version != core.StorageVersion {
			t.Fatalf("storage version %q", m.Version)
		}
		file = append(file, m.Blob...)
		remaining = append(remaining, m.RemainingBytes)
	}
	body := len(file) - sha256.Size
	if len(file)%512 != sha256.Size {
		t.Fatalf("a snapshot of %d bytes; etcdctl needs a multiple of 512 and the digest", len(file))
	}
	if sum := sha256.Sum256(file[:body]); !bytes.Equal(sum[:], file[body:]) {
		t.Fatal("the digest does not match")
	}
	if n := len(remaining); remaining[n-1] != 0 || remaining[n-2] != 0 || (n > 2 && remaining[0] != uint64(body-32<<10)) {
		t.Fatalf("bytes still to come: %v for %d bytes", remaining, body)
	}
	return file
}

// TestSnapshotSave writes and compacts with frequent checkpoints, so that
// the first segment is gone and the snapshot carries a checkpoint and the
// values it names, then saves a snapshot.
func TestSnapshotSave(t *testing.T) {
	c := newClusterWith(t, func(c *cluster) {
		c.coreConfig = func(cfg *core.Config) {
			cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
			cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
		}
		c.segmentBytes = 32 << 10
	})
	cert, _, _, err := c.ca.Issue("client", 91)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	var rev int64
	for round := range 30 {
		for i := range 20 {
			rev = retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
				return kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: fmt.Appendf(nil, "v%d.%d %0200d", i, round, 0)})
			}).Header.Revision
		}
		retry(t, func(ctx context.Context) (*pb.CompactionResponse, error) {
			return kv.Compact(ctx, &pb.CompactionRequest{Revision: rev})
		})
	}
	if _, err := os.Stat(filepath.Join(c.dirs[1], "wal", wal.SegmentName(1))); err == nil {
		t.Fatal("the first segment is still there")
	}
	file := saveSnapshot(t, c, 1)
	info, err := node.ParseSnapshotHeader(file)
	if err != nil {
		t.Fatal(err)
	}
	if info.Revision != rev || info.Index == 0 || info.Term == 0 || info.ClusterID != c.nodes[1].ClusterID() {
		t.Fatalf("header %+v, revision %d", info, rev)
	}
}

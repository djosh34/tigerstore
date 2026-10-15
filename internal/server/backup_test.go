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
	"reflect"
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
		if m.Version != core.ClusterVersion {
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

// tinyCheckpoints makes a cluster cut checkpoints and segments every few
// KiB, so that its first segment goes.
func tinyCheckpoints(c *cluster) {
	c.coreConfig = func(cfg *core.Config) {
		cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
		cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
	}
	c.segmentBytes = 32 << 10
}

// dump is what a client can read of a cluster's state through node 1.
type dump struct {
	Revision, Compacted int64
	Now, Past           []string
	Leases              []string
	Alarms              []string
	BelowCompaction     string
}

func dumpState(t *testing.T, c *cluster, compacted int64) dump {
	t.Helper()
	cert, _, _, err := c.ca.Issue("client", 92)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv, lease, m := pb.NewKVClient(conn), pb.NewLeaseClient(conn), pb.NewMaintenanceClient(conn)
	kvs := func(r *pb.RangeResponse) []string {
		var out []string
		for _, k := range r.Kvs {
			out = append(out, fmt.Sprintf("%s=%s c%d m%d v%d l%d", k.Key, k.Value, k.CreateRevision, k.ModRevision, k.Version, k.Lease))
		}
		return out
	}
	all := &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}}
	now := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) { return kv.Range(ctx, all) })
	d := dump{Revision: now.Header.Revision, Compacted: compacted, Now: kvs(now)}
	past, err := kv.Range(context.Background(), &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, Revision: compacted})
	if err != nil {
		t.Fatal(err)
	}
	d.Past = kvs(past)
	_, err = kv.Range(context.Background(), &pb.RangeRequest{Key: []byte{0}, RangeEnd: []byte{0}, Revision: compacted - 1})
	d.BelowCompaction = fmt.Sprint(err)
	ls, err := lease.LeaseLeases(context.Background(), &pb.LeaseLeasesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range ls.Leases {
		ttl, err := lease.LeaseTimeToLive(context.Background(), &pb.LeaseTimeToLiveRequest{ID: l.ID, Keys: true})
		if err != nil {
			t.Fatal(err)
		}
		d.Leases = append(d.Leases, fmt.Sprintf("%d ttl %d keys %q", l.ID, ttl.GrantedTTL, ttl.Keys))
	}
	alarms, err := m.Alarm(context.Background(), &pb.AlarmRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range alarms.Alarms {
		d.Alarms = append(d.Alarms, a.String())
	}
	return d
}

// TestSnapshotRestore writes, deletes, grants leases, raises an alarm and
// compacts with frequent checkpoints, so that the first segment is gone
// and the snapshot carries a checkpoint and the values it names. It saves
// a snapshot, restores it into three new data directories with tigerstore
// snapshot restore's code, starts a cluster on them, and compares what a
// client reads: every key now and at the compaction revision, the error
// below it, the leases with their keys, and the alarms.
func TestSnapshotRestore(t *testing.T) {
	c := newClusterWith(t, tinyCheckpoints)
	cert, _, _, err := c.ca.Issue("client", 91)
	if err != nil {
		t.Fatal(err)
	}
	conn := c.dial(1, cert)
	kv, lease := pb.NewKVClient(conn), pb.NewLeaseClient(conn)
	l := retry(t, func(ctx context.Context) (*pb.LeaseGrantResponse, error) {
		return lease.LeaseGrant(ctx, &pb.LeaseGrantRequest{TTL: 3600})
	})
	retry(t, func(ctx context.Context) (*pb.AlarmResponse, error) {
		return pb.NewMaintenanceClient(conn).Alarm(ctx, &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE, Alarm: 9, MemberID: 7})
	})
	var rev, compacted int64
	for round := range 30 {
		for i := range 20 {
			req := &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: fmt.Appendf(nil, "v%d.%d %0200d", i, round, 0)}
			if i%5 == 0 {
				req.Lease = l.ID
			}
			rev = retry(t, func(ctx context.Context) (*pb.PutResponse, error) { return kv.Put(ctx, req) }).Header.Revision
		}
		if _, err := kv.DeleteRange(context.Background(), &pb.DeleteRangeRequest{Key: fmt.Appendf(nil, "k%02d", round%20)}); err != nil {
			t.Fatal(err)
		}
		if round < 29 {
			compacted = rev
			retry(t, func(ctx context.Context) (*pb.CompactionResponse, error) {
				return kv.Compact(ctx, &pb.CompactionRequest{Revision: rev})
			})
		}
	}
	if _, err := os.Stat(filepath.Join(c.dirs[1], "wal", wal.SegmentName(1))); err == nil {
		t.Fatal("the first segment is still there")
	}
	file := saveSnapshot(t, c, 1)
	info, err := node.ParseSnapshotHeader(file)
	if err != nil {
		t.Fatal(err)
	}
	if info.Index == 0 || info.Term == 0 || info.ClusterID != c.nodes[1].ClusterID() {
		t.Fatalf("header %+v", info)
	}
	want := dumpState(t, c, compacted)
	if info.Revision != want.Revision {
		t.Fatalf("the snapshot is at revision %d, the cluster at %d", info.Revision, want.Revision)
	}
	path := filepath.Join(t.TempDir(), "snap.db")
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatal(err)
	}
	// A directory next to the data directory is not the restore's.
	var sibling string
	restored := newClusterWith(t, func(r *cluster) {
		tinyCheckpoints(r)
		sibling = filepath.Join(r.dirs[1]+".restore", "keep")
		if err := errors.Join(os.MkdirAll(filepath.Dir(sibling), 0o700), os.WriteFile(sibling, nil, 0o600)); err != nil {
			t.Fatal(err)
		}
		for id := uint64(1); id <= 3; id++ {
			if _, err := node.Restore(path, r.dirs[id], r.members(), false); err != nil {
				t.Fatal(err)
			}
		}
	})
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("the restore removed a directory next to the data directory: %v", err)
	}
	t.Logf("comparing %d keys now, %d at %d, %d leases, alarms %v, below the compaction: %s", len(want.Now), len(want.Past), compacted, len(want.Leases), want.Alarms, want.BelowCompaction)
	if got := dumpState(t, restored, compacted); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored:\n%+v\nwant:\n%+v", got, want)
	}
	// A damaged file is refused.
	file[len(file)/2] ^= 1
	if err := os.WriteFile(path, file, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := node.Restore(path, t.TempDir(), restored.members(), false); err == nil {
		t.Fatal("a damaged snapshot was restored")
	}
}

package server_test

import (
	"context"
	"fmt"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
)

// TestFollowerCatchesUpBySnapshot stops node 3, writes and compacts on the
// others with a window of a few kilobytes and frequent checkpoints, so
// that the leader drops the entries node 3 lacks from memory and the
// segments that held them from disk. Node 3 then catches up through a
// snapshot fetched over the peer transport, and serves every value. Node 1
// then restarts from its checkpoint and the log after it.
func TestFollowerCatchesUpBySnapshot(t *testing.T) {
	c := newClusterWith(t, func(c *cluster) {
		c.coreConfig = func(cfg *core.Config) {
			cfg.WindowBytes, cfg.WindowMaxBytes = 4<<10, 8<<10
			cfg.UnappliedBytes = 1 << 20
			cfg.CheckpointBytes, cfg.CheckpointRatio = 16<<10, 2
		}
		c.segmentBytes = 32 << 10
	})
	cert, _, _, err := c.ca.Issue("client", 99)
	if err != nil {
		t.Fatal(err)
	}
	kv1 := c.client(1, cert)
	value := func(i, round int) []byte { return fmt.Appendf(nil, "value %d of round %d %0200d", i, round, 0) }
	write := func(round int) int64 {
		var rev int64
		for i := range 50 {
			r := retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
				return kv1.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: value(i, round)})
			})
			rev = r.Header.Revision
		}
		return rev
	}
	write(0)
	c.running[3].stop()
	delete(c.running, 3)
	var rev int64
	for round := 1; round <= 40; round++ {
		rev = write(round)
		retry(t, func(ctx context.Context) (*pb.CompactionResponse, error) {
			return kv1.Compact(ctx, &pb.CompactionRequest{Revision: rev})
		})
	}
	c.start(3)
	kv3 := c.client(3, cert)
	retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
		r, err := kv3.Range(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Serializable: true})
		if err == nil && r.Header.Revision < rev {
			err = fmt.Errorf("node 3 is at revision %d of %d", r.Header.Revision, rev)
		}
		return r, err
	})
	check := func(kv pb.KVClient, node int) {
		t.Helper()
		r := retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Serializable: true})
		})
		if len(r.Kvs) != 50 {
			t.Fatalf("node %d has %d keys", node, len(r.Kvs))
		}
		for i, kv := range r.Kvs {
			if string(kv.Value) != string(value(i, 40)) {
				t.Fatalf("node %d has %q for k%02d", node, kv.Value, i)
			}
		}
	}
	check(kv3, 3)
	c.running[1].stop()
	delete(c.running, 1)
	c.start(1)
	check(c.client(1, cert), 1)
}

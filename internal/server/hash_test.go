package server_test

import (
	"context"
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// TestHashKV checks that every member answers Maintenance.Hash and HashKV,
// and that members which applied the same writes agree on the hashes.
func TestHashKV(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(1, cert)
	var last *pb.PutResponse
	for _, k := range []string{"a", "b", "a", "c"} {
		last = retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: []byte(k), Value: []byte(k + "1")})
		})
	}
	rev := last.Header.Revision
	retry(t, func(ctx context.Context) (*pb.CompactionResponse, error) {
		return kv.Compact(ctx, &pb.CompactionRequest{Revision: rev - 1})
	})
	var want *pb.HashKVResponse
	var wantHash *pb.HashResponse
	for id := uint64(1); id <= 3; id++ {
		m := pb.NewMaintenanceClient(c.dial(id, cert))
		// HashKV reads the member's own state, which may lag.
		got := retry(t, func(ctx context.Context) (*pb.HashKVResponse, error) {
			r, err := m.HashKV(ctx, &pb.HashKVRequest{Revision: rev})
			if err == nil && r.CompactRevision != rev-1 {
				err = errors.New("the compaction is not applied yet")
			}
			return r, err
		})
		if got.HashRevision != rev || got.Header.MemberId != id || got.Header.Revision < rev {
			t.Fatalf("member %d: %v", id, got)
		}
		if want != nil && got.Hash != want.Hash {
			t.Fatalf("member %d: hash %x, member 1: %x", id, got.Hash, want.Hash)
		}
		want = got
		h, err := m.Hash(context.Background(), &pb.HashRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if wantHash != nil && h.Hash != wantHash.Hash {
			t.Fatalf("member %d: Hash %x, member 1: %x", id, h.Hash, wantHash.Hash)
		}
		wantHash = h
		if _, err := m.HashKV(context.Background(), &pb.HashKVRequest{Revision: rev - 2}); !errors.Is(err, rpctypes.ErrGRPCCompacted) {
			t.Fatalf("member %d: HashKV below the compaction: %v", id, err)
		}
		if _, err := m.HashKV(context.Background(), &pb.HashKVRequest{Revision: rev + 100}); !errors.Is(err, rpctypes.ErrGRPCFutureRev) {
			t.Fatalf("member %d: HashKV of a future revision: %v", id, err)
		}
	}
}

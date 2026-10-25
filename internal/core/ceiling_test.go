package core

import (
	"errors"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestWriteCeiling checks that a node at the largest --max-request-bytes
// refuses every write whose entry would not fit in a peer frame or a log
// record, also one that etcd's size check does not see: a keepalive whose
// encoding carries an unknown field of 64 MiB, which gRPC lets in with its
// 512 KiB of overhead (#424).
func TestWriteCeiling(t *testing.T) {
	s := startSingleWith(t, simfs.New(), 1, func(c *Config) { c.MaxRequestBytes = MaxRequestBytesCeiling })
	grant := s.ok(1, &pb.LeaseGrantRequest{TTL: 60}).(*pb.LeaseGrantResponse)
	r := &pb.LeaseKeepAliveRequest{ID: grant.ID}
	unknown := protowire.AppendTag(nil, 2, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, make([]byte, wal.MaxEntryBytes))
	r.ProtoReflect().SetUnknown(unknown)
	if n := proto.Size(r); n > MaxRequestBytesCeiling+512<<10 {
		t.Fatalf("a keepalive of %d bytes is over gRPC's limit", n)
	}
	if got := s.do(2, r); !errors.Is(got.Err, rpctypes.ErrGRPCRequestTooLarge) {
		t.Fatalf("a keepalive of %d bytes: %v", proto.Size(r), got.Err)
	}
	if got := s.do(3, &pb.LeaseKeepAliveRequest{ID: grant.ID}); got.Err != nil {
		t.Fatalf("a keepalive after it: %v", got.Err)
	}
}

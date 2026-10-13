package server_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestRangeStream checks KV.RangeStream against what etcd v3.7.2 answered
// in the wire probe of the pre-work on #48: chunks of 10 keys and then a
// doubled limit, the header, count and more on the last chunk only, the
// store's revision in the header also for a past revision, and the same
// keys as Range once the chunks are merged.
func TestRangeStream(t *testing.T) {
	c := newCluster(t)
	cert, _, _, err := c.ca.Issue("client", 94)
	if err != nil {
		t.Fatal(err)
	}
	kv := c.client(2, cert)
	for i := range 25 {
		retry(t, func(ctx context.Context) (*pb.PutResponse, error) {
			return kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: []byte("v")})
		})
	}
	// Revision 26 holds the 25 keys; one more write makes the store 27.
	if _, err := kv.Put(context.Background(), &pb.PutRequest{Key: []byte("z"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	stream := func(r *pb.RangeRequest) ([]*pb.RangeResponse, error) {
		s, err := kv.RangeStream(context.Background(), r)
		if err != nil {
			return nil, err
		}
		var out []*pb.RangeResponse
		for {
			m, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			out = append(out, m.RangeResponse)
		}
	}
	for _, tc := range []struct {
		name   string
		r      *pb.RangeRequest
		chunks []int
	}{
		{"all", &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}, []int{10, 15}},
		{"limit", &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Limit: 12}, []int{10, 2}},
		{"past revision", &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: 20}, []int{10, 9}},
		{"count only", &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), CountOnly: true}, []int{0}},
		{"empty", &pb.RangeRequest{Key: []byte("x"), RangeEnd: []byte("y")}, []int{0}},
		{"serializable", &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Serializable: true}, []int{10, 15}},
	} {
		got, err := stream(tc.r)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(got) != len(tc.chunks) {
			t.Fatalf("%s: %d chunks, want %d", tc.name, len(got), len(tc.chunks))
		}
		merged := &pb.RangeResponse{}
		for i, m := range got {
			if len(m.Kvs) != tc.chunks[i] {
				t.Fatalf("%s: chunk %d has %d keys, want %d", tc.name, i, len(m.Kvs), tc.chunks[i])
			}
			if last := i == len(got)-1; last != (m.Header != nil) || (!last && (m.Count != 0 || m.More)) {
				t.Fatalf("%s: chunk %d of %d: %v", tc.name, i, len(got), m)
			}
			proto.Merge(merged, m)
		}
		if merged.Header.Revision != 27 || merged.Header.MemberId != 2 || merged.Header.ClusterId == 0 || merged.Header.RaftTerm == 0 {
			t.Fatalf("%s: header %v", tc.name, merged.Header)
		}
		want, err := kv.Range(context.Background(), tc.r, grpc.MaxCallRecvMsgSize(16<<20))
		if err != nil {
			t.Fatal(err)
		}
		want.Header = merged.Header
		if !proto.Equal(merged, want) {
			t.Fatalf("%s: merged stream\n%v\nRange\n%v", tc.name, merged, want)
		}
	}
	// Large values: chunk sizes follow the 1.5 MiB target.
	big := make([]byte, 400<<10)
	for i := range 25 {
		if _, err := kv.Put(context.Background(), &pb.PutRequest{Key: fmt.Appendf(nil, "b%02d", i), Value: big}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := stream(&pb.RangeRequest{Key: []byte("b"), RangeEnd: []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, m := range got {
		sizes = append(sizes, len(m.Kvs))
	}
	if fmt.Sprint(sizes) != "[10 5 5 5]" {
		t.Fatalf("chunks of 400 KiB values: %v, want [10 5 5 5]", sizes)
	}

	for _, tc := range []struct {
		r    *pb.RangeRequest
		code codes.Code
	}{
		{&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), SortOrder: pb.RangeRequest_DESCEND, SortTarget: pb.RangeRequest_KEY}, codes.Unimplemented},
		{&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), SortOrder: pb.RangeRequest_ASCEND, SortTarget: pb.RangeRequest_VALUE}, codes.Unimplemented},
		{&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), MinModRevision: 2}, codes.Unimplemented},
		{&pb.RangeRequest{Key: []byte("k"), SortOrder: 7}, codes.InvalidArgument},
		{&pb.RangeRequest{}, codes.InvalidArgument},
		{&pb.RangeRequest{Key: []byte("k"), Revision: 1000}, codes.OutOfRange},
	} {
		if _, err := stream(tc.r); status.Code(err) != tc.code {
			t.Fatalf("%v: %v, want %v", tc.r, err, tc.code)
		}
	}
	if _, err := kv.Compact(context.Background(), &pb.CompactionRequest{Revision: 26}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream(&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: 25}); !errors.Is(rpctypes.Error(err), rpctypes.ErrCompacted) {
		t.Fatalf("read below the compaction: %v", err)
	}
	if _, err := stream(&pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Revision: 26}); err != nil {
		t.Fatalf("read at the compaction: %v", err)
	}
}

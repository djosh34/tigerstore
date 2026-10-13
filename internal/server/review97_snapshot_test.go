package server_test

import (
	"context"
	"fmt"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"testing"
	"time"
)

func TestReview97SnapshotCancel(t *testing.T) {
	for _, backend := range []string{"etcd", "tiger"} {
		t.Run(backend, func(t *testing.T) {
			kv := reviewClient(t, backend)
			ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			var revision int64
			for i := range 80 {
				r, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 160<<10)})
				if err != nil {
					t.Fatal(err)
				}
				revision = r.Header.Revision
			}
			s, err := kv.RangeStream(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Limit: 60})
			if err != nil {
				t.Fatal(err)
			}
			first, err := s.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if first.RangeResponse.Header != nil {
				t.Fatal("metadata before final chunk")
			}
			for _, key := range []string{"k50", "k90", "k99"} {
				if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("new")}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := kv.DeleteRange(ctx, &pb.DeleteRangeRequest{Key: []byte("k40")}); err != nil {
				t.Fatal(err)
			}
			n := len(first.RangeResponse.Kvs)
			var final *pb.RangeResponse
			for {
				r, err := s.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range r.RangeResponse.Kvs {
					if len(v.Value) != 160<<10 {
						t.Errorf("new value in old snapshot at %s", v.Key)
					}
				}
				n += len(r.RangeResponse.Kvs)
				if r.RangeResponse.Header != nil {
					final = r.RangeResponse
				}
			}
			if final == nil || n != 60 || final.Count != 80 || !final.More || final.Header.Revision != revision {
				t.Fatalf("n=%d final=%v snapshot=%d", n, final, revision)
			}
			t.Logf("snapshot=%d, concurrent update/delete/inserts, keys=%d count=%d more=%v", revision, n, final.Count, final.More)
			cancelCtx, cancel := context.WithCancel(ctx)
			stream, err := kv.RangeStream(cancelCtx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}
			cancel()
			for {
				_, err := stream.Recv()
				if err != nil {
					if status.Code(err) != codes.Canceled {
						t.Fatal(err)
					}
					break
				}
			}
			if _, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("after"), Value: []byte("cancel")}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

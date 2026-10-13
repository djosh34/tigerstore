package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func reviewClient(t *testing.T, backend string) pb.KVClient {
	t.Helper()
	if backend == "tiger" {
		c := newClusterWith(t, func(c *cluster) { c.tick = 100 * time.Millisecond })
		cert, _, _, err := c.ca.Issue("client", 94)
		if err != nil {
			t.Fatal(err)
		}
		kv := c.client(2, cert)
		retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
			return kv.Range(ctx, &pb.RangeRequest{Key: []byte("ready")})
		})
		return kv
	}
	addr := func() string {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		a := l.Addr().String()
		l.Close()
		return a
	}
	client, peer := addr(), addr()
	cmd := exec.Command(os.Getenv("REVIEW_ETCD"), "--data-dir", t.TempDir(), "--listen-client-urls", "http://"+client, "--advertise-client-urls", "http://"+client, "--listen-peer-urls", "http://"+peer, "--initial-advertise-peer-urls", "http://"+peer, "--initial-cluster", "default=http://"+peer)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	conn, err := grpc.NewClient(client, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStaticStreamWindowSize(65535), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	kv := pb.NewKVClient(conn)
	retry(t, func(ctx context.Context) (*pb.RangeResponse, error) {
		return kv.Range(ctx, &pb.RangeRequest{Key: []byte("ready")})
	})
	return kv
}

func TestReview97Boundary(t *testing.T) {
	for _, backend := range []string{"etcd", "tiger"} {
		t.Run(backend, func(t *testing.T) {
			kv := reviewClient(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for i := range 30 {
				if _, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 78620)}); err != nil {
					t.Fatal(err)
				}
			}
			r, err := kv.Range(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l"), Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			r.Header = &pb.ResponseHeader{Revision: r.Header.Revision}
			r.Count = 11
			size := 78620 + 786431 - proto.Size(r)
			if _, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("k00"), Value: make([]byte, size)}); err != nil {
				t.Fatal(err)
			}
			stream, err := kv.RangeStream(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")})
			if err != nil {
				t.Fatal(err)
			}
			var chunks []int
			for {
				r, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				chunks = append(chunks, len(r.RangeResponse.Kvs))
			}
			t.Logf("initial internal etcd response target=786431 adjusted-value=%d chunks=%v", size, chunks)
			if fmt.Sprint(chunks) != "[10 20]" {
				t.Errorf("wire chunks differ from stock etcd: %v", chunks)
			}
		})
	}
}

func TestReview97Compaction(t *testing.T) {
	for _, backend := range []string{"etcd", "tiger"} {
		t.Run(backend, func(t *testing.T) {
			kv := reviewClient(t, backend)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			var rev int64
			for i := range 80 {
				r, err := kv.Put(ctx, &pb.PutRequest{Key: fmt.Appendf(nil, "k%02d", i), Value: make([]byte, 400<<10)})
				if err != nil {
					t.Fatal(err)
				}
				rev = r.Header.Revision
			}
			stream, err := kv.RangeStream(ctx, &pb.RangeRequest{Key: []byte("k"), RangeEnd: []byte("l")}, grpc.MaxCallRecvMsgSize(32<<20))
			if err != nil {
				t.Fatal(err)
			}
			first, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			n := len(first.RangeResponse.Kvs)
			r, err := kv.Put(ctx, &pb.PutRequest{Key: []byte("z"), Value: []byte("new")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Compact(ctx, &pb.CompactionRequest{Revision: r.Header.Revision}); err != nil {
				t.Fatal(err)
			}
			for {
				r, err := stream.Recv()
				if err != nil {
					t.Logf("pinned=%d compacted=%d keys=%d terminal=%v", rev, rev+1, n, err)
					if status.Code(err) != codes.OutOfRange {
						t.Errorf("want OutOfRange, got %v", err)
					}
					break
				}
				n += len(r.RangeResponse.Kvs)
			}
		})
	}
}

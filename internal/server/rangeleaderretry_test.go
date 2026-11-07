package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

type rangeRetryClient struct {
	pb.KVClient
	rangeCall func(context.Context, *pb.RangeRequest, ...grpc.CallOption) (*pb.RangeResponse, error)
}

func (c rangeRetryClient) Range(ctx context.Context, req *pb.RangeRequest, opts ...grpc.CallOption) (*pb.RangeResponse, error) {
	return c.rangeCall(ctx, req, opts...)
}

func TestRangeWithLeaderRetry(t *testing.T) {
	t.Run("same page and options", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req := &pb.RangeRequest{Key: []byte("range/1000"), RangeEnd: []byte("range0"), Limit: 1000, Revision: 42}
		option := grpc.UseCompressor(gzip.Name)
		want := &pb.RangeResponse{Count: 2000, More: true}
		errs := []error{rpctypes.ErrGRPCLeaderChanged, rpctypes.ErrGRPCNoLeader, nil}
		calls := 0
		client := rangeRetryClient{rangeCall: func(gotCtx context.Context, gotReq *pb.RangeRequest, opts ...grpc.CallOption) (*pb.RangeResponse, error) {
			if gotCtx != ctx || gotReq != req || gotReq.Revision != 42 || len(opts) != 1 || opts[0] != option {
				t.Fatal("retry changed the context, page, revision or compression")
			}
			if calls >= len(errs) {
				t.Fatal("retried a successful page")
			}
			err := errs[calls]
			calls++
			if err != nil {
				return nil, err
			}
			return want, nil
		}}
		got, err := rangeWithLeaderRetry(t, ctx, client, req, option)
		if err != nil || got != want || calls != 3 {
			t.Fatalf("response=%v error=%v calls=%d", got, err, calls)
		}
	})
	t.Run("other errors still fail", func(t *testing.T) {
		calls := 0
		client := rangeRetryClient{rangeCall: func(context.Context, *pb.RangeRequest, ...grpc.CallOption) (*pb.RangeResponse, error) {
			calls++
			return nil, rpctypes.ErrGRPCCompacted
		}}
		_, err := rangeWithLeaderRetry(t, context.Background(), client, &pb.RangeRequest{})
		if err != rpctypes.ErrGRPCCompacted || calls != 1 {
			t.Fatalf("error=%v calls=%d", err, calls)
		}
	})
	t.Run("deadline is not renewed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		client := rangeRetryClient{rangeCall: func(context.Context, *pb.RangeRequest, ...grpc.CallOption) (*pb.RangeResponse, error) {
			calls++
			cancel()
			return nil, rpctypes.ErrGRPCLeaderChanged
		}}
		_, err := rangeWithLeaderRetry(t, ctx, client, &pb.RangeRequest{})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("error=%v calls=%d", err, calls)
		}
	})
}
